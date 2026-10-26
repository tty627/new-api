package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func billingRecoveryDB(t *testing.T, path string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	oldDB, oldRedis, oldBatch := model.DB, common.RedisEnabled, common.BatchUpdateEnabled
	model.DB, common.RedisEnabled, common.BatchUpdateEnabled = db, false, false
	t.Cleanup(func() {
		model.DB, common.RedisEnabled, common.BatchUpdateEnabled = oldDB, oldRedis, oldBatch
		require.NoError(t, sqlDB.Close())
	})
	require.NoError(t, db.AutoMigrate(&model.User{}, &model.Token{}, &model.ForkBillingSession{}, &model.ForkBillingEvent{}))
	return db
}

func billingRecoverySession(t *testing.T) (*BillingSession, *gin.Context) {
	t.Helper()
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
	info := &relaycommon.RelayInfo{RequestId: "fork-recovery-request", UserId: 1, TokenId: 1,
		TokenKey: "fork-recovery-test-key", ForcePreConsume: true,
		UserSetting: dto.UserSetting{BillingPreference: "wallet_only"}}
	session, apiErr := NewBillingSession(ctx, info, 100)
	require.Nil(t, apiErr)
	return session, ctx
}

func TestBillingRecoveryCrashChild(t *testing.T) {
	path := os.Getenv("FORK_BILLING_CRASH_DB")
	if path == "" {
		t.Skip("subprocess fixture")
	}
	billingRecoveryDB(t, path)
	billingRecoverySession(t)
	if intent := os.Getenv("FORK_BILLING_CRASH_INTENT"); intent != "" {
		actual := 150
		if intent == model.BillingRefundPending {
			actual = 0
		}
		require.NoError(t, model.PersistBillingIntent(context.Background(), "fork-recovery-request", intent, actual))
	}
	// Exit immediately after a successful pre-consume, without Go cleanups or
	// the request's refund/settlement callback, like process termination.
	os.Exit(23)
}

func TestBillingRecoveryProcessExitEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "billing-crash.db")
	db := billingRecoveryDB(t, path)
	seedUser(t, 1, 1000)
	seedToken(t, 1, 1, "fork-recovery-test-key", 1000)
	command := exec.Command(os.Args[0], "-test.run=^TestBillingRecoveryCrashChild$")
	command.Env = append(os.Environ(), "FORK_BILLING_CRASH_DB="+path)
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, string(output))
	require.Equal(t, 23, exit.ExitCode(), string(output))
	var user model.User
	var token model.Token
	require.NoError(t, db.First(&user, 1).Error)
	require.NoError(t, db.First(&token, 1).Error)
	assert.Equal(t, 900, user.Quota)
	assert.Equal(t, 900, token.RemainQuota)
	assert.True(t, db.Migrator().HasTable("fork_billing_sessions"), "a crashed reservation needs a durable recovery/audit record")
	row, err := model.GetBillingSession(context.Background(), "fork-recovery-request")
	require.NoError(t, err)
	assert.Equal(t, model.BillingReserved, row.Status)
	assert.ErrorIs(t, RecoverBillingSession(context.Background(), row.RequestID), model.ErrBillingOutcomeUnknown)
	_, err = RecoverBillingSessions(context.Background(), 100)
	require.NoError(t, err)
	require.NoError(t, db.First(&user, 1).Error)
	assert.Equal(t, 900, user.Quota, "elapsed time and restart do not prove a refund")
}

func TestBillingRecoverySettlementFailureEvidence(t *testing.T) {
	db := billingRecoveryDB(t, ":memory:")
	seedUser(t, 1, 1000)
	seedToken(t, 1, 1, "fork-recovery-test-key", 1000)
	session, _ := billingRecoverySession(t)
	name := "fork:fail-token-settlement"
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register(name, func(tx *gorm.DB) {
		if tx.Statement.Table == "tokens" {
			tx.AddError(errors.New("injected token write failure"))
		}
	}))
	t.Cleanup(func() { require.NoError(t, db.Callback().Update().Remove(name)) })
	require.Error(t, session.Settle(150))
	var user model.User
	var token model.Token
	require.NoError(t, db.First(&user, 1).Error)
	require.NoError(t, db.First(&token, 1).Error)
	assert.Equal(t, 900, user.Quota, "token failure must roll back the same transaction's wallet delta")
	assert.Equal(t, 900, token.RemainQuota)
	require.NoError(t, db.Callback().Update().Remove(name))
	require.NoError(t, RecoverBillingSession(context.Background(), "fork-recovery-request"))
	require.NoError(t, RecoverBillingSession(context.Background(), "fork-recovery-request"))
	require.NoError(t, db.First(&user, 1).Error)
	require.NoError(t, db.First(&token, 1).Error)
	assert.Equal(t, 850, user.Quota)
	assert.Equal(t, 850, token.RemainQuota)
}

func TestBillingRecoveryRefundFailureEvidence(t *testing.T) {
	db := billingRecoveryDB(t, ":memory:")
	seedUser(t, 1, 1000)
	seedToken(t, 1, 1, "fork-recovery-test-key", 1000)
	session, ctx := billingRecoverySession(t)
	name := "fork:fail-wallet-refund"
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register(name, func(tx *gorm.DB) {
		if tx.Statement.Table == "users" {
			tx.AddError(errors.New("injected wallet write failure"))
		}
	}))
	t.Cleanup(func() {
		require.NoError(t, db.Callback().Update().Remove(name))
	})
	session.Refund(ctx)
	var user model.User
	var token model.Token
	require.NoError(t, db.First(&user, 1).Error)
	require.NoError(t, db.First(&token, 1).Error)
	assert.Equal(t, 900, user.Quota)
	assert.Equal(t, 900, token.RemainQuota, "wallet failure must not independently credit the token")
	require.NoError(t, db.Callback().Update().Remove(name))
	require.NoError(t, RecoverBillingSession(context.Background(), "fork-recovery-request"))
	require.NoError(t, RecoverBillingSession(context.Background(), "fork-recovery-request"))
	require.NoError(t, db.First(&user, 1).Error)
	require.NoError(t, db.First(&token, 1).Error)
	assert.Equal(t, 1000, user.Quota)
	assert.Equal(t, 1000, token.RemainQuota)
}

func forEachBillingRecoveryDB(t *testing.T, fn func(*testing.T, *gorm.DB)) {
	for _, dialect := range []string{"sqlite", "mysql", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			var driver gorm.Dialector
			kind := common.DatabaseTypeSQLite
			switch dialect {
			case "sqlite":
				driver = sqlite.Open(t.TempDir() + "/billing.db?_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_txlock=immediate")
			case "mysql":
				dsn := os.Getenv("TEST_MYSQL_DSN")
				if dsn == "" {
					dsn = os.Getenv("TEST_BILLING_MYSQL_DSN")
				}
				if dsn == "" {
					t.Skip("TEST_MYSQL_DSN is not configured")
				}
				driver, kind = mysql.Open(dsn), common.DatabaseTypeMySQL
			case "postgres":
				dsn := os.Getenv("TEST_POSTGRES_DSN")
				if dsn == "" {
					dsn = os.Getenv("TEST_BILLING_POSTGRES_DSN")
				}
				if dsn == "" {
					t.Skip("TEST_POSTGRES_DSN is not configured")
				}
				driver, kind = postgres.Open(dsn), common.DatabaseTypePostgreSQL
			}
			db, err := gorm.Open(driver, &gorm.Config{NamingStrategy: schema.NamingStrategy{TablePrefix: "billing_fork_"}})
			require.NoError(t, err)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			if dialect == "sqlite" {
				sqlDB.SetMaxOpenConns(1)
			} else {
				sqlDB.SetMaxOpenConns(8)
			}
			oldDB, oldLog, oldRedis, oldBatch := model.DB, model.LOG_DB, common.RedisEnabled, common.BatchUpdateEnabled
			oldMain, oldLogType := common.MainDatabaseType(), common.LogDatabaseType()
			model.DB, model.LOG_DB, common.RedisEnabled, common.BatchUpdateEnabled = db, db, false, false
			common.SetDatabaseTypes(kind, kind)
			tables := []any{&model.ForkBillingEvent{}, &model.ForkBillingSession{}, &model.SubscriptionPreConsumeRecord{}, &model.UserSubscription{}, &model.SubscriptionPlan{}, &model.Task{}, &model.Token{}, &model.User{}, &model.Channel{}}
			t.Cleanup(func() {
				assert.NoError(t, db.Migrator().DropTable(tables...))
				assert.NoError(t, sqlDB.Close())
				model.DB, model.LOG_DB, common.RedisEnabled, common.BatchUpdateEnabled = oldDB, oldLog, oldRedis, oldBatch
				common.SetDatabaseTypes(oldMain, oldLogType)
			})
			require.NoError(t, db.Migrator().DropTable(tables...))
			// Representative pre-ledger database; repeated migration preserves existing funds.
			require.NoError(t, db.AutoMigrate(&model.User{}, &model.Token{}))
			seedUser(t, 1, 1000)
			seedToken(t, 1, 1, "matrix-key", 1000)
			require.NoError(t, db.AutoMigrate(tables...))
			require.NoError(t, db.AutoMigrate(tables...))
			var user model.User
			require.NoError(t, db.First(&user, 1).Error)
			assert.Equal(t, 1000, user.Quota)
			var version string
			query := "SELECT version()"
			if dialect == "sqlite" {
				query = "SELECT sqlite_version()"
			}
			require.NoError(t, db.Raw(query).Scan(&version).Error)
			t.Logf("database: %s", version)
			fn(t, db)
		})
	}
}

func TestBillingRecoveryMatrix(t *testing.T) {
	forEachBillingRecoveryDB(t, func(t *testing.T, db *gorm.DB) {
		ctx := context.Background()
		for _, source := range []string{"wallet", "subscription"} {
			for _, op := range []string{"settle", "refund", "token_failure", "unknown", "duplicate", "cycle_reset", "task", "task_cycle"} {
				t.Run(source+"/"+op, func(t *testing.T) {
					for _, table := range []any{&model.ForkBillingEvent{}, &model.ForkBillingSession{}, &model.SubscriptionPreConsumeRecord{}, &model.UserSubscription{}, &model.SubscriptionPlan{}, &model.Task{}, &model.Token{}, &model.User{}, &model.Channel{}} {
						require.NoError(t, db.Session(&gorm.Session{AllowGlobalUpdate: true}).Unscoped().Delete(table).Error)
					}
					seedUser(t, 1, 1000)
					seedToken(t, 1, 1, "matrix-key", 1000)
					seedChannel(t, 1)
					plan := model.SubscriptionPlan{Id: 1, Title: "recovery", QuotaResetPeriod: model.SubscriptionResetNever}
					require.NoError(t, db.Create(&plan).Error)
					model.InvalidateSubscriptionPlanCache(1)
					now := model.GetDBTimestamp()
					sub := model.UserSubscription{Id: 1, UserId: 1, PlanId: 1, AmountTotal: 1000, Status: "active", StartTime: now - 1, EndTime: now + 86400}
					require.NoError(t, db.Create(&sub).Error)
					input := model.BillingReservation{RequestID: "matrix-request", UserID: 1, TokenID: 1, FundingSource: source, ModelName: "matrix-model", Quota: 100}
					_, created, err := model.ReserveBillingSession(ctx, input)
					require.NoError(t, err)
					require.True(t, created)
					expected, subExpected := 150, int64(150)
					if op == "unknown" {
						require.NoError(t, model.MarkBillingOutcomeUnknown(ctx, input.RequestID))
						assert.ErrorIs(t, model.PersistBillingIntent(ctx, input.RequestID, model.BillingRefundPending, 0), model.ErrBillingOutcomeUnknown)
						assert.ErrorIs(t, RecoverBillingSession(ctx, input.RequestID), model.ErrBillingOutcomeUnknown)
						expected, subExpected = 100, 100
					} else if op == "task" || op == "task_cycle" {
						if op == "task_cycle" && source == "subscription" {
							require.NoError(t, db.Model(&sub).Updates(map[string]any{"quota_cycle": 1, "amount_used": 40}).Error)
						}
						task := model.Task{UserId: 1, ChannelId: 1, Quota: 100, Status: model.TaskStatusInProgress, PrivateData: model.TaskPrivateData{TokenId: 1, BillingSource: source, Execution: &model.TaskExecutionSnapshot{RequestID: input.RequestID}}}
						if source == "subscription" {
							task.PrivateData.SubscriptionId = 1
						}
						createFault := "fork:task-create-intent-failure"
						require.NoError(t, db.Callback().Update().Before("gorm:update").Register(createFault, func(tx *gorm.DB) {
							if tx.Statement.Table == "billing_fork_fork_billing_sessions" {
								tx.AddError(errors.New("injected acceptance intent failure"))
							}
						}))
						require.Error(t, model.CreateAcceptedBillingTask(ctx, input.RequestID, &task, 120, nil))
						var count int64
						require.NoError(t, db.Model(&model.Task{}).Count(&count).Error)
						assert.Zero(t, count)
						require.NoError(t, db.Callback().Update().Remove(createFault))
						task.ID = 0
						require.NoError(t, model.CreateAcceptedBillingTask(ctx, input.RequestID, &task, 120, nil))
						// No live BillingSession survives this point: the startup worker
						// has everything needed for an accepted task, including its binding.
						require.NoError(t, RecoverBillingSession(ctx, input.RequestID))
						// Crash before LogTaskConsumption: accounting is already committed.
						var user model.User
						require.NoError(t, db.First(&user, 1).Error)
						assert.Equal(t, 120, user.UsedQuota)
						assert.Equal(t, 1, user.RequestCount)
						task.Status = model.TaskStatusFailure
						terminalFault := "fork:terminal-intent-failure"
						require.NoError(t, db.Callback().Update().Before("gorm:update").Register(terminalFault, func(tx *gorm.DB) {
							if tx.Statement.Table == "billing_fork_fork_billing_sessions" {
								tx.AddError(errors.New("injected terminal intention failure"))
							}
						}))
						_, err = model.CommitTaskBillingResult(ctx, input.RequestID, &task, model.TaskStatusInProgress, model.BillingRefundPending, 0)
						require.Error(t, err)
						var stillRunning model.Task
						require.NoError(t, db.First(&stillRunning, task.ID).Error)
						assert.EqualValues(t, model.TaskStatusInProgress, stillRunning.Status)
						require.NoError(t, db.Callback().Update().Remove(terminalFault))
						won, err := model.CommitTaskBillingResult(ctx, input.RequestID, &task, model.TaskStatusInProgress, model.BillingRefundPending, 0)
						require.NoError(t, err)
						require.True(t, won)
						// Stop after the terminal commit, before any foreground settlement.
						// Recovery must still find and finish the known refund.
						name := "fork:task-failure"
						require.NoError(t, db.Callback().Update().Before("gorm:update").Register(name, func(tx *gorm.DB) {
							if tx.Statement.Table == "billing_fork_tasks" {
								tx.AddError(errors.New("injected task write failure"))
							}
						}))
						require.Error(t, RecoverBillingSession(ctx, input.RequestID))
						require.NoError(t, db.Callback().Update().Remove(name))
						require.NoError(t, RecoverBillingSession(ctx, input.RequestID))
						require.NoError(t, RecoverBillingSession(ctx, input.RequestID))
						require.NoError(t, db.First(&task, task.ID).Error)
						assert.Zero(t, task.Quota)
						require.NoError(t, db.First(&user, 1).Error)
						assert.Zero(t, user.UsedQuota)
						assert.Equal(t, 1, user.RequestCount)
						expected, subExpected = 0, 0
						if op == "task_cycle" && source == "subscription" {
							subExpected = 40
						}
					} else {
						if op == "duplicate" {
							_, created, err = model.ReserveBillingSession(ctx, input)
							require.NoError(t, err)
							assert.False(t, created)
							input.UserID = 2
							_, _, err = model.ReserveBillingSession(ctx, input)
							assert.ErrorIs(t, err, model.ErrBillingIntentConflict)
							input.UserID = 1
						}
						if op == "cycle_reset" && source == "subscription" {
							require.NoError(t, db.Model(&sub).Updates(map[string]any{"quota_cycle": 1, "amount_used": 40}).Error)
							_, err = model.ResizeBillingSession(ctx, input.RequestID, 120, true)
							assert.ErrorIs(t, err, model.ErrSubscriptionCycleExpired)
							subExpected = 40
						}
						intent := model.BillingSettlePending
						if op == "refund" {
							intent = model.BillingRefundPending
							expected, subExpected = 0, 0
						}
						require.NoError(t, model.PersistBillingIntent(ctx, input.RequestID, intent, expected))
						if op == "token_failure" {
							name := "fork:matrix-token-failure"
							require.NoError(t, db.Callback().Update().Before("gorm:update").Register(name, func(tx *gorm.DB) {
								if tx.Statement.Table == "billing_fork_tokens" {
									tx.AddError(errors.New("injected token write failure"))
								}
							}))
							require.ErrorContains(t, RecoverBillingSession(ctx, input.RequestID), "injected token")
							require.NoError(t, db.Callback().Update().Remove(name))
						}
						var wg sync.WaitGroup
						errs := make(chan error, 4)
						for i := 0; i < 4; i++ {
							wg.Add(1)
							go func() { defer wg.Done(); errs <- RecoverBillingSession(ctx, input.RequestID) }()
						}
						wg.Wait()
						close(errs)
						for err := range errs {
							require.NoError(t, err)
						}
					}
					var user model.User
					var token model.Token
					require.NoError(t, db.First(&user, 1).Error)
					require.NoError(t, db.First(&token, 1).Error)
					walletExpected := 1000
					if source == "wallet" {
						walletExpected -= expected
					}
					assert.Equal(t, walletExpected, user.Quota)
					assert.Equal(t, 1000-expected, token.RemainQuota)
					assert.Equal(t, expected, token.UsedQuota)
					if source == "subscription" {
						require.NoError(t, db.First(&sub, 1).Error)
						assert.Equal(t, subExpected, sub.AmountUsed)
					}
					row, err := model.GetBillingSession(ctx, input.RequestID)
					require.NoError(t, err)
					assert.False(t, row.CachePending)
					var events int64
					require.NoError(t, db.Model(&model.ForkBillingEvent{}).Where("request_id = ?", input.RequestID).Count(&events).Error)
					assert.GreaterOrEqual(t, events, int64(2))
				})
			}
		}
	})
}

func TestBillingRecoveryIntentFailureCannotRefund(t *testing.T) {
	db := billingRecoveryDB(t, ":memory:")
	seedUser(t, 1, 1000)
	seedToken(t, 1, 1, "fork-recovery-test-key", 1000)
	session, ctx := billingRecoverySession(t)
	name := "fork:intent-failure"
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register(name, func(tx *gorm.DB) {
		if tx.Statement.Table == "fork_billing_sessions" {
			tx.AddError(fmt.Errorf("injected intent failure"))
		}
	}))
	require.Error(t, session.Settle(150))
	require.NoError(t, db.Callback().Update().Remove(name))
	session.Refund(ctx)
	row, err := model.GetBillingSession(context.Background(), "fork-recovery-request")
	require.NoError(t, err)
	assert.Equal(t, model.BillingReserved, row.Status)
	require.NoError(t, session.Settle(150))
	assert.Equal(t, 850, getUserQuota(t, 1))
	assert.Equal(t, 850, getTokenRemainQuota(t, 1))
}

func TestBillingRecoveryKnownIntentProcessExit(t *testing.T) {
	for _, intent := range []string{model.BillingSettlePending, model.BillingRefundPending} {
		t.Run(intent, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "known-intent.db")
			db := billingRecoveryDB(t, path)
			seedUser(t, 1, 1000)
			seedToken(t, 1, 1, "fork-recovery-test-key", 1000)
			command := exec.Command(os.Args[0], "-test.run=^TestBillingRecoveryCrashChild$")
			command.Env = append(os.Environ(), "FORK_BILLING_CRASH_DB="+path, "FORK_BILLING_CRASH_INTENT="+intent)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, string(output))
			require.Equal(t, 23, exit.ExitCode(), string(output))
			row, err := model.GetBillingSession(context.Background(), "fork-recovery-request")
			require.NoError(t, err)
			assert.Equal(t, intent, row.Status)
			result, err := RecoverBillingSessions(context.Background(), 100)
			require.NoError(t, err)
			assert.Equal(t, 1, result.Recovered)
			require.NoError(t, RecoverBillingSession(context.Background(), row.RequestID))
			expected := 850
			if intent == model.BillingRefundPending {
				expected = 1000
			}
			var user model.User
			var token model.Token
			require.NoError(t, db.First(&user, 1).Error)
			require.NoError(t, db.First(&token, 1).Error)
			assert.Equal(t, expected, user.Quota)
			assert.Equal(t, expected, token.RemainQuota)
		})
	}
}

func TestBillingRecoveryBoundaries(t *testing.T) {
	forEachBillingRecoveryDB(t, func(t *testing.T, db *gorm.DB) {
		ctx := context.Background()
		input := model.BillingReservation{RequestID: "bounds", UserID: 1, TokenID: 1, FundingSource: "wallet", Quota: 100}
		for _, quota := range []int{-1, common.MaxQuota + 1} {
			input.Quota = quota
			_, _, err := model.ReserveBillingSession(ctx, input)
			require.Error(t, err)
		}
		input.Quota = 100
		common.BatchUpdateEnabled = true
		_, _, err := model.ReserveBillingSession(ctx, input)
		assert.ErrorIs(t, err, model.ErrBillingBatchUnsupported)
		common.BatchUpdateEnabled = false
		_, _, err = model.ReserveBillingSession(ctx, input)
		require.NoError(t, err)
		require.Error(t, model.PersistBillingIntent(ctx, input.RequestID, model.BillingSettlePending, common.MaxQuota+1))
		require.NoError(t, db.Model(&model.User{}).Where("id = ?", 1).Update("quota", common.MaxWalletQuota).Error)
		require.NoError(t, model.PersistBillingIntent(ctx, input.RequestID, model.BillingRefundPending, 0))
		require.Error(t, RecoverBillingSession(ctx, input.RequestID))
		var token model.Token
		require.NoError(t, db.First(&token, 1).Error)
		assert.Equal(t, 900, token.RemainQuota)
		require.NoError(t, db.Model(&model.User{}).Where("id = ?", 1).Update("quota", 900).Error)
		require.NoError(t, RecoverBillingSession(ctx, input.RequestID))
		// A known usage greater than the subscription capacity is retained exactly,
		// not silently capped, moved to a wallet, or refunded by recovery.
		plan := model.SubscriptionPlan{Id: 1, Title: "one-unit", QuotaResetPeriod: model.SubscriptionResetNever}
		require.NoError(t, db.Create(&plan).Error)
		model.InvalidateSubscriptionPlanCache(1)
		now := model.GetDBTimestamp()
		sub := model.UserSubscription{Id: 1, UserId: 1, PlanId: 1, AmountTotal: 1, Status: "active", StartTime: now - 1, EndTime: now + 86400}
		require.NoError(t, db.Create(&sub).Error)
		input.RequestID, input.FundingSource, input.Quota = "one-unit", "subscription", 1
		_, _, err = model.ReserveBillingSession(ctx, input)
		require.NoError(t, err)
		require.NoError(t, model.PersistBillingIntent(ctx, input.RequestID, model.BillingSettlePending, 3))
		require.Error(t, RecoverBillingSession(ctx, input.RequestID))
		row, err := model.GetBillingSession(ctx, input.RequestID)
		require.NoError(t, err)
		assert.Equal(t, model.BillingSettlePending, row.Status)
		require.NotNil(t, row.ActualQuota)
		assert.EqualValues(t, 3, *row.ActualQuota)
		require.NoError(t, db.First(&sub, 1).Error)
		assert.EqualValues(t, 1, sub.AmountUsed)
		require.NoError(t, db.First(&token, 1).Error)
		assert.Equal(t, 999, token.RemainQuota)
		// Non-finite pre-clamp values remain JSON-safe admin audit evidence.
		input.RequestID, input.FundingSource, input.Quota = "clamped-task", "wallet", 0
		_, _, err = model.ReserveBillingSession(ctx, input)
		require.NoError(t, err)
		task := model.Task{UserId: 1, Status: model.TaskStatusSuccess, PrivateData: model.TaskPrivateData{TokenId: 1, Execution: &model.TaskExecutionSnapshot{RequestID: input.RequestID}}}
		clamp := &common.QuotaClamp{Op: "QuotaFromFloat", Kind: common.QuotaClampOverflow, Original: math.Inf(1), Clamped: common.MaxQuota}
		require.NoError(t, model.CreateAcceptedBillingTask(ctx, input.RequestID, &task, common.MaxQuota, clamp))
		row, err = model.GetBillingSession(ctx, input.RequestID)
		require.NoError(t, err)
		assert.Contains(t, row.QuotaSaturation, "+Inf")
		require.NoError(t, RecoverBillingSession(ctx, input.RequestID))
		var user model.User
		require.NoError(t, db.First(&user, 1).Error)
		assert.Equal(t, 1000-common.MaxQuota, user.Quota, "known usage can produce legitimate wallet debt")
	})
}

func TestBillingRecoveryCacheRepairAndFactoryClaim(t *testing.T) {
	db := billingRecoveryDB(t, ":memory:")
	seedUser(t, 1, 1000)
	seedToken(t, 1, 1, "fork-recovery-test-key", 1000)
	server := miniredis.RunT(t)
	oldRedis, oldClient := common.RedisEnabled, common.RDB
	common.RedisEnabled = true
	common.RDB = redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = common.RDB.Close(); common.RedisEnabled, common.RDB = oldRedis, oldClient })
	ctx := context.Background()
	tokenKey := "token:" + common.GenerateHMAC("fork-recovery-test-key")
	require.NoError(t, common.RDB.HSet(ctx, "user:1", "quota", 1000).Err())
	require.NoError(t, common.RDB.HSet(ctx, tokenKey, "remain_quota", 1000).Err())
	server.SetError("injected cache outage")
	session, c := billingRecoverySession(t)
	oldRow, err := model.GetBillingSession(ctx, "fork-recovery-request")
	require.NoError(t, err)
	assert.True(t, oldRow.CachePending)
	_, apiErr := NewBillingSession(c, session.relayInfo, 100)
	require.NotNil(t, apiErr)
	assert.ErrorIs(t, apiErr, model.ErrBillingOutcomeUnknown)
	row, err := model.ResizeBillingSession(ctx, oldRow.RequestID, 120, true)
	require.NoError(t, err)
	assert.True(t, row.CachePending)
	server.SetError("")
	model.RefreshBillingQuotaCaches(ctx, oldRow)
	current, err := model.GetBillingSession(ctx, row.RequestID)
	require.NoError(t, err)
	assert.True(t, current.CachePending, "stale revision must not acknowledge newer cache repair")
	result, err := RecoverBillingSessions(ctx, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, result.Unknown)
	current, err = model.GetBillingSession(ctx, row.RequestID)
	require.NoError(t, err)
	assert.False(t, current.CachePending)
	assert.False(t, server.Exists("user:1"))
	assert.False(t, server.Exists(tokenKey))
	var user model.User
	var token model.Token
	require.NoError(t, db.First(&user, 1).Error)
	require.NoError(t, db.First(&token, 1).Error)
	assert.Equal(t, 880, user.Quota)
	assert.Equal(t, 880, token.RemainQuota)
	// A soft-deleted token retains its financial ledger and can be refunded.
	require.NoError(t, db.Delete(&token).Error)
	require.NoError(t, model.PersistBillingIntent(ctx, row.RequestID, model.BillingRefundPending, 0))
	require.NoError(t, RecoverBillingSession(ctx, row.RequestID))
	require.NoError(t, db.Unscoped().First(&token, 1).Error)
	assert.Equal(t, 1000, token.RemainQuota)
}
