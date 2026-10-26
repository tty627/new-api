package model

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// Uses isolated prefixed tables and real server DSNs from the shared DB matrix.
func forEachSubscriptionCycleDB(t *testing.T, fn func(*testing.T)) {
	t.Helper()
	for _, dialect := range []string{"sqlite", "mysql", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			var driver gorm.Dialector
			switch dialect {
			case "sqlite":
				driver = sqlite.Open(t.TempDir() + "/cycle.db?_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_txlock=immediate")
			case "mysql":
				dsn := os.Getenv("TEST_MYSQL_DSN")
				if dsn == "" {
					t.Skip("TEST_MYSQL_DSN is not configured")
				}
				driver = mysql.Open(dsn)
			case "postgres":
				dsn := os.Getenv("TEST_POSTGRES_DSN")
				if dsn == "" {
					t.Skip("TEST_POSTGRES_DSN is not configured")
				}
				driver = postgres.Open(dsn)
			}
			db, err := gorm.Open(driver, &gorm.Config{NamingStrategy: schema.NamingStrategy{TablePrefix: "cycle_fork_"}})
			require.NoError(t, err)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			oldDB, oldLog := DB, LOG_DB
			oldMain, oldLogType := common.MainDatabaseType(), common.LogDatabaseType()
			DB, LOG_DB = db, db
			kind := common.DatabaseTypeSQLite
			if dialect == "mysql" {
				kind = common.DatabaseTypeMySQL
			}
			if dialect == "postgres" {
				kind = common.DatabaseTypePostgreSQL
			}
			common.SetDatabaseTypes(kind, kind)
			initCol()
			t.Cleanup(func() {
				assert.NoError(t, db.Migrator().DropTable(&SubscriptionPreConsumeRecord{}, &UserSubscription{}, &SubscriptionPlan{}))
				assert.NoError(t, sqlDB.Close())
				DB, LOG_DB = oldDB, oldLog
				common.SetDatabaseTypes(oldMain, oldLogType)
				initCol()
			})
			require.NoError(t, db.AutoMigrate(&SubscriptionPlan{}, &UserSubscription{}, &SubscriptionPreConsumeRecord{}))
			query := "SELECT version()"
			if dialect == "sqlite" {
				query = "SELECT sqlite_version()"
			}
			var version string
			require.NoError(t, db.Raw(query).Scan(&version).Error)
			t.Logf("database: %s", version)
			fn(t)
		})
	}
}

func cycleFixture(t *testing.T, reset string) (*SubscriptionPlan, *UserSubscription) {
	t.Helper()
	now := GetDBTimestamp()
	plan := linkTestPlan(t, 1000)
	plan.QuotaResetPeriod = reset
	require.NoError(t, DB.Save(plan).Error)
	InvalidateSubscriptionPlanCache(plan.Id)
	sub := &UserSubscription{UserId: plan.Id, PlanId: plan.Id, AmountTotal: 1000,
		StartTime: now, EndTime: now + 7*86400, Status: "active", LastResetTime: now}
	if reset == SubscriptionResetNever {
		sub.StartTime = now - 3*86400
		sub.LastResetTime = 0
	}
	sub.NextResetTime = calcNextResetTime(time.Unix(now, 0), plan, sub.EndTime)
	require.NoError(t, DB.Create(sub).Error)
	return plan, sub
}

func reserveCycle(t *testing.T, request string, sub *UserSubscription, amount int64) {
	t.Helper()
	_, err := PreConsumeUserSubscription(request, sub.UserId, "cycle-model", 0, amount)
	require.NoError(t, err)
}

func TestSubscriptionCycleOwnership(t *testing.T) {
	forEachSubscriptionCycleDB(t, func(t *testing.T) {
		for _, reset := range []string{"none", "daily", "manual_keep", "manual_advance", "never_manual"} {
			for _, operation := range []string{"refund", "settle_less", "settle_more", "resize", "accept"} {
				t.Run(reset+"/"+operation, func(t *testing.T) {
					period := SubscriptionResetDaily
					if reset == "none" || reset == "never_manual" {
						period = SubscriptionResetNever
					}
					plan, sub := cycleFixture(t, period)
					request := fmt.Sprintf("cycle-%d", sub.Id)
					reserveCycle(t, request, sub, 100)
					switch reset {
					case "daily":
						require.NoError(t, DB.Transaction(func(tx *gorm.DB) error {
							current := getSubscriptionResetSub(t, sub.Id)
							return maybeResetUserSubscriptionWithPlanTx(tx, &current, plan, sub.NextResetTime)
						}))
					case "manual_keep", "manual_advance", "never_manual":
						_, err := AdminResetUserSubscriptionsByPlan(sub.UserId, plan.Id, reset == "manual_advance")
						require.NoError(t, err)
					}
					current := getSubscriptionResetSub(t, sub.Id)
					if reset == "none" {
						assert.Zero(t, current.QuotaCycle)
					} else {
						assert.EqualValues(t, 1, current.QuotaCycle)
					}
					reserveCycle(t, request+"-new", sub, 40)
					finalAmount := int64(0)
					if operation == "settle_less" {
						finalAmount = 60
					}
					if operation == "settle_more" {
						finalAmount = 130
					}
					if operation == "resize" || operation == "accept" {
						finalAmount = 140
					}
					operationTx := func(tx *gorm.DB) error {
						switch operation {
						case "refund":
							return RefundSubscriptionPreConsumeTx(tx, request)
						case "resize":
							return ResizeSubscriptionPreConsumeTx(tx, request, finalAmount)
						case "accept":
							return ReconcileSubscriptionPreConsumeTx(tx, request, finalAmount)
						default:
							return SettleSubscriptionPreConsumeTx(tx, request, finalAmount)
						}
					}
					for range 2 {
						err := DB.Transaction(operationTx)
						if operation == "resize" && reset != "none" {
							require.ErrorIs(t, err, ErrSubscriptionCycleExpired)
						} else {
							require.NoError(t, err)
						}
					}
					want := int64(40)
					if reset == "none" {
						want += finalAmount
					}
					assert.Equal(t, want, getSubscriptionResetSub(t, sub.Id).AmountUsed)
					var record SubscriptionPreConsumeRecord
					require.NoError(t, DB.Where("request_id = ?", request).First(&record).Error)
					require.NotNil(t, record.QuotaCycle)
					assert.Zero(t, *record.QuotaCycle)
					if operation != "resize" && operation != "accept" {
						require.NotNil(t, record.SettledAmount)
						assert.Equal(t, finalAmount, *record.SettledAmount)
					}
				})
			}
		}
	})
}

func TestSubscriptionCycleLazyResetAndLegacy(t *testing.T) {
	forEachSubscriptionCycleDB(t, func(t *testing.T) {
		for _, operation := range []string{"refund", "settle", "resize"} {
			t.Run(operation, func(t *testing.T) {
				plan, sub := cycleFixture(t, SubscriptionResetDaily)
				request := fmt.Sprintf("lazy-%d", sub.Id)
				reserveCycle(t, request, sub, 100)
				// Simulate a request reserved before midnight while the scheduled
				// reset worker has not yet run. The operation must reset first.
				now := time.Unix(GetDBTimestamp(), 0)
				today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
				yesterday := today.AddDate(0, 0, -1).Unix()
				require.NoError(t, DB.Model(sub).Updates(map[string]any{"start_time": yesterday, "last_reset_time": yesterday, "next_reset_time": today.Unix()}).Error)
				err := DB.Transaction(func(tx *gorm.DB) error {
					switch operation {
					case "refund":
						return RefundSubscriptionPreConsumeTx(tx, request)
					case "settle":
						return SettleSubscriptionPreConsumeTx(tx, request, 130)
					default:
						return ResizeSubscriptionPreConsumeTx(tx, request, 140)
					}
				})
				if operation == "resize" {
					require.ErrorIs(t, err, ErrSubscriptionCycleExpired)
					// The rejected transaction rolls back even its lazy reset.
					_, err = AdminResetUserSubscriptionsByPlan(sub.UserId, plan.Id, true)
					require.NoError(t, err)
				} else {
					require.NoError(t, err)
				}
				assert.Zero(t, getSubscriptionResetSub(t, sub.Id).AmountUsed)
				// NULL means an old reservation whose cycle cannot be inferred.
				legacy := &SubscriptionPreConsumeRecord{RequestId: request + "-legacy", UserId: sub.UserId, UserSubscriptionId: sub.Id, PreConsumed: 100, Status: "consumed"}
				require.NoError(t, DB.Create(legacy).Error)
				reserveCycle(t, request+"-current", sub, 40)
				require.ErrorIs(t, DB.Transaction(func(tx *gorm.DB) error { return ResizeSubscriptionPreConsumeTx(tx, legacy.RequestId, 200) }), ErrSubscriptionCycleExpired)
				require.ErrorIs(t, DB.Transaction(func(tx *gorm.DB) error { return ReconcileSubscriptionPreConsumeTx(tx, legacy.RequestId, 200) }), ErrSubscriptionCycleExpired)
				require.NoError(t, RefundSubscriptionPreConsume(legacy.RequestId))
				assert.EqualValues(t, 40, getSubscriptionResetSub(t, sub.Id).AmountUsed)
			})
		}
	})
}

func TestSubscriptionCycleAtomicAndConcurrent(t *testing.T) {
	forEachSubscriptionCycleDB(t, func(t *testing.T) {
		plan, sub := cycleFixture(t, SubscriptionResetNever)
		sub.AmountTotal = 100
		require.NoError(t, DB.Save(sub).Error)
		var wg sync.WaitGroup
		results := make(chan error, 2)
		for range 2 {
			wg.Go(func() {
				_, err := PreConsumeUserSubscription("concurrent", sub.UserId, "model", 0, 100)
				results <- err
			})
		}
		wg.Wait()
		close(results)
		for err := range results {
			require.NoError(t, err)
		}
		assert.EqualValues(t, 100, getSubscriptionResetSub(t, sub.Id).AmountUsed)
		require.ErrorIs(t, DB.Transaction(func(tx *gorm.DB) error {
			return SettleSubscriptionPreConsumeTx(tx, "concurrent", 101)
		}), ErrSubscriptionQuotaInsufficient)
		require.Error(t, DB.Transaction(func(tx *gorm.DB) error {
			return SettleSubscriptionPreConsumeTx(tx, "concurrent", -1)
		}))
		_, err := PreConsumeUserSubscription("concurrent", sub.UserId+999, "model", 0, 100)
		require.Error(t, err)
		_, err = PreConsumeUserSubscription("concurrent", sub.UserId, "model", 0, 50)
		require.Error(t, err)
		rollback := errors.New("token update failed")
		require.ErrorIs(t, DB.Transaction(func(tx *gorm.DB) error {
			if err := RefundSubscriptionPreConsumeTx(tx, "concurrent"); err != nil {
				return err
			}
			return rollback
		}), rollback)
		assert.EqualValues(t, 100, getSubscriptionResetSub(t, sub.Id).AmountUsed)
		var record SubscriptionPreConsumeRecord
		require.NoError(t, DB.Where("request_id = ?", "concurrent").First(&record).Error)
		assert.Equal(t, "consumed", record.Status)
		results = make(chan error, 2)
		for range 2 {
			wg.Go(func() { results <- RefundSubscriptionPreConsume("concurrent") })
		}
		wg.Wait()
		close(results)
		for err := range results {
			require.NoError(t, err)
		}
		assert.Zero(t, getSubscriptionResetSub(t, sub.Id).AmountUsed)
		require.Error(t, DB.Transaction(func(tx *gorm.DB) error { return SettleSubscriptionPreConsumeTx(tx, "concurrent", 20) }))
		reserveCycle(t, "settled", sub, 50)
		require.NoError(t, DB.Transaction(func(tx *gorm.DB) error { return SettleSubscriptionPreConsumeTx(tx, "settled", 20) }))
		require.Error(t, RefundSubscriptionPreConsume("settled"))
		require.Error(t, DB.Transaction(func(tx *gorm.DB) error { return SettleSubscriptionPreConsumeTx(tx, "settled", 30) }))
		assert.EqualValues(t, 20, getSubscriptionResetSub(t, sub.Id).AmountUsed)
		require.Error(t, DB.Transaction(func(tx *gorm.DB) error { return ResizeSubscriptionPreConsumeTx(tx, "settled", math.MaxInt64) }))
		// Manual resets without a calendar reset are still distinct generations,
		// even when both reset calls happen within one clock second.
		_, err = AdminResetUserSubscriptionsByPlan(sub.UserId, plan.Id, false)
		require.NoError(t, err)
		_, err = AdminResetUserSubscriptionsByPlan(sub.UserId, plan.Id, false)
		require.NoError(t, err)
		assert.EqualValues(t, 2, getSubscriptionResetSub(t, sub.Id).QuotaCycle)
		reserveCycle(t, "finalize-race", sub, 100)
		results = make(chan error, 2)
		wg.Go(func() {
			results <- DB.Transaction(func(tx *gorm.DB) error {
				return SettleSubscriptionPreConsumeTx(tx, "finalize-race", 60)
			})
		})
		wg.Go(func() { results <- RefundSubscriptionPreConsume("finalize-race") })
		wg.Wait()
		close(results)
		successes := 0
		for err := range results {
			if err == nil {
				successes++
			}
		}
		assert.Equal(t, 1, successes, "settle and refund have a single winner")
		var finalized SubscriptionPreConsumeRecord
		require.NoError(t, DB.Where("request_id = ?", "finalize-race").First(&finalized).Error)
		require.NotNil(t, finalized.SettledAmount)
		assert.Equal(t, *finalized.SettledAmount, getSubscriptionResetSub(t, sub.Id).AmountUsed)
		// A stale final amount is bounded by its original cycle's total even
		// though it must not be charged to the new cycle.
		_, capSub := cycleFixture(t, SubscriptionResetNever)
		capSub.AmountTotal = 100
		require.NoError(t, DB.Save(capSub).Error)
		reserveCycle(t, "closed-cycle-cap", capSub, 100)
		_, err = AdminResetUserSubscriptionsByPlan(capSub.UserId, capSub.PlanId, false)
		require.NoError(t, err)
		require.ErrorIs(t, DB.Transaction(func(tx *gorm.DB) error {
			return SettleSubscriptionPreConsumeTx(tx, "closed-cycle-cap", 101)
		}), ErrSubscriptionQuotaInsufficient)
		assert.Zero(t, getSubscriptionResetSub(t, capSub.Id).AmountUsed)
		// The old subscription-ID-only entry point cannot safely attribute a
		// delta to either generation, including when called by future code.
		for _, delta := range []int64{-100, 30} {
			require.ErrorIs(t, PostConsumeUserSubscriptionDelta(capSub.Id, delta), ErrSubscriptionCycleExpired)
			assert.Zero(t, getSubscriptionResetSub(t, capSub.Id).AmountUsed)
		}
		require.NoError(t, PostConsumeUserSubscriptionDelta(capSub.Id, 0))
		// Exercise both serialization orders with an actual competing reset
		// and reservation transaction, not just a sequential cycle change.
		for _, resetFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("reset_reserve/reset_first_%t", resetFirst), func(t *testing.T) {
				plan, sub := cycleFixture(t, SubscriptionResetNever)
				request := fmt.Sprintf("reset-reserve-%t", resetFirst)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				tx := DB.WithContext(ctx).Begin()
				require.NoError(t, tx.Error)
				defer tx.Rollback()
				if resetFirst {
					_, err := adminResetUserSubscriptionsByPlanTx(tx, sub.UserId, plan, time.Now().Unix(), false)
					require.NoError(t, err)
				} else {
					_, err := PreConsumeUserSubscriptionTx(tx, request, sub.UserId, "cycle-model", 0, 50)
					require.NoError(t, err)
				}
				atSubscription := make(chan struct{})
				var once sync.Once
				callback := "cycle_reset_reserve_competition"
				require.NoError(t, DB.Callback().Query().Before("gorm:query").Register(callback, func(query *gorm.DB) {
					if query.Statement.Schema != nil && query.Statement.Schema.Name == "UserSubscription" {
						once.Do(func() { close(atSubscription) })
					}
				}))
				defer DB.Callback().Query().Remove(callback)
				done := make(chan error, 1)
				started := make(chan struct{})
				go func() {
					close(started)
					done <- DB.WithContext(ctx).Transaction(func(competing *gorm.DB) error {
						if resetFirst {
							_, err := PreConsumeUserSubscriptionTx(competing, request, sub.UserId, "cycle-model", 0, 50)
							return err
						}
						_, err := adminResetUserSubscriptionsByPlanTx(competing, sub.UserId, plan, time.Now().Unix(), false)
						return err
					})
				}()
				<-started
				if DB.Dialector.Name() != "sqlite" {
					select {
					case <-atSubscription:
					case <-ctx.Done():
						t.Fatal("competing transaction did not reach the subscription lock")
					}
				}
				require.NoError(t, tx.Commit().Error)
				require.NoError(t, <-done)
				stored := getSubscriptionResetSub(t, sub.Id)
				var record SubscriptionPreConsumeRecord
				require.NoError(t, DB.Where("request_id = ?", request).First(&record).Error)
				require.NotNil(t, record.QuotaCycle)
				assert.EqualValues(t, 1, stored.QuotaCycle)
				if resetFirst {
					assert.Equal(t, stored.QuotaCycle, *record.QuotaCycle)
					assert.EqualValues(t, 50, stored.AmountUsed)
				} else {
					assert.EqualValues(t, 0, *record.QuotaCycle)
					assert.Zero(t, stored.AmountUsed)
				}
				require.NoError(t, RefundSubscriptionPreConsume(request))
				assert.Zero(t, getSubscriptionResetSub(t, sub.Id).AmountUsed)
			})
		}
	})
}

func TestSubscriptionCycleMigrationPreservesUnknownRecords(t *testing.T) {
	forEachSubscriptionCycleDB(t, func(t *testing.T) {
		_, sub := cycleFixture(t, SubscriptionResetNever)
		reserveCycle(t, "before-upgrade", sub, 100)
		// Reconstruct the prior release's two affected tables by removing only
		// the newly introduced columns, then run the actual additive migration.
		require.NoError(t, DB.Migrator().DropColumn(&SubscriptionPreConsumeRecord{}, "QuotaCycle"))
		require.NoError(t, DB.Migrator().DropColumn(&SubscriptionPreConsumeRecord{}, "SettledAmount"))
		require.NoError(t, DB.Migrator().DropColumn(&SubscriptionPreConsumeRecord{}, "CycleAmountTotal"))
		require.NoError(t, DB.Migrator().DropColumn(&UserSubscription{}, "QuotaCycle"))
		for range 2 {
			require.NoError(t, DB.AutoMigrate(&UserSubscription{}, &SubscriptionPreConsumeRecord{}))
		}
		var record SubscriptionPreConsumeRecord
		require.NoError(t, DB.Where("request_id = ?", "before-upgrade").First(&record).Error)
		assert.Nil(t, record.QuotaCycle)
		assert.Nil(t, record.SettledAmount)
		assert.EqualValues(t, 100, record.PreConsumed)
		current := getSubscriptionResetSub(t, sub.Id)
		assert.Zero(t, current.QuotaCycle)
		assert.EqualValues(t, 100, current.AmountUsed)
		require.NoError(t, RefundSubscriptionPreConsume("before-upgrade"))
		assert.EqualValues(t, 100, getSubscriptionResetSub(t, sub.Id).AmountUsed)
		copy := record
		copy.Id = 0
		require.Error(t, DB.Create(&copy).Error, "request id uniqueness survives migration")
	})
}

func TestSubscriptionCycleCleanupPreservesRecoveryEvidence(t *testing.T) {
	forEachSubscriptionCycleDB(t, func(t *testing.T) {
		require.NoError(t, DB.AutoMigrate(&ForkBillingSession{}))
		t.Cleanup(func() { assert.NoError(t, DB.Migrator().DropTable(&ForkBillingSession{})) })
		_, sub := cycleFixture(t, SubscriptionResetNever)
		for _, item := range []struct{ request, status string }{
			{"pending-old", "consumed"}, {"session-reference", "settled"}, {"unreferenced-terminal", "refunded"},
		} {
			record := &SubscriptionPreConsumeRecord{RequestId: item.request, UserId: sub.UserId,
				UserSubscriptionId: sub.Id, PreConsumed: 10, Status: item.status}
			require.NoError(t, DB.Create(record).Error)
			require.NoError(t, DB.Model(record).UpdateColumn("updated_at", 1).Error)
		}
		require.NoError(t, DB.Create(&ForkBillingSession{RequestID: "session-reference", UserID: sub.UserId,
			FundingSource: "subscription", SubscriptionID: sub.Id, Status: BillingSettled}).Error)
		deleted, err := CleanupSubscriptionPreConsumeRecords(1)
		require.NoError(t, err)
		assert.EqualValues(t, 1, deleted)
		var records []SubscriptionPreConsumeRecord
		require.NoError(t, DB.Order("request_id").Find(&records).Error)
		require.Len(t, records, 2)
		assert.Equal(t, "pending-old", records[0].RequestId)
		assert.Equal(t, "session-reference", records[1].RequestId)
	})
}

func TestSubscriptionCycleAdministrationSerializesWithBilling(t *testing.T) {
	forEachSubscriptionCycleDB(t, func(t *testing.T) {
		require.NoError(t, DB.AutoMigrate(&User{}))
		t.Cleanup(func() { assert.NoError(t, DB.Migrator().DropTable(&User{})) })
		for _, operation := range []string{"cancel", "delete", "expire"} {
			t.Run(operation, func(t *testing.T) {
				_, sub := cycleFixture(t, SubscriptionResetNever)
				user := &User{Id: sub.UserId, Username: "cycle-" + operation, Group: "upgraded", AffCode: operation}
				require.NoError(t, DB.Create(user).Error)
				sub.UpgradeGroup, sub.DowngradeGroup = "upgraded", "default"
				require.NoError(t, DB.Save(sub).Error)
				request := "admin-" + operation
				reserveCycle(t, request, sub, 100)
				if operation == "expire" {
					require.NoError(t, DB.Model(sub).Update("end_time", GetDBTimestamp()-1).Error)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				billing := DB.WithContext(ctx).Begin()
				require.NoError(t, billing.Error)
				defer billing.Rollback()
				require.NoError(t, lockForUpdate(billing).First(&User{}, user.Id).Error)
				// Pause at the administrator's user lookup while billing holds that
				// row. On row-locking databases billing must still acquire its sub:
				// acquiring the sub before this lookup would form a real deadlock.
				atUser := make(chan struct{})
				var once sync.Once
				callback := "cycle_administration_lock_order"
				require.NoError(t, DB.Callback().Query().Before("gorm:query").Register(callback, func(tx *gorm.DB) {
					if tx.Statement.Schema != nil && tx.Statement.Schema.Name == "User" {
						once.Do(func() { close(atUser) })
					}
				}))
				defer DB.Callback().Query().Remove(callback)
				done := make(chan error, 1)
				go func() {
					var err error
					switch operation {
					case "cancel":
						_, err = AdminInvalidateUserSubscription(sub.Id)
					case "delete":
						_, err = AdminDeleteUserSubscription(sub.Id)
					case "expire":
						_, err = ExpireDueSubscriptions(100)
					}
					done <- err
				}()
				if DB.Dialector.Name() != "sqlite" {
					select {
					case <-atUser:
					case <-ctx.Done():
						t.Fatal("administrator did not reach the user lock")
					}
				}
				// SQLite uses BEGIN IMMEDIATE and serializes these transactions at
				// entry, while MySQL/PostgreSQL exercise the user/sub row-lock order.
				require.NoError(t, SettleSubscriptionPreConsumeTx(billing, request, 60))
				require.NoError(t, billing.Commit().Error)
				require.NoError(t, <-done)
				var record SubscriptionPreConsumeRecord
				require.NoError(t, DB.Where("request_id = ?", request).First(&record).Error)
				require.NotNil(t, record.SettledAmount)
				assert.EqualValues(t, 60, *record.SettledAmount)
				if operation != "delete" {
					assert.EqualValues(t, 60, getSubscriptionResetSub(t, sub.Id).AmountUsed)
				}
				require.NoError(t, DB.First(user, user.Id).Error)
				assert.Equal(t, "default", user.Group)
			})
		}
	})
}
