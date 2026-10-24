// Package contract_test pins the New API behaviour the Epay→Alipay payment
// bridge depends on (DEP-01 … DEP-05). These tests must pass on unmodified
// upstream code; a failure after an upstream sync blocks the merge (FORK-03).
package contract_test

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Calcium-Ion/go-epay/epay"
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/controller"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/QuantumNous/new-api/setting/system_setting"
	"github.com/gin-gonic/gin"
	sqlmysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	epayPID       = "contract_pid"
	epayKey       = "contract-key"
	dashboardAddr = "https://dashboard.example.com"
)

var dbSeq atomic.Int64

// dialects mirrors the upstream database matrix convention:
// SQLite always runs; MySQL/PostgreSQL run when their DSN env var is set.
var dialects = []struct{ kind, env string }{
	{"sqlite", ""}, {"mysql", "TEST_MYSQL_DSN"}, {"postgres", "TEST_POSTGRES_DSN"},
}

// forEachDB runs fn once per available database with a freshly migrated schema.
func forEachDB(t *testing.T, fn func(t *testing.T, db *gorm.DB)) {
	for _, d := range dialects {
		t.Run(d.kind, func(t *testing.T) {
			dsn := ""
			if d.env != "" {
				dsn = os.Getenv(d.env)
				if dsn == "" {
					t.Skip("set " + d.env + " to run this database")
				}
			}
			fn(t, setupDB(t, d.kind, dsn))
		})
	}
}

// setupDB points model.DB at an isolated database, runs the real upstream
// migration (model.InitDB), and restores all globals in Cleanup.
func setupDB(t *testing.T, kind, dsn string) *gorm.DB {
	t.Helper()
	prevDB, prevLogDB := model.DB, model.LOG_DB
	prevMain, prevLog := common.MainDatabaseType(), common.LogDatabaseType()
	prevMaster, prevSQLite := common.IsMasterNode, common.SQLitePath
	prevRedis, prevMemory := common.RedisEnabled, common.MemoryCacheEnabled
	prevBatch := common.BatchUpdateEnabled
	common.IsMasterNode = true
	common.RedisEnabled, common.MemoryCacheEnabled, common.BatchUpdateEnabled = false, false, false

	var cleanupDatabase func()
	switch kind {
	case "sqlite":
		// Same pragmas as the production default (common/database.go).
		common.SQLitePath = t.TempDir() + "/contract.db?_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_txlock=immediate"
		t.Setenv("SQL_DSN", "local")
	default:
		isolated, drop := isolatedServerDB(t, kind, dsn)
		cleanupDatabase = drop
		t.Setenv("SQL_DSN", isolated)
	}
	t.Setenv("LOG_SQL_DSN", "")
	require.NoError(t, model.InitDB())
	model.LOG_DB = model.DB
	db := model.DB

	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
		model.DB, model.LOG_DB = prevDB, prevLogDB
		common.SetDatabaseTypes(prevMain, prevLog)
		common.IsMasterNode, common.SQLitePath = prevMaster, prevSQLite
		common.RedisEnabled, common.MemoryCacheEnabled = prevRedis, prevMemory
		common.BatchUpdateEnabled = prevBatch
		if cleanupDatabase != nil {
			cleanupDatabase()
		}
	})
	return db
}

// configureEpay enables Epay top-up and subscription payments for one test.
func configureEpay(t *testing.T) {
	t.Helper()
	ps := operation_setting.GetPaymentSetting()
	prevConfirmed, prevTerms := ps.ComplianceConfirmed, ps.ComplianceTermsVersion
	prevAddr, prevID, prevKey := operation_setting.PayAddress, operation_setting.EpayId, operation_setting.EpayKey
	prevMethods, prevCallback := operation_setting.PayMethods, operation_setting.CustomCallbackAddress
	prevServer, prevUnit := system_setting.ServerAddress, common.QuotaPerUnit
	t.Cleanup(func() {
		ps.ComplianceConfirmed, ps.ComplianceTermsVersion = prevConfirmed, prevTerms
		operation_setting.PayAddress, operation_setting.EpayId, operation_setting.EpayKey = prevAddr, prevID, prevKey
		operation_setting.PayMethods, operation_setting.CustomCallbackAddress = prevMethods, prevCallback
		system_setting.ServerAddress, common.QuotaPerUnit = prevServer, prevUnit
	})
	ps.ComplianceConfirmed = true
	ps.ComplianceTermsVersion = operation_setting.CurrentComplianceTermsVersion
	operation_setting.PayAddress = "https://pay.example.com"
	operation_setting.EpayId, operation_setting.EpayKey = epayPID, epayKey
	operation_setting.PayMethods = []map[string]string{{"type": "alipay", "name": "Alipay"}}
	operation_setting.CustomCallbackAddress = ""
	system_setting.ServerAddress = dashboardAddr
	common.QuotaPerUnit = 500000
}

// isolatedServerDB creates a uniquely named database on a loopback MySQL or
// PostgreSQL server and returns its DSN plus a function that drops it.
func isolatedServerDB(t *testing.T, kind, dsn string) (string, func()) {
	t.Helper()
	name := fmt.Sprintf("newapi_contract_%d_%d", time.Now().UnixNano(), dbSeq.Add(1))
	var admin gorm.Dialector
	var isolated string
	if kind == "mysql" {
		cfg, err := sqlmysql.ParseDSN(dsn)
		require.NoError(t, err)
		host, _, err := net.SplitHostPort(cfg.Addr)
		require.NoError(t, err)
		require.True(t, net.ParseIP(host).IsLoopback(), "contract tests only use loopback databases")
		admin = mysql.Open(dsn)
		cfg.DBName = name
		isolated = cfg.FormatDSN()
	} else {
		parsed, err := url.Parse(dsn)
		require.NoError(t, err)
		require.True(t, net.ParseIP(parsed.Hostname()).IsLoopback(), "contract tests only use loopback databases")
		admin = postgres.Open(dsn)
		parsed.Path = "/" + name
		isolated = parsed.String()
	}
	adminDB, err := gorm.Open(admin, &gorm.Config{})
	require.NoError(t, err)
	create := "CREATE DATABASE " + name
	if kind == "mysql" {
		create += " CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
	}
	require.NoError(t, adminDB.Exec(create).Error)
	return isolated, func() {
		_ = adminDB.Exec("DROP DATABASE " + name).Error
		if sqlDB, err := adminDB.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}
}

// signedCallback builds Epay callback params signed with the test merchant key.
func signedCallback(t *testing.T, tradeNo, money, status string) url.Values {
	t.Helper()
	params := map[string]string{
		"pid": epayPID, "type": "alipay", "money": money, "name": "contract-item",
		"out_trade_no": tradeNo, "trade_no": "ALI" + tradeNo, "trade_status": status,
	}
	signed, err := epay.GenerateParamsWithError(params, epayKey)
	require.NoError(t, err)
	values := url.Values{}
	for k, v := range signed {
		values.Set(k, v)
	}
	return values
}

func callbackRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Any("/api/user/epay/notify", controller.EpayNotify)
	r.Any("/api/subscription/epay/notify", controller.SubscriptionEpayNotify)
	r.Any("/api/subscription/epay/return", controller.SubscriptionEpayReturn)
	return r
}

// postForm sends a form-encoded POST and returns the recorder.
func postForm(r http.Handler, path string, form url.Values) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func getQuery(r http.Handler, path string, query url.Values) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path+"?"+query.Encode(), nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func createUser(t *testing.T, db *gorm.DB, quota int) *model.User {
	t.Helper()
	n := dbSeq.Add(1)
	u := &model.User{Username: fmt.Sprintf("contract%d", n), Password: "unused-password-hash", Group: "default",
		Status: common.UserStatusEnabled, Role: common.RoleCommonUser, Quota: quota, AffCode: fmt.Sprintf("aff%d", n)}
	require.NoError(t, db.Create(u).Error)
	return u
}

func createPendingTopUp(t *testing.T, db *gorm.DB, userID int, tradeNo string, amount int64, money float64) {
	t.Helper()
	require.NoError(t, db.Create(&model.TopUp{UserId: userID, Amount: amount, Money: money, TradeNo: tradeNo,
		PaymentMethod: "alipay", PaymentProvider: model.PaymentProviderEpay,
		CreateTime: time.Now().Unix(), Status: common.TopUpStatusPending}).Error)
}

func createPlan(t *testing.T, db *gorm.DB, price float64) *model.SubscriptionPlan {
	t.Helper()
	plan := &model.SubscriptionPlan{Title: fmt.Sprintf("plan-%d", dbSeq.Add(1)), PriceAmount: price, Currency: "CNY",
		DurationUnit: "month", DurationValue: 1, Enabled: true, TotalAmount: 1000, QuotaResetPeriod: "never"}
	require.NoError(t, db.Create(plan).Error)
	model.InvalidateSubscriptionPlanCache(plan.Id)
	t.Cleanup(func() { model.InvalidateSubscriptionPlanCache(plan.Id) })
	return plan
}

func createPendingSubscriptionOrder(t *testing.T, db *gorm.DB, userID int, plan *model.SubscriptionPlan, tradeNo string) {
	t.Helper()
	require.NoError(t, db.Create(&model.SubscriptionOrder{UserId: userID, PlanId: plan.Id, Money: plan.PriceAmount,
		TradeNo: tradeNo, PaymentMethod: "alipay", PaymentProvider: model.PaymentProviderEpay,
		CreateTime: time.Now().Unix(), Status: common.TopUpStatusPending}).Error)
}

func userQuota(t *testing.T, db *gorm.DB, userID int) int {
	t.Helper()
	var u model.User
	require.NoError(t, db.Select("quota").First(&u, userID).Error)
	return u.Quota
}

func countSubscriptions(t *testing.T, db *gorm.DB, userID int) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Model(&model.UserSubscription{}).Where("user_id = ?", userID).Count(&n).Error)
	return n
}

func orderStatus(t *testing.T, db *gorm.DB, table any, tradeNo string) string {
	t.Helper()
	var row struct{ Status string }
	require.NoError(t, db.Model(table).Select("status").Where("trade_no = ?", tradeNo).Take(&row).Error)
	return row.Status
}
