package model

import (
	"errors"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// setupLinkTest swaps in a file-backed SQLite database with the production
// pragmas. The package's shared in-memory DB uses MaxOpenConns(1), which
// deadlocks CompleteSubscriptionOrder: upstream GetDBTimestamp queries the
// global DB while the completion transaction holds the only connection.
func setupLinkTest(t *testing.T) {
	t.Helper()
	prevDB, prevLogDB := DB, LOG_DB
	db, err := gorm.Open(sqlite.Open(t.TempDir()+"/link.db?_pragma=busy_timeout(30000)&_pragma=journal_mode(WAL)&_txlock=immediate"), &gorm.Config{})
	require.NoError(t, err)
	DB, LOG_DB = db, db
	require.NoError(t, db.AutoMigrate(&User{}, &SubscriptionPlan{}, &SubscriptionOrder{}, &UserSubscription{},
		&TopUp{}, &Log{}, &ForkSubscriptionOrderLink{}))
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
		DB, LOG_DB = prevDB, prevLogDB
	})
}

func linkTestPlan(t *testing.T, total int64) *SubscriptionPlan {
	t.Helper()
	plan := &SubscriptionPlan{Title: "link-plan", PriceAmount: 10, Currency: "CNY", DurationUnit: "month",
		DurationValue: 1, Enabled: true, TotalAmount: total, QuotaResetPeriod: "never"}
	require.NoError(t, DB.Create(plan).Error)
	InvalidateSubscriptionPlanCache(plan.Id)
	t.Cleanup(func() { InvalidateSubscriptionPlanCache(plan.Id) })
	return plan
}

func linkTestUser(t *testing.T, name string) *User {
	t.Helper()
	u := &User{Username: name, Password: "unused-password-hash", Status: common.UserStatusEnabled,
		Role: common.RoleCommonUser, Group: "default", AffCode: "aff-" + name}
	require.NoError(t, DB.Create(u).Error)
	return u
}

func newPendingOrder(userID int, plan *SubscriptionPlan, tradeNo string) *SubscriptionOrder {
	return &SubscriptionOrder{UserId: userID, PlanId: plan.Id, Money: plan.PriceAmount, TradeNo: tradeNo,
		PaymentMethod: "alipay", PaymentProvider: PaymentProviderEpay, Status: common.TopUpStatusPending}
}

func linkRow(t *testing.T, tradeNo string) ForkSubscriptionOrderLink {
	t.Helper()
	var link ForkSubscriptionOrderLink
	require.NoError(t, DB.Where("trade_no = ?", tradeNo).Take(&link).Error)
	return link
}

// LINK-01: a plan edit between order creation and payment does not change
// what the order grants; new orders use the new plan.
func TestLink01_CompletionUsesOrderSnapshot(t *testing.T) {
	setupLinkTest(t)
	user := linkTestUser(t, "link01")
	plan := linkTestPlan(t, 1000)
	require.NoError(t, newPendingOrder(user.Id, plan, "LINK01A").Insert())
	assert.Equal(t, SnapshotSourceOrder, linkRow(t, "LINK01A").SnapshotSource)

	require.NoError(t, DB.Model(plan).Update("total_amount", 500).Error)
	InvalidateSubscriptionPlanCache(plan.Id)
	require.NoError(t, CompleteSubscriptionOrder("LINK01A", "", PaymentProviderEpay, "alipay"))

	var sub UserSubscription
	require.NoError(t, DB.Where("user_id = ?", user.Id).Take(&sub).Error)
	assert.EqualValues(t, 1000, sub.AmountTotal)

	require.NoError(t, newPendingOrder(user.Id, plan, "LINK01B").Insert())
	require.NoError(t, CompleteSubscriptionOrder("LINK01B", "", PaymentProviderEpay, "alipay"))
	var newer UserSubscription
	require.NoError(t, DB.Where("user_id = ? AND id <> ?", user.Id, sub.Id).Take(&newer).Error)
	assert.EqualValues(t, 500, newer.AmountTotal)
}

// LINK-01: an order created before the fork has no snapshot; it completes
// with the current plan and gets a current_plan link row.
func TestLink01_PreForkOrderUsesCurrentPlan(t *testing.T) {
	setupLinkTest(t)
	user := linkTestUser(t, "link01legacy")
	plan := linkTestPlan(t, 1000)
	require.NoError(t, DB.Create(newPendingOrder(user.Id, plan, "LINK01LEGACY")).Error) // upstream-style insert
	require.NoError(t, DB.Model(plan).Update("total_amount", 700).Error)
	InvalidateSubscriptionPlanCache(plan.Id)

	require.NoError(t, CompleteSubscriptionOrder("LINK01LEGACY", "", PaymentProviderEpay, "alipay"))

	var sub UserSubscription
	require.NoError(t, DB.Where("user_id = ?", user.Id).Take(&sub).Error)
	assert.EqualValues(t, 700, sub.AmountTotal)
	link := linkRow(t, "LINK01LEGACY")
	assert.Equal(t, SnapshotSourceCurrentPlan, link.SnapshotSource)
	require.NotNil(t, link.UserSubscriptionId)
	assert.Equal(t, sub.Id, *link.UserSubscriptionId)
}

// LINK-02: completion links order and subscription; lookup works both ways.
func TestLink02_CompletionRecordsLink(t *testing.T) {
	setupLinkTest(t)
	user := linkTestUser(t, "link02")
	plan := linkTestPlan(t, 1000)
	require.NoError(t, newPendingOrder(user.Id, plan, "LINK02").Insert())
	require.NoError(t, CompleteSubscriptionOrder("LINK02", "", PaymentProviderEpay, "alipay"))

	var sub UserSubscription
	require.NoError(t, DB.Where("user_id = ?", user.Id).Take(&sub).Error)
	link := linkRow(t, "LINK02")
	require.NotNil(t, link.UserSubscriptionId)
	assert.Equal(t, sub.Id, *link.UserSubscriptionId)
	assert.Equal(t, LinkSourceTxn, link.LinkSource)
	var byID ForkSubscriptionOrderLink
	require.NoError(t, DB.Where("user_subscription_id = ?", sub.Id).Take(&byID).Error)
	assert.Equal(t, "LINK02", byID.TradeNo)

	status, err := GetSubscriptionLinkStatus("LINK02")
	require.NoError(t, err)
	assert.Equal(t, LinkStatusLinked, status.Status)
	assert.Equal(t, sub.Id, status.UserSubscriptionId)
}

// LINK-02: if writing the link fails, the whole completion rolls back: no
// orphan subscription, no top_ups mirror, order still pending.
func TestLink02_LinkFailureRollsBackCompletion(t *testing.T) {
	setupLinkTest(t)
	user := linkTestUser(t, "link02fail")
	plan := linkTestPlan(t, 1000)
	require.NoError(t, newPendingOrder(user.Id, plan, "LINK02FAIL").Insert())

	const cb = "fork_test:fail_link_update"
	require.NoError(t, DB.Callback().Update().Before("gorm:update").Register(cb, func(tx *gorm.DB) {
		if tx.Statement.Table == "fork_subscription_order_links" {
			_ = tx.AddError(errors.New("injected link failure"))
		}
	}))
	err := CompleteSubscriptionOrder("LINK02FAIL", "", PaymentProviderEpay, "alipay")
	require.NoError(t, DB.Callback().Update().Remove(cb))
	require.Error(t, err)

	var subs, mirrors int64
	DB.Model(&UserSubscription{}).Where("user_id = ?", user.Id).Count(&subs)
	DB.Model(&TopUp{}).Where("trade_no = ?", "LINK02FAIL").Count(&mirrors)
	assert.Zero(t, subs)
	assert.Zero(t, mirrors)
	var order SubscriptionOrder
	require.NoError(t, DB.Where("trade_no = ?", "LINK02FAIL").Take(&order).Error)
	assert.Equal(t, common.TopUpStatusPending, order.Status)
	assert.Nil(t, linkRow(t, "LINK02FAIL").UserSubscriptionId)

	// Retry after the fault clears completes and links exactly once.
	require.NoError(t, CompleteSubscriptionOrder("LINK02FAIL", "", PaymentProviderEpay, "alipay"))
	require.NotNil(t, linkRow(t, "LINK02FAIL").UserSubscriptionId)
}

// LINK-02: balance purchases are linked; admin grants are not.
func TestLink02_BalancePurchaseLinkedAdminGrantNot(t *testing.T) {
	setupLinkTest(t)
	user := linkTestUser(t, "link02bal")
	require.NoError(t, DB.Model(user).Update("quota", 100000000).Error)
	plan := linkTestPlan(t, 1000)

	require.NoError(t, PurchaseSubscriptionWithBalance(user.Id, plan.Id))
	var order SubscriptionOrder
	require.NoError(t, DB.Where("user_id = ? AND payment_provider = ?", user.Id, PaymentProviderBalance).Take(&order).Error)
	link := linkRow(t, order.TradeNo)
	require.NotNil(t, link.UserSubscriptionId)
	assert.Equal(t, SnapshotSourceOrder, link.SnapshotSource)

	_, err := AdminBindSubscription(user.Id, plan.Id, "contract")
	require.NoError(t, err)
	var adminSub UserSubscription
	require.NoError(t, DB.Where("user_id = ? AND source = ?", user.Id, "admin").Take(&adminSub).Error)
	var n int64
	DB.Model(&ForkSubscriptionOrderLink{}).Where("user_subscription_id = ?", adminSub.Id).Count(&n)
	assert.Zero(t, n)
}

// FORK_SUBSCRIPTION_LINK_ENABLED=false restores upstream behaviour.
func TestLinkDisabledRestoresUpstreamBehaviour(t *testing.T) {
	setupLinkTest(t)
	t.Setenv("FORK_SUBSCRIPTION_LINK_ENABLED", "false")
	user := linkTestUser(t, "linkoff")
	plan := linkTestPlan(t, 1000)
	require.NoError(t, newPendingOrder(user.Id, plan, "LINKOFF").Insert())
	require.NoError(t, CompleteSubscriptionOrder("LINKOFF", "", PaymentProviderEpay, "alipay"))
	var n int64
	DB.Model(&ForkSubscriptionOrderLink{}).Count(&n)
	assert.Zero(t, n)
	status, err := GetSubscriptionLinkStatus("LINKOFF")
	require.NoError(t, err)
	assert.Equal(t, LinkStatusMissing, status.Status)
}

// createLegacyCompleted simulates an order completed before the fork: no link
// row, subscription start_time offset from the order completion time.
func createLegacyCompleted(t *testing.T, user *User, plan *SubscriptionPlan, tradeNo string, completeAt, startAt int64) (*SubscriptionOrder, *UserSubscription) {
	t.Helper()
	o := newPendingOrder(user.Id, plan, tradeNo)
	o.Status, o.CreateTime, o.CompleteTime = common.TopUpStatusSuccess, completeAt-60, completeAt
	require.NoError(t, DB.Create(o).Error)
	s := &UserSubscription{UserId: user.Id, PlanId: plan.Id, AmountTotal: plan.TotalAmount, StartTime: startAt,
		EndTime: startAt + 30*86400, Status: "active", Source: "order"}
	require.NoError(t, DB.Create(s).Error)
	return o, s
}

// LINK-03: one order + one subscription within the window is backfilled.
func TestLink03_BackfillsUniqueMatch(t *testing.T) {
	setupLinkTest(t)
	user := linkTestUser(t, "link03one")
	plan := linkTestPlan(t, 1000)
	_, sub := createLegacyCompleted(t, user, plan, "LINK03ONE", 1_700_000_000, 1_700_000_002)

	decisions, err := PlanSubscriptionLinkBackfill(300)
	require.NoError(t, err)
	require.Len(t, decisions, 1)
	assert.Equal(t, LinkSourceBackfill, decisions[0].Result)
	assert.Equal(t, sub.Id, decisions[0].UserSubscriptionId)
	var rows int64
	DB.Model(&ForkSubscriptionOrderLink{}).Count(&rows)
	assert.Zero(t, rows, "planning must not write")

	require.NoError(t, ApplySubscriptionLinkBackfill(decisions))
	status, err := GetSubscriptionLinkStatus("LINK03ONE")
	require.NoError(t, err)
	assert.Equal(t, LinkStatusLinked, status.Status)
	assert.Equal(t, LinkSourceBackfill, status.LinkSource)

	again, err := PlanSubscriptionLinkBackfill(300)
	require.NoError(t, err)
	assert.Empty(t, again, "linked orders are not planned twice")
}

// LINK-03: ambiguous groups are all unlinkable, never guessed.
func TestLink03_AmbiguousGroupsAreUnlinkable(t *testing.T) {
	setupLinkTest(t)
	plan := linkTestPlan(t, 1000)
	// Three orders, two subscriptions for the same user and plan.
	multi := linkTestUser(t, "link03multi")
	createLegacyCompleted(t, multi, plan, "LINK03M1", 1_700_000_000, 1_700_000_001)
	createLegacyCompleted(t, multi, plan, "LINK03M2", 1_700_100_000, 1_700_100_001)
	extra := newPendingOrder(multi.Id, plan, "LINK03M3")
	extra.Status, extra.CompleteTime = common.TopUpStatusSuccess, 1_700_200_000
	require.NoError(t, DB.Create(extra).Error)
	// One order and one subscription, but too far apart in time.
	far := linkTestUser(t, "link03far")
	createLegacyCompleted(t, far, plan, "LINK03FAR", 1_700_000_000, 1_700_009_000)

	decisions, err := PlanSubscriptionLinkBackfill(300)
	require.NoError(t, err)
	require.Len(t, decisions, 4)
	for _, d := range decisions {
		assert.Equal(t, LinkSourceUnlinkable, d.Result, d.TradeNo)
		assert.NotEmpty(t, d.Reason)
		assert.Zero(t, d.UserSubscriptionId)
	}
	require.NoError(t, ApplySubscriptionLinkBackfill(decisions))
	for _, tn := range []string{"LINK03M1", "LINK03M2", "LINK03M3", "LINK03FAR"} {
		status, err := GetSubscriptionLinkStatus(tn)
		require.NoError(t, err)
		assert.Equal(t, LinkStatusUnlinkable, status.Status, tn)
	}
	again, err := PlanSubscriptionLinkBackfill(300)
	require.NoError(t, err)
	assert.Empty(t, again, "unlinkable orders are not re-planned")
}

// LINK-04: if an admin later deletes the linked subscription, the status says
// so instead of reporting the order as verifiably granted.
func TestLink04_DeletedSubscriptionIsReported(t *testing.T) {
	setupLinkTest(t)
	user := linkTestUser(t, "link04del")
	plan := linkTestPlan(t, 1000)
	require.NoError(t, newPendingOrder(user.Id, plan, "LINK04DEL").Insert())
	require.NoError(t, CompleteSubscriptionOrder("LINK04DEL", "", PaymentProviderEpay, "alipay"))
	status, err := GetSubscriptionLinkStatus("LINK04DEL")
	require.NoError(t, err)
	require.Equal(t, LinkStatusLinked, status.Status)

	_, err = AdminDeleteUserSubscription(status.UserSubscriptionId)
	require.NoError(t, err)
	status, err = GetSubscriptionLinkStatus("LINK04DEL")
	require.NoError(t, err)
	assert.Equal(t, LinkStatusSubscriptionDeleted, status.Status)
}
