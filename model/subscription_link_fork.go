package model

// Fork-owned: subscription order snapshot and order→entitlement linkage
// (LINK-01 … LINK-04). Upstream files only call the hooks defined here; see
// PATCHES.md for the exact call sites.

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

const (
	SnapshotSourceOrder       = "order"        // frozen when the order was created
	SnapshotSourceCurrentPlan = "current_plan" // pre-fork order completed with the plan as it is now

	LinkSourceTxn        = "txn"        // written inside the completion transaction
	LinkSourceBackfill   = "backfill"   // proven unique by the LINK-03 backfill
	LinkSourceUnlinkable = "unlinkable" // backfill could not prove a unique match

	subscriptionSnapshotVersion = 1
)

// ForkSubscriptionOrderLink stores the plan snapshot taken at order creation
// and, once completed, the user subscription that order granted.
type ForkSubscriptionOrderLink struct {
	Id                 int64  `json:"id"`
	OrderId            int    `json:"order_id" gorm:"uniqueIndex;not null"`
	TradeNo            string `json:"trade_no" gorm:"type:varchar(255);uniqueIndex;not null"`
	PlanId             int    `json:"plan_id" gorm:"index;not null"`
	Snapshot           string `json:"snapshot" gorm:"type:text"`
	SnapshotSource     string `json:"snapshot_source" gorm:"type:varchar(16);not null"`
	UserSubscriptionId *int   `json:"user_subscription_id" gorm:"uniqueIndex"`
	LinkSource         string `json:"link_source" gorm:"type:varchar(16);default:''"`
	CreatedAt          int64  `json:"created_at" gorm:"bigint"`
	LinkedAt           int64  `json:"linked_at" gorm:"bigint"`
}

func (ForkSubscriptionOrderLink) TableName() string { return "fork_subscription_order_links" }

// subscriptionPlanSnapshot lists the plan fields that decide what a completed
// order grants. Fields not listed keep their current-plan value at completion.
type subscriptionPlanSnapshot struct {
	Version                 int     `json:"version"`
	PriceAmount             float64 `json:"price_amount"`
	Currency                string  `json:"currency"`
	DurationUnit            string  `json:"duration_unit"`
	DurationValue           int     `json:"duration_value"`
	CustomSeconds           int64   `json:"custom_seconds"`
	TotalAmount             int64   `json:"total_amount"`
	QuotaResetPeriod        string  `json:"quota_reset_period"`
	QuotaResetCustomSeconds int64   `json:"quota_reset_custom_seconds"`
	UpgradeGroup            string  `json:"upgrade_group"`
	DowngradeGroup          string  `json:"downgrade_group"`
	AllowWalletOverflow     *bool   `json:"allow_wallet_overflow"`
	MaxPurchasePerUser      int     `json:"max_purchase_per_user"`
}

// forkSubscriptionLinkEnabled is the FORK_SUBSCRIPTION_LINK_ENABLED switch.
// Turning it off restores upstream behaviour; existing rows are kept.
func forkSubscriptionLinkEnabled() bool {
	if forkTestBeforeHook != nil {
		forkTestBeforeHook()
	}
	return common.GetEnvOrDefaultBool("FORK_SUBSCRIPTION_LINK_ENABLED", true)
}

// forkTestBeforeHook is set only by fork test files (see
// fork_testmain_fork_test.go); it is always nil in production builds.
var forkTestBeforeHook func()

func snapshotFromPlan(plan *SubscriptionPlan) subscriptionPlanSnapshot {
	return subscriptionPlanSnapshot{
		Version: subscriptionSnapshotVersion, PriceAmount: plan.PriceAmount, Currency: plan.Currency,
		DurationUnit: plan.DurationUnit, DurationValue: plan.DurationValue, CustomSeconds: plan.CustomSeconds,
		TotalAmount: plan.TotalAmount, QuotaResetPeriod: plan.QuotaResetPeriod,
		QuotaResetCustomSeconds: plan.QuotaResetCustomSeconds, UpgradeGroup: plan.UpgradeGroup,
		DowngradeGroup: plan.DowngradeGroup, AllowWalletOverflow: plan.AllowWalletOverflow,
		MaxPurchasePerUser: plan.MaxPurchasePerUser,
	}
}

// applyTo returns a copy of plan with the snapshot's entitlement fields.
func (s subscriptionPlanSnapshot) applyTo(plan *SubscriptionPlan) *SubscriptionPlan {
	p := *plan
	p.PriceAmount, p.Currency = s.PriceAmount, s.Currency
	p.DurationUnit, p.DurationValue, p.CustomSeconds = s.DurationUnit, s.DurationValue, s.CustomSeconds
	p.TotalAmount = s.TotalAmount
	p.QuotaResetPeriod, p.QuotaResetCustomSeconds = s.QuotaResetPeriod, s.QuotaResetCustomSeconds
	p.UpgradeGroup, p.DowngradeGroup = s.UpgradeGroup, s.DowngradeGroup
	p.AllowWalletOverflow, p.MaxPurchasePerUser = s.AllowWalletOverflow, s.MaxPurchasePerUser
	return &p
}

// forkInsertOrderWithSnapshot inserts a pending order and its plan snapshot in
// one transaction (LINK-01). Hook for SubscriptionOrder.Insert.
func forkInsertOrderWithSnapshot(o *SubscriptionOrder) error {
	return DB.Transaction(func(tx *gorm.DB) error {
		plan, err := getSubscriptionPlanByIdTx(tx, o.PlanId)
		if err != nil {
			return err
		}
		if err := tx.Create(o).Error; err != nil {
			return err
		}
		return insertOrderLinkTx(tx, o, snapshotFromPlan(plan), SnapshotSourceOrder)
	})
}

func insertOrderLinkTx(tx *gorm.DB, o *SubscriptionOrder, snap subscriptionPlanSnapshot, source string) error {
	data, err := common.Marshal(snap)
	if err != nil {
		return err
	}
	return tx.Create(&ForkSubscriptionOrderLink{OrderId: o.Id, TradeNo: o.TradeNo, PlanId: o.PlanId,
		Snapshot: string(data), SnapshotSource: source, CreatedAt: common.GetTimestamp()}).Error
}

// forkPlanForOrderTx returns the plan to grant for an order: the snapshot if
// one exists, otherwise the current plan (pre-fork orders, LINK-01 scenario 2).
func forkPlanForOrderTx(tx *gorm.DB, order *SubscriptionOrder, current *SubscriptionPlan) (*SubscriptionPlan, error) {
	if !forkSubscriptionLinkEnabled() {
		return current, nil
	}
	var link ForkSubscriptionOrderLink
	err := tx.Where("order_id = ?", order.Id).Take(&link).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return current, nil
	}
	if err != nil {
		return nil, err
	}
	if link.SnapshotSource != SnapshotSourceOrder {
		return current, nil
	}
	var snap subscriptionPlanSnapshot
	if err := common.Unmarshal([]byte(link.Snapshot), &snap); err != nil {
		return nil, fmt.Errorf("decode subscription snapshot for order %d: %w", order.Id, err)
	}
	return snap.applyTo(current), nil
}

// forkLinkOrderTx records which user subscription a completed order granted
// (LINK-02). It runs inside the completion transaction, so any failure rolls
// back the subscription and keeps the order pending. Orders created before the
// fork get a current_plan link row here.
func forkLinkOrderTx(tx *gorm.DB, order *SubscriptionOrder, plan *SubscriptionPlan, sub *UserSubscription) error {
	if !forkSubscriptionLinkEnabled() {
		return nil
	}
	if order.Id == 0 || sub == nil || sub.Id == 0 {
		return errors.New("fork link: order and subscription must be persisted")
	}
	now := common.GetTimestamp()
	subID := sub.Id
	result := tx.Model(&ForkSubscriptionOrderLink{}).
		Where("order_id = ? AND user_subscription_id IS NULL", order.Id).
		Updates(map[string]any{"user_subscription_id": subID, "link_source": LinkSourceTxn, "linked_at": now})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 1 {
		return nil
	}
	var existing int64
	if err := tx.Model(&ForkSubscriptionOrderLink{}).Where("order_id = ?", order.Id).Count(&existing).Error; err != nil {
		return err
	}
	if existing > 0 {
		return fmt.Errorf("fork link: order %d is already linked", order.Id)
	}
	data, err := common.Marshal(snapshotFromPlan(plan))
	if err != nil {
		return err
	}
	return tx.Create(&ForkSubscriptionOrderLink{OrderId: order.Id, TradeNo: order.TradeNo, PlanId: order.PlanId,
		Snapshot: string(data), SnapshotSource: SnapshotSourceCurrentPlan, UserSubscriptionId: &subID,
		LinkSource: LinkSourceTxn, CreatedAt: now, LinkedAt: now}).Error
}

// Link status values returned to the reconciler (LINK-04).
const (
	LinkStatusLinked              = "linked"
	LinkStatusUnlinkable          = "unlinkable"
	LinkStatusMissing             = "missing"
	LinkStatusSubscriptionDeleted = "subscription_deleted" // linked, but the subscription row no longer exists
)

type SubscriptionLinkStatus struct {
	TradeNo            string `json:"trade_no"`
	Status             string `json:"status"`
	UserSubscriptionId int    `json:"user_subscription_id,omitempty"`
	LinkSource         string `json:"link_source,omitempty"`
}

// GetSubscriptionLinkStatus reports whether an order has a proven entitlement.
// Only "linked" counts as evidence that the entitlement was granted.
func GetSubscriptionLinkStatus(tradeNo string) (*SubscriptionLinkStatus, error) {
	out := &SubscriptionLinkStatus{TradeNo: tradeNo, Status: LinkStatusMissing}
	var link ForkSubscriptionOrderLink
	err := DB.Where("trade_no = ?", tradeNo).Take(&link).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	out.LinkSource = link.LinkSource
	switch {
	case link.UserSubscriptionId != nil:
		out.UserSubscriptionId = *link.UserSubscriptionId
		// An admin may delete the subscription later; the grant is then no
		// longer verifiable, so report it instead of claiming "linked".
		var n int64
		if err := DB.Model(&UserSubscription{}).Where("id = ?", *link.UserSubscriptionId).Count(&n).Error; err != nil {
			return nil, err
		}
		out.Status = LinkStatusLinked
		if n == 0 {
			out.Status = LinkStatusSubscriptionDeleted
		}
	case link.LinkSource == LinkSourceUnlinkable:
		out.Status = LinkStatusUnlinkable
	}
	return out, nil
}

// forkLinkBalanceOrderTx links a balance purchase. Order, snapshot and
// subscription are created in one transaction, so the snapshot is the plan used.
func forkLinkBalanceOrderTx(tx *gorm.DB, order *SubscriptionOrder, plan *SubscriptionPlan, sub *UserSubscription) error {
	if !forkSubscriptionLinkEnabled() {
		return nil
	}
	if err := insertOrderLinkTx(tx, order, snapshotFromPlan(plan), SnapshotSourceOrder); err != nil {
		return err
	}
	return forkLinkOrderTx(tx, order, plan, sub)
}

// BackfillDecision is one row of the LINK-03 backfill report.
type BackfillDecision struct {
	OrderId            int    `json:"order_id"`
	TradeNo            string `json:"trade_no"`
	UserId             int    `json:"user_id"`
	PlanId             int    `json:"plan_id"`
	Result             string `json:"result"` // backfill | unlinkable
	UserSubscriptionId int    `json:"user_subscription_id,omitempty"`
	Reason             string `json:"reason,omitempty"`
}

// InitSubscriptionLinkBackfillDB opens an existing database without running
// server startup migrations or initialization. A dry run also opens SQLite in
// read-only mode, without the server's journal-mode or immediate-lock pragmas.
func InitSubscriptionLinkBackfillDB(apply bool) error {
	var db *gorm.DB
	var kind common.DatabaseType
	var err error
	dsn := os.Getenv("SQL_DSN")
	if dsn == "" || strings.HasPrefix(dsn, "local") {
		path, rawQuery, _ := strings.Cut(common.SQLitePath, "?")
		params, parseErr := url.ParseQuery(rawQuery)
		if parseErr != nil {
			return fmt.Errorf("invalid SQLite connection parameters: %w", parseErr)
		}
		if apply {
			params.Set("mode", "rw")
		} else {
			params = url.Values{"mode": {"ro"}, "_pragma": {"query_only(1)", "busy_timeout(30000)"}}
		}
		if !strings.HasPrefix(path, "file:") {
			path = "file:" + strings.ReplaceAll(path, "\\", "/")
		}
		db, err = gorm.Open(sqlite.Open(path+"?"+params.Encode()), newGormConfig(false))
		kind = common.DatabaseTypeSQLite
	} else {
		db, kind, err = chooseDB("SQL_DSN", false)
	}
	if err != nil {
		return err
	}
	for _, table := range []any{&SubscriptionOrder{}, &UserSubscription{}, &ForkSubscriptionOrderLink{}} {
		if !db.Migrator().HasTable(table) {
			if sqlDB, closeErr := db.DB(); closeErr == nil {
				_ = sqlDB.Close()
			}
			return errors.New("backfill requires an existing database with the fork migration applied")
		}
	}
	DB = db
	common.SetMainDatabaseType(kind)
	initCol()
	return nil
}

// PlanSubscriptionLinkBackfill decides, without writing, which completed but
// unlinked orders can be proven to own exactly one subscription (LINK-03).
// Only a single unlinked successful order and a single Source=order subscription
// for the same user and plan can be matched, within the configured time window.
// Multiple purchases remain unlinkable: chronological sorting is not proof.
func PlanSubscriptionLinkBackfill(windowSeconds int64) ([]BackfillDecision, error) {
	if windowSeconds < 0 {
		return nil, errors.New("backfill window must not be negative")
	}
	var orders []SubscriptionOrder
	err := DB.Where("status = ? AND id NOT IN (?)", common.TopUpStatusSuccess,
		DB.Model(&ForkSubscriptionOrderLink{}).Select("order_id").Where("user_subscription_id IS NOT NULL OR link_source = ?", LinkSourceUnlinkable)).
		Order("user_id, plan_id, complete_time, id").Find(&orders).Error
	if err != nil {
		return nil, err
	}
	var subs []UserSubscription
	err = DB.Where("source = ? AND id NOT IN (?)", "order",
		DB.Model(&ForkSubscriptionOrderLink{}).Select("user_subscription_id").Where("user_subscription_id IS NOT NULL")).
		Order("user_id, plan_id, start_time, id").Find(&subs).Error
	if err != nil {
		return nil, err
	}
	type key struct{ user, plan int }
	subsByKey := map[key][]UserSubscription{}
	for _, s := range subs {
		subsByKey[key{s.UserId, s.PlanId}] = append(subsByKey[key{s.UserId, s.PlanId}], s)
	}
	ordersByKey := map[key][]SubscriptionOrder{}
	var keys []key
	for _, o := range orders {
		k := key{o.UserId, o.PlanId}
		if _, seen := ordersByKey[k]; !seen {
			keys = append(keys, k)
		}
		ordersByKey[k] = append(ordersByKey[k], o)
	}
	out := []BackfillDecision{}
	for _, k := range keys {
		group, candidates := ordersByKey[k], subsByKey[k]
		reason := ""
		if len(group) != 1 || len(candidates) != 1 {
			reason = fmt.Sprintf("%d orders vs %d subscriptions", len(group), len(candidates))
		} else {
			if d := candidates[0].StartTime - group[0].CompleteTime; d > windowSeconds || d < -windowSeconds {
				reason = fmt.Sprintf("order %d and subscription %d are %ds apart", group[0].Id, candidates[0].Id, d)
			}
		}
		for i, o := range group {
			d := BackfillDecision{OrderId: o.Id, TradeNo: o.TradeNo, UserId: o.UserId, PlanId: o.PlanId}
			if reason != "" {
				d.Result, d.Reason = LinkSourceUnlinkable, reason
			} else {
				d.Result, d.UserSubscriptionId = LinkSourceBackfill, candidates[i].Id
			}
			out = append(out, d)
		}
	}
	return out, nil
}

// ApplySubscriptionLinkBackfill writes the decisions from
// PlanSubscriptionLinkBackfill in one transaction. Rows already linked are
// left untouched; unique constraints reject any conflicting write.
func ApplySubscriptionLinkBackfill(decisions []BackfillDecision) error {
	now := common.GetTimestamp()
	return DB.Transaction(func(tx *gorm.DB) error {
		for _, d := range decisions {
			var order SubscriptionOrder
			if err := tx.Where("id = ?", d.OrderId).Take(&order).Error; err != nil {
				return err
			}
			var link ForkSubscriptionOrderLink
			err := tx.Where("order_id = ?", d.OrderId).Take(&link).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				link = ForkSubscriptionOrderLink{OrderId: order.Id, TradeNo: order.TradeNo, PlanId: order.PlanId,
					SnapshotSource: SnapshotSourceCurrentPlan, CreatedAt: now}
			} else if err != nil {
				return err
			} else if link.UserSubscriptionId != nil {
				continue
			}
			link.LinkSource, link.LinkedAt = d.Result, now
			if d.Result == LinkSourceBackfill {
				subID := d.UserSubscriptionId
				link.UserSubscriptionId = &subID
			}
			if err := tx.Save(&link).Error; err != nil {
				return fmt.Errorf("backfill order %d: %w", d.OrderId, err)
			}
		}
		return nil
	})
}
