package model

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrSubscriptionCycleExpired      = errors.New("subscription reservation cycle is no longer current")
	ErrSubscriptionQuotaInsufficient = errors.New("subscription quota insufficient")
	ErrNoActiveSubscription          = errors.New("no active subscription")
)

// A generation, rather than a timestamp, also distinguishes repeated manual
// resets in the same second and resets that keep their scheduled reset times.
func advanceSubscriptionQuotaCycle(sub *UserSubscription) error {
	if sub.QuotaCycle < 0 || sub.QuotaCycle == math.MaxInt64 {
		return errors.New("subscription quota cycle overflow")
	}
	sub.QuotaCycle++
	return nil
}

func subscriptionDBTimestampTx(tx *gorm.DB) (int64, error) {
	query := "SELECT UNIX_TIMESTAMP()"
	switch tx.Dialector.Name() {
	case "sqlite":
		query = "SELECT strftime('%s','now')"
	case "postgres":
		query = "SELECT EXTRACT(EPOCH FROM NOW())::bigint"
	}
	var now int64
	if err := tx.Raw(query).Scan(&now).Error; err != nil {
		return 0, err
	}
	if now <= 0 {
		return 0, errors.New("invalid subscription database time")
	}
	return now, nil
}

// PreConsumeUserSubscriptionTx joins the caller's transaction. Callers must
// roll back the entire transaction if any reservation, token or ledger step fails.
func PreConsumeUserSubscriptionTx(tx *gorm.DB, requestID string, userID int, modelName string, quotaType int, amount int64) (*SubscriptionPreConsumeResult, error) {
	if tx == nil || userID <= 0 || strings.TrimSpace(requestID) == "" || len(requestID) > 64 || amount <= 0 || amount > math.MaxInt32 {
		return nil, errors.New("invalid subscription reservation")
	}
	var existing SubscriptionPreConsumeRecord
	query := tx.Where("request_id = ?", requestID).Limit(1).Find(&existing)
	if query.Error != nil {
		return nil, query.Error
	}
	if query.RowsAffected > 0 {
		if existing.UserId != userID || existing.PreConsumed != amount {
			return nil, errors.New("subscription reservation identity mismatch")
		}
		sub, record, err := lockSubscriptionReservationTx(tx, requestID)
		if err != nil {
			return nil, err
		}
		if record.Status != "consumed" || record.PreConsumed != amount {
			return nil, errors.New("subscription reservation is no longer pending")
		}
		return &SubscriptionPreConsumeResult{UserSubscriptionId: sub.Id, PreConsumed: record.PreConsumed,
			AmountTotal: sub.AmountTotal, AmountUsedBefore: sub.AmountUsed, AmountUsedAfter: sub.AmountUsed}, nil
	}
	var subs []UserSubscription
	now, err := subscriptionDBTimestampTx(tx)
	if err != nil {
		return nil, err
	}
	if err := lockForUpdate(tx).Where("user_id = ? AND status = ? AND end_time > ?", userID, "active", now).
		Order("end_time asc, id asc").Find(&subs).Error; err != nil {
		return nil, err
	}
	if len(subs) == 0 {
		return nil, ErrNoActiveSubscription
	}
	// A concurrent identical reservation may have committed while we waited for
	// the subscription lock. Check before quota capacity, which now includes it.
	query = lockForUpdate(tx).Where("request_id = ?", requestID).Limit(1).Find(&existing)
	if query.Error != nil {
		return nil, query.Error
	}
	if query.RowsAffected > 0 {
		if existing.UserId != userID || existing.PreConsumed != amount || existing.Status != "consumed" {
			return nil, errors.New("subscription reservation identity mismatch")
		}
		for _, sub := range subs {
			if sub.Id == existing.UserSubscriptionId {
				return &SubscriptionPreConsumeResult{UserSubscriptionId: sub.Id, PreConsumed: existing.PreConsumed,
					AmountTotal: sub.AmountTotal, AmountUsedBefore: sub.AmountUsed, AmountUsedAfter: sub.AmountUsed}, nil
			}
		}
		return nil, errors.New("subscription reservation is no longer active")
	}
	for i := range subs {
		sub := &subs[i]
		plan, err := getSubscriptionPlanByIdTx(tx, sub.PlanId)
		if err != nil {
			return nil, err
		}
		if err := maybeResetUserSubscriptionWithPlanTx(tx, sub, plan, now); err != nil {
			return nil, err
		}
		if sub.AmountUsed < 0 || sub.AmountUsed > math.MaxInt64-amount || (sub.AmountTotal > 0 && sub.AmountTotal-sub.AmountUsed < amount) {
			continue
		}
		cycle := sub.QuotaCycle
		record := &SubscriptionPreConsumeRecord{RequestId: requestID, UserId: userID, UserSubscriptionId: sub.Id,
			PreConsumed: amount, QuotaCycle: &cycle, CycleAmountTotal: sub.AmountTotal, Status: "consumed"}
		insert := tx.Clauses(clause.OnConflict{DoNothing: true})
		if tx.Dialector.Name() == "mysql" {
			// ON DUPLICATE KEY UPDATE reports 1 for an unchanged row with
			// CLIENT_FOUND_ROWS. IGNORE preserves a reliable inserted-row count.
			// All bounded string/numeric inputs are validated before this insert.
			insert = tx.Clauses(clause.Insert{Modifier: "IGNORE"})
		}
		created := insert.Create(record)
		if created.Error != nil {
			return nil, created.Error
		}
		if created.RowsAffected == 0 {
			if err := lockForUpdate(tx).Where("request_id = ?", requestID).First(&existing).Error; err != nil {
				return nil, err
			}
			if existing.UserId != userID || existing.UserSubscriptionId != sub.Id || existing.PreConsumed != amount || existing.Status != "consumed" {
				return nil, errors.New("subscription reservation identity mismatch")
			}
			return &SubscriptionPreConsumeResult{UserSubscriptionId: sub.Id, PreConsumed: existing.PreConsumed,
				AmountTotal: sub.AmountTotal, AmountUsedBefore: sub.AmountUsed, AmountUsedAfter: sub.AmountUsed}, nil
		}
		before := sub.AmountUsed
		sub.AmountUsed += amount
		if err := tx.Save(sub).Error; err != nil {
			return nil, err
		}
		return &SubscriptionPreConsumeResult{UserSubscriptionId: sub.Id, PreConsumed: amount,
			AmountTotal: sub.AmountTotal, AmountUsedBefore: before, AmountUsedAfter: sub.AmountUsed}, nil
	}
	return nil, fmt.Errorf("%w, need=%d", ErrSubscriptionQuotaInsufficient, amount)
}

// All reservation mutations lock the subscription before its record, matching
// initial reservation and reset, and never start an independent transaction.
func lockSubscriptionReservationTx(tx *gorm.DB, requestID string) (*UserSubscription, *SubscriptionPreConsumeRecord, error) {
	if tx == nil || strings.TrimSpace(requestID) == "" {
		return nil, nil, errors.New("invalid subscription reservation request")
	}
	var record SubscriptionPreConsumeRecord
	if err := tx.Where("request_id = ?", requestID).First(&record).Error; err != nil {
		return nil, nil, err
	}
	var sub UserSubscription
	if err := lockForUpdate(tx).Where("id = ?", record.UserSubscriptionId).First(&sub).Error; err != nil {
		return nil, nil, err
	}
	if err := lockForUpdate(tx).Where("id = ?", record.Id).First(&record).Error; err != nil {
		return nil, nil, err
	}
	if sub.UserId != record.UserId {
		return nil, nil, errors.New("subscription reservation owner mismatch")
	}
	plan, err := getSubscriptionPlanByIdTx(tx, sub.PlanId)
	if err != nil {
		return nil, nil, err
	}
	now, err := subscriptionDBTimestampTx(tx)
	if err != nil {
		return nil, nil, err
	}
	if err := maybeResetUserSubscriptionWithPlanTx(tx, &sub, plan, now); err != nil {
		return nil, nil, err
	}
	return &sub, &record, nil
}

// ResizeSubscriptionPreConsumeTx sets the total reservation, not a delta, so a
// retry is idempotent. Additional upstream work cannot consume a new cycle for
// an old request. Legacy records have no provable cycle and cannot be resized.
func ResizeSubscriptionPreConsumeTx(tx *gorm.DB, requestID string, totalReserved int64) error {
	if totalReserved < 0 || totalReserved > math.MaxInt32 {
		return errors.New("invalid subscription reservation amount")
	}
	sub, record, err := lockSubscriptionReservationTx(tx, requestID)
	if err != nil {
		return err
	}
	if record.Status != "consumed" {
		return errors.New("subscription reservation is no longer pending")
	}
	if record.QuotaCycle == nil || *record.QuotaCycle != sub.QuotaCycle {
		return ErrSubscriptionCycleExpired
	}
	if err := adjustSubscriptionReservationTx(tx, sub, record, totalReserved); err != nil {
		return err
	}
	record.PreConsumed = totalReserved
	return tx.Save(record).Error
}

// ReconcileSubscriptionPreConsumeTx records a known upstream task acceptance.
// Unlike a pre-submit resize, this is already incurred liability: if the cycle
// closed while the provider answered, retain it on the original request only.
// The reservation stays pending for the task's later terminal settlement.
func ReconcileSubscriptionPreConsumeTx(tx *gorm.DB, requestID string, total int64) error {
	if total < 0 || total > math.MaxInt32 {
		return errors.New("invalid subscription acceptance amount")
	}
	sub, record, err := lockSubscriptionReservationTx(tx, requestID)
	if err != nil {
		return err
	}
	if record.Status != "consumed" {
		return errors.New("subscription reservation is no longer pending")
	}
	if record.QuotaCycle == nil {
		return ErrSubscriptionCycleExpired
	}
	if record.CycleAmountTotal > 0 && total > record.CycleAmountTotal {
		return ErrSubscriptionQuotaInsufficient
	}
	if *record.QuotaCycle == sub.QuotaCycle {
		if err := adjustSubscriptionReservationTx(tx, sub, record, total); err != nil {
			return err
		}
	}
	record.PreConsumed = total
	return tx.Save(record).Error
}

// SettleSubscriptionPreConsumeTx records the final charge once. A closed or
// unknown cycle is recorded on the request but must not debit/credit the current
// cycle; the caller still settles its token and durable ledger in this transaction.
func SettleSubscriptionPreConsumeTx(tx *gorm.DB, requestID string, finalAmount int64) error {
	if finalAmount < 0 || finalAmount > math.MaxInt32 {
		return errors.New("invalid subscription settlement amount")
	}
	sub, record, err := lockSubscriptionReservationTx(tx, requestID)
	if err != nil {
		return err
	}
	if record.Status == "settled" && record.SettledAmount != nil && *record.SettledAmount == finalAmount {
		return nil
	}
	if record.Status != "consumed" {
		return errors.New("subscription reservation already finalized")
	}
	if record.QuotaCycle != nil && record.CycleAmountTotal > 0 && finalAmount > record.CycleAmountTotal {
		return ErrSubscriptionQuotaInsufficient
	}
	if record.QuotaCycle != nil && *record.QuotaCycle == sub.QuotaCycle {
		if err := adjustSubscriptionReservationTx(tx, sub, record, finalAmount); err != nil {
			return err
		}
	}
	record.Status, record.SettledAmount = "settled", &finalAmount
	return tx.Save(record).Error
}

func RefundSubscriptionPreConsumeTx(tx *gorm.DB, requestID string) error {
	sub, record, err := lockSubscriptionReservationTx(tx, requestID)
	if err != nil {
		return err
	}
	if record.Status == "refunded" {
		return nil
	}
	if record.Status != "consumed" {
		return errors.New("subscription reservation already finalized")
	}
	if record.QuotaCycle != nil && *record.QuotaCycle == sub.QuotaCycle {
		if err := adjustSubscriptionReservationTx(tx, sub, record, 0); err != nil {
			return err
		}
	}
	zero := int64(0)
	record.Status, record.SettledAmount = "refunded", &zero
	return tx.Save(record).Error
}

func adjustSubscriptionReservationTx(tx *gorm.DB, sub *UserSubscription, record *SubscriptionPreConsumeRecord, amount int64) error {
	if record.PreConsumed < 0 || sub.AmountUsed < record.PreConsumed {
		return errors.New("inconsistent subscription reservation balance")
	}
	withoutReservation := sub.AmountUsed - record.PreConsumed
	if amount > math.MaxInt64-withoutReservation || (sub.AmountTotal > 0 && amount > sub.AmountTotal-withoutReservation) {
		return ErrSubscriptionQuotaInsufficient
	}
	sub.AmountUsed = withoutReservation + amount
	return tx.Save(sub).Error
}
