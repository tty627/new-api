package model

import (
	"context"
	"fmt"
	"github.com/QuantumNous/new-api/common"
	"gorm.io/gorm"
)

// RecordLegacyMidjourneyBilling exposes an unproven historical marker in the
// same operator list without presenting that marker as a successful debit.
func RecordLegacyMidjourneyBilling(ctx context.Context, taskID int) error {
	if taskID <= 0 {
		return ErrBillingOutcomeUnknown
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row := ForkBillingSession{RequestID: fmt.Sprintf("legacy-mj:%d", taskID), Status: BillingLegacyUnknown, Revision: 1,
			LastErrorCode: "legacy_charge_unproven", CreatedAt: common.GetTimestamp(), UpdatedAt: common.GetTimestamp()}
		insert := insertBillingSessionTx(tx, &row)
		if insert.Error != nil {
			return insert.Error
		}
		if insert.RowsAffected == 0 {
			return nil
		}
		var task Midjourney
		if err := lockForUpdate(tx).First(&task, taskID).Error; err != nil {
			return err
		}
		if task.BillingRequestID != "" {
			return ErrBillingIntentConflict
		}
		row.UserID, row.TokenID, row.MidjourneyID, row.FundingSource = task.UserId, task.TokenId, task.Id, "wallet"
		row.Playground = task.TokenId == 0
		if err := tx.Save(&row).Error; err != nil {
			return err
		}
		return billingEventTx(tx, &row, "legacy_charge_unproven", row.LastErrorCode)
	})
}

// CreateMidjourneyBillingResult stores the upstream observation and its money
// disposition together. Code 21 is still a separately billed inbound request;
// upstream MjId is deliberately not the billing idempotency key.
func CreateMidjourneyBillingResult(ctx context.Context, requestID string, task *Midjourney, actual int, intent string) error {
	if err := validateBillingQuota(actual); err != nil {
		return err
	}
	if task == nil || task.Id != 0 || (intent != "" && intent != BillingTaskAcceptPending && intent != BillingSettlePending && intent != BillingRefundPending) {
		return ErrBillingIntentConflict
	}
	if intent == BillingRefundPending && actual != 0 {
		return ErrBillingIntentConflict
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row ForkBillingSession
		if err := lockForUpdate(tx).Where("request_id = ?", requestID).First(&row).Error; err != nil {
			return err
		}
		if row.Status != BillingReserved || row.TaskID != 0 || row.MidjourneyID != 0 || row.FundingSource != "wallet" ||
			task.UserId != row.UserID || task.TokenId != row.TokenID || task.BillingRequestID != requestID {
			return ErrBillingIntentConflict
		}
		if err := tx.Create(task).Error; err != nil {
			return err
		}
		row.MidjourneyID = task.Id
		if intent != "" {
			return persistBillingIntentTx(tx, &row, intent, actual)
		}
		row.LastErrorCode, row.UpdatedAt = "upstream_outcome_unknown", common.GetTimestamp()
		row.Revision++
		if err := tx.Save(&row).Error; err != nil {
			return err
		}
		return billingEventTx(tx, &row, "midjourney_outcome_unknown", row.LastErrorCode)
	})
}

// CommitMidjourneyBillingResult commits the provider terminal state with its
// exact known intention. A local timeout is represented by known=false.
func CommitMidjourneyBillingResult(ctx context.Context, task *Midjourney, fromStatus string, known bool) (bool, error) {
	if task == nil || task.Id <= 0 || task.BillingRequestID == "" {
		return false, ErrBillingOutcomeUnknown
	}
	won := false
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row ForkBillingSession
		if err := lockForUpdate(tx).Where("request_id = ?", task.BillingRequestID).First(&row).Error; err != nil {
			return err
		}
		var stored Midjourney
		if err := lockForUpdate(tx).First(&stored, task.Id).Error; err != nil {
			return err
		}
		if stored.Status != fromStatus || stored.Progress == "100%" {
			return nil
		}
		if row.MidjourneyID != task.Id || row.TaskID != 0 || row.UserID != stored.UserId || row.TokenID != stored.TokenId || stored.BillingRequestID != row.RequestID {
			return ErrBillingIntentConflict
		}
		task.Quota, task.TokenId, task.BillingChannelId = stored.Quota, stored.TokenId, stored.BillingChannelId
		if known {
			intent, actual := BillingSettlePending, int(row.ReservedQuota)
			if task.Status == "FAILURE" {
				intent, actual = BillingRefundPending, 0
			}
			if err := persistBillingIntentTx(tx, &row, intent, actual); err != nil {
				return err
			}
		} else {
			if row.Status != BillingReserved && row.Status != BillingTaskActive {
				return ErrBillingIntentConflict
			}
			row.LastErrorCode, row.UpdatedAt = "upstream_outcome_unknown", common.GetTimestamp()
			row.Revision++
			if err := tx.Save(&row).Error; err != nil {
				return err
			}
			if err := billingEventTx(tx, &row, "midjourney_outcome_unknown", row.LastErrorCode); err != nil {
				return err
			}
		}
		if err := tx.Model(&Midjourney{}).Where("id = ? AND status = ?", task.Id, fromStatus).Select("*").Updates(task).Error; err != nil {
			return err
		}
		won = true
		return nil
	})
	return won, err
}
