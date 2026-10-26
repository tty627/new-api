package model

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/QuantumNous/new-api/common"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	BillingReserved          = "reserved"
	BillingSettlePending     = "settle_pending"
	BillingRefundPending     = "refund_pending"
	BillingSettled           = "settled"
	BillingRefunded          = "refunded"
	BillingTaskAcceptPending = "task_accept_pending"
	BillingTaskActive        = "task_active"
)

var (
	ErrBillingOutcomeUnknown     = errors.New("billing upstream outcome is unknown; manual verification required")
	ErrBillingIntentConflict     = errors.New("billing intent conflicts with the durable session")
	ErrBillingWalletInsufficient = errors.New("wallet quota insufficient")
	ErrBillingTokenInsufficient  = errors.New("token quota insufficient")
	ErrBillingBatchUnsupported   = errors.New("durable billing requires batch updates disabled")
)

// ForkBillingSession is the recovery ledger in the SAME database as funds.
// A reserved row is deliberately not refundable by elapsed time: the upstream
// may have performed billable work before the process lost its response.
type ForkBillingSession struct {
	RequestID               string `json:"request_id" gorm:"primaryKey;type:varchar(64)"`
	UserID                  int    `json:"user_id" gorm:"index;not null"`
	TokenID                 int    `json:"token_id" gorm:"index;not null"`
	Playground              bool   `json:"playground" gorm:"not null"`
	FundingSource           string `json:"funding_source" gorm:"type:varchar(16);not null"`
	ModelName               string `json:"model_name" gorm:"type:varchar(255)"`
	SubscriptionID          int    `json:"subscription_id"`
	TaskID                  int64  `json:"task_id" gorm:"type:bigint;index"`
	TaskAccountedQuota      int64  `json:"task_accounted_quota" gorm:"type:bigint"`
	TaskAccepted            bool   `json:"task_accepted"`
	QuotaSaturation         string `json:"quota_saturation,omitempty" gorm:"type:text"`
	SubscriptionAmountTotal int64  `json:"subscription_amount_total" gorm:"type:bigint"`
	SubscriptionUsedAfter   int64  `json:"subscription_used_after" gorm:"type:bigint"`
	ReservedQuota           int64  `json:"reserved_quota" gorm:"type:bigint;not null"`
	ActualQuota             *int64 `json:"actual_quota" gorm:"type:bigint"`
	Status                  string `json:"status" gorm:"type:varchar(32);not null;index:idx_fork_billing_recovery,priority:1"`
	Attempts                int    `json:"attempts"`
	NextRetryAt             int64  `json:"next_retry_at" gorm:"type:bigint;index:idx_fork_billing_recovery,priority:2"`
	LastErrorCode           string `json:"last_error_code" gorm:"type:varchar(64)"`
	CachePending            bool   `json:"cache_pending" gorm:"not null"`
	Revision                int64  `json:"revision" gorm:"type:bigint;not null"`
	CreatedAt               int64  `json:"created_at" gorm:"type:bigint;not null"`
	UpdatedAt               int64  `json:"updated_at" gorm:"type:bigint;not null"`
}

// ForkBillingEvent is append-only. No token key, prompt, response or database
// error text belongs in the ledger or its audit events.
type ForkBillingEvent struct {
	ID              int64  `json:"id" gorm:"primaryKey"`
	RequestID       string `json:"request_id" gorm:"type:varchar(64);index;not null"`
	Action          string `json:"action" gorm:"type:varchar(32);not null"`
	ReservedQuota   int64  `json:"reserved_quota" gorm:"type:bigint;not null"`
	ActualQuota     *int64 `json:"actual_quota" gorm:"type:bigint"`
	ErrorCode       string `json:"error_code" gorm:"type:varchar(64)"`
	Revision        int64  `json:"revision" gorm:"type:bigint;not null"`
	QuotaSaturation string `json:"quota_saturation,omitempty" gorm:"type:text"`
	CreatedAt       int64  `json:"created_at" gorm:"type:bigint;not null"`
}

type BillingReservation struct {
	RequestID     string
	UserID        int
	TokenID       int
	Playground    bool
	FundingSource string
	ModelName     string
	Quota         int
}

func validateBillingQuota(quota int) error {
	if quota < 0 || quota > common.MaxQuota {
		return errors.New("billing quota outside the single-request boundary")
	}
	if common.BatchUpdateEnabled {
		return ErrBillingBatchUnsupported
	}
	return nil
}

func billingEventTx(tx *gorm.DB, row *ForkBillingSession, action, code string) error {
	return tx.Create(&ForkBillingEvent{RequestID: row.RequestID, Action: action,
		ReservedQuota: row.ReservedQuota, ActualQuota: row.ActualQuota,
		ErrorCode: code, Revision: row.Revision, QuotaSaturation: row.QuotaSaturation, CreatedAt: common.GetTimestamp()}).Error
}

func lockBillingAccounts(tx *gorm.DB, row *ForkBillingSession, final bool) (*User, *Token, error) {
	var user User
	query := func() *gorm.DB {
		q := tx.Session(&gorm.Session{NewDB: true})
		if final {
			q = q.Unscoped()
		}
		return q
	}
	if err := lockForUpdate(query()).First(&user, row.UserID).Error; err != nil {
		return nil, nil, err
	}
	if row.Playground {
		return &user, nil, nil
	}
	var token Token
	if err := lockForUpdate(query()).Where("id = ? AND user_id = ?", row.TokenID, row.UserID).First(&token).Error; err != nil {
		return nil, nil, err
	}
	return &user, &token, nil
}

func applyBillingWalletTx(tx *gorm.DB, user *User, delta int64, requireAvailable bool) error {
	if delta == 0 {
		return nil
	}
	if requireAvailable && delta > 0 && int64(user.Quota) < delta {
		return ErrBillingWalletInsufficient
	}
	after, err := common.WalletQuotaFromDecimalStrict(decimal.NewFromInt(int64(user.Quota)).Sub(decimal.NewFromInt(delta)))
	if err != nil {
		return err
	}
	result := tx.Unscoped().Model(&User{}).Where("id = ? AND quota = ?", user.Id, user.Quota).Update("quota", after)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errors.New("wallet changed during billing transaction")
	}
	return nil
}

func applyBillingTokenTx(tx *gorm.DB, token *Token, delta int64, requireAvailable bool) error {
	if token == nil || delta == 0 {
		return nil
	}
	if requireAvailable && delta > 0 && !token.UnlimitedQuota && int64(token.RemainQuota) < delta {
		return ErrBillingTokenInsufficient
	}
	remaining, err := common.WalletQuotaFromDecimalStrict(decimal.NewFromInt(int64(token.RemainQuota)).Sub(decimal.NewFromInt(delta)))
	if err != nil {
		return err
	}
	used, err := common.WalletQuotaFromDecimalStrict(decimal.NewFromInt(int64(token.UsedQuota)).Add(decimal.NewFromInt(delta)))
	if err != nil || used < 0 {
		return errors.New("token billing total outside bounds")
	}
	result := tx.Unscoped().Model(&Token{}).Where("id = ? AND remain_quota = ? AND used_quota = ?", token.Id, token.RemainQuota, token.UsedQuota).
		Updates(map[string]any{"remain_quota": remaining, "used_quota": used, "accessed_time": common.GetTimestamp()})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return errors.New("token changed during billing transaction")
	}
	return nil
}

// ReserveBillingSession commits the ledger, funding and token reservation as one
// unit. Reusing an identity never charges twice; callers must not re-submit an
// upstream operation when created=false (its outcome may already be unknown).
func ReserveBillingSession(ctx context.Context, input BillingReservation) (row *ForkBillingSession, created bool, err error) {
	if err = validateBillingQuota(input.Quota); err != nil {
		return nil, false, err
	}
	if strings.TrimSpace(input.RequestID) == "" || len(input.RequestID) > 64 || utf8.RuneCountInString(input.ModelName) > 255 || input.UserID <= 0 ||
		(!input.Playground && input.TokenID <= 0) || (input.FundingSource != "wallet" && input.FundingSource != "subscription") {
		return nil, false, errors.New("invalid durable billing reservation")
	}
	now := common.GetTimestamp()
	row = &ForkBillingSession{RequestID: input.RequestID, UserID: input.UserID, TokenID: input.TokenID,
		Playground: input.Playground, FundingSource: input.FundingSource, ModelName: input.ModelName,
		ReservedQuota: int64(input.Quota), Status: BillingReserved, CachePending: true, Revision: 1, CreatedAt: now, UpdatedAt: now}
	err = DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		insert := insertBillingSessionTx(tx, row)
		if insert.Error != nil {
			return insert.Error
		}
		if insert.RowsAffected == 0 {
			if err := lockForUpdate(tx).Where("request_id = ?", input.RequestID).First(row).Error; err != nil {
				return err
			}
			if row.UserID != input.UserID || row.TokenID != input.TokenID || row.Playground != input.Playground ||
				row.FundingSource != input.FundingSource || row.ModelName != input.ModelName || row.ReservedQuota != int64(input.Quota) {
				return ErrBillingIntentConflict
			}
			return nil
		}
		user, token, err := lockBillingAccounts(tx, row, false)
		if err != nil {
			return err
		}
		if row.FundingSource == "wallet" {
			if err := applyBillingWalletTx(tx, user, row.ReservedQuota, true); err != nil {
				return err
			}
		} else {
			result, err := PreConsumeUserSubscriptionTx(tx, row.RequestID, row.UserID, row.ModelName, 0, row.ReservedQuota)
			if err != nil {
				return err
			}
			row.SubscriptionID, row.SubscriptionAmountTotal, row.SubscriptionUsedAfter = result.UserSubscriptionId, result.AmountTotal, result.AmountUsedAfter
		}
		if err := applyBillingTokenTx(tx, token, row.ReservedQuota, true); err != nil {
			return err
		}
		if err := tx.Save(row).Error; err != nil {
			return err
		}
		if err := billingEventTx(tx, row, "reserved", ""); err != nil {
			return err
		}
		created = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	RefreshBillingQuotaCaches(ctx, row)
	return row, created, nil
}

func insertBillingSessionTx(tx *gorm.DB, row *ForkBillingSession) *gorm.DB {
	if tx.Dialector.Name() == "mysql" {
		return tx.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(row)
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(row)
}

func GetBillingSession(ctx context.Context, requestID string) (*ForkBillingSession, error) {
	var row ForkBillingSession
	err := DB.WithContext(ctx).Where("request_id = ?", requestID).First(&row).Error
	return &row, err
}

// AttachBillingTask binds only a persisted task with matching request provenance.
// It does not move funds; a crash here leaves the original reservation visible.
func AttachBillingTask(ctx context.Context, requestID string, taskID int64) error {
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row ForkBillingSession
		if err := lockForUpdate(tx).Where("request_id = ?", requestID).First(&row).Error; err != nil {
			return err
		}
		var task Task
		if err := lockForUpdate(tx).First(&task, taskID).Error; err != nil {
			return err
		}
		if task.PrivateData.Execution == nil || task.PrivateData.Execution.RequestID != requestID ||
			task.UserId != row.UserID || task.PrivateData.TokenId != row.TokenID ||
			(task.PrivateData.BillingSource != "" && task.PrivateData.BillingSource != row.FundingSource) ||
			(task.PrivateData.SubscriptionId != row.SubscriptionID) || (row.TaskID != 0 && row.TaskID != taskID) {
			return ErrBillingIntentConflict
		}
		if row.TaskID == taskID {
			return nil
		}
		if row.Status != BillingReserved {
			return ErrBillingIntentConflict
		}
		row.TaskID, row.UpdatedAt = taskID, common.GetTimestamp()
		row.Revision++
		if err := tx.Save(&row).Error; err != nil {
			return err
		}
		return billingEventTx(tx, &row, "task_attached", "")
	})
}

// CreateAcceptedBillingTask joins task insertion, immutable request binding and
// accepted/terminal intention. A crash cannot leave a visible task unlinked.
func CreateAcceptedBillingTask(ctx context.Context, requestID string, task *Task, quota int, clamp *common.QuotaClamp, omitColumns ...string) error {
	if err := validateBillingQuota(quota); err != nil {
		return err
	}
	if task == nil || task.ID != 0 {
		return ErrBillingIntentConflict
	}
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row ForkBillingSession
		if err := lockForUpdate(tx).Where("request_id = ?", requestID).First(&row).Error; err != nil {
			return err
		}
		if row.TaskID != 0 || row.Status != BillingReserved || task.PrivateData.Execution == nil ||
			task.PrivateData.Execution.RequestID != requestID || task.UserId != row.UserID ||
			task.PrivateData.TokenId != row.TokenID || task.PrivateData.SubscriptionId != row.SubscriptionID ||
			(task.PrivateData.BillingSource != "" && task.PrivateData.BillingSource != row.FundingSource) {
			return ErrBillingIntentConflict
		}
		if err := tx.Omit(omitColumns...).Create(task).Error; err != nil {
			return err
		}
		row.TaskID = task.ID
		if err := setBillingClamp(&row, clamp); err != nil {
			return err
		}
		intent := BillingTaskAcceptPending
		if task.Status == TaskStatusSuccess || task.Status == TaskStatusFailure {
			intent = BillingSettlePending
		}
		return persistBillingIntentTx(tx, &row, intent, quota)
	})
}

func setBillingClamp(row *ForkBillingSession, clamp *common.QuotaClamp) error {
	if clamp == nil {
		return nil
	}
	audit := clamp.AuditMap()
	if math.IsNaN(clamp.Original) || math.IsInf(clamp.Original, 0) {
		audit["original"] = fmt.Sprint(clamp.Original)
	}
	encoded, err := common.Marshal(audit)
	if err == nil {
		row.QuotaSaturation = string(encoded)
	}
	return err
}

// ImportLegacyWalletTask records the already persisted wallet task liability.
// An old subscription has no provable cycle and is intentionally not imported.
func ImportLegacyWalletTask(ctx context.Context, taskID int64) (string, error) {
	requestID := fmt.Sprintf("legacy-task:%d", taskID)
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row := ForkBillingSession{RequestID: requestID, Status: BillingTaskActive, Revision: 1,
			CreatedAt: common.GetTimestamp(), UpdatedAt: common.GetTimestamp()}
		insert := insertBillingSessionTx(tx, &row)
		if insert.Error != nil {
			return insert.Error
		}
		if insert.RowsAffected == 0 {
			return nil
		}
		var task Task
		if err := lockForUpdate(tx).First(&task, taskID).Error; err != nil {
			return err
		}
		if task.PrivateData.Execution != nil && task.PrivateData.Execution.RequestID != "" {
			return ErrBillingOutcomeUnknown
		}
		if task.PrivateData.BillingSource == "subscription" || task.PrivateData.SubscriptionId != 0 {
			return ErrBillingOutcomeUnknown
		}
		if err := validateBillingQuota(task.Quota); err != nil {
			return err
		}
		row.UserID, row.TokenID, row.Playground = task.UserId, task.PrivateData.TokenId, task.PrivateData.TokenId == 0
		row.FundingSource, row.TaskID, row.TaskAccepted = "wallet", taskID, true
		row.ReservedQuota, row.TaskAccountedQuota = int64(task.Quota), int64(task.Quota)
		if err := tx.Save(&row).Error; err != nil {
			return err
		}
		return billingEventTx(tx, &row, "legacy_task_imported", "")
	})
	return requestID, err
}

// ResizeBillingSession uses an absolute reservation target, so a repeated or
// ambiguous-commit caller cannot add the same extra reservation twice.
func ResizeBillingSession(ctx context.Context, requestID string, target int, requireAvailable bool) (*ForkBillingSession, error) {
	if err := validateBillingQuota(target); err != nil {
		return nil, err
	}
	var row ForkBillingSession
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockForUpdate(tx).Where("request_id = ?", requestID).First(&row).Error; err != nil {
			return err
		}
		if row.Status != BillingReserved {
			return ErrBillingIntentConflict
		}
		if int64(target) <= row.ReservedQuota {
			return nil
		}
		delta := int64(target) - row.ReservedQuota
		user, token, err := lockBillingAccounts(tx, &row, false)
		if err != nil {
			return err
		}
		if row.FundingSource == "wallet" {
			if err := applyBillingWalletTx(tx, user, delta, requireAvailable); err != nil {
				return err
			}
		} else if err := ResizeSubscriptionPreConsumeTx(tx, requestID, int64(target)); err != nil {
			return err
		}
		if err := applyBillingTokenTx(tx, token, delta, true); err != nil {
			return err
		}
		row.ReservedQuota, row.CachePending, row.UpdatedAt = int64(target), true, common.GetTimestamp()
		row.Revision++
		if row.FundingSource == "subscription" {
			row.SubscriptionUsedAfter += delta
		}
		if err := tx.Save(&row).Error; err != nil {
			return err
		}
		return billingEventTx(tx, &row, "reserved_more", "")
	})
	if err != nil {
		return nil, err
	}
	RefreshBillingQuotaCaches(ctx, &row)
	return &row, nil
}

// PersistBillingIntent is a separate commit BEFORE applying any final delta.
// A failed apply can then be retried after process death without guessing the
// upstream outcome. The first known intent wins; conflicting intents fail.
func PersistBillingIntent(ctx context.Context, requestID, intent string, actual int) error {
	if err := validateBillingQuota(actual); err != nil {
		return err
	}
	if intent != BillingSettlePending && intent != BillingRefundPending && intent != BillingTaskAcceptPending {
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
		return persistBillingIntentTx(tx, &row, intent, actual)
	})
}

func persistBillingIntentTx(tx *gorm.DB, row *ForkBillingSession, intent string, actual int) error {
	if intent == BillingTaskAcceptPending && row.Status == BillingTaskActive && row.ActualQuota != nil && *row.ActualQuota == int64(actual) {
		return nil
	}
	if row.LastErrorCode == "upstream_outcome_unknown" {
		return ErrBillingOutcomeUnknown
	}
	if row.Status != BillingReserved && row.Status != BillingTaskActive {
		expected := BillingSettled
		if intent == BillingRefundPending {
			expected = BillingRefunded
		}
		if (row.Status == intent || row.Status == expected) && row.ActualQuota != nil && *row.ActualQuota == int64(actual) {
			return nil
		}
		return ErrBillingIntentConflict
	}
	value := int64(actual)
	row.Status, row.ActualQuota, row.UpdatedAt = intent, &value, common.GetTimestamp()
	row.Revision++
	row.NextRetryAt, row.LastErrorCode = 0, ""
	if err := tx.Save(row).Error; err != nil {
		return err
	}
	return billingEventTx(tx, row, intent, "")
}

// CommitTaskBillingResult commits the observed task outcome and its known money
// intention together. Empty intent is explicit uncertainty, never a refund.
func CommitTaskBillingResult(ctx context.Context, requestID string, task *Task, fromStatus TaskStatus, intent string, actual int, clamps ...*common.QuotaClamp) (bool, error) {
	if task == nil || task.ID <= 0 || (intent != "" && intent != BillingSettlePending && intent != BillingRefundPending) {
		return false, ErrBillingIntentConflict
	}
	if err := validateBillingQuota(actual); err != nil {
		return false, err
	}
	if intent == BillingRefundPending && actual != 0 {
		return false, ErrBillingIntentConflict
	}
	won := false
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row ForkBillingSession
		if err := lockForUpdate(tx).Where("request_id = ?", requestID).First(&row).Error; err != nil {
			return err
		}
		var stored Task
		if err := lockForUpdate(tx).First(&stored, task.ID).Error; err != nil {
			return err
		}
		if stored.Status != fromStatus || stored.Status == TaskStatusSuccess || stored.Status == TaskStatusFailure {
			return nil
		}
		if row.TaskID != task.ID || stored.UserId != row.UserID || stored.PrivateData.TokenId != row.TokenID {
			return ErrBillingIntentConflict
		}
		// The polling snapshot can predate an acceptance recovery. Never put an
		// old quota back over the amount already atomically applied by that worker.
		task.Quota = stored.Quota
		for _, clamp := range clamps {
			if err := setBillingClamp(&row, clamp); err != nil {
				return err
			}
		}
		if intent == "" {
			if row.Status != BillingTaskActive && row.Status != BillingReserved {
				return ErrBillingIntentConflict
			}
			row.LastErrorCode, row.UpdatedAt = "upstream_outcome_unknown", common.GetTimestamp()
			row.Revision++
			if err := tx.Save(&row).Error; err != nil {
				return err
			}
			if err := billingEventTx(tx, &row, "task_outcome_unknown", row.LastErrorCode); err != nil {
				return err
			}
		} else if err := persistBillingIntentTx(tx, &row, intent, actual); err != nil {
			return err
		}
		result := tx.Model(&Task{}).Where("id = ? AND status = ?", task.ID, fromStatus).Select("*").Updates(task)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("task changed during billing transaction")
		}
		won = true
		return nil
	})
	return won, err
}

func ApplyBillingIntent(ctx context.Context, requestID string) error {
	_, err := ApplyBillingIntentOnce(ctx, requestID)
	return err
}

func ApplyBillingIntentOnce(ctx context.Context, requestID string) (bool, error) {
	if common.BatchUpdateEnabled {
		return false, ErrBillingBatchUnsupported
	}
	var row ForkBillingSession
	applied := false
	err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockForUpdate(tx).Where("request_id = ?", requestID).First(&row).Error; err != nil {
			return err
		}
		if row.Status == BillingSettled || row.Status == BillingRefunded {
			return nil
		}
		if row.Status != BillingSettlePending && row.Status != BillingRefundPending && row.Status != BillingTaskAcceptPending {
			return ErrBillingOutcomeUnknown
		}
		if row.ActualQuota == nil || *row.ActualQuota < 0 || *row.ActualQuota > common.MaxQuota {
			return ErrBillingIntentConflict
		}
		var task Task
		if row.TaskID > 0 {
			if err := lockForUpdate(tx).First(&task, row.TaskID).Error; err != nil {
				return err
			}
			if task.UserId != row.UserID || task.PrivateData.TokenId != row.TokenID {
				return ErrBillingIntentConflict
			}
		}
		user, token, err := lockBillingAccounts(tx, &row, true)
		if err != nil {
			return err
		}
		delta := *row.ActualQuota - row.ReservedQuota
		if row.FundingSource == "wallet" {
			if err := applyBillingWalletTx(tx, user, delta, false); err != nil {
				return err
			}
		} else if row.Status == BillingTaskAcceptPending {
			if err := ReconcileSubscriptionPreConsumeTx(tx, requestID, *row.ActualQuota); err != nil {
				return err
			}
		} else if row.Status == BillingRefundPending {
			if err := RefundSubscriptionPreConsumeTx(tx, requestID); err != nil {
				return err
			}
		} else if err := SettleSubscriptionPreConsumeTx(tx, requestID, *row.ActualQuota); err != nil {
			return err
		}
		if err := applyBillingTokenTx(tx, token, delta, false); err != nil {
			return err
		}
		if row.TaskID > 0 {
			if err := tx.Model(&Task{}).Where("id = ?", row.TaskID).Update("quota", *row.ActualQuota).Error; err != nil {
				return err
			}
			statDelta := *row.ActualQuota - row.TaskAccountedQuota
			stats := map[string]any{"used_quota": gorm.Expr("used_quota + ?", statDelta)}
			if !row.TaskAccepted {
				stats["request_count"] = gorm.Expr("request_count + 1")
			}
			{
				if err := tx.Unscoped().Model(&User{}).Where("id = ?", row.UserID).Updates(stats).Error; err != nil {
					return err
				}
				if err := tx.Model(&Channel{}).Where("id = ?", task.ChannelId).Update("used_quota", gorm.Expr("used_quota + ?", statDelta)).Error; err != nil {
					return err
				}
			}
			row.TaskAccountedQuota, row.TaskAccepted = *row.ActualQuota, true
		}
		if row.Status == BillingTaskAcceptPending {
			row.Status = BillingTaskActive
			row.TaskAccepted = true
			row.ReservedQuota, row.TaskAccountedQuota = *row.ActualQuota, *row.ActualQuota
		} else if row.Status == BillingRefundPending {
			row.Status = BillingRefunded
		} else {
			row.Status = BillingSettled
		}
		row.Revision++
		row.CachePending, row.UpdatedAt, row.NextRetryAt, row.LastErrorCode = true, common.GetTimestamp(), 0, ""
		if err := tx.Save(&row).Error; err != nil {
			return err
		}
		applied = true
		return billingEventTx(tx, &row, row.Status, "")
	})
	if err != nil {
		if !errors.Is(err, ErrBillingOutcomeUnknown) && !errors.Is(err, ErrBillingIntentConflict) {
			auditCtx, cancelAudit := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelAudit()
			_ = DB.WithContext(auditCtx).Transaction(func(tx *gorm.DB) error {
				var failed ForkBillingSession
				if e := lockForUpdate(tx).Where("request_id = ?", requestID).First(&failed).Error; e != nil {
					return e
				}
				if failed.Status != BillingSettlePending && failed.Status != BillingRefundPending && failed.Status != BillingTaskAcceptPending {
					return nil
				}
				failed.Attempts++
				failed.Revision++
				failed.LastErrorCode, failed.UpdatedAt = "apply_failed", common.GetTimestamp()
				failed.NextRetryAt = failed.UpdatedAt + int64(min(300, 1<<min(failed.Attempts, 8)))
				if e := tx.Save(&failed).Error; e != nil {
					return e
				}
				return billingEventTx(tx, &failed, "retry_failed", "apply_failed")
			})
		}
		return false, err
	}
	RefreshBillingQuotaCaches(ctx, &row)
	return applied, nil
}

// RefreshBillingQuotaCaches never replays a money delta into Redis or the batch
// queue. DB reservations remain authoritative even if cache invalidation fails.
// CachePending allows a later worker to repair a crash after commit.
func RefreshBillingQuotaCaches(ctx context.Context, row *ForkBillingSession) {
	if row == nil || !row.CachePending {
		return
	}
	if err := invalidateUserCache(row.UserID); err != nil {
		return
	}
	if !row.Playground {
		var token Token
		if err := DB.WithContext(ctx).Unscoped().Select("id", "key").First(&token, row.TokenID).Error; err != nil {
			return
		}
		if err := invalidateTokenCacheForMutation(token.Key); err != nil {
			return
		}
	}
	result := DB.WithContext(ctx).Model(&ForkBillingSession{}).Where("request_id = ? AND revision = ?", row.RequestID, row.Revision).Update("cache_pending", false)
	if result.Error == nil && result.RowsAffected == 1 {
		row.CachePending = false
	}
}

func ListUnresolvedBillingSessions(ctx context.Context, limit int) ([]ForkBillingSession, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	var rows []ForkBillingSession
	err := DB.WithContext(ctx).Where("status IN ? OR cache_pending = ?", []string{BillingReserved, BillingSettlePending, BillingRefundPending, BillingTaskAcceptPending, BillingTaskActive}, true).
		Order("created_at asc, request_id asc").Limit(limit).Find(&rows).Error
	return rows, err
}

func ListRecoverableBillingSessions(ctx context.Context, limit int) ([]ForkBillingSession, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	var rows []ForkBillingSession
	err := DB.WithContext(ctx).Where("(status IN ? AND next_retry_at <= ?) OR cache_pending = ?", []string{BillingSettlePending, BillingRefundPending, BillingTaskAcceptPending}, common.GetTimestamp(), true).
		Order("next_retry_at asc, request_id asc").Limit(limit).Find(&rows).Error
	return rows, err
}

func MarkBillingOutcomeUnknown(ctx context.Context, requestID string) error {
	return DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row ForkBillingSession
		if err := lockForUpdate(tx).Where("request_id = ?", requestID).First(&row).Error; err != nil {
			return err
		}
		if row.Status != BillingReserved && row.Status != BillingTaskActive {
			return ErrBillingIntentConflict
		}
		if row.LastErrorCode == "upstream_outcome_unknown" {
			return nil
		}
		row.LastErrorCode, row.UpdatedAt = "upstream_outcome_unknown", common.GetTimestamp()
		row.Revision++
		if err := tx.Save(&row).Error; err != nil {
			return err
		}
		return billingEventTx(tx, &row, "upstream_unknown", row.LastErrorCode)
	})
}

func (row ForkBillingSession) String() string {
	return fmt.Sprintf("billing request=%s status=%s funding=%s", row.RequestID, row.Status, row.FundingSource)
}
