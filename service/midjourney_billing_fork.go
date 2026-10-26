package service

import (
	"context"
	"errors"
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/gin-gonic/gin"
)

// Midjourney remains a wallet-only compatibility route. Its historical provider
// code 21 still charges each accepted inbound request, identified by RequestId.
func PreConsumeMidjourneyBilling(c *gin.Context, info *relaycommon.RelayInfo, quota int, charge bool) error {
	if info == nil || info.Billing != nil || info.BillingSource == BillingSourceSubscription {
		return model.ErrBillingIntentConflict
	}
	if !charge {
		quota = 0
	}
	if info.RequestId == "" {
		info.RequestId = common.NewRequestId()
	}
	ctx, cancel := billingOperationContext()
	defer cancel()
	row, created, err := model.ReserveBillingSession(ctx, model.BillingReservation{RequestID: info.RequestId, UserID: info.UserId, TokenID: info.TokenId,
		Playground: info.IsPlayground, FundingSource: BillingSourceWallet, ModelName: info.GetBillingModelName(), Quota: quota})
	if err != nil {
		return err
	}
	if !created {
		return model.ErrBillingOutcomeUnknown
	}
	s := &BillingSession{durableRequestID: info.RequestId, relayInfo: info, funding: &WalletFunding{userId: info.UserId}}
	s.syncDurableBilling(row)
	info.Billing = s
	return nil
}

func RecordMidjourneySubmitFailure(info *relaycommon.RelayInfo, description string) {
	s, ok := info.Billing.(*BillingSession)
	if !ok || s.durableRequestID == "" {
		return
	}
	ctx, cancel := billingOperationContext()
	defer cancel()
	// Only failures before the HTTP operation prove that nothing was submitted.
	local := description == "read_request_body_failed" || description == "marshal_request_body_failed" || description == "create_request_failed"
	if !local {
		_ = model.MarkBillingOutcomeUnknown(ctx, s.durableRequestID)
		return
	}
	if err := model.PersistBillingIntent(ctx, s.durableRequestID, model.BillingRefundPending, 0); err == nil {
		_ = model.ApplyBillingIntent(ctx, s.durableRequestID)
	}
}

func PersistMidjourneyBilling(info *relaycommon.RelayInfo, task *model.Midjourney, quota int, charge, accepted, known bool) (bool, error) {
	s, ok := info.Billing.(*BillingSession)
	if !ok || s.durableRequestID == "" || task == nil {
		return false, model.ErrBillingOutcomeUnknown
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fundingSettled = true
	task.TokenId, task.BillingRequestID = info.TokenId, s.durableRequestID
	task.BillingChannelId = task.ChannelId
	if info.ChannelMeta != nil && info.ChannelId > 0 {
		task.BillingChannelId = info.ChannelId
	}
	actual, intent := quota, model.BillingTaskAcceptPending
	if !charge {
		actual = 0
	}
	if !known {
		intent = ""
		task.Quota = s.preConsumedQuota
		task.Status, task.Progress = "FAILURE", "100%"
		task.FailReason = "Upstream outcome requires verification"
	} else {
		task.Quota = actual
		if !accepted || task.Status == "FAILURE" {
			intent, actual = model.BillingRefundPending, 0
			task.Status, task.Progress = "FAILURE", "100%"
		} else if task.Status == "SUCCESS" || task.Progress == "100%" {
			intent = model.BillingSettlePending
		}
	}
	ctx, cancel := billingOperationContext()
	defer cancel()
	if err := model.CreateMidjourneyBillingResult(ctx, s.durableRequestID, task, actual, intent); err != nil {
		return false, err
	}
	if intent == "" {
		return false, model.ErrBillingOutcomeUnknown
	}
	applied, err := model.ApplyBillingIntentOnce(ctx, s.durableRequestID)
	if err == nil {
		task.Quota = actual
	}
	return applied && charge && accepted && actual > 0, err
}

func CommitMidjourneyOutcome(ctx context.Context, task *model.Midjourney, previous string, known bool) (bool, error) {
	if task.BillingRequestID == "" {
		// Old quota markers do not prove whether either historical debit committed.
		// Persist the observation, keep money unchanged and expose the ambiguity.
		if task.FailReason == "" {
			task.FailReason = "Legacy billing requires verification"
		}
		if err := model.RecordLegacyMidjourneyBilling(ctx, task.Id); err != nil {
			return false, err
		}
		return task.UpdateWithStatus(previous)
	}
	won, err := model.CommitMidjourneyBillingResult(ctx, task, previous, known)
	if err != nil || !won || !known {
		return won, err
	}
	if task.Status == "FAILURE" {
		if !RefundMidjourneyQuota(ctx, task, "Composition failed") {
			return true, errors.New("Midjourney refund remains pending")
		}
		return true, nil
	}
	err = model.ApplyBillingIntent(ctx, task.BillingRequestID)
	return true, err
}

func refundMidjourneyDurable(ctx context.Context, task *model.Midjourney, reason string) bool {
	if task == nil || task.Id <= 0 {
		return false
	}
	if task.BillingRequestID == "" {
		_ = model.RecordLegacyMidjourneyBilling(ctx, task.Id)
		return false
	}
	row, err := model.GetBillingSession(ctx, task.BillingRequestID)
	if err != nil || row.MidjourneyID != task.Id || row.UserID != task.UserId || row.TokenID != task.TokenId {
		return false
	}
	if err = model.PersistBillingIntent(ctx, row.RequestID, model.BillingRefundPending, 0); err != nil {
		return false
	}
	applied, err := model.ApplyBillingIntentOnce(ctx, row.RequestID)
	if err != nil {
		logger.LogWarn(ctx, "Midjourney refund remains pending for recovery")
		return false
	}
	quota := task.Quota
	task.Quota = 0
	if !applied || quota == 0 {
		return true
	}
	other := model.NewLogOther()
	other.SetPublic("task_id", task.MjId)
	other.SetPublic("reason", reason)
	model.RecordTaskBillingLog(model.RecordTaskBillingLogParams{UserId: task.UserId, LogType: model.LogTypeRefund, ChannelId: task.GetBillingChannelId(),
		ModelName: CovertMjpActionToModelName(task.Action), Quota: quota, TokenId: task.TokenId, Other: other})
	return true
}
