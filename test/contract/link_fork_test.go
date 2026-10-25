package contract_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Calcium-Ion/go-epay/epay"
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/controller"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func linkStatusViaAPI(t *testing.T, tradeNo string) model.SubscriptionLinkStatus {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/api/subscription/admin/order-links/:trade_no", controller.AdminGetSubscriptionOrderLink)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/subscription/admin/order-links/"+tradeNo, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var resp struct {
		Success bool                         `json:"success"`
		Data    model.SubscriptionLinkStatus `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
	require.True(t, resp.Success, rec.Body.String())
	return resp.Data
}

// LINK-03 must not infer a pair from ordering when several grants share the
// same user, plan and timestamp, even though both sides have equal counts.
func TestLINK_BackfillDoesNotGuessEqualCountPairs(t *testing.T) {
	forEachDB(t, func(t *testing.T, db *gorm.DB) {
		user := createUser(t, db, 0)
		plan := createPlan(t, db, 10)
		for _, tradeNo := range []string{"AMBIGUOUS-A", "AMBIGUOUS-B"} {
			require.NoError(t, db.Create(&model.SubscriptionOrder{UserId: user.Id, PlanId: plan.Id,
				TradeNo: tradeNo, Status: common.TopUpStatusSuccess, CompleteTime: 1_700_000_000}).Error)
			require.NoError(t, db.Create(&model.UserSubscription{UserId: user.Id, PlanId: plan.Id,
				Source: "order", StartTime: 1_700_000_001, Status: "active"}).Error)
		}
		decisions, err := model.PlanSubscriptionLinkBackfill(300)
		require.NoError(t, err)
		require.Len(t, decisions, 2)
		var rows int64
		require.NoError(t, db.Model(&model.ForkSubscriptionOrderLink{}).Count(&rows).Error)
		assert.Zero(t, rows, "planning must not write link rows")
		for _, decision := range decisions {
			assert.Equal(t, model.LinkSourceUnlinkable, decision.Result)
			assert.Zero(t, decision.UserSubscriptionId)
		}
		require.NoError(t, model.ApplySubscriptionLinkBackfill(decisions))
		for _, tradeNo := range []string{"AMBIGUOUS-A", "AMBIGUOUS-B"} {
			status, err := model.GetSubscriptionLinkStatus(tradeNo)
			require.NoError(t, err)
			assert.Equal(t, model.LinkStatusUnlinkable, status.Status)
		}
	})
}

// LINK-01/02/04 end to end on every database: an order placed through the
// real Epay request handler, then edited plan, then a signed notify, yields a
// subscription from the snapshot and a "linked" status from the admin API.
func TestLINK_EpayOrderIsSnapshottedAndLinked(t *testing.T) {
	forEachDB(t, func(t *testing.T, db *gorm.DB) {
		configureEpay(t)
		user := createUser(t, db, 0)
		plan := createPlan(t, db, 15.00)

		pay := newPayRouter(user.Id)
		data := requestSubscriptionPay(t, pay, plan.Id)
		tradeNo := data["out_trade_no"]
		require.NotEmpty(t, tradeNo)
		assert.Equal(t, model.LinkStatusMissing, linkStatusViaAPI(t, tradeNo).Status, "pending order is not linked yet")

		require.NoError(t, db.Model(plan).Update("total_amount", 1).Error)
		model.InvalidateSubscriptionPlanCache(plan.Id)
		rec := postForm(callbackRouter(), "/api/subscription/epay/notify", signedCallback(t, tradeNo, data["money"], epay.StatusTradeSuccess))
		require.Equal(t, "success", rec.Body.String())

		var sub model.UserSubscription
		require.NoError(t, db.Where("user_id = ?", user.Id).Take(&sub).Error)
		assert.EqualValues(t, 1000, sub.AmountTotal, "granted from the order-time snapshot")
		status := linkStatusViaAPI(t, tradeNo)
		assert.Equal(t, model.LinkStatusLinked, status.Status)
		assert.Equal(t, sub.Id, status.UserSubscriptionId)
		assert.Equal(t, model.LinkStatusMissing, linkStatusViaAPI(t, "NO-SUCH-ORDER").Status)
	})
}
