package contract_test

import (
	"net/http"
	"testing"

	"github.com/Calcium-Ion/go-epay/epay"
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// DEP-04: the subscription return route verifies the Epay signature; a bare
// redirect without signed params must not complete the order.
func TestDEP04_SubscriptionReturnRequiresSignature(t *testing.T) {
	forEachDB(t, func(t *testing.T, db *gorm.DB) {
		configureEpay(t)
		user := createUser(t, db, 0)
		plan := createPlan(t, db, 9.90)
		createPendingSubscriptionOrder(t, db, user.Id, plan, "DEP04BARE")
		router := callbackRouter()

		bare := signedCallback(t, "DEP04BARE", "9.90", epay.StatusTradeSuccess)
		bare.Del("sign")
		bare.Del("sign_type")
		for name, rec := range map[string]interface{ Header() http.Header }{
			"no params": getQuery(router, "/api/subscription/epay/return", nil),
			"unsigned":  getQuery(router, "/api/subscription/epay/return", bare),
		} {
			assert.Equal(t, dashboardAddr+"/wallet?pay=fail", rec.Header().Get("Location"), name)
		}
		assert.Equal(t, common.TopUpStatusPending, orderStatus(t, db, &model.SubscriptionOrder{}, "DEP04BARE"))
		assert.Zero(t, countSubscriptions(t, db, user.Id))
	})
}

// DEP-04: a signed browser return that arrives before the async notify grants
// once; the later notify is still acknowledged with "success".
func TestDEP04_ReturnBeforeNotifyGrantsOnce(t *testing.T) {
	forEachDB(t, func(t *testing.T, db *gorm.DB) {
		configureEpay(t)
		user := createUser(t, db, 0)
		plan := createPlan(t, db, 9.90)
		createPendingSubscriptionOrder(t, db, user.Id, plan, "DEP04ORDER")
		router := callbackRouter()
		form := signedCallback(t, "DEP04ORDER", "9.90", epay.StatusTradeSuccess)

		ret := getQuery(router, "/api/subscription/epay/return", form)
		assert.Equal(t, http.StatusFound, ret.Code)
		assert.Equal(t, dashboardAddr+"/wallet?pay=success", ret.Header().Get("Location"))

		assert.Equal(t, "success", postForm(router, "/api/subscription/epay/notify", form).Body.String())
		assert.EqualValues(t, 1, countSubscriptions(t, db, user.Id))
	})
}

// DEP-05: completing a subscription writes a same-trade_no top_ups mirror with
// Amount=0, Money copied from the order and an empty PaymentProvider.
// The reconciler relies on these exact rules to merge the mirror.
func TestDEP05_SubscriptionCompletionWritesTopupMirror(t *testing.T) {
	forEachDB(t, func(t *testing.T, db *gorm.DB) {
		configureEpay(t)
		user := createUser(t, db, 0)
		plan := createPlan(t, db, 12.34)
		createPendingSubscriptionOrder(t, db, user.Id, plan, "DEP05MIRROR")

		rec := postForm(callbackRouter(), "/api/subscription/epay/notify", signedCallback(t, "DEP05MIRROR", "12.34", epay.StatusTradeSuccess))
		require.Equal(t, "success", rec.Body.String())

		var order model.SubscriptionOrder
		require.NoError(t, db.Where("trade_no = ?", "DEP05MIRROR").Take(&order).Error)
		var mirrors []model.TopUp
		require.NoError(t, db.Where("trade_no = ?", "DEP05MIRROR").Find(&mirrors).Error)
		require.Len(t, mirrors, 1)
		m := mirrors[0]
		assert.EqualValues(t, 0, m.Amount)
		assert.Equal(t, order.Money, m.Money)
		assert.Equal(t, user.Id, m.UserId)
		assert.Equal(t, "", m.PaymentProvider)
		assert.Equal(t, common.TopUpStatusSuccess, m.Status)
		// The mirror is display-only: it must not credit wallet quota.
		assert.Equal(t, 0, userQuota(t, db, user.Id))
	})
}
