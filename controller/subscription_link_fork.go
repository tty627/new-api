package controller

// Fork-owned: read-only order→entitlement link lookup for the payment bridge
// reconciler (LINK-04). Admin session or a PAT with billing:read.

import (
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
)

func AdminGetSubscriptionOrderLink(c *gin.Context) {
	tradeNo := strings.TrimSpace(c.Param("trade_no"))
	if tradeNo == "" || len(tradeNo) > 255 {
		common.ApiErrorT(c, "Invalid parameters")
		return
	}
	status, err := model.GetSubscriptionLinkStatus(tradeNo)
	if err != nil {
		common.ApiError(c, err)
		return
	}
	common.ApiSuccess(c, status)
}
