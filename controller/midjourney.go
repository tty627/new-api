package controller

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting"
	"github.com/QuantumNous/new-api/setting/system_setting"

	"github.com/gin-gonic/gin"
)

// midjourneyPollSummary is the result recorded on a midjourney_poll system task
// row, summarizing one polling pass.
type midjourneyPollSummary struct {
	UnfinishedTasks int `json:"unfinished_tasks"`
	ChannelsScanned int `json:"channels_scanned"`
	NullTasksFailed int `json:"null_tasks_failed"`
}

// runMidjourneyTaskUpdateOnce performs one Midjourney polling pass synchronously.
// It honors ctx cancellation (the system-task runner cancels it when the lease
// is lost) and, when report is non-nil, reports progress as (processedChannels,
// totalChannels) so the system task surfaces a percentage.
func runMidjourneyTaskUpdateOnce(ctx context.Context, report func(processed, total int)) midjourneyPollSummary {
	summary := midjourneyPollSummary{}
	if ctx == nil {
		ctx = context.Background()
	}

	tasks := model.GetAllUnFinishTasks()
	if len(tasks) == 0 {
		return summary
	}
	summary.UnfinishedTasks = len(tasks)

	logger.LogInfo(ctx, common.LogText("unfinished tasks found: %v", len(tasks)))
	taskChannelM := make(map[int][]string)
	taskM := make(map[string][]*model.Midjourney)
	nullTasks := make([]*model.Midjourney, 0)
	for _, task := range tasks {
		if task.MjId == "" {
			// 统计失败的未完成任务
			nullTasks = append(nullTasks, task)
			continue
		}
		key := fmt.Sprintf("%d:%s", task.ChannelId, task.MjId)
		taskM[key] = append(taskM[key], task)
		taskChannelM[task.ChannelId] = append(taskChannelM[task.ChannelId], task.MjId)
	}
	if len(nullTasks) > 0 {
		summary.NullTasksFailed = len(nullTasks)
		for _, task := range nullTasks {
			markMidjourneyUncertain(ctx, task, "Missing upstream task identity; billing requires verification")
		}
	}
	if len(taskChannelM) == 0 {
		return summary
	}

	totalChannels := len(taskChannelM)
	processedChannels := 0
	for channelId, taskIds := range taskChannelM {
		if ctx != nil && ctx.Err() != nil {
			break
		}
		if report != nil {
			report(processedChannels, totalChannels)
		}
		processedChannels++
		summary.ChannelsScanned++
		logger.LogInfo(ctx, common.LogText("channel #%d has %d unfinished tasks", channelId, len(taskIds)))
		if len(taskIds) == 0 {
			continue
		}
		midjourneyChannel, err := model.CacheGetChannel(channelId)
		if err != nil {
			logger.LogError(ctx, common.LogText("CacheGetChannel: %v", err))
			for _, id := range taskIds {
				for _, task := range taskM[fmt.Sprintf("%d:%s", channelId, id)] {
					markMidjourneyUncertain(ctx, task, "Channel unavailable; billing requires verification")
				}
			}
			continue
		}
		requestUrl := fmt.Sprintf("%s/mj/task/list-by-condition", *midjourneyChannel.BaseURL)

		body, err := common.Marshal(map[string]any{
			"ids": taskIds,
		})
		if err != nil {
			logger.LogError(ctx, common.LogText("Get Task marshal body error: %v", err))
			continue
		}
		timeout := time.Second * 15
		requestCtx, cancel := context.WithTimeout(ctx, timeout)
		req, err := http.NewRequestWithContext(requestCtx, "POST", requestUrl, bytes.NewBuffer(body))
		if err != nil {
			cancel()
			logger.LogError(ctx, common.LogText("Get Task error: %v", err))
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("mj-api-secret", midjourneyChannel.Key)
		resp, err := service.GetHttpClient().Do(req)
		if err != nil {
			logger.LogError(ctx, common.LogText("Get Task Do req error: %v", err))
			cancel()
			continue
		}
		if resp.StatusCode != http.StatusOK {
			logger.LogError(ctx, common.LogText("Get Task status code: %d", resp.StatusCode))
			resp.Body.Close()
			cancel()
			continue
		}
		responseBody, err := io.ReadAll(resp.Body)
		if err != nil {
			logger.LogError(ctx, common.LogText("Get Mjp Task parse body error: %v", err))
			resp.Body.Close()
			cancel()
			continue
		}
		var responseItems []dto.MidjourneyDto
		err = common.Unmarshal(responseBody, &responseItems)
		if err != nil {
			logger.LogError(ctx, common.LogText("Get Mjp Task parse body error2: %v, body: %s", err, string(responseBody)))
			resp.Body.Close()
			cancel()
			continue
		}
		resp.Body.Close()
		req.Body.Close()
		cancel()

		for _, fetchedItem := range responseItems {
			matching := taskM[fmt.Sprintf("%d:%s", channelId, fetchedItem.MjId)]
			if len(matching) == 0 {
				logger.LogWarn(ctx, common.LogText("Midjourney task response ignored: unknown mj_id=%s", fetchedItem.MjId))
				continue
			}
			for _, task := range matching {
				responseItem := fetchedItem
				observed := responseItem

				useTime := (time.Now().UnixNano() / int64(time.Millisecond)) - task.SubmitTime
				// 如果时间超过一小时，且进度不是100%，则认为任务失败
				timedOut := useTime > 3600000 && task.Progress != "100%" && observed.Status != "SUCCESS" && observed.Status != "FAILURE"
				if timedOut {
					observed.FailReason = "Upstream task timed out; billing requires verification"
					observed.Status = "FAILURE"
				}
				responseItem = observed
				if !checkMjTaskNeedUpdate(task, responseItem) {
					continue
				}
				preStatus := task.Status
				task.Code = 1
				task.Progress = responseItem.Progress
				task.PromptEn = responseItem.PromptEn
				task.State = responseItem.State
				// Keep the gateway's submission time for timeout decisions. A poll
				// that omits upstream timestamps must not make a fresh task age from 0.
				if task.SubmitTime <= 0 && responseItem.SubmitTime > 0 {
					task.SubmitTime = responseItem.SubmitTime
				}
				task.StartTime = responseItem.StartTime
				task.FinishTime = responseItem.FinishTime
				task.ImageUrl = responseItem.ImageUrl
				task.Status = responseItem.Status
				task.FailReason = responseItem.FailReason
				if responseItem.Properties != nil {
					propertiesStr, _ := common.Marshal(responseItem.Properties)
					task.Properties = string(propertiesStr)
				}
				if responseItem.Buttons != nil {
					buttonStr, _ := common.Marshal(responseItem.Buttons)
					task.Buttons = string(buttonStr)
				}
				// 映射 VideoUrl
				task.VideoUrl = responseItem.VideoUrl

				// 映射 VideoUrls - 将数组序列化为 JSON 字符串
				if responseItem.VideoUrls != nil && len(responseItem.VideoUrls) > 0 {
					videoUrlsStr, err := common.Marshal(responseItem.VideoUrls)
					if err != nil {
						logger.LogError(ctx, common.LogText("failed to serialize VideoUrls: %v", err))
						task.VideoUrls = "[]" // 失败时设置为空数组
					} else {
						task.VideoUrls = string(videoUrlsStr)
					}
				} else {
					task.VideoUrls = "" // 空值时清空字段
				}

				if (task.Progress != "100%" && responseItem.FailReason != "") || (task.Progress == "100%" && task.Status == "FAILURE") {
					logger.LogInfo(ctx, common.LogText("%s failed: %s", task.MjId, task.FailReason))
					task.Progress = "100%"
				}
				terminal := task.Status == "SUCCESS" || task.Status == "FAILURE" || task.Progress == "100%"
				var err error
				if terminal {
					task.Progress = "100%"
					known := !timedOut && (observed.Status == "SUCCESS" || observed.Status == "FAILURE")
					_, err = service.CommitMidjourneyOutcome(ctx, task, preStatus, known)
				} else {
					_, err = task.UpdateWithStatus(preStatus)
				}
				if err != nil {
					logger.LogError(ctx, common.LogText("UpdateMidjourneyTask task error: %s", err.Error()))
				}
			}
		}
	}
	if report != nil && (ctx == nil || ctx.Err() == nil) {
		report(totalChannels, totalChannels)
	}
	return summary
}

func markMidjourneyUncertain(ctx context.Context, task *model.Midjourney, reason string) {
	previous := task.Status
	task.Status, task.Progress, task.FailReason = "FAILURE", "100%", reason
	if _, err := service.CommitMidjourneyOutcome(ctx, task, previous, false); err != nil {
		logger.LogError(ctx, "Could not persist unknown Midjourney billing outcome")
	}
}

func checkMjTaskNeedUpdate(oldTask *model.Midjourney, newTask dto.MidjourneyDto) bool {
	if oldTask.Code != 1 {
		return true
	}
	if oldTask.Progress != newTask.Progress {
		return true
	}
	if oldTask.PromptEn != newTask.PromptEn {
		return true
	}
	if oldTask.State != newTask.State {
		return true
	}
	if oldTask.SubmitTime != newTask.SubmitTime {
		return true
	}
	if oldTask.StartTime != newTask.StartTime {
		return true
	}
	if oldTask.FinishTime != newTask.FinishTime {
		return true
	}
	if oldTask.ImageUrl != newTask.ImageUrl {
		return true
	}
	if oldTask.Status != newTask.Status {
		return true
	}
	if oldTask.FailReason != newTask.FailReason {
		return true
	}
	if oldTask.FinishTime != newTask.FinishTime {
		return true
	}
	if oldTask.Progress != "100%" && newTask.FailReason != "" {
		return true
	}
	// 检查 VideoUrl 是否需要更新
	if oldTask.VideoUrl != newTask.VideoUrl {
		return true
	}
	// 检查 VideoUrls 是否需要更新
	if newTask.VideoUrls != nil && len(newTask.VideoUrls) > 0 {
		newVideoUrlsStr, _ := common.Marshal(newTask.VideoUrls)
		if oldTask.VideoUrls != string(newVideoUrlsStr) {
			return true
		}
	} else if oldTask.VideoUrls != "" {
		// 如果新数据没有 VideoUrls 但旧数据有，需要更新（清空）
		return true
	}

	return false
}

func GetAllMidjourney(c *gin.Context) {
	pageInfo := common.GetPageQuery(c)

	// 解析其他查询参数
	queryParams := model.TaskQueryParams{
		ChannelID:      c.Query("channel_id"),
		MjID:           c.Query("mj_id"),
		StartTimestamp: c.Query("start_timestamp"),
		EndTimestamp:   c.Query("end_timestamp"),
	}

	items := model.GetAllTasks(pageInfo.GetStartIdx(), pageInfo.GetPageSize(), queryParams)
	total := model.CountAllTasks(queryParams)

	if setting.MjForwardUrlEnabled {
		for i, midjourney := range items {
			midjourney.ImageUrl = system_setting.ServerAddress + "/mj/image/" + midjourney.MjId
			items[i] = midjourney
		}
	}
	pageInfo.SetTotal(int(total))
	pageInfo.SetItems(items)
	common.ApiSuccess(c, pageInfo)
}

func GetUserMidjourney(c *gin.Context) {
	pageInfo := common.GetPageQuery(c)

	userId := c.GetInt("id")

	queryParams := model.TaskQueryParams{
		MjID:           c.Query("mj_id"),
		StartTimestamp: c.Query("start_timestamp"),
		EndTimestamp:   c.Query("end_timestamp"),
	}

	items := model.GetAllUserTask(userId, pageInfo.GetStartIdx(), pageInfo.GetPageSize(), queryParams)
	total := model.CountAllUserTask(userId, queryParams)

	if setting.MjForwardUrlEnabled {
		for i, midjourney := range items {
			midjourney.ImageUrl = system_setting.ServerAddress + "/mj/image/" + midjourney.MjId
			items[i] = midjourney
		}
	}
	pageInfo.SetTotal(int(total))
	pageInfo.SetItems(items)
	common.ApiSuccess(c, pageInfo)
}
