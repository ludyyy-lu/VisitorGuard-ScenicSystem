package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

// Kafka 配置
const (
	kafkaBroker = "172.23.79.129:9092"
	kafkaTopic  = "scenic_visitor_topic"
)

// VisitorEvent 定义了游客事件的数据结构
// Flink SQL 在消费 JSON 数据时，会自动将 JSON 的字段名映射为表中的列名
type VisitorEvent struct {
	EventTime string `json:"event_time"` // 事件发生时间 (格式: "2025-07-15T10:30:00Z")
	AreaID    string `json:"area_id"`    // 区域ID (例如: "gate_a", "mountain_top")
	Action    string `json:"action"`     // 动作 (in: 进入, out: 离开)
	UserID    string `json:"user_id"`    // 游客的唯一ID，用于模拟
}

func main() {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    kafkaTopic,
		Balancer: &kafka.RoundRobin{},
	}
	defer writer.Close()

	log.Println("开始向 Kafka 发送模拟游客数据 (加速验证模式)...")

	areaIDs := []string{"好莱坞大道", "哈利波特的魔法世界", "小黄人乐园", "侏罗纪世界大冒险", "变形金刚基地", "功夫熊猫盖世之地"}

	// 【核心改造】我们引入一个 map 来追踪“已进入但未离开”的游客
	visitorsInArea := make(map[string]string) // key: user_id, value: area_id

	for {
		// 增加一个随机选择，决定是生成新游客进入，还是让老游客离开
		if len(visitorsInArea) == 0 || rand.Intn(100) < 70 { // 70% 的概率生成新游客进入
			// --- 生成一个新的“进入”事件 ---
			userID := fmt.Sprintf("user-%d", rand.Intn(10000))
			// 确保这个新用户当前不在任何区域
			if _, ok := visitorsInArea[userID]; ok {
				continue // 如果用户已在场，就跳过本次循环，避免重复进入
			}
			areaID := areaIDs[rand.Intn(len(areaIDs))]

			// 记录这个游客已经进入了该区域
			visitorsInArea[userID] = areaID

			event := VisitorEvent{
				EventTime: time.Now().Format("2006-01-02T15:04:05.000"),
				AreaID:    areaID,
				Action:    "in",
				UserID:    userID,
			}
			sendMessage(writer, event)

		} else {
			// --- 30% 的概率，让一个已在场的游客“离开” ---
			var userIDToExit string
			// 随机从在场游客中选一个
			for u := range visitorsInArea {
				userIDToExit = u
				break
			}
			areaID := visitorsInArea[userIDToExit]

			// 从在场游客记录中移除
			delete(visitorsInArea, userIDToExit)

			event := VisitorEvent{
				EventTime: time.Now().Format("2006-01-02T15:04:05.000"),
				AreaID:    areaID,
				Action:    "out",
				UserID:    userIDToExit,
			}
			sendMessage(writer, event)
		}

		time.Sleep(time.Duration(200+rand.Intn(800)) * time.Millisecond)
	}
}

// 辅助函数，用于发送消息，避免重复代码
func sendMessage(writer *kafka.Writer, event VisitorEvent) {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		log.Printf("无法序列化JSON: %v\n", err)
		return
	}
	msg := kafka.Message{Value: eventJSON}
	err = writer.WriteMessages(context.Background(), msg)
	if err != nil {
		log.Printf("无法写入消息%s到 Kafka: %v\n", string(eventJSON), err)
	} else {
		log.Printf("成功发送消息: %s\n", string(eventJSON))
	}
}
