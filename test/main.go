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

const (
	kafkaBroker = "172.23.79.129:9092"
	kafkaTopic  = "scenic_visitor_topic"
)

type VisitorEvent struct {
	EventTime string `json:"event_time"`
	AreaID    string `json:"area_id"`
	Action    string `json:"action"` // "in" or "out"
	UserID    string `json:"user_id"`
}

type SimulatedVisitor struct {
	UserID      string
	EnterTime   time.Time
	CurrentArea string
	Route       []string
	LastMove    time.Time
}

func randomAreaExcluding(current string, areas []string) string {
	for {
		newArea := areas[rand.Intn(len(areas))]
		if newArea != current {
			return newArea
		}
	}
}

func sendKafkaEvent(writer *kafka.Writer, userID, area, action string) {
	event := VisitorEvent{
		EventTime: time.Now().UTC().Format("2006-01-02T15:04:05.000"),
		AreaID:    area,
		Action:    action,
		UserID:    userID,
	}
	eventJSON, err := json.Marshal(event)
	if err != nil {
		log.Printf("JSON序列化失败: %v\n", err)
		return
	}
	err = writer.WriteMessages(context.Background(), kafka.Message{
		Value: eventJSON,
	})
	if err != nil {
		log.Printf("写入 Kafka 失败: %v\n", err)
	} else {
		log.Printf("发送事件: %s\n", string(eventJSON))
	}
}

func main() {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    kafkaTopic,
		Balancer: &kafka.RoundRobin{},
	}
	defer writer.Close()

	log.Println("🚀 开始发送模拟游客数据到 Kafka...")

	areaIDs := []string{"好莱坞大道", "小黄人乐园", "哈利波特的魔法世界", "变形金刚基地", "侏罗纪世界大冒险"}

	activeVisitors := make(map[string]*SimulatedVisitor)
	userCounter := 0

	ticker := time.NewTicker(1 * time.Second)
	for now := range ticker.C {
		// 每秒新增 3～8 个新游客
		newVisitors := rand.Intn(6) + 3
		for i := 0; i < newVisitors; i++ {
			userID := fmt.Sprintf("user-%d", userCounter)
			startArea := areaIDs[rand.Intn(len(areaIDs))]
			visitor := &SimulatedVisitor{
				UserID:      userID,
				EnterTime:   now,
				CurrentArea: startArea,
				Route:       []string{startArea},
				LastMove:    now,
			}
			activeVisitors[userID] = visitor
			sendKafkaEvent(writer, userID, startArea, "in")
			userCounter++
		}

		// 遍历每个活跃游客
		for id, visitor := range activeVisitors {
			elapsed := now.Sub(visitor.LastMove)
			totalStay := now.Sub(visitor.EnterTime)

			// 1. 判断是否离开（超过 2～5 分钟 stay）
			if totalStay > time.Duration(120+rand.Intn(180))*time.Second {
				sendKafkaEvent(writer, id, visitor.CurrentArea, "out")
				delete(activeVisitors, id)
				continue
			}

			// 2. 判断是否切换区域（每 20～60 秒有概率移动）
			if elapsed > time.Duration(20+rand.Intn(40))*time.Second {
				newArea := randomAreaExcluding(visitor.CurrentArea, areaIDs)
				sendKafkaEvent(writer, id, visitor.CurrentArea, "out")
				sendKafkaEvent(writer, id, newArea, "in")
				visitor.CurrentArea = newArea
				visitor.Route = append(visitor.Route, newArea)
				visitor.LastMove = now
			}
		}

		// ⚠️ 输出当前在线游客数量（可用于预警）
		if len(activeVisitors) > 150 {
			log.Printf("⚠️ 当前园区人数达到 %d，超过预警阈值！", len(activeVisitors))
		} else {
			log.Printf("当前园区人数：%d", len(activeVisitors))
		}
	}
}
