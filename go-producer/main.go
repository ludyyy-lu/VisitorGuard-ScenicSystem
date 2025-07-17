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
	// 初始化一个 Kafka writer (生产者)
	// Balancer 使用 RoundRobin 会将消息轮流发送到不同的分区，有助于负载均衡
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    kafkaTopic,
		Balancer: &kafka.RoundRobin{},
		// 开启此项以在发送消息时打印日志
		// Logger: kafka.LoggerFunc(log.Printf),
	}
	defer writer.Close()

	log.Println("开始向 Kafka 发送模拟游客数据...")
	log.Printf("Topic: %s\n", kafkaTopic)

	// 定义一些模拟的景区区域ID
	areaIDs := []string{"好莱坞大道", "小黄人乐园", "哈利波特的魔法世界", "变形金刚基地", "侏罗纪世界大冒险"}

	// 使用无限循环来持续生成数据
	for {
		// 随机选择一个区域
		area := areaIDs[rand.Intn(len(areaIDs))]

		// 随机决定是进入还是离开
		action := "in"
		if rand.Intn(2) == 1 {
			action = "out"
		}

		// 创建一个游客事件
		event := VisitorEvent{
			EventTime: time.Now().UTC().Format("2006-01-02T15:04:05.000"),
			AreaID:    area,
			Action:    action,
			UserID:    fmt.Sprintf("user-%d", rand.Intn(1000)),
		}

		// 将结构体序列化为 JSON 字节流
		eventJSON, err := json.Marshal(event)
		if err != nil {
			log.Printf("无法序列化JSON: %v\n", err)
			continue
		}

		// 创建一条 Kafka 消息
		msg := kafka.Message{
			Value: eventJSON,
		}

		// 使用 WriteMessages 将消息发送到 Kafka
		err = writer.WriteMessages(context.Background(), msg)
		if err != nil {
			log.Printf("无法写入消息到 Kafka: %v\n", err)
		} else {
			// 在控制台打印出我们刚刚发送的数据，方便调试
			log.Printf("成功发送消息: %s\n", string(eventJSON))
		}
		time.Sleep(time.Duration(200+rand.Intn(1000)) * time.Millisecond)
	}
}
