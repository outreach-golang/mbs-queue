package mbs_queue

import (
	"context"
	rmqClient "github.com/apache/rocketmq-clients/golang/v5"
	"time"
)

type ProducerOptions struct {
	Endpoint       string
	AccessKey      string
	SecretKey      string
	Topic          []string
	GroupName      string
	ConsoleEnabled bool
}

type Producer struct {
	Options  ProducerOptions
	Producer rmqClient.Producer
}

type ProducerOption func(*ProducerOptions)

func WithEndpoint(endpoint string) ProducerOption {
	return func(options *ProducerOptions) {
		options.Endpoint = endpoint
	}
}

func WithCredentials(accessKey, secretKey string) ProducerOption {
	return func(options *ProducerOptions) {
		options.AccessKey = accessKey
		options.SecretKey = secretKey
	}
}

func WithTopic(topic []string) ProducerOption {
	return func(options *ProducerOptions) {
		options.Topic = topic
	}
}

func WithGroupName(groupName string) ProducerOption {
	return func(options *ProducerOptions) {
		options.GroupName = groupName
	}
}

func WithConsoleEnabled(enabled bool) ProducerOption {
	return func(options *ProducerOptions) {
		options.ConsoleEnabled = enabled
	}
}

// SendNormalMessage 普通同步无序消息
func (p *Producer) SendNormalMessage(ctx context.Context, topic string, tag string, messageBody string) (string, error) {
	var messageID string

	msg := rmqClient.Message{
		Topic: topic,
		Body:  []byte(messageBody),
		Tag:   &tag,
	}

	serRes, err := p.Producer.Send(ctx, &msg)
	if err != nil {
		return messageID, err
	}

	for _, re := range serRes {
		tmp := re
		messageID = tmp.MessageID
	}

	return messageID, nil
}

type asyncCallbackData struct {
	MessageID string
	Err       error
}

// SendAsyncMessage 普通异步无序消息
func (p *Producer) SendAsyncMessage(ctx context.Context, topic string, tag string, messageBody string) (string, error) {
	var (
		callbackDataChan = make(chan asyncCallbackData, 1)
	)

	msg := rmqClient.Message{
		Topic: topic,
		Body:  []byte(messageBody),
		Tag:   &tag,
	}

	p.Producer.SendAsync(ctx, &msg, func(ctx context.Context, resp []*rmqClient.SendReceipt, err error) {
		var res asyncCallbackData

		if err != nil {
			res.Err = err
		}

		for _, re := range resp {
			tmp := re
			res.MessageID = tmp.MessageID
		}

		callbackDataChan <- res

	})

	res := <-callbackDataChan

	return res.MessageID, res.Err
}

// SendFifoMessage 同步有序消息
func (p *Producer) SendFifoMessage(ctx context.Context, topic string, tag string, messageBody string) (string, error) {
	var messageID string

	msg := rmqClient.Message{
		Topic: topic,
		Body:  []byte(messageBody),
		Tag:   &tag,
	}

	msg.SetMessageGroup("fifo")

	serRes, err := p.Producer.Send(ctx, &msg)
	if err != nil {
		return messageID, err
	}

	for _, re := range serRes {
		tmp := re
		messageID = tmp.MessageID
	}

	return messageID, nil
}

// SendDelayMessage 同步无序延时消息
func (p *Producer) SendDelayMessage(ctx context.Context, topic string, tag string, messageBody string, triggerTime time.Time) (string, error) {
	var messageID string

	msg := rmqClient.Message{
		Topic: topic,
		Body:  []byte(messageBody),
		Tag:   &tag,
	}

	msg.SetDelayTimestamp(triggerTime)

	serRes, err := p.Producer.Send(ctx, &msg)
	if err != nil {
		return messageID, err
	}

	for _, re := range serRes {
		tmp := re
		messageID = tmp.MessageID
	}

	return messageID, nil
}

func (p *Producer) Stop() error {
	err := p.Producer.GracefulStop()

	return err
}
