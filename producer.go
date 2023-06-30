package mbs_queue

import (
	"context"
	rmqClient "github.com/apache/rocketmq-clients/golang/v5"
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

func (p *Producer) Stop() error {
	err := p.Producer.GracefulStop()

	return err
}
