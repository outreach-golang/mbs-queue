package mbs_queue

import (
	"context"
	rmqClient "github.com/apache/rocketmq-clients/golang/v5"
)

type ProducerOptions struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Topic     []string
	GroupName string
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

func (p *Producer) SendNormalMessage(ctx context.Context, topic string, tag string, messageBody string) error {
	msg := rmqClient.Message{
		Topic: topic,
		Body:  []byte(messageBody),
		Tag:   &tag,
	}
	_, err := p.Producer.Send(ctx, &msg)
	if err != nil {
		return err
	}

	return nil
}

func (p *Producer) Stop() error {
	err := p.Producer.GracefulStop()

	return err
}
