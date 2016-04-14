package main

import (
	"flag"
	"fmt"
	mqtt "github.com/it-man-cn/paho.mqtt.golang"
	//"math/rand"
	"time"
)

var (
	uri string
	m   *message
)

type message struct {
	Topic     string
	QoS       byte
	Payload   interface{}
	Sent      time.Time
	Delivered time.Time
	Error     bool
}

func main() {
	var (
		broker  = flag.String("broker", "tcp://localhost:1883", "MQTT broker endpoint as scheme://host:port")
		topic   = flag.String("topic", "/test", "MQTT topic for outgoing messages")
		qos     = flag.Int("qos", 1, "QoS for published messages")
		size    = flag.Int("size", 100, "Size of the messages payload (bytes)")
		clients = flag.Int("clients", 10, "Number of clients")
		start   = flag.Int("start", 1, "begin clientID")
	)

	flag.Parse()
	if *clients < 1 {
		fmt.Println("Invlalid arguments")
		return
	}
	if len(*broker) == 0 {
		uri = "tcp://localhost:1883"
	} else {
		uri = *broker
	}
	m = &message{
		Topic:   *topic,
		QoS:     byte(*qos),
		Payload: make([]byte, *size),
	}
	for i := *start; i < *start+*clients; i++ {
		go startClient(i)
	}
	select {}
}

func startClient(clientID int) {
	onConnected := func(client mqtt.Client) {
		fmt.Printf("CLIENT %v is connected to the broker %v\n", clientID, uri)
		for {
			time.Sleep(1 * time.Minute)
			token := client.Publish(m.Topic, m.QoS, false, m.Payload)
			token.Wait()
			if token.Error() != nil {
				fmt.Printf("CLIENT %v Error sending message: %v\n", clientID, token.Error())
			}
		}
	}
	opts := mqtt.NewClientOptions().
		AddBroker(uri).
		SetProtocolVersion(4).
		SetClientID(fmt.Sprintf("mqtt%d", clientID)).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetKeepAlive(300 * time.Second).
		SetOnConnectHandler(onConnected).
		SetConnectionLostHandler(func(client mqtt.Client, reason error) {
		fmt.Printf("CLIENT %v lost connection to the broker: %v. Will reconnect...\n", clientID, reason.Error())
	})
	client := mqtt.NewClient(opts)
	//wait random seconds
	//time.Sleep(time.Duration(rand.Intn(30)) * time.Second)
	token := client.Connect()
	token.Wait()

	if token.Error() != nil {
		fmt.Printf("CLIENT %v had error connecting to the broker: %v\n", clientID, token.Error())
		client.Disconnect(1000) //milliseconds
	}

}
