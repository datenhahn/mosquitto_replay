package main

import (
	"flag"
	paho "github.com/eclipse/paho.mqtt.golang"
	"log"

	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Message struct {
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
}

var CONNECT_TIMEOUT = 5 * time.Second

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {

	host := "localhost"
	port := "1883"
	topic := "#"
	outfile := "mqtt_messages.log"

	flag.StringVar(&host, "host", host, "The mqtt broker to connect to.")
	flag.StringVar(&port, "port", port, "The port to connect to.")
	flag.StringVar(&topic, "topic", topic, "Topic to subscribe to.")
	flag.StringVar(&outfile, "outfile", outfile, "Outputfile to write recordings to.")
	flag.Parse()

	log.Printf("[main] host %s", host)
	log.Printf("[main] port %s", port)
	log.Printf("[main] topic %s", topic)
	log.Printf("[main] outfile %s", outfile)

	brokerAddr := fmt.Sprintf("%s:%s", host, port)

	opts := paho.NewClientOptions()
	opts.AddBroker(brokerAddr)

	client := paho.NewClient(opts)
	token := client.Connect()
	success := token.WaitTimeout(CONNECT_TIMEOUT)

	if success {

		client.Subscribe(topic, byte(1), func(client paho.Client, message paho.Message) {

			jsonMessage := &Message{
				Topic:   message.Topic(),
				Payload: string(message.Payload()),
			}

			jsonString, jsonErr := json.Marshal(jsonMessage)
			check(jsonErr)

			log.Printf("Received Message: %s", jsonString)

			f, openErr := os.OpenFile(outfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)

			check(openErr)

			_, writeErr1 := f.Write(jsonString)
			check(writeErr1)

			_, writeErr2 := f.WriteString("\n")
			check(writeErr2)

			f.Close()

		})

		select {}

	} else {
		log.Fatalf("Timemout Reached, Could not connect to broker %s : %s", brokerAddr, token.Error())
	}

}
