package main

import (
	"flag"
	paho "github.com/eclipse/paho.mqtt.golang"
	"log"

	"bufio"
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
	infile := "mqtt_messages.log"

	flag.StringVar(&host, "host", host, "The mqtt broker to connect to.")
	flag.StringVar(&port, "port", port, "The port to connect to.")
	flag.StringVar(&infile, "infile", infile, "Inputfile to read messages from.")
	flag.Parse()

	log.Printf("[main] host %s", host)
	log.Printf("[main] port %s", port)
	log.Printf("[main] infile %s", infile)

	brokerAddr := fmt.Sprintf("%s:%s", host, port)

	opts := paho.NewClientOptions()
	opts.AddBroker(brokerAddr)

	client := paho.NewClient(opts)
	token := client.Connect()
	success := token.WaitTimeout(CONNECT_TIMEOUT)

	if success {

		f, openErr := os.Open(infile)
		check(openErr)

		scanner := bufio.NewScanner(f)

		for scanner.Scan() {
			text := scanner.Text()

			var message Message
			unmarshalErr := json.Unmarshal([]byte(text), &message)
			check(unmarshalErr)

			log.Printf("Publishing message: %s", text)
			client.Publish(message.Topic, 0, false, []byte(message.Payload))
		}

		f.Close()
		client.Disconnect(0)

	} else {
		log.Fatalf("Timemout Reached, Could not connect to broker %s : %s", brokerAddr, token.Error())
	}

}
