package main

import (
	"fmt"

	"github.com/ContinuumLLC/zk_heartbeat/heartbeat/config"
	"github.com/ContinuumLLC/zk_heartbeat/heartbeat"
)

func main() {
	conf := config.HeartBeatConfig{
		Servers:                   []string{"localhost:2181"},
		SessionTimeoutInSecond:    2,
		ServiceName:               "TestService",
		EventRefreshTimeInSeconds: 50,
	}

	hreatBeatController := heartbeat.GetHeartBeatController(conf)
	liveNodes, registeredNodes, err := hreatBeatController.RegisterServiceHeartBeat("TestService2", `{"serviceid":"2"}`)
	if err != nil {
		fmt.Println(err.Error())
	}

	fmt.Println("liveNodes: ", liveNodes)
	fmt.Println("RegisteredNodes: ", registeredNodes)
	<-make(chan int)
}
