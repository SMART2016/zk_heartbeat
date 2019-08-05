package main

import (
	"fmt"

	"github.com/SMART2016/zk_heartbeat/config"
)

func main() {
	conf := config.HeartBeatConfig{
		Servers:                []string{"localhost:2181"},
		SessionTimeoutInSecond: 20,
		ServiceName:            "TestService",
	}

	hreatBeatController := GetHeartBeatController(conf)
	liveNodes, registeredNodes, err := hreatBeatController.RegisterServiceHeartBeat("TestService1", `{"serviceid":"1"}`)
	if err != nil {
		fmt.Println(err.Error())
	}

	fmt.Println("liveNodes: ", liveNodes)
	fmt.Println("RegisteredNodes: ", registeredNodes)
	<-make(chan int)
}
