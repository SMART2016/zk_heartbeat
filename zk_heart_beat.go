package main

import (
	"fmt"
	"time"

	"github.com/SMART2016/zk_heartbeat/config"
	"github.com/samuel/go-zookeeper/zk"
)

type HeartbeatChangeHandler func(zk.Event)

type HeartBeatInstance struct {
	conf                  config.HeartBeatConfig
	parentHeartBeatPath   string
	connection            *zk.Conn
	watchHeartbeatChannel <-chan zk.Event
	heartbeatHandler      HeartbeatChangeHandler
}

//Every Service instance cal call this function to register there Heartbeat functionality to Zookeeper
//The Heartbeat will be bounded to a single zookeeper persistent parent node based on the service name.
//The entire service cluster should be having same service name to leverage Heartbeat properly.
//pathID: any identifier which will uniquely identify the instance like IP or PodId
func (h HeartBeatInstance) RegisterServiceHeartBeat(pathID, data string) ([]string, error) {
	//Get ZK connection
	if err := h.connect(); err != nil {
		return nil, fmt.Errorf("Error Getting zk cluster Connection. Reason: %v", err)
	}

	//Create Persistent Parent Node
	if _, err := h.createParentNode("/"+h.conf.ServiceName, h.conf.ServiceName); err != nil {
		return nil, fmt.Errorf("Error Creating Parent zk HeartBeat Node. Reason: %v", err)
	} else {
		h.parentHeartBeatPath = "/" + h.conf.ServiceName
	}

	//Create a child empheral node for the current instance to register to heatbeat contract.
	if _, err := h.createChildEmpheralNode(pathID, data); err != nil {
		return nil, fmt.Errorf("Error creating child Empheral node for current instance hearbeat. Reason: %v", err)
	}

	registeredNodes, err := h.getRegisteredNodeInfo()
	if err != nil {
		return nil, fmt.Errorf("Error Fetching Registered nodes for the Parent Hearbeat node [ %s ]... Reason: %v", h.parentHeartBeatPath, err)
	}

	//TODO heartbeat watcher to be started  to listen on h.watchHeartbeatChannel

	return registeredNodes, nil
}

func (h HeartBeatInstance) getRegisteredNodeInfo() ([]string, error) {
	var registeredNodes []string
	found, _, err := h.connection.Exists(h.parentHeartBeatPath)
	if found {
		registeredNodes, _, heartbeatChannel, err := h.connection.ChildrenW(h.parentHeartBeatPath) //[]string, *Stat, <-chan Event, error
		if err != nil {
			return nil, fmt.Errorf("Failed to fetch Registered Nodes for the Parent Hearbeat node [ %s ]... Reason: %v", h.parentHeartBeatPath, err)
		}
		h.watchHeartbeatChannel = heartbeatChannel
		registeredNodes = registeredNodes
	} else {
		return nil, fmt.Errorf("Unable to find the path for the Parent Hearbeat node [ %s ]... Reason: %v", h.parentHeartBeatPath, err)
	}
	return registeredNodes, nil
}

//Acquiring connection with the zookeeper cluster
func (h HeartBeatInstance) connect() error {
	if (h.connection == &zk.Conn{}) {
		conn, _, err := zk.Connect(h.conf.Servers, (time.Duration(h.conf.SessionTimeoutInSecond) * time.Second))

		if err != nil {
			return err
		}
		h.connection = conn
	}
	return nil
}

//Create a persistent Parent Node based on the service name from the configuration
//Parent Node will be persistent and will not be removed based on any service node going down
//TODO It will also create a supplimentary Node to keep track of all the services nodes that registered at any point in time
func (h HeartBeatInstance) createParentNode(path, data string) (*zk.Stat, error) {
	acl := zk.WorldACL(zk.PermAll)

	found, s, err := h.connection.Exists(path)

	if err != nil {
		return nil, err
	}

	if !found {
		_, err = h.connection.Create(path, []byte(data), 0, acl)

	}
	return s, err
}

//Create an empheral childnode Node based on the Parent node from the configuration
//Child Node is empheral which means that the node created will exist only till the zookeeper client/ Current service instance
// is attached and live.
//TODO It will also add child nodes to the supplimentatry parent node for tracking registered nodes.
func (h HeartBeatInstance) createChildEmpheralNode(path, data string) (*zk.Stat, error) {
	acl := zk.WorldACL(zk.PermAll)
	flag := int32(zk.FlagEphemeral)
	path = h.parentHeartBeatPath + "/" + path
	found, s, err := h.connection.Exists(path)
	if err != nil {
		return nil, err
	}

	if !found {
		_, err = h.connection.Create(path, []byte(data), flag, acl)
	}
	return s, err
}

func (h HeartBeatInstance) watchHeartBeat() {
	for {
		select {
		case heartbeatEvent := <-h.watchHeartbeatChannel:
			h.heartbeatHandler(heartbeatEvent)
		default:

		}
	}
}
