package heartbeat

import (
	"fmt"
	"time"

	"github.com/ContinuumLLC/zk_heartbeat/heartbeat/config"
	"github.com/samuel/go-zookeeper/zk"
)

var connection1 *zk.Conn

//Custom function that the service will implement to handle hearbeat eventsand new node registration
type HeartbeatChangeHandler func(*zk.Conn, Controller, zk.Event)
type NodeRegisterHandler func(*zk.Conn, Controller, zk.Event)

type Controller interface {
	RegisterServiceHeartBeat(string, string) ([]string, []string, error)
	SetHeartbeatHandler(HeartbeatChangeHandler)
	SetNoderegisterHandler(NodeRegisterHandler)
	UpdateActualregisteredNodes(newTotalregisterednodes []string)
	UpdateLiveNodes(newLibeNodes []string)
	GetActualregisteredNodes() []string
	GetLiveNodes() []string
}

//conf: 						The heartbeat config that we need to set for connect and talking to zk
//parentHeartBeatPath: 			The path for the parent heartbeat node where the child empheral volatile nodes will be registered
//suppParentPath: 				The path for the supplementary parent which will be holding permanent service nodes that were registered during there startup.
//heartbeatChannel:				The channel where zk sends heartbeat change events when any service node gets added or removed
//nodeRegChannel:				The channel where zk sends Registration change events when any service node gets registered
//heartbeatHandler:				The handler function which is a hook implemented by the service to do something with the heartbeat event, if none set then default will act.
//nodeRegistrationHandler:		The handler function which is a hook implemented by the service to do something with the node registration event, if none set then default will act.
//totalRegisteredNodes:			The set of service nodes that were registered throughout
//totalLiveNodes:				The set of service nodes that are live at any moment in time
type HeartBeatController struct {
	conf                    config.HeartBeatConfig
	parentHeartBeatPath     string
	suppParentPath          string
	connection              *zk.Conn
	heartbeatChannel        <-chan zk.Event
	nodeRegChannel          <-chan zk.Event
	heartbeatHandler        HeartbeatChangeHandler
	nodeRegistrationHandler NodeRegisterHandler
	totalRegisteredNodes    []string
	totalLiveNodes          []string
}

func GetHeartBeatController(heartBeatConf config.HeartBeatConfig) Controller {
	controller := &HeartBeatController{
		conf:                    heartBeatConf,
		heartbeatHandler:        handleHearBeat,
		nodeRegistrationHandler: handleNewNodeRegistration,
	}

	return controller
}

func (h *HeartBeatController) SetHeartbeatHandler(heartbeatFunc HeartbeatChangeHandler) {
	h.heartbeatHandler = heartbeatFunc
}

func (h *HeartBeatController) SetNoderegisterHandler(nodeRegisterFunc NodeRegisterHandler) {
	h.nodeRegistrationHandler = nodeRegisterFunc
}

func (h *HeartBeatController) UpdateActualregisteredNodes(newTotalregisterednodes []string) {
	h.totalRegisteredNodes = newTotalregisterednodes
}

func (h *HeartBeatController) UpdateLiveNodes(newLibeNodes []string) {
	h.totalLiveNodes = newLibeNodes
}

func (h *HeartBeatController) GetActualregisteredNodes() []string {
	return h.totalRegisteredNodes
}

func (h *HeartBeatController) GetLiveNodes() []string {
	return h.totalLiveNodes
}

//HeartBeat Interface Function: Every Service instance cal call this function to register there Heartbeat functionality to Zookeeper
//The Heartbeat will be bounded to a single zookeeper persistent parent node based on the service name.
//The entire service cluster should be having same service name to leverage Heartbeat properly.
//pathID: any identifier which will uniquely identify the instance like IP or PodId
func (h *HeartBeatController) RegisterServiceHeartBeat(pathID, data string) ([]string, []string, error) {
	//Get ZK connection
	if err := h.connect(); err != nil {
		return nil, nil, fmt.Errorf("Error Getting zk cluster Connection. Reason: %v", err)
	}
	h.connection = connection1

	//Create Persistent Parent Node to hold live service nodes as colatile nodes and the child nodes will go away as the client is disconnected from ZK
	if _, err := h.createParentNode("/"+h.conf.ServiceName, h.conf.ServiceName); err != nil {
		return nil, nil, fmt.Errorf("Error Creating Parent zk HeartBeat Node. Reason: %v", err)
	} else {
		h.parentHeartBeatPath = "/" + h.conf.ServiceName
		h.suppParentPath = h.parentHeartBeatPath + "_childinfo"
	}

	//Create a child empheral node for the current instance to register to heatbeat contract.This is a volatile zk node
	if _, err := h.createChildEmpheralNode(pathID, data); err != nil {
		return nil, nil, fmt.Errorf("Error creating child Empheral node for current instance hearbeat. Reason: %v", err)
	}

	//Watch on the Parent hearbeat node and the supplimentary registration node to get the current live nodes and the total actual registered nodes.
	registeredLiveNodes, registeredTotalNodes, hearBeatChannel, actualNodeRegChannel, err := h.getRegisteredNodeInfo()
	if err != nil {
		return nil, nil, fmt.Errorf("Error Fetching Registered nodes for the Parent Hearbeat node [ %s ]... Reason: %v", h.parentHeartBeatPath, err)
	}
	h.heartbeatChannel = hearBeatChannel
	h.nodeRegChannel = actualNodeRegChannel
	h.totalLiveNodes = registeredLiveNodes
	h.totalRegisteredNodes = registeredTotalNodes

	fmt.Println("channels", h.heartbeatChannel)
	fmt.Println("channels", h.nodeRegChannel)

	//TODO heartbeat watcher to be started  to listen on h.heartbeatChannel and h.nodeRegChannel
	go h.watchHeartBeat()
	go h.watchNodeRegistration()

	return registeredLiveNodes, registeredTotalNodes, nil
}

//This method returns the heartbeat channel to which the current node should listen, to identify if any change happened in other service nodes in the cluster.
//When called for the first time its also returns the current live nodes registered for the service cluster.
func (h *HeartBeatController) getRegisteredNodeInfo() (registeredLiveNodes []string, registeredActualNodes []string, heartbeatChannel <-chan zk.Event, actualNodeRegChannel <-chan zk.Event, err error) {

	//Getting the current live node info
	found, _, err := h.connection.Exists(h.parentHeartBeatPath)
	if found {
		registeredLiveNodes, _, heartbeatChannel, err = h.connection.ChildrenW(h.parentHeartBeatPath) //[]string, *Stat, <-chan Event, error
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("Failed to fetch Registered Nodes for the Parent Hearbeat node [ %s ]... Reason: %v", h.parentHeartBeatPath, err)
		}
		//Getting info for the total number of nodes registered for the service cluster
		foundSupp, _, err := h.connection.Exists(h.suppParentPath)
		if foundSupp {
			registeredActualNodes, _, actualNodeRegChannel, err = h.connection.ChildrenW(h.suppParentPath) //[]string, *Stat, <-chan Event, error
			if err != nil {
				return nil, nil, nil, nil, fmt.Errorf("Failed to fetch Registered Nodes for the Parent Hearbeat node [ %s ]... Reason: %v", h.parentHeartBeatPath, err)
			}

		} else {
			return nil, nil, nil, nil, fmt.Errorf("Unable to find the path for the Suuplementary Parent Hearbeat node [ %s ]... Reason: %v", h.suppParentPath, err)
		}

	} else {
		return nil, nil, nil, nil, fmt.Errorf("Unable to find the path for the Parent Hearbeat node [ %s ]... Reason: %v", h.parentHeartBeatPath, err)
	}
	return registeredLiveNodes, registeredActualNodes, heartbeatChannel, actualNodeRegChannel, nil
}

//Acquiring connection with the zookeeper cluster
func (h *HeartBeatController) connect() error {
	if h.connection == nil {
		conn, _, err := zk.Connect(h.conf.Servers, (time.Duration(h.conf.SessionTimeoutInSecond) * time.Second))

		if err != nil {
			return err
		}
		connection1 = conn
	}
	return nil
}

//Create a persistent Parent Node based on the service name from the configuration
//Parent Node will be persistent and will not be removed based on any service node going down
//TODO It will also create a supplimentary Node to keep track of all the services nodes that registered at any point in time
func (h *HeartBeatController) createParentNode(path, data string) (*zk.Stat, error) {
	acl := zk.WorldACL(zk.PermAll)

	found, s, err := h.connection.Exists(path)

	if err != nil {
		return nil, err
	}

	if !found {
		_, err = h.connection.Create(path, []byte(data), 0, acl)
		if err != nil {
			return nil, fmt.Errorf("Error creating path: %s... Reason: %v", path, err)
		}
		supplParentPath := path + "_childinfo"
		found, s, err = h.connection.Exists(supplParentPath)
		if err != nil {
			return nil, err
		}

		if !found {
			suppParentNodeData := ""
			_, err = h.connection.Create(supplParentPath, []byte(suppParentNodeData), 0, acl)
			if err != nil {
				return nil, fmt.Errorf("Error creating supplimentary Parent info path: %s... Reason: %v", supplParentPath, err)
			}

		}
	}

	return s, err
}

//Create an empheral childnode Node based on the Parent node from the configuration
//Child Node is empheral which means that the node created will exist only till the zookeeper client/ Current service instance
// is attached and live.
//TODO It will also add child nodes to the supplimentatry parent node for tracking registered nodes.
func (h *HeartBeatController) createChildEmpheralNode(path, data string) (*zk.Stat, error) {
	acl := zk.WorldACL(zk.PermAll)
	flag := int32(zk.FlagEphemeral)
	childpath := h.parentHeartBeatPath + "/" + path
	suppChildPath := h.suppParentPath + "/" + path

	found, s, err := h.connection.Exists(childpath)
	if err != nil {
		return nil, fmt.Errorf("Error While checking child empheral path [ %s ] for existance.. Reason: %v", path, err)
	}

	if !found {
		_, err = h.connection.Create(childpath, []byte(data), flag, acl)
		if err != nil {
			return nil, fmt.Errorf("Error creating child empheral node for path : [ %s ].. Reason: %v", path, err)
		}
		found, s, err = h.connection.Exists(suppChildPath)
		if err != nil {
			return nil, fmt.Errorf("Error While checking Supplementary child path [ %s ] for existance.. Reason: %v", suppChildPath, err)
		}
		if !found {
			suppchildNodeData := ""
			_, err = h.connection.Create(suppChildPath, []byte(suppchildNodeData), 0, acl)
			if err != nil {
				return nil, fmt.Errorf("Error creating supplimentary child info path: %s... Reason: %v", suppChildPath, err)
			}

		}
	}
	return s, err
}

//Watcher: to handle the heartbeat change event recieved from the Parent node
func (h *HeartBeatController) watchHeartBeat() {
	for {
		heartbeatEvent := <-h.heartbeatChannel
		h.heartbeatHandler(h.connection, h, heartbeatEvent)

		time.Sleep(time.Duration(h.conf.EventRefreshTimeInSeconds) * time.Second)
	}
}

func handleHearBeat(conn *zk.Conn, controller Controller, hearBeatEvent zk.Event) {
	fmt.Println("HeartbeatEvent: ", hearBeatEvent)
	if hearBeatEvent.Err != nil {
		fmt.Println("HeartBeat Event error:", hearBeatEvent.Err)
	} else {
		liveNodes, _, err := conn.Children(hearBeatEvent.Path)
		if err != nil {
			fmt.Println("HeartbeatEvent: Error: ", err)
		} else {
			fmt.Println("HeartbeatEvent: total Live Nodes: ", liveNodes)
			controller.UpdateLiveNodes(liveNodes)
		}
	}
}

//Watcher: to handle the Registration of new nodes in the cluster
func (h *HeartBeatController) watchNodeRegistration() {
	for {
		registration := <-h.nodeRegChannel
		h.nodeRegistrationHandler(h.connection, h, registration)
		time.Sleep(time.Duration(h.conf.EventRefreshTimeInSeconds) * time.Second)
	}
}

func handleNewNodeRegistration(conn *zk.Conn, controller Controller, registerEvent zk.Event) {
	fmt.Println("RegistrationEvent: ", registerEvent)
	if registerEvent.Err != nil {
		fmt.Println("Register Event error:", registerEvent.Err)
	} else {
		totalChilds, _, err := conn.Children(registerEvent.Path)
		if err != nil {
			fmt.Println("RegistrationEvent: Error: ", err)
		} else {
			fmt.Println("RegistrationEvent: total registrations: ", totalChilds)
			controller.UpdateActualregisteredNodes(totalChilds)
		}
	}
}
