package config

//Config : Config is a struct to keep all the configuration about the Sync Service, needed while connecting to Servers
type HeartBeatConfig struct {
	Servers                   []string //The ZK cluster to connect to
	SessionTimeoutInSecond    int      //The empheral node is removed after this duration post client and zk connection is broken
	ServiceName               string   //This will be used to create the Parent Heartbeat node in zk
	EventRefreshTimeInSeconds int      //This is the time when the heartbeat event watcher and registration event watcher will wait before fetching the next even from zk
}
