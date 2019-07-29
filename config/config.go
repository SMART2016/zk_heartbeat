package config

//Config : Config is a struct to keep all the configuration about the Sync Service, needed while connecting to Servers
type HeartBeatConfig struct {
	Servers                []string
	SessionTimeoutInSecond int
	ServiceName            string
}
