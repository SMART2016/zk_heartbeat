# zk_heartbeat
Heartbeat implementation with Zookeeper

## Docker exec
docker exec -it <container-id> /bin/bash

## mockgen command: 
> version: commit hash for mock: 600781dde9cca80734169b9e969d9054ccc57937
> mockgen -source=/home/ankita/go/src/github.com/ContinuumLLC/zk_heartbeat/heartbeat/zk_heart_beat.go -destination=mocks/mock_zk_heart_beat.go -package=mocks github.com/ContinuumLLC/zk_heartbeat/heartbeat Controller