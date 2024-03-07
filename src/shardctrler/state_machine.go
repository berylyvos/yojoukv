package shardctrler

import "sort"

type CtrlerSM struct {
	Configs []Config
}

func NewCtrlerSM() *CtrlerSM {
	sm := &CtrlerSM{Configs: make([]Config, 1)}
	sm.Configs[0] = DefaultConfig()
	return sm
}

func (sm *CtrlerSM) Query(num int) (Config, Err) {
	if num < 0 || num >= len(sm.Configs) {
		return sm.Configs[len(sm.Configs)-1], OK
	}
	return sm.Configs[num], OK
}

func (sm *CtrlerSM) Join(groups map[int][]string) Err {
	newConfig, _ := sm.newConfig(false)

	// join new groups
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	gidShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidShards[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidShards[gid] = append(gidShards[gid], shard)
	}

	// rebalance shards: move max to min each time
	for {
		maxGID, minGID := maxShardNumGID(gidShards), minShardNumGID(gidShards)
		if maxGID != 0 && len(gidShards[maxGID])-len(gidShards[minGID]) <= 1 {
			break
		}
		gidShards[minGID] = append(gidShards[minGID], gidShards[maxGID][0])
		gidShards[maxGID] = gidShards[maxGID][1:]
	}

	var newShards [NShards]int
	for gid, shards := range gidShards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	sm.Configs = append(sm.Configs, newConfig)

	return OK
}

func (sm *CtrlerSM) Leave(gids []int) Err {
	newConfig, gidShards := sm.newConfig(true)

	var freeShards []int
	for _, gid := range gids {
		delete(newConfig.Groups, gid)
		if shards, ok := gidShards[gid]; ok {
			freeShards = append(freeShards, shards...)
			delete(gidShards, gid)
		}
	}

	var newShards [NShards]int
	if len(newConfig.Groups) != 0 {
		for _, shard := range freeShards {
			minGID := minShardNumGID(gidShards)
			gidShards[minGID] = append(gidShards[minGID], shard)
		}
		for gid, shards := range gidShards {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}

	newConfig.Shards = newShards
	sm.Configs = append(sm.Configs, newConfig)
	return OK
}

func (sm *CtrlerSM) Move(shard, gid int) Err {
	newConfig, _ := sm.newConfig(false)
	newConfig.Shards[shard] = gid
	sm.Configs = append(sm.Configs, newConfig)
	return OK
}

func (sm *CtrlerSM) newConfig(needMap bool) (cfg Config, gidShards map[int][]int) {
	num := len(sm.Configs)
	lastConfig := sm.Configs[num-1]

	cfg = Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}
	if !needMap {
		return
	}

	gidShards = make(map[int][]int)
	for gid := range cfg.Groups {
		gidShards[gid] = make([]int, 0)
	}
	for shard, gid := range cfg.Shards {
		gidShards[gid] = append(gidShards[gid], shard)
	}

	return
}

func copyGroups(groups map[int][]string) map[int][]string {
	newGroup := make(map[int][]string, len(groups))
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroup[gid] = newServers
	}
	return newGroup
}

func maxShardNumGID(gidShards map[int][]int) int {
	// if group-0 has shards, meaning that there's unassigned shards
	if g0shards, ok := gidShards[0]; ok && len(g0shards) > 0 {
		return 0
	}

	var gids []int
	for gid := range gidShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	maxGID, maxShardNum := -1, -1
	for _, gid := range gids {
		if len(gidShards[gid]) > maxShardNum {
			maxShardNum, maxGID = len(gidShards[gid]), gid
		}
	}
	return maxGID
}

func minShardNumGID(gidShards map[int][]int) int {
	var gids []int
	for gid := range gidShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	minGID, minShardNum := -1, NShards+1
	for _, gid := range gids {
		if gid != 0 && len(gidShards[gid]) < minShardNum {
			minShardNum, minGID = len(gidShards[gid]), gid
		}
	}
	return minGID
}
