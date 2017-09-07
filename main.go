package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

type partition struct {
	Topic      string `json:"topic"`
	PartitonNo int    `json:"partition"`
	Replicas   []int  `json:"replicas"`
}

type parttionreassignment struct {
	Version    int         `json:"version"`
	Partitions []partition `json:"partitions"`
}

var (
	topic             = os.Getenv("topic")
	numofpartitions   = os.Getenv("num_partitions")
	brokeridstart     = os.Getenv("brokerid_start")
	replicatioincount = os.Getenv("replica_count")
	version           = os.Getenv("version")
)

func main() {
	fmt.Println("tst")

	nump, err := strconv.Atoi(numofpartitions)
	if err != nil {
		fmt.Println("pass correct number of partitions")
		return
	}
	if topic == "" {
		fmt.Println("pass the correct topic")
		return
	}
	var rc int
	if replicatioincount == "" {
		rc = 3
	} else {
		rc, err = strconv.Atoi(replicatioincount)
		if err != nil {
			fmt.Println("enter valid replication count ")
			return
		}
	}

	bs, err := strconv.Atoi(brokeridstart)
	if err != nil {
		fmt.Println("passs the coorect broker id start")
	}

	pr := parttionreassignment{}
	v, _ := strconv.Atoi(version)
	if v == 0 {
		v = 1
	}
	pr.Version = v
	be := bs + nump - 1

	for i := 0; i < nump; i++ {
		p := partition{}
		p.Topic = topic
		p.PartitonNo = i
		leader := bs + p.PartitonNo

		for j := 0; j < rc; j++ {
			replicaid := leader + j
			if replicaid > be {
				replicaid = replicaid - nump
			}
			p.Replicas = append(p.Replicas, replicaid)
		}
		pr.Partitions = append(pr.Partitions, p)
	}
	result, _ := json.Marshal(pr)
	fmt.Println(string(result))

}
