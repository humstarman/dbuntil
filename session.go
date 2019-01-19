package dbutil

import (
	"fmt"
	"log"
	"os"

	"github.com/go-redis/redis"
	"github.com/gocql/gocql"
)

const (
	Keyspace = "gmt"
	Column   = "history"
	Rport0   = "6379"
	Rport1   = "6380"
	Cport    = 9042
	List     = "newbie"
	Rip      = "127.0.0.1"
)

type Session struct {
	Key    string
	Value  string
	Client *redis.ClusterClient
}

func CreateSession(vals ...string) (*Session, error) {
	s := Session{}
	s.Key = vals[0]
	if len(vals) < 2 {
		s.Value = ""
	} else {
		s.Value = vals[1]
	}
	addrs := make([]string, 2, 2)
	addrs[0] = fmt.Sprintf("%v:%v", Rip, Rport0)
	addrs[1] = fmt.Sprintf("%v:%v", Rip, Rport1)
	log.Printf("Redis cluster addrs: %v\n", addrs)
	o := redis.ClusterOptions{
		Addrs: addrs,
	}
	s.Client = redis.NewClusterClient(&o)
	//defer client.Close()
	_, err := s.Client.Ping().Result()
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (this *Session) GetFromCassandra() error {
	ip := os.Getenv("POD_IP")
	cluster := gocql.NewCluster(fmt.Sprintf("%v", ip))
	cluster.Keyspace = Keyspace
	cluster.Port = Cport
	log.Printf("Cassndra using: %v.%v\n", Keyspace, Column)
	//cluster.Consistency = gocql.Quorum
	cluster.Consistency = gocql.One
	session, err := cluster.CreateSession()
	defer session.Close()
	var v string
	err = session.Query(`SELECT value FROM gmt.history WHERE key = ? LIMIT 1`, this.Key).Consistency(gocql.One).Scan(&v)
	if err != nil {
		log.Println(err)
		return err
	}
	this.Value = v
	return nil
}

func (this *Session) Get() (string, error) {
	defer this.Client.Close()
	v, err := this.Client.Get(this.Key).Result()
	this.Value = v
	if err == nil {
		// 2.1 if succ, ret
		fmt.Printf("%v,%v\n", this.Key, this.Value)
		return this.Value, err
	}
	if err.Error() != "redis: nil" {
		// 2.2 if failed with other reasons, ret
		log.Println(err)
		return "", err
	}
	// 2.3 if not exists, get from Cassandra
	log.Println("not in Redis, try Cassandra")
	err = this.GetFromCassandra()
	if err != nil {
		log.Println(err)
		return "", err
	}
	err = this.Put()
	if err != nil {
		log.Println(err)
		return "", err
	}
	return this.Value, err
}

func (this *Session) Put() error {
	defer this.Client.Close()
	err := this.Client.Set(this.Key, this.Value, 0).Err()
	if err != nil {
		log.Println(err)
		return err
	}
	err = this.Client.LPush(List, this.Key).Err()
	if err != nil {
		err2 := this.Client.Del(this.Key).Err()
		if err2 != nil {
			log.Println(err)
			return err
		}
		log.Println(err)
		return err
	}
	return nil
}
