package upstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/spf13/viper"
	"github.com/xjdrew/glog"
)

var _DB HostDB = HostDB{
	tables: map[string]*HostTable{},
}

type HostRecord struct {
	Name    string `json:"name"`
	Host    string `json:"host"`
	Port    int    `json:"port"`
	Weight  int    `json:"weight"`
	Version string `json:"version"`

	numVer uint64
	strVer string
	addr   string
	key    string
}

type HostTable struct {
	weight  int
	numVer  uint64
	recList []*HostRecord
}

func (p *HostTable) put(rec *HostRecord) {
	for _, exist := range p.recList {
		if exist.key == rec.key {
			exist.addr = rec.addr
			return
		}
	}

	p.weight += rec.Weight
	p.recList = append(p.recList, rec)
}

func (p *HostTable) delete(key string) bool {
	for i, rec := range p.recList {
		if rec.key == key {
			p.weight -= rec.Weight
			p.recList = append(p.recList[:i], p.recList[i+1:]...)
			return true
		}
	}
	return false
}

func (p *HostTable) query() string {
	// 主机全部离线
	if len(p.recList) == 0 {
		return ""
	}

	// 停用按权重分配
	// w := rand.Intn(p.weight)
	// for _, rec := range p.records {
	// 	if rec.Weight >= w {
	// 		return rec.addr
	// 	}
	// 	w -= rec.Weight
	// }
	// return ""

	// 随机分配1个
	i := rand.Intn(len(p.recList))
	return p.recList[i].addr
}

type HostDB struct {
	mu     sync.RWMutex
	tables map[string]*HostTable // key: client version string
}

func (p *HostDB) put(rec *HostRecord) {
	defer p.mu.Unlock()
	p.mu.Lock()

	tb := p.tables[rec.strVer]
	if tb == nil {
		tb = &HostTable{numVer: rec.numVer}
		p.tables[rec.strVer] = tb
	}

	tb.put(rec)
}

func (p *HostDB) delete(key string) {
	defer p.mu.Unlock()
	p.mu.Lock()

	for _, tb := range p.tables {
		if tb.delete(key) {
			break
		}
	}
}

func (p *HostDB) query(strVer string) string {
	defer p.mu.RUnlock()
	p.mu.RLock()

	if len(p.tables) == 0 {
		return ""
	}

	// 版本精确匹配
	if tb := p.tables[strVer]; tb != nil {
		return tb.query()
	}

	// 查找最临近的最小版本主机表
	numVer, err := toNumVer(strVer)
	if err != nil {
		glog.Errorf("invalid version from client: %v", strVer)
		glog.Flush()
		return ""
	}

	tables := []*HostTable{}
	for _, tb := range p.tables {
		tables = append(tables, tb)
	}

	sort.Slice(tables, func(i, j int) bool {
		lop := tables[i]
		rop := tables[j]
		return lop.numVer > rop.numVer
	})

	for _, tb := range tables {
		if numVer > tb.numVer {
			return tb.query()
		}
	}

	// 前端上传的版本号 比最小版本服还小
	return ""
}

func toNumVer(strVer string) (uint64, error) {
	strs := strings.Split(strVer, ".")
	if len(strs) != 3 {
		return 0, errors.New("invalid version string")
	}

	major, _ := strconv.Atoi(strs[0])
	minor, _ := strconv.Atoi(strs[1])
	revision, _ := strconv.Atoi(strs[2])

	// 版本号每个分量各16bit
	return (uint64(major) << 32) | (uint64(minor) << 16) | uint64(revision), nil
}

func parseStrVer(s string) string {
	return s[1:]
}

func parseHost(key, value []byte) (*HostRecord, error) {
	rec := &HostRecord{}
	err := json.Unmarshal(value, rec)
	if err != nil {
		return rec, err
	}

	// 提取版本号
	strVer := rec.Version
	numVer, err := toNumVer(strVer)
	if err != nil {
		return nil, errors.New("client version is invalid")
	}

	rec.strVer = strVer
	rec.numVer = numVer
	rec.addr = fmt.Sprintf("%v:%v", rec.Host, rec.Port)
	rec.key = string(key)
	return rec, nil
}

func openEtcd(etcdHost string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   []string{etcdHost},
		DialTimeout: 5 * time.Second,
	})
}

func getExistKeyValues(cli *clientv3.Client, etcdPrefix string) bool {
	resp, err := cli.Get(context.Background(), etcdPrefix, clientv3.WithPrefix())
	if err != nil {
		glog.Errorf("GET exist key values error: %v", err)
		glog.Flush()
		return false
	}

	for _, kv := range resp.Kvs {
		rec, err := parseHost(kv.Key, kv.Value)
		if err != nil {
			glog.Errorf("GET decode error: %v, key: %v, value: %v", err, string(kv.Key), string(kv.Value))
			glog.Flush()
			return false
		}

		glog.Infof("GET host key: %v, hostport: %v:%v", string(kv.Key), rec.Host, rec.Port)
		glog.Flush()
		_DB.put(rec)
	}

	return true
}

func watchEtcd(etcdHost, etcdPrefix string) {
	var cli *clientv3.Client
	var err error

	for {
		glog.Infof("begin connect etcd host %v", etcdHost)
		glog.Flush()

		cli, err = openEtcd(etcdHost)
		if err != nil {
			glog.Errorf("connect etcd host %v error: %v", etcdHost, err)
			glog.Flush()
			time.Sleep(time.Second)
			continue

		} else {
			glog.Infof("connect etcd host %v succeed", etcdHost)
			glog.Flush()

			if !getExistKeyValues(cli, etcdPrefix) {
				cli.Close()
				continue
			}

			break
		}
	}

	wch := cli.Watch(context.Background(), etcdPrefix, clientv3.WithPrefix())
	for msg := range wch {
		for _, ev := range msg.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				rec, err := parseHost(ev.Kv.Key, ev.Kv.Value)
				if err != nil {
					glog.Errorf("put event decode error: %v, key: %v, value: %v", err, string(ev.Kv.Key), string(ev.Kv.Value))
					glog.Flush()
					continue
				}
				glog.Infof("PUT host key: %v, hostport: %v:%v", string(ev.Kv.Key), rec.Host, rec.Port)
				glog.Flush()
				_DB.put(rec)

			case clientv3.EventTypeDelete:
				glog.Infof("DEL host %v", string(ev.Kv.Key))
				glog.Flush()
				_DB.delete(string(ev.Kv.Key))

			default:
				glog.Errorf("unexpected etcd event: %v", ev.Type)
				glog.Flush()
			}
		}
	}

	cli.Close()
	glog.Errorf("disconnect from etcd host: %v", etcdHost)
	glog.Flush()
}

// 模块api
func WatchHost() {
	etcdHost := viper.GetString("etcd_host")
	if etcdHost == "" {
		glog.Exit("etcd_host not found in config")
	}

	etcdPrefix := viper.GetString("etcd_prefix")
	if etcdPrefix == "" {
		glog.Exit("etcd_prefix not found in config")
	}

	go func() {
		for {
			watchEtcd(etcdHost, etcdPrefix)
		}
	}()

	glog.Infof("waiting for the first upstream host be online: %v", etcdHost)
	glog.Flush()
}

func QueryHost(strVer string) string {
	return _DB.query(strVer)
}
