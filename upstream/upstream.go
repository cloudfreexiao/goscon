package upstream

import (
	"errors"
	"math/rand"
	"net"
	"regexp"
	"sync/atomic"

	"github.com/ejoy/goscon/scp"
	"github.com/xjdrew/glog"
)

// ErrNoHost .
var ErrNoHost = errors.New("no host")

const defaultWeight = 100

// ResolveRule describes upstream resolver config
type ResolveRule struct {
	// prefix + name + suffix provides the domain name.
	Prefix string
	Suffix string

	Port string

	// The `targetServer` name must match the pattern.
	Pattern   string
	rePattern *regexp.Regexp
}

var errNoPort = errors.New("no port")

// Normalize .
func (r *ResolveRule) Normalize() error {
	if r.Pattern != "" {
		rePattern, err := regexp.Compile(r.Pattern)
		if err != nil {
			return err
		}
		r.rePattern = rePattern
	}
	if r.Port == "" {
		return errNoPort
	}
	return nil
}

// FullName returns hostport defined by rule.
func (r *ResolveRule) FullName(name string) string {
	return net.JoinHostPort(r.Prefix+name+r.Suffix, r.Port)
}

// Validate validates the `targetServer` name.
func (r *ResolveRule) Validate(name string) bool {
	if r.rePattern == nil {
		return true
	}
	return r.rePattern.MatchString(name)
}

// Option describes upstream option
type Option struct {
	Net    string
	Resolv *ResolveRule
}

// Host indicates a backend server
type Host struct {
	Name   string
	Addr   string
	Weight int

	addrs []*net.TCPAddr
}

type hostGroup struct {
	hosts  []*Host
	weight int
}

// upstreams 代表后端服务
type upstreams struct {
	option atomic.Value // *Option

	allHosts    atomic.Value // *hostGroup
	byNameHosts atomic.Value // map[string]*hostGroup
}

// SetOption .
func (u *upstreams) SetOption(option *Option) {
	u.option.Store(option)
}

// reference to the host:port format of `net.Dial`.
func lookupTCPAddrs(hostport string) ([]*net.TCPAddr, error) {
	host, service, err := net.SplitHostPort(hostport)
	if err != nil {
		return nil, err
	}
	addrs, err := net.LookupHost(host)
	if err != nil {
		return nil, err
	}
	tcpAddrs := make([]*net.TCPAddr, len(addrs))
	for i, addr := range addrs {
		addr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(addr, service))
		if err != nil { // only error when lookup port failed
			return nil, err
		}
		tcpAddrs[i] = addr
	}
	return tcpAddrs, nil
}

// UpdateHosts .
func (u *upstreams) UpdateHosts(option *Option, hosts []Host) error {
	sz := len(hosts)
	if option.Resolv == nil && sz == 0 {
		return ErrNoHost
	}
	allHosts := new(hostGroup)
	allHosts.hosts = make([]*Host, 0, sz)
	allHosts.weight = 0

	byNameHosts := make(map[string]*hostGroup)
	for _, host := range hosts {
		h := host
		addrs, err := lookupTCPAddrs(h.Addr)
		if err != nil {
			return err
		}
		h.addrs = addrs
		if h.Weight <= 0 {
			// set default weight
			h.Weight = defaultWeight
		}
		allHosts.hosts = append(allHosts.hosts, &h)
		allHosts.weight = allHosts.weight + h.Weight

		if h.Name != "" {
			hg := byNameHosts[h.Name]
			if hg == nil {
				hg = new(hostGroup)
				byNameHosts[h.Name] = hg
			}
			hg.hosts = append(hg.hosts, &h)
			hg.weight = hg.weight + h.Weight
		}
	}

	u.allHosts.Store(allHosts)
	u.byNameHosts.Store(byNameHosts)
	return nil
}

func chooseByLocalHosts(group *hostGroup) *Host {
	if group == nil || len(group.hosts) == 0 {
		return nil
	}

	v := rand.Intn(group.weight)
	for _, host := range group.hosts {
		if host.Weight >= v {
			return host
		}
		v -= host.Weight
	}
	return nil
}

func chooseByResolver(name string, rule *ResolveRule) *Host {
	hosts := make([]*Host, 0, 2)
	if rule.Validate(name) {
		hostport := rule.FullName(name)
		addrs, err := lookupTCPAddrs(hostport)
		if err == nil {
			h := Host{
				Name:  name,
				Addr:  hostport,
				addrs: addrs,
			}
			hosts = append(hosts, &h)
		}
	}
	lenhosts := len(hosts)
	if lenhosts == 0 {
		return nil
	}
	return hosts[rand.Intn(lenhosts)]
}

// GetPreferedHost choose a host by name, if several hosts have same
// name then random choose by weight
func (u *upstreams) GetPreferredHost(name string) *Host {
	mapHosts := u.byNameHosts.Load().(map[string]*hostGroup)
	h := chooseByLocalHosts(mapHosts[name])
	if h != nil {
		return h
	}
	option := u.option.Load().(*Option)
	if option.Resolv != nil {
		h = chooseByResolver(name, option.Resolv)
	}
	if h == nil {
		glog.Errorf("prefered name is malformed, name=%s", name)
	}
	return h
}

// GetRandomHost chooses a host randomly from all hosts.
func (u *upstreams) GetRandomHost() *Host {
	mapHosts := u.allHosts.Load().(*hostGroup)
	return chooseByLocalHosts(mapHosts)
}

// GetHost prefers static hosts map, and will use resolver if config.
// When preferred is empty string, GetHost only searches static hosts map.
func (u *upstreams) GetHost(preferred string) *Host {
	var h *Host
	if preferred != "" {
		h = u.GetPreferredHost(preferred)
	}
	if h == nil {
		h = u.GetRandomHost()
	}
	return h
}

func upgradeConn(network string, localConn net.Conn, remoteConn *scp.Conn) (conn net.Conn, err error) {
	if network == "scp" {
		scon, _ := scp.Client(localConn, &scp.Config{
			TargetServer: remoteConn.TargetServer(),
			Flag:         scp.SCPFlagForbidForwardIP,
		})

		err = scon.Handshake()
		if err != nil {
			glog.Errorf("scp handshake failed: client=%s, err=%s", scon.RemoteAddr().String(), err.Error())
			return
		}
		conn = scon
	} else {
		conn = localConn
	}
	return
}

// NewConn creates a new connection to target server, pair with remoteConn
func (u *upstreams) NewConn(remoteConn *scp.Conn) (conn net.Conn, err error) {
	//------------------------------------------
	// 2023.3.23 停用原版从配置的主机列表获取主机
	//------------------------------------------

	// tserver := remoteConn.TargetServer()
	// host := u.GetHost(tserver)
	// if host == nil {
	// 	err = ErrNoHost
	// 	glog.Errorf("get host <%s> failed: %s", tserver, err.Error())
	// 	return
	// }

	// rand.Shuffle(len(host.addrs), func(i, j int) { host.addrs[i], host.addrs[j] = host.addrs[j], host.addrs[i] })
	// var tcpConn *net.TCPConn
	// for _, addr := range host.addrs {
	// 	tcpConn, err = net.DialTCP("tcp", nil, addr)
	// 	if err == nil {
	// 		break
	// 	}
	// }
	// if err != nil {
	// 	glog.Errorf("connect to <%s> failed: %s", host.Addr, err.Error())
	// 	return
	// }

	//------------------------------------------------------------
	// 2023.3.23 从discovery.go实现的接口 获取一个host:port
	//      3.31 与特定版本逻辑服解耦 提升通用性
	//------------------------------------------------------------
	addr := QueryHost(remoteConn.Version)
	if addr == "" {
		err = ErrNoHost
		glog.Error("get host failed, cause none host is online")
		return
	}

	tcpConn, err := net.Dial("tcp", addr)
	if err != nil {
		glog.Errorf("connect to <%v> failed: %v", addr, err)
		return
	}

	option := u.option.Load().(*Option)
	conn, err = upgradeConn(option.Net, tcpConn, remoteConn)
	if err != nil {
		conn.Close()
		return
	}

	err = OnAfterConnected(conn, remoteConn)
	return
}

var defaultUpstreams upstreams

// SetOption sets option
func SetOption(option *Option) {
	defaultUpstreams.SetOption(option)
}

// UpdateHosts refresh backend hosts list
func UpdateHosts(option *Option, hosts []Host) error {
	return defaultUpstreams.UpdateHosts(option, hosts)
}

// NewConn create a new connection, pair with remoteConn
func NewConn(remoteConn *scp.Conn) (conn net.Conn, err error) {
	return defaultUpstreams.NewConn(remoteConn)
}
