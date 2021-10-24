package vrr

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type Server struct {
	mu sync.Mutex

	ID            int
	configuration map[int]string

	replica  *Replica
	rpcProxy *RPCProxy

	rpcServer *rpc.Server
	listener  net.Listener

	peerClients map[int]*rpc.Client

	ready <-chan interface{}
	quit  chan interface{}
	wg    sync.WaitGroup
}

type RPCProxy struct {
	r *Replica
}

func NewServer(ready <-chan interface{}) *Server {
	s := new(Server)
	s.peerClients = make(map[int]*rpc.Client)
	s.ready = ready
	s.quit = make(chan interface{})

	return s
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.replica = NewReplica(s.ID, s.configuration, s, s.ready)

	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{r: s.replica}
	s.rpcServer.RegisterName("Replica", s.rpcProxy)

	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("new server listens at %s", s.listener.Addr())
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error: ", err)
				}
			}
			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

func (s *Server) Shutdown() {
	s.replica.Stop()
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

func (s *Server) ConnectToPeer(peerID int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerID] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			log.Printf("[%d] Error ConnectToPeer peerID = %d: %s", s.ID, peerID, err.Error())
			return err
		}
		s.peerClients[peerID] = client
	}
	return nil
}

func (s *Server) DisconnectPeer(peerID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerID] != nil {
		err := s.peerClients[peerID].Close()
		if err != nil {
			return err
		}
		s.peerClients[peerID] = nil
	}
	return nil
}

func (s *Server) Call(ID int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[ID]
	s.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("call client %d after it is closed", ID)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

func (rpp *RPCProxy) Commit(args CommitArgs, reply *CommitReply) error {
	time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)

	return rpp.r.Commit(args, reply)
}

func (rpp *RPCProxy) StartViewChange(args StartViewChangeArgs, reply *StartViewChangeReply) error {
	time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)

	return rpp.r.StartViewChange(args, reply)
}

func (rpp *RPCProxy) DoViewChange(args DoViewChangeArgs, reply *DoViewChangeReply) error {
	time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)

	return rpp.r.DoViewChange(args, reply)
}

func (rpp *RPCProxy) StartView(args StartViewArgs, reply *StartViewReply) error {
	time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)

	return rpp.r.StartView(args, reply)
}

func (rpp *RPCProxy) StateTransfer(args GetStateArgs, reply *NewStateReply) error {
	time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)

	return rpp.r.StateTransfer(args, reply)
}
