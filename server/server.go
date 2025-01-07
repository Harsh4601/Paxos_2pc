package main

import (
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type ResponseMessage struct {
	Acknowledged bool
}

func init() {
	gob.Register(PrepareMessage{})
	gob.Register(PromiseMessage{})
	gob.Register(ResponseMessage{})
	gob.Register(Transaction{})
	gob.Register(BallotPair{})
	gob.Register(Server{})
	gob.Register(AcceptMessage{})
	gob.Register(AcceptedMessage{})
	gob.Register(CommitMessage{})
	gob.Register(FetchDataStoreRequest{})
	gob.Register(FetchDataStoreResponse{})
}

const (
	ClientsPerCluster = 1000
	Clusters          = 3
	ShardsPerCluster  = 3
	TotalShards       = Clusters * ShardsPerCluster
)

type PrepareMessage struct {
	BallotPair   BallotPair // Ballot number and Server ID
	Sender       string     // Name of the leader server
	SenderID     int        // Server ID of the leader
	LenDataStore int
}

type PromiseMessage struct {
	Ack          bool
	BallotPair   BallotPair
	AcceptNum    BallotPair
	AcceptVal    Transaction
	ServerId     int
	LenDataStore int
}

type AcceptMessage struct {
	BallotPair BallotPair
	MajorBlock Transaction
}

type AcceptedMessage struct {
	BallotPair BallotPair
	MajorBlock Transaction
}
type CommitMessage struct {
	// BallotPair BallotPair
	FinalBlock Transaction
	Balances   map[string]int
}

type FetchDataStoreRequest struct {
	ServerID     int
	LenDatastore int
}
type FetchDataStoreResponse struct {
	PendingTransaction map[int64]Transaction
}

type NetworkStatus struct {
	ActiveServers  map[string]bool
	ContactServers map[string]bool
}

type BallotPair struct {
	BallotNumber int `json:"ballot_number"`
	ServerID     int `json:"server_id"`
}

type Transaction struct {
	Txnid    int64  `json:"txnid"`
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
	Amount   int    `json:"amount"`
}

type DataStoreTxn struct {
	BP     BallotPair  `json:"bp"`
	Txn    Transaction `json:"txn"`
	Status string      `json:"status"`
}
type Server struct {
	Name                  string
	Port                  string
	ServerID              int
	Db_path               string
	ClientBalances        map[string]int
	ClientLocks           map[string]bool
	ClusterId             int
	ClusterServersPort    []string
	BallotPair            BallotPair
	ActiveServers         map[string]bool
	ContactServers        map[string]bool
	AcceptNum             BallotPair
	AcceptVal             Transaction
	WAL                   map[int64]Transaction
	PromisesReceived      int
	acceptedCount         int
	ExecutedTxn           map[int64]Transaction
	OngoingTxn            Transaction
	ActiveStatus          map[string]bool
	Mutex                 sync.Mutex
	Wg                    sync.WaitGroup
	PaxosStatus           bool
	highest_len_datastore int
	Highest_Len_Server_ID int
	txnExecuted           int
	totalLatency          time.Duration
	startTime             time.Time
	processedTxns         map[int64]bool
}

type PerformanceMetrics struct {
	Throughput           float64
	AverageLatency       float64 // in milliseconds
	TransactionsExecuted int
}

func getClusterForID(id int) int {
	if id >= 1 && id <= 1000 {
		return 1
	} else if id >= 1001 && id <= 2000 {
		return 2
	} else if id >= 2001 && id <= 3000 {
		return 3
	} else {
		return 0 // invalid ID
	}
}
func getClustersForTransaction(txn Transaction) []int {
	clusters := make(map[int]bool)

	senderID, err := strconv.Atoi(txn.Sender)
	if err == nil {
		cluster := getClusterForID(senderID)
		if cluster != 0 {
			clusters[cluster] = true
		}
	}

	receiverID, err := strconv.Atoi(txn.Receiver)
	if err == nil {
		cluster := getClusterForID(receiverID)
		if cluster != 0 {
			clusters[cluster] = true
		}
	}

	clustersList := []int{}
	for cluster := range clusters {
		clustersList = append(clustersList, cluster)
	}

	return clustersList
}

func (s *Server) HandleRequest(args *string, reply *string) error {
	*reply = "Response from server " + s.Name + " on port " + s.Port
	return nil
}

func (s *Server) AbortCrossShard(txnId int64, reply *bool) error {
	log.Printf("[%s] Abort Cross Shard Txn", s.Name)

	txnStartTime := time.Now()

	if _, exist := s.ClientBalances[s.AcceptVal.Sender]; exist {
		sender, _ := strconv.Atoi(s.AcceptVal.Sender)
		balance, _ := s.fetchClientBalance(sender)
		// fmt.Printf("[%s]  Old balances Sender %v: %d \n", s.Name, s.AcceptVal.Sender, balance)
		balance += s.AcceptVal.Amount
		s.updateClientBalance(sender, balance)
		fmt.Printf("[%s] New BALANCE Sender: %d \n", s.Name, balance)
	}
	if _, exist := s.ClientBalances[s.AcceptVal.Receiver]; exist {
		receiver, _ := strconv.Atoi(s.AcceptVal.Receiver)
		balance, _ := s.fetchClientBalance(receiver)
		// fmt.Printf("[%s]  Old balances Sender %v: %d \n", s.Name, s.AcceptVal.Sender, balance)
		balance -= s.AcceptVal.Amount
		s.updateClientBalance(receiver, balance)
		fmt.Printf("[%s] New BALANCE Receiver: %d \n", s.Name, receiver)
	}
	if _, exist := s.ClientLocks[s.WAL[txnId].Sender]; exist {

		s.ClientLocks[s.WAL[txnId].Sender] = false
	}
	if _, exist := s.ClientLocks[s.WAL[txnId].Receiver]; exist {
		s.ClientLocks[s.WAL[txnId].Receiver] = false
	}
	delete(s.WAL, txnId)

	txnLatency := time.Since(txnStartTime)
	s.totalLatency += txnLatency
	s.txnExecuted++

	*reply = false
	return nil

}

func (s *Server) CommitCrossShard(txnId int64, reply *bool) error {
	log.Printf("[%s] Commiting Cross Shard Txn", s.Name)

	txnStartTime := time.Now()

	s.ExecutedTxn[s.WAL[txnId].Txnid] = s.WAL[txnId]

	if _, exist := s.ClientLocks[s.WAL[txnId].Sender]; exist {

		sender, _ := strconv.Atoi(s.WAL[txnId].Sender)
		newTxn := DataStoreTxn{
			BP:     s.BallotPair,
			Txn:    s.WAL[txnId],
			Status: "C",
		}
		s.appendToClientDataStore(sender, newTxn)
		s.ClientLocks[s.WAL[txnId].Sender] = false
	}
	if _, exist := s.ClientLocks[s.WAL[txnId].Receiver]; exist {
		receiver, _ := strconv.Atoi(s.WAL[txnId].Receiver)
		newTxn := DataStoreTxn{
			BP:     s.BallotPair,
			Txn:    s.WAL[txnId],
			Status: "C",
		}
		s.appendToClientDataStore(receiver, newTxn)
		s.ClientLocks[s.WAL[txnId].Receiver] = false
	}
	delete(s.WAL, txnId)

	txnLatency := time.Since(txnStartTime)
	s.totalLatency += txnLatency
	s.txnExecuted++

	*reply = true

	return nil

}

func (s *Server) HandleClientRequest(txn Transaction, reply *bool) error {

	s.Mutex.Lock()
	s.Wg.Add(1)
	log.Printf("SERVER %s RECEIVED REQUEST FROM CLIENT: %+v", s.Name, txn)

	txnStartTime := time.Now()

	if len(getClustersForTransaction(txn)) == 2 {
		log.Printf("Cross SHard Transactions: %v", txn)
		s.initiatePaxos(txn)
	}
	if len(getClustersForTransaction(txn)) == 1 {
		log.Printf("Intra Shard Transactions: %v", txn)
		s.initiatePaxos(txn)
		// log.Print("LOCK UNLOCKED")
	}
	s.Wg.Wait()
	// s.initiatePaxos()

	txnLatency := time.Since(txnStartTime)
	s.totalLatency += txnLatency
	s.txnExecuted++

	*reply = s.PaxosStatus
	s.Mutex.Unlock()

	return nil

}

var minorBlock []Transaction

func (s *Server) initiatePaxos(txn Transaction) error {

	s.BallotPair.BallotNumber++
	s.OngoingTxn = txn
	s.PromisesReceived = 0
	s.acceptedCount = 0
	prepareMsg := PrepareMessage{
		BallotPair: BallotPair{
			BallotNumber: s.BallotPair.BallotNumber, // 1
			ServerID:     s.ServerID,
		},
		Sender:       s.Name,
		SenderID:     s.ServerID,
		LenDataStore: len(s.ExecutedTxn),
	}
	prepareMsg.LenDataStore = len(s.ExecutedTxn)
	minorBlock = nil
	// Sending the Prepare message to other servers
	// fmt.Printf("Server %s Sent Prepare Message: %v\n", s.Name, prepareMsg)
	inactiveserver := 0
	for _, i := range s.ClusterServersPort {
		go func(i string) {
			if len(i) != 0 { // Skip sending to itself
				addr := i
				client, err := rpc.Dial("tcp", addr)
				if err != nil {
					return
				}
				defer client.Close()

				var promiseMessage PromiseMessage

				err = client.Call("Server.HandlePrepareMessage", prepareMsg, &promiseMessage)
				if err != nil {
					if err.Error() == "inactive_leader" {
						inactiveserver++
						if inactiveserver == 2 {
							s.PaxosStatus = false
							s.Wg.Done()
						}
					}
				}
			}
		}(i)
	}
	return nil

}

func (s *Server) HandlePrepareMessage(prepareMsg *PrepareMessage, promiseMsg *PromiseMessage) error {

	// log.Print("Received Prepare Msg from Server: ", prepareMsg.Sender)
	if !s.ActiveStatus[s.Name] {
		log.Print("Server Inactive ", prepareMsg.Sender)
		s.BallotPair.BallotNumber = prepareMsg.BallotPair.BallotNumber
		return fmt.Errorf("inactive_leader")
	}
	// log.Print(len(s.ExecutedTxn), prepareMsg.LenDataStore)

	if len(s.ExecutedTxn) < prepareMsg.LenDataStore {
		log.Printf("[%s] Follower Sync Required \n", s.Name)
		// follower sync
		s.FollowerSync(prepareMsg.BallotPair.ServerID)

	} else if len(s.ExecutedTxn) > prepareMsg.LenDataStore {
		log.Printf("Leader Sync Required \n")
		promiseMsg.Ack = false
		promiseMsg.LenDataStore = len(s.ExecutedTxn)
		promiseMsg.ServerId = s.ServerID

		client, err := rpc.Dial("tcp", fmt.Sprintf("localhost:800%d", prepareMsg.BallotPair.ServerID))
		if err != nil {
			return fmt.Errorf("could not connect to leader %s: %v", prepareMsg.Sender, err)
		}
		defer client.Close()

		var responseMsg ResponseMessage
		err = client.Call("Server.ReceivePromise", promiseMsg, &responseMsg)
		if err != nil {
			return fmt.Errorf("error sending promise message to leader %s: %v", prepareMsg.Sender, err)
		}

	}

	if prepareMsg.BallotPair.BallotNumber > s.BallotPair.BallotNumber {

		promiseMsg.Ack = true
		promiseMsg.LenDataStore = len(s.ExecutedTxn)
		promiseMsg.ServerId = s.ServerID
		promiseMsg.BallotPair = BallotPair{
			BallotNumber: prepareMsg.BallotPair.BallotNumber,
			ServerID:     prepareMsg.BallotPair.ServerID,
		}

		promiseMsg.AcceptNum = s.AcceptNum
		promiseMsg.AcceptVal = s.AcceptVal

		s.BallotPair.BallotNumber = prepareMsg.BallotPair.BallotNumber // setting other server's ballot number equal to leader's ballot number

		client, err := rpc.Dial("tcp", fmt.Sprintf("localhost:800%d", prepareMsg.BallotPair.ServerID))
		if err != nil {
			return fmt.Errorf("could not connect to leader %s: %v", prepareMsg.Sender, err)
		}
		defer client.Close()

		var responseMsg ResponseMessage
		err = client.Call("Server.ReceivePromise", promiseMsg, &responseMsg)
		if err != nil {
			return fmt.Errorf("error sending promise message to leader %s: %v", prepareMsg.Sender, err)
		}
		// return nil

	}

	return nil
}

var promise_mu sync.Mutex

func (s *Server) ReceivePromise(promiseMsg *PromiseMessage, responseMsg *ResponseMessage) error {
	promise_mu.Lock()
	s.PromisesReceived++
	// log.Printf("[%s] Promise Received: %v", s.Name, s.PromisesReceived)

	responseMsg.Acknowledged = true
	majority := 2
	var flag_ongoing = false
	if s.PromisesReceived+1 == majority {
		// log.Printf("[%s] Waiting for more promises", s.Name)
		time.Sleep(50 * time.Millisecond)
		flag_ongoing = true
	}
	promise_mu.Unlock()
	// log.Print(promiseMsg)
	if promiseMsg.LenDataStore > s.highest_len_datastore {
		s.highest_len_datastore = promiseMsg.LenDataStore
		s.Highest_Len_Server_ID = promiseMsg.ServerId
	}
	if s.PromisesReceived+1 >= (majority) && flag_ongoing {
		flag_ongoing = false

		if len(s.ExecutedTxn) < s.highest_len_datastore {
			// log.Printf("I AM UNDERGOING SYNCING from: %v", s.Highest_Len_Server_ID)
			s.PromisesReceived = 0
			s.acceptedCount = 0
			log.Print(s.PromisesReceived)
			minorBlock = []Transaction{}
			s.FollowerSync(s.Highest_Len_Server_ID)
			return nil
		}
		if s.PromisesReceived+1 >= majority {
			// log.Printf("[%s] Okay 1", s.Name)
			// log.Print(getClustersForTransaction(s.OngoingTxn))
			if len(getClustersForTransaction(s.OngoingTxn)) == 1 {
				// log.Printf("[%s] Number of Clusters is 1", s.Name)
				sender, _ := strconv.Atoi(s.OngoingTxn.Sender)
				balance, _ := s.fetchClientBalance(sender)
				if balance < s.OngoingTxn.Amount {
					// log.Printf("[%s]Insufficient Balance", s.Name)
					s.PaxosStatus = false
					s.Wg.Done()
					return nil
				} else {
					// log.Printf("[%s]sufficient Balance: %d", s.Name, s.ClientBalances[s.OngoingTxn.Sender])

				}
				if s.ClientLocks[s.OngoingTxn.Sender] || s.ClientLocks[s.OngoingTxn.Receiver] {
					log.Printf("[%s]Couldnt Acquire Locks", s.Name)
					s.PaxosStatus = false
					s.Wg.Done()
					return nil
				} else {
					s.ClientLocks[s.OngoingTxn.Sender] = true
					s.ClientLocks[s.OngoingTxn.Receiver] = true
				}
				// log.Printf("[%s] Lock Made of Clients is 2", s.Name)

			} else if len(getClustersForTransaction(s.OngoingTxn)) == 2 {
				// log.Printf("[%s] Number of Clusters is 2", s.Name)
				if _, exist := s.ClientLocks[s.OngoingTxn.Sender]; exist {
					if s.ClientLocks[s.OngoingTxn.Sender] {
						// log.Printf("[%s]Couldnt Acquire Sender Lock", s.Name)
						s.PaxosStatus = false
						s.Wg.Done()
						return nil
					}
					sender, _ := strconv.Atoi(s.OngoingTxn.Sender)
					balance, _ := s.fetchClientBalance(sender)
					if balance < s.OngoingTxn.Amount {
						// log.Printf("[%s]Insufficient Balance", s.Name)
						s.PaxosStatus = false
						s.Wg.Done()
						return nil
					} else {
						// log.Printf("[%s]sufficient Balance: %d", s.Name, s.ClientBalances[s.OngoingTxn.Sender])

					}
					s.ClientLocks[s.OngoingTxn.Sender] = true
				}
				if _, exist := s.ClientLocks[s.OngoingTxn.Receiver]; exist {
					if s.ClientLocks[s.OngoingTxn.Receiver] {
						// log.Printf("[%s]Couldnt Acquire Receiver Lock", s.Name)
						s.PaxosStatus = false
						s.Wg.Done()
						return nil
					}
					s.ClientLocks[s.OngoingTxn.Receiver] = true
				}
				// log.Printf("[%s] Lock Made of Clients is 1", s.Name)

			}

			acceptMsg := AcceptMessage{
				BallotPair: promiseMsg.BallotPair,
				MajorBlock: s.OngoingTxn,
			}
			// log.Print("Majority Promises Received...Sending Accept Messages")

			// promiseMsg.LocalTxn = []Transaction{} // clearing promise message local transactions of other servers
			s.AcceptNum = acceptMsg.BallotPair
			s.AcceptVal = s.OngoingTxn
			s.handleAcceptMessage(acceptMsg)
			// s.OngoingTxn
		} else {
			log.Print("PAXOS FAILED: NOT SUFFICIENT PROMISES RECEIVED")
			s.PromisesReceived = 0
			log.Print(s.PromisesReceived)
			return nil
		}
	}

	return nil
}

func (s *Server) handleAcceptMessage(acceptMsg AcceptMessage) {

	for _, i := range s.ClusterServersPort {
		go func(i string) {
			if len(i) != 0 { // Skip sending to itself
				addr := i

				client, err := rpc.Dial("tcp", addr)
				if err != nil {
					// fmt.Printf("Error connecting to server %d: %v\n", id, err)
					return
				}
				defer client.Close()

				var response string
				err = client.Call("Server.ReceiveAccept", &acceptMsg, &response)
				if err != nil {
					// fmt.Printf("Error sending accept message to server %d: %v\n", id, err)
				} else {
					// fmt.Printf("Server %d sent accept message to server %d\n", s.ServerID, id)
				}
			}
		}(i)
	}

}

func (s *Server) ReceiveAccept(acceptMsg *AcceptMessage, response *string) error {

	if !s.ActiveStatus[s.Name] {
		// log.Printf("REJECT; server %s is inactive", s.Name)
		return nil
	}

	// fmt.Printf("Server %d: Received accept message: %+v\n", s.ServerID, *acceptMsg)
	s.AcceptNum = acceptMsg.BallotPair
	s.AcceptVal = acceptMsg.MajorBlock
	acceptedMsg := AcceptedMessage{
		BallotPair: acceptMsg.BallotPair,
		MajorBlock: acceptMsg.MajorBlock,
	}
	if len(getClustersForTransaction(s.AcceptVal)) == 1 {
		if s.ClientLocks[s.AcceptVal.Sender] || s.ClientLocks[s.AcceptVal.Receiver] {
			return nil
		} else {
			s.ClientLocks[s.AcceptVal.Sender] = true
			s.ClientLocks[s.AcceptVal.Receiver] = true
		}
	}

	if len(getClustersForTransaction(s.AcceptVal)) == 2 {
		if _, exist := s.ClientLocks[s.AcceptVal.Sender]; exist {
			if s.ClientLocks[s.AcceptVal.Sender] {
				return nil
			}
			s.ClientLocks[s.AcceptVal.Sender] = true
		}
		if _, exist := s.ClientLocks[s.AcceptVal.Receiver]; exist {
			if s.ClientLocks[s.AcceptVal.Receiver] {
				return nil
			}
			s.ClientLocks[s.AcceptVal.Receiver] = true
		}
	}

	leaderAddr := fmt.Sprintf("localhost:800%d", acceptMsg.BallotPair.ServerID)
	client, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		log.Fatalf("Error connecting to leader: %v\n", err)
	}

	defer client.Close()

	var leaderResponse ResponseMessage
	// fmt.Printf("Server %d: Sending accepted message\n", s.ServerID)

	err = client.Call("Server.ReceiveAccepted", &acceptedMsg, &leaderResponse)
	if err != nil {
		fmt.Printf("Error sending accepted message to leader: %v\n", err)
	} else {
		// fmt.Printf("Server %d: Sent accepted message to leader\n", s.ServerID)
	}

	*response = "Accept message received and Accepted message sent"
	return nil
}

func (s *Server) ReceiveAccepted(acceptedMsg *AcceptedMessage, responseMsg *ResponseMessage) error {

	// fmt.Printf("Leader Server %d: Received accepted message: %+v\n", s.ServerID, *acceptedMsg)

	s.acceptedCount++

	majority := 2
	var flag_done = false
	if s.acceptedCount+1 == majority {

		time.Sleep(50 * time.Millisecond)
		flag_done = true

	}

	if flag_done {
		flag_done = false

		if _, exist := s.ClientBalances[s.AcceptVal.Sender]; exist {
			sender, _ := strconv.Atoi(s.AcceptVal.Sender)
			balance, _ := s.fetchClientBalance(sender)
			// fmt.Printf("[%s]  Old balances Sender %v: %d \n", s.Name, s.AcceptVal.Sender, balance)
			balance -= s.AcceptVal.Amount
			s.updateClientBalance(sender, balance)
			fmt.Printf("[%s] New BALANCE Sender: %d \n", s.Name, balance)
		}
		if _, exist := s.ClientBalances[s.AcceptVal.Receiver]; exist {
			receiver, _ := strconv.Atoi(s.AcceptVal.Receiver)
			balance, _ := s.fetchClientBalance(receiver)
			// fmt.Printf("[%s]  Old balances Sender %v: %d \n", s.Name, s.AcceptVal.Sender, balance)
			balance += s.AcceptVal.Amount
			s.updateClientBalance(receiver, balance)
			fmt.Printf("[%s] New BALANCE Receiver: %d \n", s.Name, receiver)
		}
		if len(getClustersForTransaction(s.AcceptVal)) == 1 {
			s.ExecutedTxn[s.AcceptVal.Txnid] = s.AcceptVal
			sender, _ := strconv.Atoi(s.AcceptVal.Sender)
			newTxn := DataStoreTxn{
				BP:     s.BallotPair,
				Txn:    s.AcceptVal,
				Status: "",
			}
			s.appendToClientDataStore(sender, newTxn)

			if _, exist := s.ClientLocks[s.AcceptVal.Sender]; exist {

				s.ClientLocks[s.AcceptVal.Sender] = false
			}
			if _, exist := s.ClientLocks[s.AcceptVal.Receiver]; exist {

				s.ClientLocks[s.AcceptVal.Receiver] = false
			}

			// s.Mutex.Lock()
			// s.txn_executed++
			// txnLatency := time.Since(txnStartTime)
			// s.total_latency += txnLatency
			// s.Mutex.Unlock()

		} else {

			if _, exist := s.ClientLocks[s.AcceptVal.Sender]; exist {

				sender, _ := strconv.Atoi(s.AcceptVal.Sender)
				newTxn := DataStoreTxn{
					BP:     s.BallotPair,
					Txn:    s.AcceptVal,
					Status: "P",
				}
				s.appendToClientDataStore(sender, newTxn)
			}
			if _, exist := s.ClientLocks[s.AcceptVal.Receiver]; exist {
				receiver, _ := strconv.Atoi(s.AcceptVal.Receiver)
				newTxn := DataStoreTxn{
					BP:     s.BallotPair,
					Txn:    s.AcceptVal,
					Status: "P",
				}
				s.appendToClientDataStore(receiver, newTxn)
			}
			s.WAL[s.AcceptVal.Txnid] = s.AcceptVal
		}
		// fmt.Printf("Leader Server %d: Updated balances after PAXOS: %d\n", s.ServerID, s.ClientBalances[s.AcceptVal.Receiver])

		s.highest_len_datastore = len(s.ExecutedTxn)
		s.Highest_Len_Server_ID = s.ServerID

		s.AcceptNum = BallotPair{BallotNumber: 0, ServerID: s.ServerID}
		commitMsg := CommitMessage{}
		s.acceptedCount = 0

		s.sendCommitMessage(commitMsg)

	}
	responseMsg.Acknowledged = true
	return nil
}

func (s *Server) SendPaxosToClient(paxosStatus bool, txn Transaction) {

	client, err := rpc.Dial("tcp", "localhost:8000")
	if err != nil {
		// fmt.Printf("Error connecting to server %d: %v\n", id, err)
		return
	}
	defer client.Close()

	return
}

func (s *Server) sendCommitMessage(commitMsg CommitMessage) {

	for _, i := range s.ClusterServersPort {
		go func(i string) {
			if len(i) != 0 { // Skip sending to itself
				addr := i
				client, err := rpc.Dial("tcp", addr)
				if err != nil {
					// fmt.Printf("Error connecting to server %d: %v\n", id, err)
					return
				}
				defer client.Close()

				var response ResponseMessage
				err = client.Call("Server.ReceiveCommit", &commitMsg, &response)
				if err != nil {
					// fmt.Printf("Error sending commit message to server %d: %v\n", id, err)
				} else {
					// fmt.Printf("Leader Server %d: Sent commit message to server %d\n", s.ServerID, id)
				}

			}
		}(i)
	}
	time.Sleep(10 * time.Millisecond)
	s.PaxosStatus = true
	s.Wg.Done()

}

func (s *Server) ReceiveCommit(commitMsg *CommitMessage, responseMsg *ResponseMessage) error {
	if !s.ActiveStatus[s.Name] {
		// log.Printf("REJECT; server %s is inactive", s.Name)
		// fmt.Printf("Server %d: Latest balance: %+v\n", s.ServerID, s.Balances)
		return nil
	}

	txnStartTime := time.Now()

	if _, exist := s.ClientBalances[s.AcceptVal.Sender]; exist {
		sender, _ := strconv.Atoi(s.AcceptVal.Sender)
		balance, _ := s.fetchClientBalance(sender)
		// fmt.Printf("[%s]  Old balances Sender %v: %d \n", s.Name, s.AcceptVal.Sender, balance)
		balance -= s.AcceptVal.Amount
		s.updateClientBalance(sender, balance)
		// fmt.Printf("[%s]  Received commit message. Updated balances Sender %v: %d \n", s.Name, s.AcceptVal.Sender, balance)
	}
	if _, exist := s.ClientBalances[s.AcceptVal.Receiver]; exist {
		receiver, _ := strconv.Atoi(s.AcceptVal.Receiver)
		balance, _ := s.fetchClientBalance(receiver)
		// fmt.Printf("[%s]  Old balances Receiver %v: %d \n", s.Name, s.AcceptVal.Receiver, balance)
		balance += s.AcceptVal.Amount
		s.updateClientBalance(receiver, balance)
		// fmt.Printf("[%s]  Received commit message. Updated balances Receiver %v: %d \n", s.Name, s.AcceptVal.Receiver, s.ClientBalances[s.AcceptVal.Receiver])
	}
	if len(getClustersForTransaction(s.AcceptVal)) == 1 {
		s.ExecutedTxn[s.AcceptVal.Txnid] = s.AcceptVal
		sender, _ := strconv.Atoi(s.AcceptVal.Sender)
		newTxn := DataStoreTxn{
			BP:     s.BallotPair,
			Txn:    s.AcceptVal,
			Status: "",
		}
		s.appendToClientDataStore(sender, newTxn)

		if _, exist := s.ClientLocks[s.AcceptVal.Sender]; exist {

			s.ClientLocks[s.AcceptVal.Sender] = false
		}
		if _, exist := s.ClientLocks[s.AcceptVal.Receiver]; exist {

			s.ClientLocks[s.AcceptVal.Receiver] = false
		}

	} else {
		if _, exist := s.ClientLocks[s.AcceptVal.Sender]; exist {

			sender, _ := strconv.Atoi(s.AcceptVal.Sender)
			newTxn := DataStoreTxn{
				BP:     s.BallotPair,
				Txn:    s.AcceptVal,
				Status: "P",
			}
			s.appendToClientDataStore(sender, newTxn)
		}
		if _, exist := s.ClientLocks[s.AcceptVal.Receiver]; exist {
			receiver, _ := strconv.Atoi(s.AcceptVal.Receiver)
			newTxn := DataStoreTxn{
				BP:     s.BallotPair,
				Txn:    s.AcceptVal,
				Status: "P",
			}
			s.appendToClientDataStore(receiver, newTxn)
		}
		s.WAL[s.AcceptVal.Txnid] = s.AcceptVal
	}
	s.AcceptNum = BallotPair{BallotNumber: 0, ServerID: s.ServerID}
	// s.AcceptVal = nil

	// fmt.Printf("Server %d: Received commit message. Updated balances: %+v\n", s.ServerID, s.ClientBalances[s.AcceptVal.Receiver])
	// fmt.Printf("Server %d: Latest balance: %+v\n", s.ServerID, s.Balances)

	txnLatency := time.Since(txnStartTime)
	s.totalLatency += txnLatency
	s.txnExecuted++

	responseMsg.Acknowledged = true
	return nil
}

func (s *Server) FollowerSync(catchup_server_id int) {

	request := FetchDataStoreRequest{ServerID: s.ServerID,
		LenDatastore: len(s.ExecutedTxn),
	}
	// Assuming s.LeaderAddress is the leader server address (host:port)
	leaderAddr := fmt.Sprintf("localhost:800%d", catchup_server_id)

	client, err := rpc.Dial("tcp", leaderAddr)
	if err != nil {
		log.Printf("Server %d: Error connecting to leader: %v\n", s.ServerID, err)
		return
	}
	defer client.Close()
	var response FetchDataStoreResponse

	err = client.Call("Server.GetDataStore", request, &response)
	if err != nil {
		log.Printf("Server %d: Error during follower sync RPC call: %v\n", s.ServerID, err)
		return
	}
	log.Printf("[%s] ******* SYNCING IN PROGRESS ********", s.Name)
	for ballotpair, pend_txn := range response.PendingTransaction {
		if _, exist := s.ExecutedTxn[ballotpair]; !exist {
			log.Printf("[%s] Syncing Txn: %v ", s.Name, pend_txn)

			// s.ClientBalances[pend_txn.Receiver] += pend_txn.Amount
			// s.ClientBalances[pend_txn.Sender] -= pend_txn.Amount
			if _, exist := s.ClientBalances[pend_txn.Sender]; exist {
				sender, _ := strconv.Atoi(pend_txn.Sender)
				balance, _ := s.fetchClientBalance(sender)
				// fmt.Printf("[%s]  Old balances Sender %v: %d \n", s.Name, s.AcceptVal.Sender, balance)
				balance -= s.AcceptVal.Amount
				s.updateClientBalance(sender, balance)
				fmt.Printf("[%s] New BALANCE Sender: %d \n", s.Name, balance)
			}
			if _, exist := s.ClientBalances[pend_txn.Receiver]; exist {
				receiver, _ := strconv.Atoi(pend_txn.Receiver)
				balance, _ := s.fetchClientBalance(receiver)
				// fmt.Printf("[%s]  Old balances Receiver %v: %d \n", s.Name, s.AcceptVal.Receiver, balance)
				balance += s.AcceptVal.Amount
				s.updateClientBalance(receiver, balance)
				fmt.Printf("[%s] New BALANCE Receiver: %d \n", s.Name, receiver)
			}
			newTxn := DataStoreTxn{
				BP: BallotPair{
					BallotNumber: -1,
					ServerID:     -1,
				},
				Txn:    pend_txn,
				Status: "",
			}
			_, exist1 := s.ClientBalances[pend_txn.Sender]
			_, exist2 := s.ClientBalances[pend_txn.Receiver]
			if exist1 || exist2 {
				if exist1 {
					sender, _ := strconv.Atoi(pend_txn.Sender)
					s.appendToClientDataStore(sender, newTxn)
				} else {
					receiver, _ := strconv.Atoi(pend_txn.Receiver)
					s.appendToClientDataStore(receiver, newTxn)
				}

			}
			// log.Print(pend_txn)
			s.ExecutedTxn[ballotpair] = pend_txn
		}

	}

	s.highest_len_datastore = len(s.ExecutedTxn)
	s.Highest_Len_Server_ID = s.ServerID

}
func (s *Server) fetchClientBalance(clientID int) (int, error) {
	dbPath := s.Db_path

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return 0, fmt.Errorf("database file does not exist: %s", dbPath)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open database %s: %v", dbPath, err)
	}
	defer db.Close()

	var amount int
	query := `SELECT Amount FROM clients WHERE ClientID = ?;`
	err = db.QueryRow(query, clientID).Scan(&amount)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("client with ID %d not found", clientID)
		}
		return 0, fmt.Errorf("failed to fetch balance for client %d: %v", clientID, err)
	}

	return amount, nil
}

// updateClientBalance updates the balance of a client in the database
func (s *Server) updateClientBalance(clientID int, newAmount int) error {
	dbPath := s.Db_path

	// Check if the database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("database file does not exist: %s", dbPath)
	}

	// Connect to the SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database %s: %v", dbPath, err)
	}
	defer db.Close()

	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction in %s: %v", dbPath, err)
	}

	// Update the client's balance
	updateSQL := `UPDATE clients SET Amount = ? WHERE ClientID = ?;`
	result, err := tx.Exec(updateSQL, newAmount, clientID)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to update balance for client %d: %v", clientID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to get rows affected for client %d: %v", clientID, err)
	}

	if rowsAffected == 0 {
		tx.Rollback()
		return fmt.Errorf("client with ID %d not found", clientID)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction in %s: %v", dbPath, err)
	}

	// log.Printf("Updated balance for client %d to %d", clientID, newAmount)
	return nil
}

func (s *Server) fetchClientDataStore(clientID int) ([]DataStoreTxn, error) {
	dbPath := s.Db_path

	// Check if the database file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("database file does not exist: %s", dbPath)
	}

	// Connect to the SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database %s: %v", dbPath, err)
	}
	defer db.Close()

	var dataStoreStr string
	query := `SELECT DataStore FROM clients WHERE ClientID = ?;`
	err = db.QueryRow(query, clientID).Scan(&dataStoreStr)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("client with ID %d not found", clientID)
		}
		return nil, fmt.Errorf("failed to fetch DataStore for client %d: %v", clientID, err)
	}

	var dataStore []DataStoreTxn
	if dataStoreStr == "" || dataStoreStr == "[]" {
		dataStore = []DataStoreTxn{}
	} else {
		err = json.Unmarshal([]byte(dataStoreStr), &dataStore)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal DataStore for client %d: %v", clientID, err)
		}
	}

	return dataStore, nil
}

func (s *Server) appendToClientDataStore(clientID int, newTxn DataStoreTxn) error {
	dataStore, err := s.fetchClientDataStore(clientID)
	if err != nil {
		return err
	}

	// Append the new transaction
	dataStore = append(dataStore, newTxn)
	// log.Print(dataStore)
	// Marshal the updated DataStore back to JSON
	dataStoreBytes, err := json.Marshal(dataStore)
	if err != nil {
		return fmt.Errorf("failed to marshal DataStore for client %d: %v", clientID, err)
	}

	// Update the DataStore in the database
	dbPath := s.Db_path

	// Connect to the SQLite database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database %s: %v", dbPath, err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction in %s: %v", dbPath, err)
	}

	updateSQL := `UPDATE clients SET DataStore = ?;`
	result, err := tx.Exec(updateSQL, string(dataStoreBytes))
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to update DataStore for client %d: %v", clientID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to get rows affected for client %d: %v", clientID, err)
	}

	if rowsAffected == 0 {
		tx.Rollback()
		return fmt.Errorf("client with ID %d not found", clientID)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction in %s: %v", dbPath, err)
	}

	// log.Printf("Appended new transaction to DataStore for client %d", clientID)
	return nil
}

func (s *Server) GetDataStore(request *FetchDataStoreRequest, response *FetchDataStoreResponse) error {

	response.PendingTransaction = s.ExecutedTxn

	return nil
}

func (s *Server) HandleNetworkStatus(networkStatus NetworkStatus, reply *string) error {

	s.ActiveServers = networkStatus.ActiveServers
	s.ActiveStatus = networkStatus.ActiveServers

	s.ContactServers = networkStatus.ContactServers

	// log.Printf("[%s] UPDATED NETWORK STATUS: ActiveServers: %v, ContactServers: %v", s.Name, s.ActiveServers, s.ContactServers)

	return nil
}

func startServer(s *Server, wg *sync.WaitGroup) {
	defer wg.Done()

	serverName := s.Name
	var serverID int
	switch serverName {
	case "S1":
		serverID = 1
	case "S2":
		serverID = 2
	case "S3":
		serverID = 3
	case "S4":
		serverID = 4
	case "S5":
		serverID = 5
	case "S6":
		serverID = 6
	case "S7":
		serverID = 7
	case "S8":
		serverID = 8
	case "S9":
		serverID = 9
	default:
		log.Fatalf("Invalid server name: %s. Must be one of S1, S2, S3, S4, or S5.", serverName)
		return
	}

	s.processedTxns = make(map[int64]bool)

	s.startTime = time.Now()
	s.txnExecuted = 0
	s.totalLatency = 0

	s.ServerID = serverID
	s.ClusterId = (serverID - 1) / 3
	s.Db_path = initializeShard(s.ClusterId+1, serverID)
	for i := range 3 {
		if (3*s.ClusterId + i + 1) != s.ServerID {
			s.ClusterServersPort = append(s.ClusterServersPort, fmt.Sprintf("localhost:800%d", s.ClusterId*3+i+1))
		} else {
			s.ClusterServersPort = append(s.ClusterServersPort, "")
		}
	}
	s.ClientBalances = make(map[string]int)
	s.ClientLocks = make(map[string]bool)
	s.ExecutedTxn = make(map[int64]Transaction)
	s.WAL = make(map[int64]Transaction)
	for idx := range 1000 {
		s.ClientBalances[fmt.Sprintf("%d", s.ClusterId*1000+idx+1)] = 10
		s.ClientLocks[fmt.Sprintf("%d", s.ClusterId*1000+idx+1)] = false
	}

	server := rpc.NewServer()
	// Register the server's methods under the service name "Server"
	server.RegisterName("Server", s)
	// log.Print(s)
	listener, err := net.Listen("tcp", ":"+s.Port)
	if err != nil {
		log.Fatalf("Error starting server %s: %v", s.Name, err)
	}
	defer listener.Close()

	fmt.Printf("Server %s listening on port %s\n", s.Name, s.Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Server %s: Error accepting connection: %v", s.Name, err)
			continue
		}
		// Use the custom RPC server to serve the connection
		go server.ServeConn(conn)
	}

}

func getClusterRange(cluster int) (int, int) {
	startID := (cluster-1)*1000 + 1
	endID := cluster * 1000
	return startID, endID
}

func initializeShard(cluster, shard int) string {
	shardName := fmt.Sprintf("database_C%d_S%d.db", cluster, shard)
	dbPath := path.Join("database", shardName)

	os.MkdirAll("database", os.ModePerm)

	db, _ := sql.Open("sqlite3", dbPath)

	defer db.Close()

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS clients (
		ClientID INTEGER PRIMARY KEY,
		Amount INTEGER NOT NULL,
		DataStore TEXT,
		WAL TEXT
	);
	`
	_, _ = db.Exec(createTableSQL)

	startID, endID := getClusterRange(cluster)
	// log.Print(startID, endID)
	tx, _ := db.Begin()

	updateSQL := `INSERT OR REPLACE INTO clients ( ClientID, Amount, DataStore, WAL) VALUES (?,?,?,?);`

	for clientID := startID; clientID <= endID; clientID++ {
		_, err := tx.Exec(updateSQL,clientID, 10, "[]", "[]")

		// _, err := stmt.Exec(clientID, 10, "[]", "[]")
		if err != nil {
			log.Print(err)
		}
	}

	_ = tx.Commit()

	// log.Printf("Initialized shard %s with clients %d to %d", shardName, startID, endID)
	return dbPath
}

func (s *Server) GetClientBalance(clientID int, balance *int) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	amount, err := s.fetchClientBalance(clientID)
	if err != nil {
		*balance = 0
		return nil
	}
	*balance = amount
	return nil
}

func (s *Server) GetCommittedTransactions(_ struct{}, committedTxns *[]Transaction) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	var txns []Transaction

	for _, txn := range s.ExecutedTxn {
		txns = append(txns, txn)
	}

	*committedTxns = txns
	return nil
}

func (s *Server) GetPerformanceMetrics(_ struct{}, metrics *PerformanceMetrics) error {
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	elapsedTime := time.Since(s.startTime).Seconds()

	throughput := float64(s.txnExecuted) / elapsedTime

	var avgLatency float64
	if s.txnExecuted > 0 {
		avgLatency = s.totalLatency.Seconds() * 1000 / float64(s.txnExecuted)
	} else {
		avgLatency = 0
	}

	*metrics = PerformanceMetrics{
		Throughput:           throughput,
		AverageLatency:       avgLatency,
		TransactionsExecuted: s.txnExecuted,
	}

	return nil
}

func main() {
	var wg sync.WaitGroup

	servers := []Server{
		{Name: "S1", Port: "8001"},
		{Name: "S2", Port: "8002"},
		{Name: "S3", Port: "8003"},
		{Name: "S4", Port: "8004"},
		{Name: "S5", Port: "8005"},
		{Name: "S6", Port: "8006"},
		{Name: "S7", Port: "8007"},
		{Name: "S8", Port: "8008"},
		{Name: "S9", Port: "8009"},
	}

	for i := range servers {
		wg.Add(1)

		go startServer(&servers[i], &wg)
	}

	wg.Wait()
}
