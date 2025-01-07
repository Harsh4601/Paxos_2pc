// client.go
package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ClientServer struct {
	Name string
	Port string
}

type Transaction struct {
	Txnid    int64
	Sender   string
	Receiver string
	Amount   int
}

type NetworkStatus struct {
	ActiveServers  map[string]bool
	ContactServers map[string]bool
}

type PerformanceMetrics struct {
    Throughput           float64
    AverageLatency       float64 // in milliseconds
    TransactionsExecuted int
}

func (c *ClientServer) ForwardRequest(args *string, reply *string) error {
	serverPort := "8001" // Example: Forwarding to S1
	serverAddr := "localhost:" + serverPort

	client, err := rpc.Dial("tcp", serverAddr)
	if err != nil {
		return fmt.Errorf("Error connecting to server S1: %v", err)
	}
	defer client.Close()

	// Make an RPC call to S1
	err = client.Call("Server.HandleRequest", args, reply)
	if err != nil {
		return fmt.Errorf("Error calling Server.HandleRequest: %v", err)
	}
	return nil
}

func sendPrintBalance() {
	var clientID int
	fmt.Print("Enter Client ID: ")
	fmt.Scanln(&clientID)

	allServers := []string{"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9"}
	serverAddresses := map[string]string{
		"S1": "localhost:8001",
		"S2": "localhost:8002",
		"S3": "localhost:8003",
		"S4": "localhost:8004",
		"S5": "localhost:8005",
		"S6": "localhost:8006",
		"S7": "localhost:8007",
		"S8": "localhost:8008",
		"S9": "localhost:8009",
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	balances := make(map[string]int)
	wg.Add(len(allServers))

	for _, server := range allServers {
		go func(server string) {
			defer wg.Done()
			addr := serverAddresses[server]
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				log.Printf("Error connecting to server %s: %v", server, err)
				return
			}
			defer client.Close()

			var balance int
			err = client.Call("Server.GetClientBalance", clientID, &balance)
			if err != nil {
				log.Printf("Error calling GetClientBalance on server %s: %v", server, err)
				return
			}

			mu.Lock()
			balances[server] = balance
			mu.Unlock()
		}(server)
	}

	wg.Wait()

	fmt.Printf("Balances for Client ID %d:\n", clientID)
	for _, server := range allServers {
		mu.Lock()
		balance, exists := balances[server]
		mu.Unlock()
		if exists {
			fmt.Printf("Server %s: Balance = %d\n", server, balance)
		} else {
			fmt.Printf("Server %s: Balance not available\n", server)
		}
	}
}

func sendPrintDatastore() {
	allServers := []string{"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9"}
	serverAddresses := map[string]string{
		"S1": "localhost:8001",
		"S2": "localhost:8002",
		"S3": "localhost:8003",
		"S4": "localhost:8004",
		"S5": "localhost:8005",
		"S6": "localhost:8006",
		"S7": "localhost:8007",
		"S8": "localhost:8008",
		"S9": "localhost:8009",
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	transactions := make(map[string][]Transaction)
	wg.Add(len(allServers))

	for _, server := range allServers {
		go func(server string) {
			defer wg.Done()
			addr := serverAddresses[server]
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				log.Printf("Error connecting to server %s: %v", server, err)
				return
			}
			defer client.Close()

			var committedTxns []Transaction
			err = client.Call("Server.GetCommittedTransactions", struct{}{}, &committedTxns)
			if err != nil {
				log.Printf("Error calling GetCommittedTransactions on server %s: %v", server, err)
				return
			}

			mu.Lock()
			transactions[server] = committedTxns
			mu.Unlock()
		}(server)
	}

	wg.Wait()

	fmt.Println("Committed Transactions on each server:")
	for _, server := range allServers {
		mu.Lock()
		txns, exists := transactions[server]
		mu.Unlock()
		if exists {
			fmt.Printf("Server %s:\n", server)
			if len(txns) == 0 {
				fmt.Println("  No committed transactions.")
			} else {
				for _, txn := range txns {
					fmt.Printf("  Transaction ID: %d, Sender: %s, Receiver: %s, Amount: %d\n",
						txn.Txnid, txn.Sender, txn.Receiver, txn.Amount)
				}
			}
		} else {
			fmt.Printf("Server %s: Data not available.\n", server)
		}
	}
}


func sendPerformance() {
    allServers := []string{"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9"}
    serverAddresses := map[string]string{
        "S1": "localhost:8001",
        "S2": "localhost:8002",
        "S3": "localhost:8003",
        "S4": "localhost:8004",
        "S5": "localhost:8005",
        "S6": "localhost:8006",
        "S7": "localhost:8007",
        "S8": "localhost:8008",
        "S9": "localhost:8009",
    }

    var wg sync.WaitGroup
    var mu sync.Mutex
    metricsMap := make(map[string]PerformanceMetrics)
    wg.Add(len(allServers))

    for _, server := range allServers {
        go func(server string) {
            defer wg.Done()
            addr := serverAddresses[server]
            client, err := rpc.Dial("tcp", addr)
            if err != nil {
                log.Printf("Error connecting to server %s: %v", server, err)
                return
            }
            defer client.Close()

            var metrics PerformanceMetrics
            err = client.Call("Server.GetPerformanceMetrics", struct{}{}, &metrics)
            if err != nil {
                log.Printf("Error calling GetPerformanceMetrics on server %s: %v", server, err)
                return
            }

            mu.Lock()
            metricsMap[server] = metrics
            mu.Unlock()
        }(server)
    }

    wg.Wait()

    fmt.Println("Performance Metrics from all servers:")
    for _, server := range allServers {
        mu.Lock()
        metrics, exists := metricsMap[server]
        mu.Unlock()
        if exists {
            fmt.Printf("Server %s:\n", server)
            fmt.Printf("  Transactions Executed: %d\n", metrics.TransactionsExecuted)
            fmt.Printf("  Throughput: %.2f transactions/sec\n", metrics.Throughput)
            fmt.Printf("  Average Latency: %.2f ms\n", metrics.AverageLatency)
        } else {
            fmt.Printf("Server %s: Data not available.\n", server)
        }
    }
}


func sendTransactionToContactServers(transaction Transaction, contactServers map[int]string) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	// log.Printf("Executing Txn %s->%s: %v ", transaction.Sender, transaction.Receiver, transaction.Amount)

	clusters := getClustersForTransaction(transaction)
	abort_flag := false
	replies := make([]bool, 0)
	var timer *time.Timer

	if len(clusters) == 2 {
		timer = time.NewTimer(1000 * time.Millisecond)
		wg.Add(1)
	}

	for _, cluster := range clusters {
		go func(cluster int) {
			serverName, ok := contactServers[cluster]
			if !ok {
				log.Printf("No contact server for cluster %d", cluster)
				return
			}
			addr := getServerAddress(serverName)
			// log.Print(serverName)

			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				log.Printf("Error connecting to server %s: %v", addr, err)
				return
			}
			defer client.Close()

			var reply bool
			err = client.Call("Server.HandleClientRequest", transaction, &reply)
			if err != nil {
				log.Printf("Error sending transaction to server %s: %v", addr, err)
			}

			mu.Lock()
			defer mu.Unlock()
			// log.Printf("Status of Txn %s->%s: %v ::: %v", transaction.Sender, transaction.Receiver, transaction.Amount, reply)

			if len(clusters) == 2 {
				if reply == false {
					// log.Print("FALSE FLAG RAISED")
					if !abort_flag {
						abort_flag = true
						wg.Done()
					}
					return
				}
				replies = append(replies, reply)
				if len(replies) == 2 {
					// log.Print("2 Replies RECEIVED")
					if !abort_flag {
						wg.Done()
					}
					return
				}
			}

		}(cluster)
	}

	if len(clusters) == 2 {
		go func() {
			<-timer.C
			mu.Lock()
			defer mu.Unlock()
			if len(replies) < 2 && !abort_flag {
				log.Printf("TIMER EXPIRED for Txn %s->%s: %v", transaction.Sender, transaction.Receiver, transaction.Amount)
				abort_flag = true
				wg.Done()
			}
		}()
	}

	wg.Wait()

	if len(clusters) == 2 {
		if abort_flag {
			// log.Printf("ABORTING Txn %s->%s: %v", transaction.Sender, transaction.Receiver, transaction.Amount)
			performAbortAcrossClusters(transaction, clusters)
		} else {
			// log.Printf("COMMITTING Txn %s->%s: %v", transaction.Sender, transaction.Receiver, transaction.Amount)
			performCommitAcrossClusters(transaction, clusters)
		}
	}
	time.Sleep(10 * time.Millisecond)
}

func performAbortAcrossClusters(transaction Transaction, clusters []int) {
	for _, cluster := range clusters {
		for i := 0; i < 3; i++ {
			serverName := fmt.Sprintf("S%d", (cluster-1)*3+i+1)
			addr := getServerAddress(serverName)
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				log.Printf("Error connecting to server %s: %v", addr, err)
				continue
			}
			defer client.Close()

			var reply bool
			err = client.Call("Server.AbortCrossShard", transaction.Txnid, &reply)
			if err != nil {
				log.Printf("Error sending AbortCrossShard to server %s: %v", addr, err)
			}
			// log.Printf("[%s] AbortCrossShard Txn %s->%s: %v ::: %v", serverName, transaction.Sender, transaction.Receiver, transaction.Amount, reply)
		}
	}
}

func performCommitAcrossClusters(transaction Transaction, clusters []int) {
	for _, cluster := range clusters {
		for i := 0; i < 3; i++ {
			serverName := fmt.Sprintf("S%d", (cluster-1)*3+i+1)
			addr := getServerAddress(serverName)
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				log.Printf("Error connecting to server %s: %v", addr, err)
				continue
			}
			defer client.Close()

			var reply bool
			err = client.Call("Server.CommitCrossShard", transaction.Txnid, &reply)
			if err != nil {
				log.Printf("Error sending CommitCrossShard to server %s: %v", addr, err)
			}
			// log.Printf("[%s] CommitCrossShard Txn %s->%s: %v ::: %v", serverName, transaction.Sender, transaction.Receiver, transaction.Amount, reply)
		}
	}
}

func sendNetworkStatus(networkStatus NetworkStatus) {
	allServers := []string{"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9"}

	for _, server := range allServers {
		serverAddr := getServerAddress(strings.TrimSpace(server))
		client, err := rpc.Dial("tcp", serverAddr)
		if err != nil {
			log.Printf("Error connecting to server %s: %v", server, err)
			continue
		}
		defer client.Close()

		var reply string
		err = client.Call("Server.HandleNetworkStatus", networkStatus, &reply)
		if err != nil {
			log.Printf("Error calling remote procedure on server %s: %v", server, err)
		}
	}
}

func processTransactions(csvFile string) {

	file, err := os.Open(csvFile)
	if err != nil {
		log.Fatalf("Failed to open CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read CSV file: %v", err)
	}

	var currentSet int
	var currentTransactions []Transaction
	var activeServers []string
	var contactServers map[int]string

	allServers := []string{"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9"}

	for _, record := range records {
		if len(record) == 0 {
			continue
		}

		if record[2] != "" {
			if currentSet != 0 {
				fmt.Printf("Processing transactions for Set %d:\n", currentSet)

				activeServerMap := make(map[string]bool)
				contactServerMap := make(map[string]bool)

				for _, server := range allServers {
					activeServerMap[strings.TrimSpace(server)] = false
					contactServerMap[strings.TrimSpace(server)] = false
				}

				for _, server := range activeServers {
					activeServerMap[strings.TrimSpace(server)] = true
				}

				for _, server := range contactServers {
					contactServerMap[strings.TrimSpace(server)] = true
				}

				networkStatus := NetworkStatus{
					ActiveServers:  activeServerMap,
					ContactServers: contactServerMap,
				}

				sendNetworkStatus(networkStatus)

				for _, txn := range currentTransactions {
					txn.Txnid = int64(time.Now().Nanosecond())*10 + int64(txn.Amount)

					go sendTransactionToContactServers(txn, contactServers)
				
				}

				var response int

				for {
					fmt.Print("1. Continue to the next set? \n")
					fmt.Print("2. Print Balance \n")
					fmt.Print("3. Print Datastore\n")
					fmt.Print("4. Performance Metrics \n")
					// fmt.Print("5. Performance Metrics\n")
					fmt.Print("5. Exit\n")
					fmt.Scanln(&response)
					if response == 1 {
						break
					}
					switch response {
					case 1:
						break
					case 2:
						sendPrintBalance()
						continue
					case 3:
						sendPrintDatastore()
						continue
					case 4:
						sendPerformance()
						continue
					case 5:
						break

					}

				}
				if response == 6 {
					break
				}

			}

			// Reset for the next set
			currentSet, _ = strconv.Atoi(record[0])
			activeServers = parseServers(strings.TrimSpace(record[2]))

			contactServersList := parseServers(strings.TrimSpace(record[3]))
			contactServers = make(map[int]string)
			if len(contactServersList) >= 3 {
				contactServers[1] = contactServersList[0]
				contactServers[2] = contactServersList[1]
				contactServers[3] = contactServersList[2]
			} else {
				log.Fatalf("Invalid contactServers in CSV: %v", record[3])
			}

			currentTransactions = nil
		}

		if len(record) > 1 {
			transactionStr := record[1]
			transaction := parseTransaction(transactionStr)
			currentTransactions = append(currentTransactions, transaction)
		}
	}

	if currentSet != 0 && len(currentTransactions) > 0 {
		fmt.Printf("Processing transactions for Set %d:\n", currentSet)

		activeServerMap := make(map[string]bool)
		contactServerMap := make(map[string]bool)

		for _, server := range allServers {
			activeServerMap[server] = false
			contactServerMap[strings.TrimSpace(server)] = false

		}

		for _, server := range activeServers {
			activeServerMap[server] = true
		}

		for _, server := range contactServers {
			contactServerMap[strings.TrimSpace(server)] = true
		}

		networkStatus := NetworkStatus{
			ActiveServers:  activeServerMap,
			ContactServers: contactServerMap,
		}

		sendNetworkStatus(networkStatus)

		for _, txn := range currentTransactions {

			go sendTransactionToContactServers(txn, contactServers)

		}

	}
	var response int
	for {
		fmt.Print("1. Continue to the next set? \n")
		fmt.Print("2. Print Balance \n")
		fmt.Print("3. Print Datastore \n")
		fmt.Print("4. Performance Metrics \n")
		fmt.Print("5. Exit\n")
		fmt.Scanln(&response)
		if response == 1 {
			break
		}
		switch response {
		case 1:
			break
		case 2:
			sendPrintBalance()
			continue
		case 3:
			sendPrintDatastore()
			continue
		case 4:
			sendPerformance()
			continue
		case 5:
			break

		}
	}

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

func parseServers(serversStr string) []string {
	serversStr = strings.Trim(serversStr, "[]")
	if serversStr == "" {
		return []string{}
	}
	servers := strings.Split(serversStr, ",")
	for i, server := range servers {
		servers[i] = strings.TrimSpace(server)
	}
	return servers
}

func parseTransaction(transStr string) Transaction {
	transStr = strings.Trim(transStr, "()")
	parts := strings.Split(transStr, ",")
	sender := strings.TrimSpace(strings.TrimSpace(parts[0]))
	receiver := strings.TrimSpace(strings.TrimSpace(parts[1]))
	amount, _ := strconv.Atoi(strings.TrimSpace(parts[2]))
	return Transaction{Sender: sender, Receiver: receiver, Amount: amount}
}

func getServerAddress(sender string) string {
	serverMap := map[string]string{
		"S1": "localhost:8001",
		"S2": "localhost:8002",
		"S3": "localhost:8003",
		"S4": "localhost:8004",
		"S5": "localhost:8005",
		"S6": "localhost:8006",
		"S7": "localhost:8007",
		"S8": "localhost:8008",
		"S9": "localhost:8009",
	}
	return serverMap[sender]
}

func main() {

	csvFile := "Test_Cases_-_Lab3.csv"

	if len(os.Args) < 3 {
		fmt.Println("Usage: go run client.go <name> <port>")
		return
	}

	name := os.Args[1]
	port := os.Args[2]

	clientServer := &ClientServer{
		Name: name,
		Port: port,
	}

	err := rpc.RegisterName("Client", clientServer)
	if err != nil {
		log.Fatalf("Error registering RPC client server: %v", err)
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Error starting client server: %v", err)
	}
	defer listener.Close()

	fmt.Printf("Client server %s listening on port %s\n", name, port)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %v", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	processTransactions(csvFile)

}
