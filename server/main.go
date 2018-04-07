package main

import (
	"fmt"
	"goproxy4blockchain/jsonrpc"
	"goproxy4blockchain/utils"
	"net"
	"runtime"
	"strconv"
)

//MethodParams for JSON-RPC 2.0 parameters.
type MethodParams struct {
	Channel string `json:"channel"`
	Key     string `json:"key"`
}

func sendJsonrpcRequest() {
	//rpcClient := jsonrpc.NewClient("http://my-rpc-service:8080/rpc")
	rpcClient := jsonrpc.NewClient("https://www.ninechain.net/api/v2")
	if rpcClient == nil {
		fmt.Println("rpcClient is nil!")
		return
	}
	rpcResp, err := rpcClient.Call("source-state", &MethodParams{Channel: "vvtrip", Key: "00000000000000000000000000000001"})
	if err != nil {
		//utils.LOG.Error("rpcClient.CallFor failed: " + err.Error())
		fmt.Printf("xxx err for rpcClient.Call:%v", err.Error())
	}
	id := rpcResp.ID
	fmt.Printf("xxx rpcResp.id:%v\n", id)
	jsonrpc := rpcResp.JSONRPC
	fmt.Printf("xxx rpcResp.jsonrpc:%v\n", jsonrpc)
	rpcresult := rpcResp.Result
	state := rpcresult["state"].(string)
	fmt.Printf("xxx rpcResp.Result.state:%v\n", state)
}

func startServer(configpath string) {
	//	setup a socket and listen the port
	configmap := utils.GetYamlConfig(configpath)
	host := utils.GetElement("host", configmap)
	timeinterval, err := strconv.Atoi(utils.GetElement("beatinginterval", configmap))
	utils.CheckError(err)
	netListen, err := net.Listen("tcp", host)
	utils.CheckError(err)
	defer netListen.Close()
	utils.Log("Waiting for clients")

	for {
		conn, err := netListen.Accept()
		if err != nil {
			continue
		}

		utils.Log(conn.RemoteAddr().String(), " tcp connect success")
		go handleConnection(conn, timeinterval)
	}

	// you can run this part of code in Window System

	//netListen, err := net.Listen("tcp", "localhost:1024")
	//utils.CheckError(err)
	//defer netListen.Close()
	//utils.Log("Waiting for clients")
	//
	//for {
	//	conn, err := netListen.Accept()
	//	if err != nil {
	//		continue
	//	}
	//
	//	utils.Log(conn.RemoteAddr().String(), " tcp connect success")
	//	go handleConnection(conn, 3)
	//}
}

//handle the connection
func handleConnection(conn net.Conn, timeout int) {

	tmpBuffer := make([]byte, 0)

	buffer := make([]byte, 1024)
	messnager := make(chan byte)
	defer conn.Close()
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			utils.Log(conn.RemoteAddr().String(), " connection error: ", err)
			return
		}

		tmpBuffer = utils.Depack(append(tmpBuffer, buffer[:n]...))
		utils.Log("receive data string:", string(tmpBuffer))
		utils.TaskDeliver(tmpBuffer, conn)
		//start heartbeating
		go utils.HeartBeating(conn, messnager, timeout)
		//check if get message from client
		go utils.GravelChannel(tmpBuffer, messnager)
	}
}

// main function
func main() {
	runtime.GOMAXPROCS(runtime.NumCPU()*3/2 + 1)
	//utils.LOG.Info("BlockChain Proxy Version: 1.0.0.0 - build-2018-04-07 12:01:00")
	startServer("./conf/config.yaml")
}
