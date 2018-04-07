package main

import (
	"fmt"
	"goproxy4blockchain/jsonrpc"
	"log"
	"net"
	"os"
	"runtime"
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

//处理连接
func handleConnection(conn net.Conn) {

	buffer := make([]byte, 2048)

	for {

		n, err := conn.Read(buffer)

		if err != nil {
			Log(conn.RemoteAddr().String(), " connection error: ", err)
			return
		}

		Log(conn.RemoteAddr().String(), "receive data string:\n", string(buffer[:n]))

	}

}

//Log is for printing the debug messages.
func Log(v ...interface{}) {
	log.Println(v...)
}

// CheckError is for check error and print the message
func CheckError(err error) {
	if err != nil {
		//fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		fmt.Printf("Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU()*3/2 + 1)
	//utils.LOG.Info("BlockChain Proxy Version: 1.0.0.0 - build-2018-04-07 12:01:00")

	netListen, err := net.Listen("tcp", "localhost:10399")
	CheckError(err)
	defer netListen.Close()

	Log("Waiting for clients")
	for {
		conn, err := netListen.Accept()
		if err != nil {
			continue
		}

		Log(conn.RemoteAddr().String(), " tcp connect success")
		fmt.Print(conn.RemoteAddr().String(), " tcp connect success")
		go handleConnection(conn)
	}
}
