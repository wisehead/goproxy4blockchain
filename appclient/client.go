package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"goproxy4blockchain/utils"
)

// Msg is a struct for sending message
type Msg struct {
	Meta    map[string]interface{} `json:"meta"`
	Content interface{}            `json:"content"`
}

// RPCRequest represents a JSON-RPC request object.
type RPCRequest struct {
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
	ID      uint        `json:"id"` //chenhui
	JSONRPC string      `json:"jsonrpc"`
}

func send(conn net.Conn) {
	for i := 0; i < 6; i++ {
		//session := GetSession()
		message := &Msg{
			Meta: map[string]interface{}{
				"meta": "test",
				"ID":   strconv.Itoa(i),
			},
			Content: RPCRequest{
				Method: "source-state",
				Params: map[string]interface{}{
					"key":     "00000000000000000000000000000001",
					"channel": "vvtrip",
				},
				ID:      0,
				JSONRPC: "2.0",
			},
			/*
				Content: Msg{
					Meta: map[string]interface{}{
						"author": "nucky lu",
					},
					Content: session,
				},
			*/
		}
		result, _ := json.Marshal(message)
		conn.Write(utils.Enpack((result)))
		buf := make([]byte, 1024) //定义一个切片的长度是1024。
		n, err := conn.Read(buf)  //接收到的内容大小。
		utils.CheckError(err)
		utils.Log("receiving ", n, " bytes response from Proxy: ", string(buf[:n]))
		//conn.Write([]byte(message))
		//time.Sleep(1 * time.Second)
	}
	fmt.Println("send over")
	defer conn.Close()
}

//GetSession is for a random number
func GetSession() string {
	gs1 := time.Now().Unix()
	gs2 := strconv.FormatInt(gs1, 10)
	return gs2
}

func main() {
	server := "localhost:10399"
	tcpAddr, err := net.ResolveTCPAddr("tcp4", server)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}

	fmt.Println("connect success")
	send(conn)

}
