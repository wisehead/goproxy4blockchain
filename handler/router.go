package handler

import (
	"encoding/json"
	"fmt"
	"goproxy4blockchain/jsonrpc"
	"goproxy4blockchain/utils"
	"net"
)

//in this part, we try to decouple the whole code by a route-controller structure;
//before this server running, all the controller would be written in the router by function init();
//when the client send a json, this server decode this json and decide which controller to process this message;

//我在Server的内部加入一层Router,通过Router对通过Socket发来的信息，通过我们设定的规则进行解析判断后，调用相关的Controller进行任务的分发处理。
//在这个过程中不仅Controller彼此独立，匹配规则和Controller之间也是相互独立的。

/*
// RPCRequest represents a JSON-RPC request object.
type RPCRequest struct {
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
	ID      uint        `json:"id"` //chenhui
	JSONRPC string      `json:"jsonrpc"`
}
*/

//MethodParams for JSON-RPC 2.0 parameters.
type MethodParams struct {
	Channel string `json:"channel"`
	Key     string `json:"key"`
}

//Msg defined between app client and goproxy4blockchain
type Msg struct {
	Meta    map[string]interface{} `json:"meta"`
	Content jsonrpc.RPCRequest     `json:"content"`
}

/*
type Msg struct {
	Meta    map[string]interface{} `json:"meta"`
	Content interface{}            `json:"content"`
}
*/

//Controller is an interface, you can implement by yourself.
type Controller interface {
	Excute(message Msg) []byte
}

var routers [][2]interface{}

//Route is to add the pred and controller pair into routers;
func Route(pred interface{}, controller Controller) {
	switch pred.(type) {
	case func(entry Msg) bool:
		{
			var arr [2]interface{}
			arr[0] = pred
			arr[1] = controller
			routers = append(routers, arr)
		}
	case map[string]interface{}:
		{
			defaultPred := func(entry Msg) bool {
				for keyPred, valPred := range pred.(map[string]interface{}) {
					val, ok := entry.Meta[keyPred]
					if !ok {
						return false
					}
					if val != valPred {
						return false
					}
				}
				return true
			}
			var arr [2]interface{}
			arr[0] = defaultPred
			arr[1] = controller
			routers = append(routers, arr)
			fmt.Println(routers)
		}
	default:
		fmt.Println("didn't find requested controller")
	}
}

//TaskDeliver is to handle the message from app client
func TaskDeliver(postdata []byte, conn net.Conn) {
	for _, v := range routers {
		pred := v[0]
		act := v[1]
		var entermsg Msg
		err := json.Unmarshal(postdata, &entermsg)
		if err != nil {
			utils.Log(err)
		}

		rpcRequest := entermsg.Content
		utils.Log("xxx parsing the JSONRPC2.0 message from app client...")
		id := rpcRequest.ID
		utils.Log("xxx rpcRequest.id:", id)
		jsonrpc := rpcRequest.JSONRPC
		utils.Log("xxx rpcRequest.jsonrpc:", jsonrpc)
		method := rpcRequest.Method
		utils.Log("xxx rpcRequest.Method:", method)

		f := rpcRequest.Params
		key := f.(map[string]interface{})["key"].(string)
		utils.Log("rpcRequest.Params.Key:", key)
		channel := f.(map[string]interface{})["channel"].(string)
		utils.Log("rpcRequest.Params.Channel:", channel)

		if pred.(func(entermsg Msg) bool)(entermsg) {
			result := act.(Controller).Excute(entermsg)
			utils.Log("sending result to app client: ", string(result))
			conn.Write(result)
			return
		}
	}
}

//sendJsonrpcRequest is to send request to block chain service.
func sendJsonrpcRequest(method string) (*jsonrpc.RPCResponse, error) {
	var err error
	//rpcClient := jsonrpc.NewClient("http://my-rpc-service:8080/rpc")
	rpcClient := jsonrpc.NewClient("https://www.ninechain.net/api/v2")
	if rpcClient == nil {
		//fmt.Println("rpcClient is nil!")
		utils.Log("rpcClient is nil!")
		return nil, err
	}
	rpcResp, err := rpcClient.Call(method, &MethodParams{Channel: "vvtrip", Key: "00000000000000000000000000000001"})
	if err != nil {
		//utils.LOG.Error("rpcClient.CallFor failed: " + err.Error())
		//fmt.Printf("xxx err for rpcClient.Call:%v", err.Error())
		utils.Log("xxx err for rpcClient.Call:", err.Error())
	}
	id := rpcResp.ID
	utils.Log("xxx sendJsonrpcRequest() rpcResp.id:", id)
	jsonrpc := rpcResp.JSONRPC
	utils.Log("xxx sendJsonrpcRequest() rpcResp.jsonrpc:", jsonrpc)
	//rpcresult := rpcResp.Result
	//fix-me:
	//need serveral parser functions to parse different result structs.
	//tx_id := rpcresult["tx_id"].(string)
	//utils.Log("xxx sendJsonrpcRequest() rpcResp.Result.tx_id:", tx_id)
	return rpcResp, nil
}

//this is a sample of how to setup a controller;
//please pay attention: all the controller must be registered in the function init()
//一个controller实例, 注意： 所有的controller必须在init()函数内注册后才能被router分配

//EchoController is the general interface for all other controllers.
type EchoController struct {
}

//Excute is the function that each Controller needs to implement.
func (echoCtrl *EchoController) Excute(message Msg) []byte {
	mirrormsg, err := json.Marshal(message)

	var entermsg Msg
	err = json.Unmarshal(mirrormsg, &entermsg)
	if err != nil {
		utils.Log(err)
	}

	rpcRequest := entermsg.Content
	utils.Log("xxx parsing the JSONRPC2.0 message from app client...")
	/*
		id := rpcRequest.ID
		Log("xxx rpcRequest.id:", id)
		jsonrpc := rpcRequest.JSONRPC
		Log("xxx rpcRequest.jsonrpc:", jsonrpc)
	*/
	method := rpcRequest.Method
	utils.Log("xxx Excute() parsing Method:", method)
	/*
		f := rpcRequest.Params
		key := f.(map[string]interface{})["key"].(string)
		Log("rpcRequest.Params.Key:", key)
		channel := f.(map[string]interface{})["channel"].(string)
		Log("rpcRequest.Params.Channel:", channel)
	*/
	rpcResp, err := sendJsonrpcRequest(method)
	respMsg, err := json.Marshal(rpcResp)
	utils.Log("echo the message:", string(respMsg))
	utils.CheckError(err)
	return respMsg
}

func init() {
	var echo EchoController
	routers = make([][2]interface{}, 0, 20)
	Route(func(entry Msg) bool {
		if entry.Meta["meta"] == "test" {
			return true
		}
		return false
	}, &echo)
}
