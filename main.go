package main

import (
	"fmt"
	"goproxy/jsonrpc"
)

//MethodParams for JSON-RPC 2.0 parameters.
type MethodParams struct {
	Channel string `json:"channel"`
	Key     string `json:"key"`
}

type User struct {
	Name      string
	IsAdmin   bool
	Followers uint
}

func main() {
	//rpcClient := jsonrpc.NewClient("http://my-rpc-service:8080/rpc")
	rpcClient := jsonrpc.NewClient("https://www.ninechain.net/api/v2")
	if rpcClient == nil {
		fmt.Println("rpcClient is nil!")
		return
	}

	//var person *Person
	//err := rpcClient.CallFor(&person, "source-state", 4711)
	/*
			rpcClient.Call("anonymousStructWithTags", struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}{"Alex", 33})
	*/
	/*
		data := []byte(`{"Name":"gopher","IsAdmin":false,"Followers":8900}`)
		var newUser = new(User)
		err := json.Unmarshal(data, &newUser)
		if err != nil {
			fmt.Errorf("Can not decode data: %v\n", err)
		}
		fmt.Printf("%v\n", newUser)
	*/

	rpcClient.Call("source-state", &MethodParams{Channel: "vvtrip", Key: "00000000000000000000000000000001"})
	/*
		if err != nil {
			utils.LOG.Error("rpcClient.CallFor failed: " + err.Error())
		}
	*/
	/*
		if person == nil {
			fmt.Println("person is nil!")
			return
		}

		person.Age = 33
		rpcClient.Call("updatePerson", person)
	*/
}
