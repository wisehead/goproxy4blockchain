package utils

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
)

type backupValue struct {
	Backup int `json:"backup"`
}

type backupResponse struct {
	Success int         `json:"success"`
	Msg     string      `json:"msg"`
	Data    backupValue `json:"data"`
}

//--
func IsBackup(url string, ip string) bool {

	var err error
	url += "?ip=" + ip

	_, data, err := HttpGet(url)
	if err != nil {
		LOG.Error("Check Machine %s failed: %s", ip, err.Error())
		return false
	}

	backupResp := backupResponse{}
	err = json.Unmarshal(data, &backupResp)
	if err != nil {
		LOG.Error("Check Machine %s failed: %s", ip, err.Error())
		return false
	}
	return backupResp.Data.Backup == 1

}

func HttpGet(url string) (int, []byte, error) {

	resp, err := http.Get(url)
	if err != nil {
		return 0, nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}

	return resp.StatusCode, body, nil
}
