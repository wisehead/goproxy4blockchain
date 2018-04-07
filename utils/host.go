package utils

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/op/go-logging"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

type MySQLConnAddr struct {
	Username string
	Host     string
}

var HostIP uint
var mySQLPort int

var CurrentProcessList map[string]*MySQLConnAddr

var LOG = logging.MustGetLogger("example")

func init() {
	InitLog()
	InitHostIP()
	CurrentProcessList = make(map[string]*MySQLConnAddr)
}

//--
func InitLog() {

	var err error
	var format = logging.MustStringFormatter(
		"%{time:2006-01-02 15:04:05.000} %{level:.4s} %{message}",
	)

	var logfile *os.File

	//backend1 := logging.NewLogBackend(os.Stderr, "", 0)
	logfile, err = os.OpenFile("log/davinci.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		fmt.Println("Open logfile failed. ", err)
	}

	syscall.Dup2(int(logfile.Fd()), 1)
	syscall.Dup2(int(logfile.Fd()), 2)

	logfile.Close()

	backend2 := logging.NewLogBackend(os.Stderr, "", 0)

	// For messages written to backend2 we want to add some additional
	// information to the output, including the used log level and the name of
	// the function.
	backend2Formatter := logging.NewBackendFormatter(backend2, format)

	// Only errors and more severe messages should be sent to backend1
	//backend1Leveled := logging.AddModuleLevel(backend1)
	//backend1Leveled.SetLevel(logging.ERROR, "")

	// Set the backends to be used.
	logging.SetBackend(backend2Formatter)
}

//--
func OpenFile(name string) (file *os.File, err error) {

	r, e := syscall.Open(name, syscall.O_RDONLY, 0)
	if e != nil {
		return nil, &os.PathError{"open", name, e}
	}

	return os.NewFile(uintptr(r), name), nil
}

func InitHostIP() {
	ifaces, _ := net.Interfaces()
	for _, i := range ifaces {
		addrs, _ := i.Addrs()
		for _, addr := range addrs {

			switch v := addr.(type) {
			case *net.IPNet:
				if v.IP.IsGlobalUnicast() {
					ipv4 := v.IP.To4()
					HostIP = (uint(ipv4[0]) << 24) +
						(uint(ipv4[1]) << 16) +
						(uint(ipv4[2]) << 8) +
						uint(ipv4[3])
					//fmt.Printf("%d:%d.%d.%d.%d\n", HostIP, ipv4[0], ipv4[1], ipv4[2], ipv4[3])
				}

			default:
				fmt.Printf("default: %v\n", v)
			}

			if HostIP != 0 {
				break
			}
		}

		if HostIP != 0 {
			break
		}
	}
}

func InitMySQLPort(logname string) error {

	tcpPrefix := "Tcp port:"
	file, err := os.Open(logname)
	if err != nil {
		return err
	}

	defer file.Close()

	file.Seek(0, os.SEEK_SET)
	reader := bufio.NewReader(file)

	for i := 0; i < 10; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}

		if len(line) == 0 || !strings.HasPrefix(line, tcpPrefix) {
			continue
		}

		mySQLPort = int(ParseFirstInt(line[len(tcpPrefix):]))
	}

	if mySQLPort == 0 {
		return errors.New("Cannot parse MySQL Port in " + logname)
	}

	return nil

}

func getMysqlVariables(mysqlbase string, key string) string {

	sql := fmt.Sprintf("SHOW GLOBAL VARIABLES WHERE Variable_name='%s'", key)
	cmd := exec.Command(mysqlbase+"/bin/mysql",
		"--defaults-extra-file="+mysqlbase+"/etc/user.admin.cnf", "-Ns", "-e", sql)

	output, err := cmd.Output()
	if err != nil {
		return ""
	}

	if len(key)+1 >= len(output) {
		return ""
	}

	if output[len(output)-1] == '\n' || output[len(output)-1] == '\r' {
		output = output[:len(output)-1]
	}

	i := len(key) + 1
	for i < len(output) && (output[i] == ' ' || output[i] == '\t') {
		i++
	}

	return string(output[i:])

}

//--
func GetLogFiles(mysqlbase string) (bool, string, string) {

	var is_on bool
	var glog_file string
	var slog_file string

	is_on = strings.EqualFold(getMysqlVariables(mysqlbase, "general_log"), "ON")

	if is_on {
		glog_file = getMysqlVariables(mysqlbase, "general_log_file")
	}

	slog_file = getMysqlVariables(mysqlbase, "slow_query_log_file")

	return is_on, glog_file, slog_file
}

//--
func UpdateProcessList(noahPath string, port int) bool {

	now := time.Now()
	filename := fmt.Sprintf("%s/processlist_%d.%d%02d%02d",
		noahPath, port, now.Year(), now.Month(), now.Day())

	if stat, err := os.Stat(filename); err != nil || stat.IsDir() {
		filename = fmt.Sprintf("%s/processlist.%d%02d%02d",
			noahPath, now.Year(), now.Month(), now.Day())
	}

	if stat, err := os.Stat(filename); err != nil || stat.IsDir() {
		return false
	}

	cmd := exec.Command("tail", "-4096", filename)
	output, err := cmd.Output()
	if err != nil || len(output) < 10 {
		return false
	}

	if output[len(output)-1] == '\n' || output[len(output)-1] == '\r' {
		output = output[:len(output)-1]
	}

	lines := strings.FieldsFunc(string(output), func(c rune) bool { return c == '\r' || c == '\n' })

	lastLine := len(lines) - 1

	for lastLine >= 0 && len(lines[lastLine]) < 10 {
		lastLine--
	}

	if lastLine >= 0 {

		maxTime := ParseNoahTime(lines[lastLine][:9])

		if now.Unix()-maxTime.Unix() < 30 {

			tMap := make(map[string]*MySQLConnAddr)
			for i, line := range lines {

				if i > lastLine {
					break
				}

				if len(line) < 10 {
					continue
				}

				noahTime := ParseNoahTime(line[:9])

				if noahTime.Unix() == maxTime.Unix() {
					threadid, addr := getConnAddr(line[9:])
					if len(threadid) > 0 && addr != nil {
						tMap[threadid] = addr
					}
				}

			}

			CurrentProcessList = tMap
		}

	}

	return true
}

func GetMySQLSystemThreadIds(mysqlbase string) []string {
	sql := "SHOW PROCESSLIST"
	cmd := exec.Command(mysqlbase+"/bin/mysql",
		"--defaults-extra-file="+mysqlbase+"/etc/user.admin.cnf", "-Ns", "-e", sql)

	output, err := cmd.Output()
	if err != nil {
		return nil
	}

	tMap := make(map[string]*MySQLConnAddr)
	if output[len(output)-1] == '\n' || output[len(output)-1] == '\r' {
		output = output[:len(output)-1]
	}

	result := make([]string, 0, 2)
	lines := strings.Split(string(output), "\n")
	if len(lines) > 0 {
		for _, line := range lines {
			if len(line) <= 10 {
				continue
			}

			threadid, addr := getConnAddr(line)
			if len(threadid) > 0 && addr != nil {
				if addr.Username == "system user" {
					result = append(result, threadid)
					LOG.Warning("Find System User thread %s", threadid)
				} else {
					tMap[threadid] = addr
				}
			}

		}
	}

	CurrentProcessList = tMap
	return result
}

/*
+----+------+-----------+------+---------+------+-------+------------------+
| Id | User | Host      | db   | Command | Time | State | Info             |
+----+------+-----------+------+---------+------+-------+------------------+
|  1 | root | localhost | NULL | Query   |    0 | NULL  | show PROCESSLIST |
+----+------+-----------+------+---------+------+-------+------------------+
*/
/*
17:46:42 31791380       admin   localhost       NULL    Query   0       NULL    SHOW PROCESSLIST
17:46:52 31791389       admin   localhost       NULL    Query   0       NULL    SHOW PROCESSLIST
17:47:02 31791398       admin   localhost       NULL    Query   0       NULL    SHOW PROCESSLIST
17:47:12 31791409       admin   localhost       NULL    Query   0       NULL    SHOW PROCESSLIST
17:47:22 31791418       admin   localhost       NULL    Query   0       NULL    SHOW PROCESSLIST
17:47:32 31791427       admin   localhost       NULL    Query   0       NULL    SHOW PROCESSLIST
*/
//--
func getConnAddr(line string) (string, *MySQLConnAddr) {

	var i int = 0
	var j int = 0
	var threadid string

	if len(line) < 4 {
		return "", nil
	}

	addr := new(MySQLConnAddr)

	if line[i] >= '1' && line[i] <= '9' {

		for i < len(line) && line[i] >= '0' && line[i] <= '9' {
			i++
		}

		threadid = line[:i]
	} else {
		return "", nil
	}

	i++
	if i < len(line) {

		j = i
		if line[j] == '\t' {
			addr.Username = "null"
		} else {
			for j < len(line) && line[j] != '\t' {
				j++
			}
			addr.Username = line[i:j]
		}
	} else {
		return "", nil
	}

	i = j + 1
	if i < len(line) {
		j = i
		if line[j] == '\t' {
			addr.Host = "null"
		} else {
			for j < len(line) && line[j] != '\t' {
				j++
			}
			host := line[i:j]
			idx := strings.IndexByte(host, ':')
			if idx > 0 {
				addr.Host = host[:idx]
			} else {
				addr.Host = host
			}
		}
	}

	if len(addr.Username) == 0 || len(addr.Host) == 0 {
		return "", nil
	}

	return threadid, addr
}

func GetFileSize(filename string) int64 {
	if fi, err := os.Stat(filename); err == nil {
		return fi.Size()
	}
	return -1
}

/*
func getSystemUser(line string) string {

	var i int = 0
	var j int = 0
	var threadid string
	var username string

	if len(line) < 4 {
		return ""
	}

	if line[i] >= '1' && line[i] <= '9' {

		for i < len(line) && line[i] >= '0' && line[i] <= '9' {
			i++
		}

		threadid = line[:i]
	} else {
		return ""
	}

	i++
	if i < len(line) {
		j = i
		for j < len(line) && line[j] != '\t' {
			j++
		}
		username = line[i:j]
		if username != "system user" {
			return ""
		}
	}

	if j+1 < len(line) && line[j] == '\t' && line[j+1] == '\t' {
		return threadid
	}

	return ""
}
*/
