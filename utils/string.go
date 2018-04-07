package utils

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"time"
	"unicode"
)

const MAX_LOG_LINE = 4096

/**
 * 解析MySQL日志中的时间，时间样例为：060102 15:04:05
 */
func ParseMySQLTime(str string) string {
	if t, err := time.Parse("060102 15:04:05", str); err != nil {
		return ""
	} else {
		ptime := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d",
			t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
		return ptime
	}
}

//--
func ParseNoahTime(str string) time.Time {

	noahTime := time.Now()
	if len(str) >= len("00:00:00") && str[2] == ':' && str[5] == ':' {
		hour := int(ParseFirstInt(str))
		minute := int(ParseFirstInt(str[3:]))
		second := int(ParseFirstInt(str[6:]))
		noahTime = time.Date(noahTime.Year(), noahTime.Month(), noahTime.Day(), hour, minute, second, 0, time.Local)
	}

	return noahTime
}

/**
 * 解析时间中的日期 2015-05-05 19:18:26 -> 20150505
 */
func GetDate(str string) string {
	if len(str) >= len("2011-01-01") && str[4] == '-' && str[7] == '-' {
		return str[:4] + str[5:7] + str[8:10]
	} else {
		return ""
	}
}

func FormatTime(t time.Time) string {
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d",
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
}

func RemoveNewLine(str string) string {
	length := len(str)
	if length == 0 {
		return ""
	}

	i := 0
	buf := make([]byte, length)

	for j := 0; j < length; j++ {
		if str[j] != '\n' {
			buf[i] = str[j]
			i++
		}
	}

	if i == 0 {
		return ""
	}

	return string(buf[:i])
}

func EscapeString(str string) string {

	pos := 0
	buf := make([]byte, len(str)*2)

	for _, c := range []byte(str) {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos += 1
		}
	}

	return string(buf[:pos])
}

func toMatchEnd(str string, start int, isMatch func(c byte) bool) int {
	for isMatch(str[start]) {
		start++
	}
	return start
}

func ToCommentEnd(sql string) int {

	i := 0
	length := len(sql)
	if length >= 4 && sql[0] == '/' && sql[1] == '*' {
		i += 4
		for i < length {
			if sql[i-1] == '*' && sql[i] == '/' {
				return i + 1
			} else {
				i++
			}
		}
	}

	return i
}
func NextWord(str string) string {

	i := 0
	j := 0
	for i < len(str) && unicode.IsSpace(rune(str[i])) {
		i++
	}

	if i < len(str) {
		j = i + 1
		for j < len(str) && (unicode.IsLetter(rune(str[j])) || unicode.IsDigit(rune(str[j])) || str[j] == '_') {
			j++
		}
		return str[i:j]
	}

	return ""
}

func ParseComputeInt(str string) int64 {

	var result int64 = 0
	i := 0

	for i < len(str) && unicode.IsDigit(rune(str[i])) {
		result = result*10 + (int64)(str[i]-'0')
		i++
	}

	if i < len(str) && str[i] == ' ' {
		i++
	}

	if i < len(str) {
		switch str[i] {
		case 'g', 'G':
			result *= 1024 * 1024 * 1024
		case 'm', 'M':
			result *= 1024 * 1024
		case 'k', 'K':
			result *= 1024
		}
	}

	return result
}

//--
func ParseFirstInt(str string) int64 {

	var result int64 = 0
	i := 0
	for i < len(str) && !unicode.IsDigit(rune(str[i])) {
		i++
	}

	for i < len(str) && unicode.IsDigit(rune(str[i])) {
		result = result*10 + (int64)(str[i]-'0')
		i++
	}

	return result
}

func ParseFirstFloat(str string) float64 {

	var result float64 = 0
	i := 0
	for i < len(str) && !unicode.IsDigit(rune(str[i])) {
		i++
	}

	for i < len(str) && unicode.IsDigit(rune(str[i])) {
		result = result*10 + (float64)(str[i]-'0')
		i++
	}

	if str[i] == '.' {
		i++
		pow := 0.1
		for i < len(str) && unicode.IsDigit(rune(str[i])) {
			result += (float64)(str[i]-'0') * pow
			i++
			pow /= 10
		}
	}

	return result
}

func ParseSeconds(str string) time.Duration {
	ed := strings.Index(str, ":")
	if ed < 0 {
		return time.Duration(ParseFirstInt(str))
	}
	return time.Duration(ParseFirstInt(str[:ed])*60 + ParseFirstInt(str[ed:]))
}

func ParseUsedDB(sql string) string {

	i := ToCommentEnd(sql)
	length := len(sql)

	for i < length && (sql[i] == '\t' || sql[i] == ' ') {
		i++
	}

	if i+5 < length && strings.EqualFold("USE", sql[i:i+3]) {
		return NextWord(sql[i+3:])
	}

	return ""
}

//--
func RandStr(length int) string {
	buf := make([]byte, length, length)
	for i := 0; i < length; i++ {
		buf[i] = byte(33 + rand.Int()%93)
	}
	return string(buf)
}

/**
 * For multi Query, extract the first SQL
 */
//--
func getFirstSQL(sql string) (string, int) {

	i := 0
	length := len(sql)

	for i < length {

		if sql[i] == '\\' && i < length-1 {
			i += 2
			continue
		}

		if sql[i] == ';' {
			return sql[:i], i + 1
		} else if sql[i] == '"' || sql[i] == '`' || sql[i] == '\'' {
			i += toQuoteEnd(sql[i:], "")
		} else {
			i++
		}
	}

	return sql, i
}

func ToBracketEnd(text string) int {
	length := len(text)
	i := 0
	for i < length && (text[i] == '\t' || text[i] == ' ') {
		i++
	}

	if i < length && text[i] == '[' {
		i++
		for i < length && text[i-1] != ']' {
			i++
		}
	}
	return i
}

//--
func GetMultiSQL(sql string) []string {
	sqlList := make([]string, 0, 1)
	fs, i := getFirstSQL(sql)

	for len(fs) > 0 {
		sqlList = append(sqlList, fs)

		for i < len(sql) && unicode.IsSpace(rune(sql[i])) {
			i++
		}

		if i == len(sql) {
			break
		}

		sql = sql[i:]
		fs, i = getFirstSQL(sql)
	}

	return sqlList
}

func GetCharsetFromSQL(sql string) string {

	if len(sql) > 10 && strings.EqualFold("SET", sql[0:3]) {
		sql = strings.ToUpper(sql[3:])
		i := strings.Index(sql, "NAMES")
		length := len(sql)
		if i > 0 && i+5 < length {
			i += 5
			for i < length && (sql[i] == ' ' || sql[i] == '\t') {
				i++
			}
			j := i + 1
			if j < length {
				for j < length && sql[j] >= 'A' && sql[j] <= 'Z' {
					j++
				}
				return sql[i:j]
			}
		}
	}

	return ""
}

func IsMasterNeed(sql string) bool {

	if len(sql) > 3 && strings.EqualFold("SET", sql[0:3]) {
		if strings.Contains(strings.ToUpper(sql[3:]), "NAMES") {
			return true
		}
		return false
	}

	return true
}

func IsSlaveNeed(sql string) bool {

	whiteSQL := []string{"USE", "SELECT", "PREPARE", "EXEC", "SHOW"}

	_ = whiteSQL
	for _, wsq := range whiteSQL {
		if len(sql) > len(wsq) && strings.EqualFold(wsq, sql[:len(wsq)]) {
			return true
		}
	}

	return false
}

func IsAuditSql(sql string) bool {

	if len(sql) <= 4 || len(sql) >= 64 {
		return true
	}

	if strings.EqualFold(sql, "SELECT 1") {
		return false
	}

	if strings.IndexByte(sql, ';') >= 0 {
		return true
	}

	blackList := [][]string{
		{"SHOW", "STATUS"},
		{"SHOW", "VARIABLES"},
		{"SELECT @@VERSION_COMMENT LIMIT"},
		{"SELECT", "FROM heartbeat WHERE"},
		{"REPLACE INTO heartbeat SET"},
		{"SHOW", "INNODB", "STATUS"},
		{"SHOW", "PROCESSLIST"},
		{"SHOW", "PLUGINS"},
		{"SHOW", "FULL", "TABLES", "FROM", "LIKE", "PROBABLYNOT"},
		{"SET", "AUTOCOMMIT"},
	}

	for _, blackSQL := range blackList {

		length := len(blackSQL[0])
		if len(sql) > length && strings.EqualFold(blackSQL[0], sql[:length]) {
			upperSQL := strings.ToUpper(sql[length:])

			i := 1
			for i < len(blackSQL) && len(upperSQL) > 0 {
				keyWord := blackSQL[i]
				idx := strings.Index(upperSQL, keyWord)
				if idx < 0 {
					break
				}
				idx += len(keyWord)
				if idx < len(upperSQL) {
					upperSQL = upperSQL[idx:]
				} else {
					upperSQL = ""
				}
				i++
			}

			if i == len(blackSQL) {
				return false
			}
		}
	}

	return true
}

//--
func IntToIP(v uint) string {
	return fmt.Sprintf("%d.%d.%d.%d", (v>>24)&0xFF, (v>>16)&0xFF, (v>>8)&0xFF, v&0xFF)
}

func IntReverse(v uint) uint {
	var r uint
	for v != 0 {
		r = r*uint(10) + v%uint(10)
		v /= uint(10)
	}
	return r
}

func ToPositive(str string) string {

	v := ParseFirstInt(str)
	v = int64(1024)*int64(1024)*int64(1024)*int64(4) - v

	return fmt.Sprintf("%d", v)
}

func Md5Sum(str string) string {
	h := md5.New()
	h.Write([]byte(str))
	return hex.EncodeToString(h.Sum(nil))
}
