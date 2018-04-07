package utils

import (
	"strings"
	"unicode"
)

var sqlKeyWordList []SqlTypeMap

type SqlTypeMap struct {
	key   []string
	value string
}

type FingerPrint struct {
	Sqltype string
	Finger  string
}

const (
	GenericNone = iota
	GenericInt
	GenericString
)

func init() {
	sqlKeyWordList = []SqlTypeMap{
		SqlTypeMap{[]string{"SHOW", "PROCESSLIST"}, "SQLCOM_SHOW_PROCESSLIST"},
		SqlTypeMap{[]string{"SHOW", "SLAVE", "STAT"}, "SQLCOM_SHOW_SLAVE_STATUS"},
		SqlTypeMap{[]string{"SHOW", "MASTER", "STAT"}, "SQLCOM_SHOW_MASTER_STATUS"},
		SqlTypeMap{[]string{"SHOW", "INNODB", "STAT"}, "SQLCOM_SHOW_INNODB_STATUS"},
		SqlTypeMap{[]string{"SHOW", "STAT"}, "SQLCOM_SHOW_STATUS"},
		SqlTypeMap{[]string{"INSERT", "SELECT"}, "SQLCOM_INSERT_SELECT"},

		SqlTypeMap{[]string{"SELECT"}, "SQLCOM_SELECT"},
		SqlTypeMap{[]string{"INSERT"}, "SQLCOM_INSERT"},
		SqlTypeMap{[]string{"REPLACE"}, "SQLCOM_REPLACE"},
		SqlTypeMap{[]string{"UPDATE"}, "SQLCOM_UPDATE"},
		SqlTypeMap{[]string{"DELETE"}, "SQLCOM_DELETE"},

		SqlTypeMap{[]string{"USE"}, "SQLCOM_CHANGE_DB"},
		SqlTypeMap{[]string{"SHOW"}, "SQLCOM_SHOW"},

		SqlTypeMap{[]string{"CREATE"}, "SQLCOM_CREATE"},
		SqlTypeMap{[]string{"ALTER"}, "SQLCOM_ALTER"},
		SqlTypeMap{[]string{"TRUNCATE"}, "SQLCOM_TRUNCATE"},
		SqlTypeMap{[]string{"DROP"}, "SQLCOM_DROP"},
		SqlTypeMap{[]string{"SET"}, "SQLCOM_SET_OPTION"},
		SqlTypeMap{[]string{"BEGIN"}, "SQLCOM_COMMIT"},
		SqlTypeMap{[]string{"COMMIT"}, "SQLCOM_COMMIT"},
		SqlTypeMap{[]string{"ROLLBACK"}, "SQLCOM_ROLLBACK"},
		SqlTypeMap{[]string{"LOAD"}, "SQLCOM_LOAD"},
		SqlTypeMap{[]string{"REVOKE"}, "SQLCOM_REVOKE"},
	}

}

/**
 *
 * 用于生成SQL指纹，只需遍历SQL 2次
 * 1：去除连续空格
 * 2：替换字符串，数字，IN，VALUES列表，注释等
 */
func (fp *FingerPrint) GenerateFinger(sql string, charset string) {

	isHintSQL := false
	length := len(sql)
	result := make([]byte, 0, length*2+1)
	i := 0

	for length > 0 && unicode.IsSpace(rune(sql[length-1])) {
		length--
	}

	for i < length {
		switch c := sql[i]; {
		case c == '\'' || c == '"':
			i += toQuoteEnd(sql[i:], charset)
			result = append(result, '\'', 's', '\'')
		case (c >= '0' && c <= '9') || c == '.':

			i += toNumberEnd(sql[i:])

			if i > 0 && sql[i-1] == '.' {
				if i > 1 && sql[i-2] >= '0' && sql[i-2] <= '9' {
					result = append(result, '1')
				}
				result = append(result, '.')
			} else {
				result = append(result, '1')
			}

		case c == 'I' || c == 'i':
			vl := len(" IN")

			if i > 0 && i+vl+1 < length && strings.EqualFold(sql[i-1:i-1+vl], " IN") && !isEnglisthLetter(sql[i+vl-1]) {
				i += vl - 1
				step, gtype := toArgListEnd(sql[i:], charset)
				i += step
				if gtype == GenericInt {
					result = append(result, []byte("IN(1) ")...)
				} else if gtype == GenericString {
					result = append(result, []byte("IN('s') ")...)
				} else {
					result = append(result, []byte("IN")...)
					result, i = toValueListEnd(sql, charset, i, result)
				}
			} else {
				result = append(result, sql[i])
				i++
			}
		case c == 'V' || c == 'v':
			vl := len(" VALUES")
			if i > 0 && i+vl+1 < length && strings.EqualFold(sql[i-1:i-1+vl], " VALUES") && !isEnglisthLetter(sql[i-1+vl]) {
				i += vl - 1
				result = append(result, []byte("VALUES")...)
				result, i = toValueListEnd(sql, charset, i, result)
			} else {
				result = append(result, sql[i])
				i++
			}
		case c == '/':
			if i+4 < length && sql[i+1] == '*' {
				if sql[i+2] == '!' {
					isHintSQL = true
					result = append(result, sql[i:i+3]...)
					i += 3
				} else {
					i += toCommentEnd(sql[i:])
				}
			} else if i < length {
				result = append(result, sql[i])
				i++
			}
		default:

			if sql[i] == ' ' || sql[i] == '\t' {
				if len(result) > 0 && result[len(result)-1] != ' ' {
					result = append(result, ' ')
				}
			} else {
				result = append(result, sql[i])
			}
			i++
		}
	}

	if isHintSQL {
		inHint := false
		length := len(result)
		src := result
		result = make([]byte, 0, length+1)
		i := 0
		for i < length {
			if !inHint {
				if i+5 < length && src[i] == '/' && src[i+1] == '*' && src[i+2] == '!' {
					i += 3
					if '0' <= src[i] && src[i] <= '9' {
						i++
					}
					if len(result) > 0 && result[len(result)-1] != ' ' {
						result = append(result, ' ')
					}
					inHint = true
					continue
				}
			} else {
				if i+1 < length && src[i] == '*' && src[i+1] == '/' {
					i += 2
					inHint = false
					continue
				}
			}

			if src[i] == ' ' && len(result) > 0 && result[len(result)-1] == ' ' {
				i++
				continue
			}

			result = append(result, src[i])
			i++
		}
	}
	fp.Finger = string(result)
	fp.Sqltype = getSQLCom(strings.ToUpper(fp.Finger))
}

/**
 * 通过正则表达式替换生成SQL指纹，处理速度约6000 msg/s
 */
func (fp *FingerPrint) RegGenerateFinger(sql string) {

	sql = removeQuote(sql)
	sql = spaceReg.ReplaceAllString(sql, " ")
	sql = numReg.ReplaceAllString(sql, "1")

	sql = removeArglist(sql)

	fp.Finger = sql
	fp.Sqltype = getSQLCom(strings.ToUpper(sql))
}

/**
 * 找到匹配的括号末尾
 */
func toQuoteEnd(text string, charset string) int {

	length := len(text)
	if length < 2 || (text[0] != '"' && text[0] != '`' && text[0] != '\'') {
		return 1
	}

	i := 1

	isGBK := false
	isUtf8 := false
	isLatin1 := strings.EqualFold(charset, "LATIN1")
	if !isLatin1 {
		isUtf8 = strings.EqualFold(charset, "UTF8")
	}

	if !isLatin1 && !isUtf8 {
		isGBK = strings.EqualFold(charset, "GBK")
	}

	for i = 1; i < length; i++ {
		if i+2 < length && !isLatin1 && !isGBK &&
			(text[i]&0xE0) == 0xE0 &&
			((text[i+1]&0x80) == 0x80 && (text[i+2]&0x80) == 0x80 || isUtf8) {
			// UTF-8 三字节编码
			i += 2
			isUtf8 = true
		} else if text[i] == '\\' || (!isLatin1 && !isUtf8 && rune(text[i]) >= 0x81) {
			// GBK等双字节
			if i+1 < length {
				if text[i] == '\\' || (rune(text[i]) >= 0x81 &&
					rune(text[i+1]) >= 0x40 && rune(text[i+1]) <= 0xFE) {
					i++
				}
			}
		} else if text[i] == text[0] {
			return i + 1
		}
	}

	return length
}

func toHexEnd(text string) int {

	i := 0
	length := len(text)

	if length == 0 {
		return 0
	}

	for (text[i] >= '0' && text[i] <= '9') ||
		(text[i] >= 'A' && text[i] <= 'F') ||
		(text[i] >= 'a' && text[i] <= 'f') {
		i++
		if i >= length {
			break
		}
	}

	return i
}

/**
 * 提取数字，状态机如下
 *  0  1  2
 *  73.47E-6
 *
 */
func toNumberEnd(text string) int {

	state := 0
	length := len(text)

	if length > 2 && text[0] == '0' && (text[1] == 'x' || text[1] == 'X') {
		return 2 + toHexEnd(text[2:])
	}

	for i := 1; i < length; i++ {
		switch state {
		case 0: //Inite state
			if text[i] >= '0' && text[i] <= '9' {
				state = 0
			} else if text[i] == '.' {
				state = 1
			} else {
				return i
			}
		case 1:
			if text[i] >= '0' && text[i] <= '9' {
				state = 1
			} else if text[i] == 'e' || text[i] == 'E' {
				state = 2
				if i+1 < length && text[i+1] == '-' {
					i++
				}
			} else {
				return i
			}
		case 2:
			if text[i] >= '0' && text[i] <= '9' {
				state = 2
			} else {
				return i
			}
		}
	}
	return length
}

/**
 * 提取(,,,,),(,,,,)类似的列表
 */
func toArgListEnd(text string, charset string) (int, int) {

	i := 0
	length := len(text)
	gtype := GenericNone

	for i < length && text[i] == ' ' {
		i++
	}

	if i >= length {
		return i, gtype
	}

	if text[i] == '(' {
		j := i + 1
		for j < length && text[j] == ' ' {
			j++
		}
		if j >= length {
			return j, gtype
		}

		if text[j] == '"' || unicode.IsDigit(rune(text[j])) || text[j] == '\'' {

			if gtype == GenericNone {
				if text[j] == '"' || text[j] == '\'' {
					gtype = GenericString
				} else {
					gtype = GenericInt
				}
			}

			i += toValueEnd(text[i:], charset)

			for i < length {

				for i < length && text[i] == ' ' {
					i++
				}

				if i < length && text[i] == ',' {
					i++
					i += toValueEnd(text[i:], charset)
				} else {
					break
				}
			}
		}

	}

	return i, gtype
}

func toValueListEnd(sql string, charset string, i int, result []byte) ([]byte, int) {

	for i < len(sql) && unicode.IsSpace(rune(sql[i])) {
		i++
	}

	j := i
	for j < len(sql) {

		bracket := 0
		if sql[j] == '"' || sql[j] == '\'' {
			j += toQuoteEnd(sql[j:], charset)
			//fmt.Printf("toQuoteEnd %d %d %d %d: %02x %02x %02x %02x\n", j-3, j-2, j-1, j, sql[j-3], sql[j-2], sql[j-1], sql[j])
			result = append(result, '\'', 's', '\'')
		} else if (sql[j] >= '0' && sql[j] <= '9') || sql[j] == '.' {

			j += toNumberEnd(sql[j:])

			if j > 0 && sql[j-1] == '.' {
				if j > 1 && sql[j-2] >= '0' && sql[j-2] <= '9' {
					result = append(result, '1')
				}
				result = append(result, '.')
			} else {
				result = append(result, '1')
			}
		} else {
			if sql[j] == '(' {
				bracket++
			} else if sql[j] == ')' {
				bracket--
			}
			if sql[j] != ' ' && sql[j] != '\t' {
				result = append(result, sql[j])
			}
			j++
		}

		if j < len(sql) && bracket <= 0 && sql[j] == ')' {
			if sql[j] != ' ' && sql[j] != '\t' {
				result = append(result, sql[j])
			}
			j++
			break
		}

	}

	i = j
	if i < len(sql) {
		for j < len(sql) && (sql[j] == ' ' || sql[j] == '\t' || sql[j] == ',') {
			k := j
			for k < len(sql) && (sql[k] == ' ' || sql[k] == '\t' || sql[k] == ',') {
				k++
			}

			j = k
			if sql[k] != '(' {
				break
			}

			bracket := 0
			for j < len(sql) && (bracket > 1 || sql[j] != ')') {

				if sql[j] == '"' || sql[j] == '\'' {
					j += toQuoteEnd(sql[j:], charset)
				} else {
					if sql[j] == '(' {
						bracket++
					} else if sql[j] == ')' {
						bracket--
					}
					j++
				}
			}

			if j < len(sql) && sql[j] == ')' {
				j++
			}
		}

		if i != j {
			result = append(result, ' ')
			i = j
		}

	}

	return result, i
}

/**
 * 提取一个形如(,,,,)的值列表
 */
func toValueEnd(text string, charset string) int {
	length := len(text)
	i := 0
	for i < length && text[i] == ' ' {
		i++
	}
	if i < length && text[i] != '(' {
		return i
	}

	for i < length && text[i] != ')' {
		if text[i] == '"' || text[i] == '\'' {
			i += toQuoteEnd(text[i:], charset)
		} else {
			i++
		}
	}

	if i < length {
		i++
	}
	return i
}

/**
 * 提取注释
 */
func toCommentEnd(text string) int {
	length := len(text)
	if length <= 4 {
		return length
	}

	for i := 2; i < length; i++ {
		if text[i-1] == '*' && text[i] == '/' {
			return i + 1
		}
	}
	return length
}

/**
 * 移除空格，放到新的字符串中
 */
func removeSpace(sql string) string {

	length := len(sql)
	if length < 2 {
		return sql
	}

	buf := make([]byte, 0, length)
	buf = append(buf, sql[0])
	j := 1
	for j < length-1 {

		if !unicode.IsSpace(rune(sql[j-1])) && unicode.IsSpace(rune(sql[j])) {
			buf = append(buf, ' ')
			j++
		} else if unicode.IsSpace(rune(sql[j-1])) && unicode.IsSpace(rune(sql[j])) {
			j++
		} else {
			buf = append(buf, sql[j])
			j++
		}
	}

	if !unicode.IsSpace(rune(sql[j])) {
		buf = append(buf, sql[j])
	}

	return string(buf)
}

/**
 * 将双引号和单引号内容替换成'S'
 */
func removeQuote(sql string) string {

	length := len(sql)
	if length < 2 {
		return sql
	}

	sq := 0
	bq := 0
	result := make([]byte, 0, length*2+1)

	for i := 0; i < length; i++ {
		switch sql[i] {
		case '\'':
			if sq == 0 && bq == 0 {
				sq = 1
				result = append(result, '\'')
			} else if sq == 1 {
				sq = 0
				result = append(result, 's')
				result = append(result, '\'')
			}
		case '"':
			if sq == 0 && bq == 0 {
				bq = 1
				result = append(result, '\'')
			} else if bq == 1 {
				bq = 0
				result = append(result, 's')
				result = append(result, '\'')
			}
		default:
			if sq == 0 && bq == 0 {
				result = append(result, sql[i])
			} else if sql[i] == '\\' {
				i++
			}
		}
	}

	return string(result[:len(result)])
}

func removeArglist(sql string) string {
	sql = inReg.ReplaceAllString(sql, "in (1)")
	sql = valuesReg.ReplaceAllString(sql, "values (1)")
	return sql
}

func isEnglisthLetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

/**
 * 获取SQL的Com类型
 */
func getSQLCom(sql string) string {

	for _, typeMap := range sqlKeyWordList {

		if len(typeMap.key) == 1 {
			if strings.HasPrefix(sql, typeMap.key[0]) {
				return typeMap.value
			}
		} else {
			j := 0
			for _, keyword := range typeMap.key {
				t := strings.Index(sql[j:], keyword)
				if t >= 0 {
					j += t + len(keyword)
				} else {
					j = -1
					break
				}
			}

			if j > 0 {
				return typeMap.value
			}
		}
	}
	return "SQLCOM_OTHER"
}
