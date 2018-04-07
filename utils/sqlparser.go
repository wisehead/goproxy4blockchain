package utils

import (
	"davinci/vt/sqlparser"
	"fmt"
	"regexp"
	"strings"
)

var numReg *regexp.Regexp
var spaceReg *regexp.Regexp
var quoteReg *regexp.Regexp
var doubleQuoteReg *regexp.Regexp
var inReg *regexp.Regexp
var valuesReg *regexp.Regexp
var blackSQLPrefix []string

type Marshal struct {
	stmt      sqlparser.Statement
	footprint string
}

func init() {

	numReg = regexp.MustCompile(`[+-]?((\d+(\.\d+)?)|((\.\d+)))([Ee]\d+)?`)
	spaceReg = regexp.MustCompile(`\s{2,}`)
	quoteReg = regexp.MustCompile(`".*?"`)
	doubleQuoteReg = regexp.MustCompile(`'.*?'`)
	inReg = regexp.MustCompile(`[Ii][Nn]\s*\([^),]*\)\s*,\s*\([^),]*\)`)
	valuesReg = regexp.MustCompile(`[Vv][Aa][Ll][Uu][Ee][Ss]\s*\([^)]*\)\s*,\s*\([^)]*\)`)

}

func (marshal *Marshal) GetStatment() sqlparser.Statement {
	return marshal.stmt
}

func (marshal *Marshal) GetTableName() string {

	if marshal.stmt == nil {
		return "null"
	}

	switch stmt := marshal.stmt.(type) {
	case *sqlparser.Select:
		return getTableNameList(stmt.From)
	case *sqlparser.Update:
		return getTableNameList(stmt.From)
	case *sqlparser.Insert:
		return sqlparser.String(stmt.Table)
	case *sqlparser.Delete:
		return getTableNameList(stmt.From)
	case *sqlparser.Transaction:
		return "null"
	case *sqlparser.DDL:
		return string(stmt.Table)
	}

	return "null"
}

func getTableNameList(tables sqlparser.TableExprs) string {
	tbname := make([]byte, 0, 10)
	for i, tb := range tables {
		if i != 0 {
			tbname = append(tbname, ',')
		}
		tbname = append(tbname, sqlparser.String(tb)...)
	}
	return string(tbname)
}

func FootPrintFormator(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
	switch node := node.(type) {
	case sqlparser.StrVal:
		buf.WriteArg(fmt.Sprintf("'S'"))
	case sqlparser.NumVal:
		buf.WriteArg(fmt.Sprintf("8"))
	case sqlparser.ValTuple:
		buf.WriteArg(fmt.Sprintf("(8)"))
	case sqlparser.ValArg:
		buf.WriteArg(fmt.Sprintf("?"))
	default:
		node.Format(buf)
	}
}

func (marshal *Marshal) Marshal(sql string) error {

	var err error

	sql = strings.Trim(sql, " \t\n")

	marshal.stmt, err = sqlparser.Parse(sql)
	if err != nil {
		return err
	}

	buf := sqlparser.NewTrackedBuffer(FootPrintFormator)
	buf.Myprintf("%v", marshal.stmt)

	//buf.ParsedQuery()

	marshal.footprint = numReg.ReplaceAllString(buf.String(), "8")
	//utils.LOG.Debug("ms = " + sqlparser.String(stmt))
	//utils.LOG.Debug("table = " + utils.GetTableName(stmt))
	return nil
}

func (marshal *Marshal) GetFootPrint() string {
	return marshal.footprint
}
