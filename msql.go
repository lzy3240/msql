package msql

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/demdxx/gocast"
	_ "github.com/go-sql-driver/mysql" //...
)

//Msql 结构体
type Msql struct {
	user     string
	passwd   string
	host     string
	port     string
	database string
	charset  string
	Db       *sql.DB
}

//NewMsql 初始化连接
func NewMsql(user, passwd, host, port, database, charset string) Msql {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s", user, passwd, host, port, database, charset)
	//打开连接失败
	db, err := sql.Open("mysql", dbDSN)
	//defer MysqlDb.Close();
	if err != nil {
		fmt.Println("dbDSN: " + dbDSN)
		panic("数据源配置不正确: " + err.Error())
	}

	// 最大连接数
	db.SetMaxOpenConns(100)
	// 闲置连接数
	db.SetMaxIdleConns(20)
	// 最大连接周期
	db.SetConnMaxLifetime(100 * time.Second)

	if err = db.Ping(); nil != err {
		panic("数据库链接失败: " + err.Error())
	}

	return Msql{
		user:     user,
		passwd:   passwd,
		host:     host,
		port:     port,
		database: database,
		charset:  charset,
		Db:       db,
	}
}

// Queryby ...查询操作
func (m *Msql) Queryby(db *sql.DB, sqlstr string, args ...interface{}) *[]map[string]interface{} {
	// rows, err := db.Query(sqlstr)
	// if err != nil {
	// 	fmt.Printf("err:%v\n", err)
	// }
	// defer rows.Close()

	// `SELECT * FROM user WHERE mobile=?`
	stmt, err := db.Prepare(sqlstr)
	checkErr(err)
	defer stmt.Close()
	rows, err := stmt.Query(args...)
	checkErr(err)
	//遍历每一行
	colNames, _ := rows.Columns()
	var cols = make([]interface{}, len(colNames))
	for i := 0; i < len(colNames); i++ {
		cols[i] = new(interface{})
	}
	var maps = make([]map[string]interface{}, 0)
	for rows.Next() {
		err := rows.Scan(cols...)
		checkErr(err)
		var rowMap = make(map[string]interface{})
		for i := 0; i < len(colNames); i++ {
			rowMap[colNames[i]] = convertRow(*(cols[i].(*interface{})))
		}
		maps = append(maps, rowMap)
	}
	//fmt.Println(maps)
	return &maps //返回指针
}

// convertRow 行数据转换
func convertRow(row interface{}) interface{} {
	switch row.(type) {
	case int:
		return gocast.ToInt(row)
	case int32:
		return gocast.ToFloat32(row)
	case int64:
		return gocast.ToFloat64(row)
	case float32:
		return gocast.ToFloat32(row)
	case float64:
		return gocast.ToFloat64(row)
	case string:
		return gocast.ToString(row)
	case []byte:
		return gocast.ToString(row)
	case bool:
		return gocast.ToBool(row)
	}
	return row
}

//Modifyby 修改数据操作
func (m *Msql) Modifyby(db *sql.DB, sqlstr string, args ...interface{}) int64 {
	// `INSERT user (uname, age, mobile) VALUES (?, ?, ?)`
	// "update user set mobile=? where id=?"
	// "DELETE FROM user where id=?"
	stmt, err := db.Prepare(sqlstr) // Exec、Prepare均可实现增删改查
	checkErr(err)
	defer stmt.Close()
	res, err := stmt.Exec(args...)
	checkErr(err)
	//判断执行结果
	num, err := res.RowsAffected()
	checkErr(err)
	return num
}

//checkErr 检查错误
func checkErr(err error) {
	if err != nil {
		fmt.Println(err) //panic(err)
	}
}
