package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v4"
	. "github.com/lemonmigration/configutil"
	"github.com/tealeg/xlsx"
	"gopkg.in/yaml.v2"
)

/*
 CGO_ENABLED=0  GOOS=linux GOARCH=amd64 go build -o transdata ./main.go
 CGO_ENABLED=0 GOOS=windows  GOARCH=amd64 go build -o transdata.exe  ./main.go
*/
/*
V0.1 支持从一个source库迁移数据到多个库
V0.2 支持从excel导入数据到多个库中
*/
func GetAllCols(sourceConnStr string, sourcetable string) []string {
	//获取表的所有列
	sourceConn, err := pgx.Connect(context.Background(), sourceConnStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to source database: %v\n", err)
		log.Fatalf("Failed to connect to source database: %v", err)
	}
	if sourceConn == nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to target database: %v\n", err)
		log.Fatalf("Failed to connect to target database: %v", err)
	}
	defer sourceConn.Close(context.Background())

	rows, err := sourceConn.Query(context.Background(), "SELECT column_name FROM information_schema.columns WHERE table_name = '"+sourcetable+"'")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to query the database:%v\n", err)
		log.Fatalf("Unable to query the database:%v", err)
	}

	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		err := rows.Scan(&columnName)
		if err != nil {
			fmt.Println("Unable to scan row:", err)
		}
		columns = append(columns, columnName)
	}
	return columns
}

//迁移数据
func main() {
	//获取可执行文件所在的目录路径
	executable, err := os.Executable()
	if err != nil {
		log.Fatalf("Failed to exec: %v", err)
	}
	dir := filepath.Dir(executable)
	configPath := filepath.Join(dir, "conf.yaml")
	configFile := flag.String("c", configPath, "path to config file")
	sourcedb_flag := flag.String("D", "", "data from  db")
	targetdb_flag := flag.String("d", "", "data send to db,if have mult target please spit with ,")
	sourcetable_flag := flag.String("T", "", "data from table")
	targetable_flag := flag.String("t", "", "data send to  table")
	xlxs_flag := flag.String("f", "", "the xlsx file path")
	batch := flag.String("b", "10000", "how many rows will be copy")
	flag.Parse()
	if *configFile == "" {
		fmt.Fprintf(os.Stderr, "Please specify a config file :-c\n")
		os.Exit(1)
	}
	if *sourcedb_flag == "" && *xlxs_flag == "" {
		fmt.Fprintf(os.Stderr, "Please specify source db flag:-D\n")
		os.Exit(1)
	}
	if *targetdb_flag == "" {
		fmt.Fprintf(os.Stderr, "Please specify target db flag:-d\n")
		os.Exit(1)
	}
	if *sourcetable_flag == "" && *xlxs_flag == "" {
		fmt.Fprintf(os.Stderr, "Please specify source table flag:-T\n")
		os.Exit(1)
	}
	if *targetable_flag == "" {
		fmt.Fprintf(os.Stderr, "Please specify target table flag:-t\n")
		os.Exit(1)
	}
	//-f和-D -T不兼容，只能指定-t -d
	if *xlxs_flag != "" {
		if *sourcedb_flag != "" {
			fmt.Fprintf(os.Stderr, "-f and -D do not compatible.\n")
			os.Exit(1)
		}
		if *sourcetable_flag != "" {
			fmt.Fprintf(os.Stderr, "-f and -T do not compatible.\n")
			os.Exit(1)
		}
	}
	// 读取配置文件
	configData, err := ioutil.ReadFile(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read config file: %v\n", err)
		os.Exit(1)
	}
	//解析配置文件
	var config Config
	err = yaml.Unmarshal(configData, &config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FFailed to parse config file: %v\n", err)
		os.Exit(1)
	}

	//配置的数量是否满足使用
	conf_db_cnt := len(config.Databases)
	if conf_db_cnt <= 0 {
		fmt.Fprintf(os.Stderr, "please config yaml with sourcedb  and targetdb1..targedbN\n")
		os.Exit(1)
	}

	target_db_cn := conf_db_cnt - 1
	if target_db_cn <= 0 {
		fmt.Fprintf(os.Stderr, "please config yaml with targetdb1..targedbN\n")
		os.Exit(1)
	}

	//-D -d组成数据库连接串，并初始化连接串，默认0为连接sourcedb，剩余为targetdb
	//source连接串
	sourceConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		config.Databases[*sourcedb_flag].Host, config.Databases[*sourcedb_flag].Port, config.Databases[*sourcedb_flag].User,
		config.Databases[*sourcedb_flag].Password, config.Databases[*sourcedb_flag].DBName)

	//target连接串
	targedb_will_connect := strings.Split(*targetdb_flag, ",")
	//使用make初始化连接
	mymap := make(map[string]DBConfig)
	for _, value := range targedb_will_connect {
		existsTarget := 0
		//校验targetdb串和配置文件是否
		//fmt.Printf("Index: %d, Value: %s\n", index, value)
		for k, v := range config.Databases {
			if k == value {
				existsTarget += 1
				mymap[k] = v
			}
		}
		//没有找到yaml配置
		if existsTarget == 0 {
			fmt.Fprintf(os.Stderr, "Failed to find [%v] in config.yaml\n", value)
			log.Fatalf("Failed to find [%v] in config.yaml", value)
		}
	}

	if *xlxs_flag != "" {
		files, err := xlsx.OpenFile(*xlxs_flag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to open xlsx: %v\n", *xlxs_flag)
			log.Fatalf("Unable to open xlsx: %v", err)
			os.Exit(1)
		}
		sheet := files.Sheets[0]
		rows := sheet.Rows[1:]           //跳过行头
		cols := len(sheet.Rows[0].Cells) //多少列
		batchs, err := strconv.Atoi(*batch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "-b is int value: %v\n", *batch)
			os.Exit(1)
		}
		var results_sclice [][]interface{}
		for _, row := range rows {
			tempRow := make([][]interface{}, 1)
			tempRow[0] = make([]interface{}, cols)
			for i, cell := range row.Cells {
				tempRow[0][i] = cell.String()
			}
			results_sclice = append(results_sclice, tempRow...)
			//分批写入
			if len(results_sclice) >= batchs {
				for key, value := range mymap {
					targetDBConfig := DBConfig{Host: value.Host, Port: value.Port, User: value.User, Password: value.Password, DBName: value.DBName}
					err = targetDBConfig.ReadXlsAndInsertToDB(*targetable_flag, results_sclice)
					if err != nil {
						fmt.Fprintf(os.Stderr, "copy data failed: %v\n", err)
						os.Exit(1)
					}
					fmt.Fprintf(os.Stderr, "copy data success: %v 【%v】条\n", key, batchs)
				}
				//写完后清空
				results_sclice = results_sclice[:0] //清空
			}
		}
		//不够batch的写入
		if len(results_sclice) > 0 {
			for key, value := range mymap {
				targetDBConfig := DBConfig{Host: value.Host, Port: value.Port, User: value.User, Password: value.Password, DBName: value.DBName}
				err = targetDBConfig.ReadXlsAndInsertToDB(*targetable_flag, results_sclice)
				if err != nil {
					fmt.Fprintf(os.Stderr, "copy data failed: %v\n", err)
					os.Exit(1)
				}
				fmt.Fprintf(os.Stderr, "copy data success: %v 【%v】条\n", key, len(results_sclice))
			}
		}
		os.Exit(0)
	}
	//获取source所有列
	columns := GetAllCols(sourceConnStr, *sourcetable_flag)

	sourceConn, err := pgx.Connect(context.Background(), sourceConnStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to source database: %v\n", err)
		log.Fatalf("Failed to connect to source database: %v", err)
	}
	if sourceConn == nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to target database:%v\n", err)
		log.Fatalf("Failed to connect to target database: %v", err)
	}
	defer sourceConn.Close(context.Background())

	// 获取所有行
	rows, err := sourceConn.Query(context.Background(), "SELECT * FROM "+*sourcetable_flag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to retrieve rows from source table:: %v\n", err)
		log.Fatalf("Unable to retrieve rows from source table:: %v", err)
		os.Exit(1)
	}
	defer rows.Close()

	// 获取结果集的列信息 将rows复制到内存中多次使用
	//方法1：定义一个切片存储结果
	fields := rows.FieldDescriptions()
	var results_sclice [][]interface{}
	for rows.Next() {
		row_sclice, err := rows.Values()
		if err != nil {
			fmt.Fprintf(os.Stderr, "get row from postgres err: %v\n", err)
			log.Fatal(err)
		}
		row := make([][]interface{}, 1)
		row[0] = make([]interface{}, len(fields))
		for i := range fields {
			row[0][i] = row_sclice[i]
		}
		results_sclice = append(results_sclice, row...)
	}

	/*
		//方法2：定义一个map来存储查询结果
		var results []map[string]interface{}
		// 循环遍历结果集并将行添加到切片中
		for rows.Next() {
			// 创建一个 map 来存储行数据
			row := make(map[string]interface{})
			// 创建一个切片来存储每个列的值,相当于初始化三个列
			values := make([]interface{}, len(fields))
			for i := range values {
				values[i] = new(interface{})
			}
			// 扫描行并将每个列的值存储在values(切片)中
			err := rows.Scan(values...)
			if err != nil {
				fmt.Fprintf(os.Stderr, "get row from postgres err: %v\n", err)
				log.Fatal(err)
			}
			//string(field.Name)表示列名，并从切片(value)中把数据写入到map中
			for i, field := range fields {
				row[string(field.Name)] = *(values[i].(*interface{})) //
			}
			// 将行数据添加到结果集切片中
			results = append(results, row)
		}
	*/
	var wg sync.WaitGroup
	//wg.Add(len(mymap))
	//连接targedb
	for key, value := range mymap {
		wg.Add(1)
		targetDBConfig := DBConfig{Host: value.Host, Port: value.Port, User: value.User, Password: value.Password, DBName: value.DBName}
		//使用map迁移数据
		//go targetDBConfig.Migrate(key, columns, results, *targetable_flag, &wg)
		//使用切片迁移数据
		go targetDBConfig.Migrate_Sclice(key, columns, results_sclice, *targetable_flag, &wg)
	}
	wg.Wait()
}
