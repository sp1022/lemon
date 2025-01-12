package configutil

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"unsafe"

	"github.com/jackc/pgx/v4"
)

type Config struct {
	Databases map[string]DBConfig `yaml:"databases"`
}

type DBConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
}

//重新实现CopyFromSource接口
/*
要将results切片转换为pgx.Rows以进行COPY FROM操作，按照以下步骤进行操作：
创建一个pgx.CopyFromSource接口的实现，该接口的作用是在每次调用Next()方法时返回下一个要插入到数据库中的行的值。
*/
/*
rowsCopySource结构体包含一个指向原始切片的指针和一个表示当前正在处理的行的索引。在Next()方法中，
简单地检查当前行是否小于原始切片的长度。在Values()方法中，将当前行的值复制到一个interface{}切片中，并将当前行的索引递增。
*/
type rowsCopySource struct {
	rows     []map[string]interface{}
	rowIndex int
}

func (rc *rowsCopySource) Next() bool {
	return rc.rowIndex < len(rc.rows)
}

func (rc *rowsCopySource) Values() ([]interface{}, error) {
	row := rc.rows[rc.rowIndex]
	values := make([]interface{}, 0, len(row))

	for _, v := range row {
		values = append(values, v)
	}

	rc.rowIndex++

	return values, nil
}

func (rc *rowsCopySource) Err() error {
	return nil
}

func (rc *rowsCopySource) Close() error {
	return nil
}

func (targetDBConfig *DBConfig) Migrate(key string, columns []string, results []map[string]interface{}, targetable string, wg *sync.WaitGroup) {
	targetConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		targetDBConfig.Host, targetDBConfig.Port, targetDBConfig.User, targetDBConfig.Password, targetDBConfig.DBName)

	targetConn, err := pgx.Connect(context.Background(), targetConnStr)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to target database: %v\n", err)
		log.Fatalf("Failed to connect to target database: %v", err)
	}

	if targetConn == nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to target database: %v\n", err)
		log.Fatalf("Failed to connect to target database: %v", err)
	}

	defer targetConn.Close(context.Background())

	// 开始一个事务
	tx, err := targetConn.Begin(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to begin transaction: %v\n", err)
		log.Fatalf("Unable to begin transaction: %v", err)
	}

	defer tx.Rollback(context.Background())
	//执行copy
	/*
		创建一个新的pgx.Rows实例，该实例使用我们刚刚创建的rowsCopySource作为数据源
	*/
	rows := &rowsCopySource{
		rows: results,
	}
	//掉用接口
	pgxRows := pgx.CopyFromSource(rows)

	_, err = tx.CopyFrom(
		context.Background(),
		pgx.Identifier{targetable},
		columns,
		pgxRows,
	)

	if err != nil {
		fmt.Fprintf(os.Stderr, "UUnable to copy rows to target table: %v\n", err)
		log.Fatalf("Unable to copy rows to target table: %v", err)
	}
	// 提交事务
	err = tx.Commit(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to commit transaction:%v\n", err)
		log.Fatalf("Unable to commit transaction: %v", err)
	}
	fmt.Fprintf(os.Stderr, "trans data to ["+targetDBConfig.DBName+"] done!\n")
	wg.Done()
}

func (targetDBConfig *DBConfig) Migrate_Sclice(key string, columns []string, results [][]interface{}, targetable string, wg *sync.WaitGroup) {

	//fmt.Println(unsafe.Sizeof(results))
	fmt.Println(uintptr(len(results)) * unsafe.Sizeof(results[0]))
	targetConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		targetDBConfig.Host, targetDBConfig.Port, targetDBConfig.User, targetDBConfig.Password, targetDBConfig.DBName)

	targetConn, err := pgx.Connect(context.Background(), targetConnStr)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to target database: %v\n", err)
		log.Fatalf("Failed to connect to target database: %v", err)
	}

	if targetConn == nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to target database: %v\n", err)
		log.Fatalf("Failed to connect to target database: %v", err)
	}

	defer targetConn.Close(context.Background())

	// 开始一个事务
	tx, err := targetConn.Begin(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to begin transaction: %v\n", err)
		log.Fatalf("Unable to begin transaction: %v", err)
	}

	defer tx.Rollback(context.Background())
	//执行copy
	/*
		创建一个新的pgx.Rows实例，该实例使用我们刚刚创建的rowsCopySource作为数据源
	*/
	//掉用接口
	pgxRows := pgx.CopyFromRows(results)

	_, err = tx.CopyFrom(
		context.Background(),
		pgx.Identifier{targetable},
		columns,
		pgxRows,
	)

	if err != nil {
		fmt.Fprintf(os.Stderr, "UUnable to copy rows to target table: %v\n", err)
		log.Fatalf("Unable to copy rows to target table: %v", err)
	}
	// 提交事务
	err = tx.Commit(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to commit transaction:%v\n", err)
		log.Fatalf("Unable to commit transaction: %v", err)
	}
	fmt.Fprintf(os.Stderr, "trans data to ["+key+"] done!\n")
	wg.Done()
}
