package configutil

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v4"
)

func (targetDBConfig *DBConfig) ReadXlsAndInsertToDB(targetable string, results_sclice [][]interface{}) error {

	targetConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		targetDBConfig.Host, targetDBConfig.Port, targetDBConfig.User, targetDBConfig.Password, targetDBConfig.DBName)

	targetConn, err := pgx.Connect(context.Background(), targetConnStr)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to target database: %v\n", err)
		log.Fatalf("Failed to connect to target database: %v", err)
		return err
	}

	defer targetConn.Close(context.Background())
	/***********get all cols from database*********/
	var columns []string
	query := `SELECT column_name 
              FROM information_schema.columns 
              WHERE table_name = $1`
	cols, err := targetConn.Query(context.Background(), query, targetable)
	if err != nil {
		return err
	}
	defer cols.Close()

	for cols.Next() {
		var colInfo string
		err := cols.Scan(&colInfo)
		if err != nil {
			return err
		}
		columns = append(columns, colInfo)
	}
	/***********begin commit*********/
	// 开始一个事务
	tx, err := targetConn.Begin(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to begin transaction: %v\n", err)
		log.Fatalf("Unable to begin transaction: %v", err)
		return err
	}
	defer tx.Rollback(context.Background())

	copyfrom := pgx.CopyFromSlice(len(results_sclice), func(rowIdx int) ([]interface{}, error) {
		if rowIdx >= len(results_sclice) {
			return nil, nil // All rows have been copied
		}
		return results_sclice[rowIdx], nil
	})
	_, err = tx.CopyFrom(context.Background(), pgx.Identifier{targetable}, columns, copyfrom)
	if err != nil {
		return err
	}
	err = tx.Commit(context.Background())
	if err != nil {
		return err
	}
	return nil
}
