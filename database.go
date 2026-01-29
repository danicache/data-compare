package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog/log"
)

// just a place to store database related functions

var tableIndex int = 1
var tableKey string
var keysQuery string

// give a tableName, create that table
func createTable(db *sql.DB, tableName string) (string, error) {

	// table names will be created as table1, table2 so store original name
	tableKey = tableName

	// add the index since we are creating table1 and a table2
	tableName = fmt.Sprintf("%v%v", tableName, tableIndex)

	// Read the SQL file
	sqlFile := tableKey + ".sql"
	sqlBytes, err := os.ReadFile(sqlFile)
	if err != nil {
		return "", fmt.Errorf("failed to read SQL file %s: %w", sqlFile, err)
	}

	//read the keys file
	keysSqlFile := tableKey + "_keys.sql"
	sqlKeysBytes, err := os.ReadFile(keysSqlFile)
	if err != nil {
		return "", fmt.Errorf("failed to read keys SQL file %s: %w", keysSqlFile, err)
	}

	keysQuery = string(sqlKeysBytes)
	log.Debug().Str("query", keysQuery).Msg("keysQuery")

	// update sql with updated tableName
	sqlUpdated := strings.Replace(string(sqlBytes), tableKey, tableName, -1)

	log.Info().Msgf("creating table %s...", tableName)

	// Drop the table
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if _, err := db.Exec(dropSQL); err != nil {
		return "", fmt.Errorf("failed to drop table %s: %w", tableName, err)
	}

	// Execute the SQL
	if _, err := db.Exec(sqlUpdated); err != nil {
		return "", fmt.Errorf("failed to execute SQL from %s: %w", sqlFile, err)
	}

	tableIndex++

	return tableName, nil
}

// the first part of the filename should be the table name (key)
func evaluateFileAndCreateTable(db *sql.DB, filename string) (string, error) {

	split := strings.Split(filename, "_")

	tableKey := split[0]

	return createTable(db, tableKey)
}

func loadData(db *sql.DB, filename string) string {

	log.Info().Str("filename", filename).Msg("Loading data from")

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal().Err(err).Msg("can not open file")
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = '~'
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatal().Err(err).Msg("can not read file")
	}

	totalRows := len(records)

	if len(records) == 0 {
		log.Fatal().Msg("No data in file to process")
	}

	header := records[0]
	if len(header) == 0 {
		log.Fatal().Msg("No header row in file")
	}

	dateColumnIndex := -1
	for i, col := range header {
		if col == "delivered_date" {
			dateColumnIndex = i
			break
		}
	}

	// Create table
	// tableName := "data"
	// createQuery := fmt.Sprintf("CREATE TABLE %s (%s TEXT PRIMARY KEY, %s TEXT)",
	// 	tableName, header[0], strings.Join(header[1:], " TEXT, "))
	// _, err = db.Exec(createQuery)
	// if err != nil {
	// 	// If table already exists, clear it for a fresh start
	// 	if strings.Contains(err.Error(), "already exists") {
	// 		log.Printf("Table %s already exists. Clearing it.", tableName)
	// 		_, err = db.Exec(fmt.Sprintf("DELETE FROM %s", tableName))
	// 		if err != nil {
	// 			log.Fatal(err)
	// 		}
	// 	} else {
	// 		log.Fatal(err)
	// 	}
	// }

	tableName, err := evaluateFileAndCreateTable(db, filename)
	if err != nil {
		log.Fatal().Err(err).Msgf("failed to create table for %s", filename)
	}

	recordCount := 0
	insertFailedCount := 0

	// Insert data
	for i, record := range records[1:] {

		placeholders := strings.Repeat("?,", len(record))
		placeholders = placeholders[:len(placeholders)-1]
		stmt, err := db.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (%s)", tableName, placeholders))
		if err != nil {
			log.Fatal().Err(err).Msgf("Failed to prepare statement for row %d: %v", i+2, err)
		}
		defer stmt.Close()

		args := make([]interface{}, len(record))
		for j, v := range record {

			// if its the date column normalize that data
			if j == dateColumnIndex {

				dt, err := parseFlexible(v, []string{
					"01/02/2006 15:04:05",    // 24-hour format
					"01/02/2006 03:04:05 PM", // 12-hour AM/PM format
					"1/2/2006 3:04:05 PM",    // no leading zero
				})

				if err != nil {
					log.Error().Err(err).Str("value", v).Msg("can't parse date loading value as is")
					args[j] = v
					continue
				}

				args[j] = dt
				continue
			}

			// for most cases, assign value as is
			args[j] = v
		}

		_, err = stmt.Exec(args...)
		if err != nil {
			log.Fatal().Err(err).Msgf("Failed to insert row id %v: %v", record[0], err)

			insertFailedCount++

			// skip good count
			continue
		}

		recordCount++
	}

	log.Info().Msg("Data loaded into database")

	// -1 since first row is header and not data
	log.Info().Msgf("    file rows      : %v", totalRows-1)
	log.Info().Msgf("    inserted rows  : %v", recordCount)
	log.Info().Msgf("    failed inserts : %v", insertFailedCount)

	return tableName
}

func getKeysQuery(key, tableName string) string {
	// update sql with updated tableName
	sqlUpdated := strings.Replace(keysQuery, key, tableName, -1)
	return sqlUpdated
}
