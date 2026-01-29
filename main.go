package main

import (
	"database/sql"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	// format the output for console
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func main() {

	// setup log file and console out
	// logFile, err := os.Create("compare.log")
	// if err != nil {
	// 	log.Fatal().Err(err).Msg("can not create log file")
	// }
	// defer logFile.Close()

	// log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	logSection("SETUP...")

	log.Info().Msg("creating database...")

	db, err := sql.Open("sqlite3", "./compare.db")
	if err != nil {
		log.Fatal().Err(err).Msg("can not open database")
	}
	defer db.Close()

	log.Debug().Str("filename", "compare.db").Msg("created SQLLite database file")

	// read in 2 arguments
	if len(os.Args) < 3 {
		log.Fatal().Msg("need 2 arguments")
	}
	file1 := os.Args[1]
	file2 := os.Args[2]

	logSection("LOADING DATA...")

	// Step 1: Load file1.txt into the database
	table1Name := loadData(db, file1)

	// step 1.1 load second file into a tablename2 table
	table2Name := loadData(db, file2)

	// Step 2: Compare file2.txt with the database
	//compareData(db, file2)

	logSection("COMPARING DATA...")

	// compare 1 to 2
	compareTables(db, table1Name, table2Name)

	// compare 2 to 1
	compareTables(db, table2Name, table1Name)

}

// old compare, no longer used
// func compareData(db *sql.DB, filename string) {

// 	file, err := os.Open(filename)
// 	if err != nil {
// 		log.Fatal().Err(err).Msg("can not open file")
// 	}
// 	defer file.Close()

// 	reader := csv.NewReader(file)
// 	reader.Comma = '~'
// 	records, err := reader.ReadAll()
// 	if err != nil {
// 		log.Fatal().Err(err).Msg("can not read file")
// 	}

// 	if len(records) == 0 {
// 		return
// 	}

// 	header := records[0]
// 	tableName := "data"
// 	diffRowCount := 0

// 	for _, record := range records[1:] {
// 		id := record[0]
// 		query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", tableName, header[0])
// 		row := db.QueryRow(query, id)

// 		// Prepare to scan into a slice of sql.NullString
// 		dbRecords := make([]sql.NullString, len(record))
// 		dbRecordsPtr := make([]interface{}, len(record))
// 		for i := range dbRecords {
// 			dbRecordsPtr[i] = &dbRecords[i]
// 		}

// 		err := row.Scan(dbRecordsPtr...)
// 		if err == sql.ErrNoRows {
// 			log.Printf("ID %s from %s not found in database.", id, filename)
// 			continue
// 		} else if err != nil {
// 			log.Printf("Error querying for ID %s: %v", id, err)
// 			continue
// 		}

// 		diffFound := false

// 		// Compare each column
// 		for j, fileValue := range record {
// 			dbValue := dbRecords[j].String

// 			if !valuesEqual(header[j], fileValue, dbValue) {
// 				log.Printf("Difference for ID %s in column '%s': file has '%s', database has '%s'",
// 					id, header[j], fileValue, dbValue)
// 				diffFound = true
// 			}
// 		}

// 		// count that this row has differences
// 		if diffFound {
// 			diffRowCount++
// 		}
// 	}

// 	log.Info().Int("count", diffRowCount).Msg("Differences")

// 	log.Info().Msg("COMPARISON COMPLETE")
// }

func logSection(title string) {
	log.Info().Msgf("------------------------------------------------------------------")
	log.Info().Msgf(title)
	log.Info().Msgf("------------------------------------------------------------------")
}
