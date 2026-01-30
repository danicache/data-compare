package main

import (
	"database/sql"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var tableKey string

func main() {

	// parse arguments and key
	// read in exactly 2 arguments
	if len(os.Args) < 3 {
		log.Fatal().Msg("need 2 arguments")
	}
	file1 := os.Args[1]
	file2 := os.Args[2]

	tableKey = strings.Split(file1, "_")[0]

	// ------------------
	// setup the log file
	// ------------------

	// 1. Open the log file for writing, replaced every time the comparison happens
	logFile, err := os.OpenFile(
		tableKey+".log",
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		0644,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to open log file")
	}
	defer logFile.Close() // Ensure the file is closed when the program exits

	log.Info().Str("file", tableKey+".log").Msg("Log file created")

	// 2. Create a console writer for pretty output in the terminal
	consoleWriter := zerolog.ConsoleWriter{Out: os.Stdout}

	// Output to file without colors (NoColor: true)
	fileWriter := zerolog.ConsoleWriter{Out: logFile, NoColor: true, TimeFormat: "2006-01-02 15:04:05"}

	// 3. Combine the console writer and the file writer using io.MultiWriter or zerolog.MultiLevelWriter
	// zerolog.MultiLevelWriter is recommended to handle log levels correctly for each writer
	multiWriter := zerolog.MultiLevelWriter(consoleWriter, fileWriter)

	// 4. Create a new logger with the multi-writer
	logger := zerolog.New(multiWriter).With().Timestamp().Logger()

	// 5. Optionally, set this new logger as the global logger
	log.Logger = logger

	// -------------------
	// create the database
	// -------------------

	logSection("SETUP...")

	log.Info().Msg("creating database...")

	db, err := sql.Open("sqlite3", "./compare.db")
	if err != nil {
		log.Fatal().Err(err).Msg("can not open database")
	}
	defer db.Close()

	log.Debug().Str("filename", "compare.db").Msg("created SQLLite database file")

	// ------------------------------------
	// load the data from files into the db
	// ------------------------------------

	logSection("LOADING DATA...")

	// Step 1: Load file1.txt into the database
	table1Name := loadData(db, file1)

	// step 2 load second file into a tableName2 table
	table2Name := loadData(db, file2)

	// ----------------------
	// execute the comparison
	// ----------------------

	logSection("COMPARING DATA...")

	// compare 1 to 2
	compareTables(db, table1Name, table2Name)

	// compare 2 to 1
	compareTables(db, table2Name, table1Name)

	// DONE
	log.Info().Msg("DONE")

}

func logSection(title string) {
	log.Info().Msgf("------------------------------------------------------------------")
	log.Info().Msgf(title)
	log.Info().Msgf("------------------------------------------------------------------")
}
