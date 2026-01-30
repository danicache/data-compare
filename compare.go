package main

import (
	"database/sql"
	"fmt"
	"time"

	"reflect"

	"github.com/rs/zerolog/log"
)

func compareTables(db *sql.DB, table1, table2 string) {

	log.Info().Msgf("COMPARING %s --> %s...", table1, table2)

	// grab the key(s) from table1 which will be used to select rows from both tables

	log.Info().Msgf("getting keys from table1: %s", table1)

	kQuery := getKeysQuery(tableKey, table1)

	// use keys to query a row from table1
	keys, err := QueryToMap(db, kQuery)
	if err != nil {
		log.Fatal().Err(err).Str("query", kQuery).
			Msgf("Error getting keys %s: %v", table1, err)
	}

	// how many records were compared
	compareCount := 0

	// how many records were different
	diffRowCount := 0

	for _, v := range keys {

		// use keys to query a row from table1
		// there should be only one row
		query, values := SelectWhere(table1, v)
		row1, err := QueryToMap(db, query, values...)
		if err != nil {
			log.Fatal().Err(err).Str("query", query).
				Msgf("Error querying table1: %v", err)
		}

		if len(row1) == 0 {
			log.Error().
				Msgf("No matching record found in %s for keys (this should not happen): %v",
					table1, v)
			continue
		}

		if len(row1) > 1 {
			log.Error().Msgf("More than one record found in %s for keys: %v", table1, v)
			continue
		}

		// use keys to query a row from table2
		query, values = SelectWhere(table2, v)
		row2, err := QueryToMap(db, query, values...)
		if err != nil {
			log.Error().Err(err).Str("query", query).
				Msgf("Error querying %s: %v", table2, err)
		}

		if len(row2) == 0 {
			log.Error().Msgf("No matching record found in %s for keys: %v", table2, v)
			diffRowCount++
			continue
		}

		if len(row2) > 1 {
			log.Error().Msgf("More than one record found in %s for keys: %v", table2, v)
			continue
		}

		// compare table1 ro to table 2 row
		isEqual, diffs := CompareRows(row1[0], row2[0])
		if !isEqual {
			log.Warn().Msgf("Difference found for keys: %v", v)
			for _, diff := range diffs {
				log.Warn().Msgf("  %s", diff)
			}
			diffRowCount++
		}

		compareCount++
	}

	log.Info().Msg("COMPARISON COMPLETE")
	log.Info().Msgf("    Compare Count    : %v", compareCount)
	log.Info().Msgf("    Difference Count : %v", diffRowCount)

}

func SelectWhere(tableName string, keys map[string]any) (string, []any) {

	query := fmt.Sprintf("SELECT * FROM %s WHERE ", tableName)
	first := true

	values := []any{}

	for k, v := range keys {
		if first {
			query += fmt.Sprintf("%s = ?", k)
			first = false
		} else {
			query += fmt.Sprintf(" AND %s = ?", k)
		}

		values = append(values, v)
	}

	return query, values
}

func QueryToMap(db *sql.DB, query string, args ...any) ([]map[string]any, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// 1. Get column names from the provided query
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]any

	for rows.Next() {
		// 2. Prepare pointers to receive column data
		columns := make([]any, len(cols))
		columnPointers := make([]any, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// 3. Scan the row into our pointer slice
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		// 4. Map the captured data back to column names
		m := make(map[string]any)
		for i, colName := range cols {
			val := columnPointers[i].(*any)
			m[colName] = *val
		}
		results = append(results, m)
	}

	return results, rows.Err()
}

// CompareRows checks if two rows (maps) have identical columns and values.
func CompareRows(row1, row2 map[string]any) (bool, []string) {
	var diffs []string

	// 1. Check if both maps have the same number of columns
	if len(row1) != len(row2) {
		return false, []string{"different column counts"}
	}

	// 2. Compare values for each column
	for col, val1 := range row1 {
		val2, exists := row2[col]
		if !exists {
			diffs = append(diffs, fmt.Sprintf("column %s missing in second row", col))
			continue
		}

		// treat date columns differently, will need to make this configurable
		// if col == "delivered_date" {
		// 	if !dateTimeEqual(val1, val2) {
		// 		diffs = append(diffs, fmt.Sprintf("column %s mismatch: %v != %v", col, val1, val2))
		// 	}
		// 	continue
		// } DP-REMOVED since the delivered date is normalized

		// Use reflect.DeepEqual to handle various types like []byte, etc.
		if !reflect.DeepEqual(val1, val2) {
			diffs = append(diffs, fmt.Sprintf("column %s mismatch: %v != %v", col, val1, val2))
		}
	}

	return len(diffs) == 0, diffs
}

func dateTimeEqual(d1, d2 any) bool {

	result, err := compareDateTime(stringValue(d1), stringValue(d2))
	if err != nil {
		log.Error().Err(err).Msg("Error comparing dates")
		return false
	}

	if result == 0 {
		return true
	}

	return false
}

// compareDateTime accepts date strings and tries multiple possible formats
//
//	-1 if dt1 < dt2
//	0 if dt1 == dt2
//	1 if dt1 > dt2
func compareDateTime(dt1, dt2 string) (int, error) {
	// Define the possible formats to check against
	layouts := []string{
		"01/02/2006 15:04:05",    // 24-hour format
		"01/02/2006 03:04:05 PM", // 12-hour AM/PM format
		"1/2/2006 3:04:05 PM",    // 12 hour format, no leading 0
	}

	// if both dates are empty then they are equal
	if dt1 == "" || dt2 == "" {
		return 0, nil
	}

	t1, err := parseFlexible(dt1, layouts)
	if err != nil {
		return -1, fmt.Errorf("could not parse first date: %w", err)
	}

	t2, err := parseFlexible(dt2, layouts)
	if err != nil {
		return -1, fmt.Errorf("could not parse second date: %w", err)
	}

	return t1.Compare(t2), nil
}

// parseFlexible attempts to parse a string using a list of layouts
func parseFlexible(value string, layouts []string) (time.Time, error) {

	// don't try to parse empty values
	if len(value) < 1 {
		return time.Time{}, nil
	}

	var err error
	var t time.Time

	for _, layout := range layouts {
		t, err = time.Parse(layout, value)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("no matching format for value: %s", value)
}

func stringValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}

	return ""
}
