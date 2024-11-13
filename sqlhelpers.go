package dbresolver

import (
	"database/sql"
	"errors"
)

type Row []interface{}
type Rows []*Row

func ToRows(rows *sql.Rows) (Rows, error) {
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results Rows

	// Create a slice to hold the values for each row
	for rows.Next() {
		values := make(Row, len(columns))
		valuePtrs := make(Row, len(columns))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		results = append(results, &values)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func ToRow(rows *sql.Rows) (*Row, error) {
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make(Row, len(columns))
	valuePtrs := make(Row, len(columns))

	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if rows.Next() {
		values := make(Row, len(columns))
		valuePtrs := make(Row, len(columns))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			return nil, err
		}

		return &values, nil
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return nil, errors.New("something_went_wrong")

}
