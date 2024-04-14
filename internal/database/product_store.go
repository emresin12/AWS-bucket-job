package database

import (
	"cimri/internal/model"
	"database/sql"
	"fmt"
	"log"
	"strings"
)

type ProductStore struct {
	db *sql.DB
}

func NewProductStore(db *sql.DB) *ProductStore {
	return &ProductStore{db: db}
}

func (store *ProductStore) InsertProductsInBatches(products []*model.Product, batchSize int) error {

	query := "insert into products (id,price,title, category, brand, url, description) values "

	var (
		valueStrings []string
		valueVals    []interface{}
	)

	residual := len(products) % batchSize
	batchCount := len(products) / batchSize

	for i := range batchSize {
		values := fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d)", 7*i+1, 7*i+2, 7*i+3, 7*i+4, 7*i+5, 7*i+6, 7*i+7)
		valueStrings = append(valueStrings, values)
	}

	queryString := query + strings.Join(valueStrings, ",") + " ON CONFLICT (id) DO NOTHING"
	stmt, queryPrepareErr := store.db.Prepare(queryString)
	defer stmt.Close()

	if queryPrepareErr != nil {
		log.Fatal("prepare err: ", queryPrepareErr)
	}
	for n_batch := range batchCount {

		for i := range batchSize {
			p := products[n_batch*batchSize+i]
			valueVals = append(valueVals, p.ID, p.Price, p.Title, p.Category, p.Brand, p.Url, p.Description)
		}

		_, queryExecErr := stmt.Exec(valueVals...)
		if queryExecErr != nil {
			return queryExecErr
		}

		valueVals = valueVals[:0]
	}

	if residual != 0 {
		valueStrings = valueStrings[:0]
		for i := range residual {
			values := fmt.Sprintf("($%d,$%d,$%d,$%d,$%d,$%d,$%d)", 7*i+1, 7*i+2, 7*i+3, 7*i+4, 7*i+5, 7*i+6, 7*i+7)
			valueStrings = append(valueStrings, values)
			p := products[batchCount*batchSize+i]
			valueVals = append(valueVals, p.ID, p.Price, p.Title, p.Category, p.Brand, p.Url, p.Description)
		}

		queryString = query + strings.Join(valueStrings, ",") + " ON CONFLICT (id) DO NOTHING"

		_, err := store.db.Exec(queryString, valueVals...)
		if err != nil {
			return err
		}
	}

	return nil
}
