package api

import (
	"cimri/internal/api/handlers"
	"cimri/internal/database"
	"database/sql"
	"github.com/gin-gonic/gin"
)

func SetupRouter(db *sql.DB) *gin.Engine {
	router := gin.Default()
	productHandler := handlers.NewProductHandler(database.NewProductStore(db))

	router.GET("/product/:id", productHandler.GetProductById)
	return router
}
