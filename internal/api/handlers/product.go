package handlers

import (
	"cimri/internal/database"
	"database/sql"
	"errors"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

type ProductHandler struct {
	Store *database.ProductStore
}

func NewProductHandler(store *database.ProductStore) *ProductHandler {
	return &ProductHandler{Store: store}
}

func (h *ProductHandler) GetProductById(c *gin.Context) {
	id, invalidIdErr := strconv.Atoi(c.Param("id"))
	if invalidIdErr != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": invalidIdErr})
		return
	}

	product, err := h.Store.GetById(id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			c.JSON(http.StatusNotFound, gin.H{"error": "product not found"})
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error: " + err.Error()})
		}
		return
	}

	c.JSON(http.StatusOK, product)

}
