package model

type Product struct {
	ID          int     `json:"id"`
	Price       float64 `json:"price"`
	Title       string  `json:"title"`
	Category    string  `json:"category"`
	Brand       string  `json:"brand"`
	Url         string  `json:"url"`
	Description string  `json:"description"`
}
