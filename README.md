# Golang Case Study

## Description
This project consists of two main parts
- A job that processes JSON Lines files to write data to database.
- A microservice that provides and endpoint to fetch records by ID

## Installation
- Clone the repository:
```bash
git clone https://emresin@bitbucket.org/emresinspace/cimrijob.git
```
- Create an `.env` file using `.env.example` and fill it with required credentials.
- To build the images use
```bash
docker-compose build
```
- To run the project:
```bash
docker-compose up
```

Then everything will be ready.

For demonstration purposes job and microservice is starting at the same time and job runs only one time. After job is completed data will be available to retrieval from the service.

Here is a sample request for id=1000:
```curl
curl --location --request GET 'http://127.0.0.1:8080/product/1000'
```

Response:
```json
{
    "id": 1000,
    "price": 63816.95,
    "title": "title1000",
    "category": "cep-telefonlari",
    "brand": "lg",
    "url": "http://site.example.com/?id=1000",
    "description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nam vel elit tortor. Fusce posuere ante sollicitudin risus tempus, quis accumsan tortor accumsan."
}
```

