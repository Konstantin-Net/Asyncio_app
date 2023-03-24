## REST API Flask (backend) для сайта объявлений.
###  Тестовые запросы в Visual Studio Code (с приложением REST Client)

```bash
# API-запросы

@baseUrl = http://localhost:5000


# создание объявления

POST {{baseUrl}}/ads
Content-Type: application/json

{
    "title": "New ad",
    "description": "This is a new ad",
    "owner": "John"
}

###

# получение объявлений

GET {{baseUrl}}/ads/1
Content-Type: application/json

###

# удаление объявления

DELETE {{baseUrl}}/ads/1
Content-Type: application/json

```