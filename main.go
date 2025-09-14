package main

import (
	"net/http"
	"fmt"

	weatherAPI "github.com/ilyaytrewq/WeatherServiceAPI/weather_service"
)

func main() {

	if err := weatherAPI.InitClickhouse(); err != nil {
		fmt.Printf("Failed to initialize ClickHouse: %v\n", err)
		return
	} else {
		fmt.Printf("Connected to ClickHouse successfully: %v\n", weatherAPI.ClickhouseConn) 
	}

	if err := weatherAPI.InitPostgres(); err != nil {
		fmt.Printf("Failed to initialize Postgres: %v\n", err)
		return
	} else {
		fmt.Printf( "Connected to Postgres successfully: %v\n", weatherAPI.DB)
	}

	if err := weatherAPI.InitRabbit(); err != nil {
		fmt.Printf("Failed to initialize RabbitMQ: %v\n", err)
		return
	} else {
		fmt.Printf("Connected to RabbitMQ successfully\n")
	}

	http.HandleFunc("/v1/", weatherAPI.Handler)
	fmt.Println("Starting server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("Server failed to start: %v\n", err)
	}
}
