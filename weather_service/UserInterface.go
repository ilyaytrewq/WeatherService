package weatherservice

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"
)

type UserData struct {
	Email    string   `json:"email"`
	Password string   `json:"password,omitempty"`
	Cities   []string `json:"cities"`
}

var (
	DB           *sql.DB
	errUserExist = errors.New("user already exists")
)

func InitPostgres() error {
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	user := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	dbname := os.Getenv("POSTGRES_DB")

	if host == "" || port == "" || user == "" || dbname == "" {
		return fmt.Errorf("postgres environment variables are not set properly")
	}

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	var err error
	DB, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		return fmt.Errorf("failed to open Postgres: %w", err)
	}

	if err = DB.Ping(); err != nil {
		return fmt.Errorf("failed to ping Postgres: %w", err)
	}

	_, err = DB.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			email VARCHAR(255) NOT NULL PRIMARY KEY,
			password VARCHAR(255) NOT NULL,
			cities TEXT[] DEFAULT '{}'
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create users table: %w", err)
	}

	startPeriodicEmailSending(10 * 60)

	return nil
}

func createUser(r *http.Request) error {
	var userData UserData
	if err := json.NewDecoder(r.Body).Decode(&userData); err != nil {
		log.Printf("createUser: decode error: %v", err)
		return fmt.Errorf("createUser: decode error: %w", err)
	}

	var existsEmail string
	err := DB.QueryRow("SELECT email FROM users WHERE email=$1", userData.Email).Scan(&existsEmail)
	if err == nil {
		return errUserExist
	}
	if err != sql.ErrNoRows {
		log.Printf("createUser: select error: %v", err)
		return fmt.Errorf("createUser: select error: %w", err)
	}

	safeToLog := struct {
		Email  string
		Cities []string
	}{
		Email:  userData.Email,
		Cities: userData.Cities,
	}
	log.Printf("createUser: received user data: %+v", safeToLog)

	if userData.Email == "" || userData.Password == "" {
		return errors.New("createUser: email and password are required")
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(userData.Password), bcrypt.DefaultCost)
	if err != nil {
		log.Printf("createUser: password hashing error: %v", err)
		return fmt.Errorf("createUser: password hashing error: %w", err)
	}

	addedCities, err := addCitiesToDB(userData.Cities)
	if err != nil {
		log.Printf("createUser: addCitiesToDB error: %v", err)
		return fmt.Errorf("createUser: addCitiesToDB error: %w", err)
	}
	_, err = DB.Exec(`
		INSERT INTO users (email, password, cities)
		VALUES ($1, $2, $3);
	`, userData.Email, string(hash), pq.Array(addedCities))
	if err != nil {
		log.Printf("createUser: insert error: %v", err)
		return fmt.Errorf("createUser: insert error: %w", err)
	}

	go func(email string) {
		ctx := context.Background()
		if err := publishWelcomeEmail(ctx, email); err != nil {
			log.Printf("createUser: failed to publish welcome email for %s: %v", email, err)
		}
	}(userData.Email)

	log.Printf("createUser: user %s created (or already exists)", userData.Email) // CHANGED
	return nil
}

func changeUserData(r *http.Request) error {
	var req UserData
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("changeUserData: decode error: %v", err)
		return fmt.Errorf("changeUserData: decode error: %w", err)
	}
	log.Printf("changeUserData: received request for %s, cities=%v", req.Email, req.Cities)

	if req.Email == "" || req.Password == "" {
		return errors.New("changeUserData: email and password are required")
	}

	var storedHash string
	err := DB.QueryRow("SELECT password FROM users WHERE email=$1", req.Email).Scan(&storedHash)
	if err == sql.ErrNoRows {
		log.Printf("changeUserData: user %s not found", req.Email)
		return errors.New("changeUserData: user not found")
	}
	if err != nil {
		log.Printf("changeUserData: select error: %v", err)
		return fmt.Errorf("changeUserData: select error: %w", err)
	}

	if err := bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(req.Password)); err != nil {
		log.Printf("changeUserData: incorrect password for %s", req.Email)
		return errors.New("changeUserData: incorrect password")
	}

	addedCities, err := addCitiesToDB(req.Cities)
	log.Printf("changeUserData: addedCities=%v", addedCities)
	if ; err != nil {
		log.Printf("changeUserData: addCitiesToDB error: %v", err)
		return fmt.Errorf("changeUserData: addCitiesToDB error: %w", err)
	}

	_, err = DB.Exec("UPDATE users SET cities = $1 WHERE email = $2", pq.Array(addedCities), req.Email)
	if err != nil {
		log.Printf("changeUserData: update error: %v", err)
		return fmt.Errorf("changeUserData: update error: %w", err)
	}

	log.Printf("changeUserData: user %s cities updated", req.Email)
	return nil
}

func getUserData(r *http.Request) (UserData, error) {
	var req UserData
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("getUserData: decode error: %v", err)
		return UserData{}, fmt.Errorf("getUserData: decode error: %w", err)
	}
	log.Printf("getUserData: request for %s", req.Email)

	if req.Email == "" || req.Password == "" {
		return UserData{}, errors.New("getUserData: email and password are required")
	}

	var storedHash string
	var cities []string
	err := DB.QueryRow("SELECT password, cities FROM users WHERE email=$1", req.Email).Scan(&storedHash, pq.Array(&cities))
	if err == sql.ErrNoRows {
		log.Printf("getUserData: user %s not found", req.Email)
		return UserData{}, errors.New("getUserData: user not found")
	}
	if err != nil {
		log.Printf("getUserData: select error: %v", err)
		return UserData{}, fmt.Errorf("getUserData: select error: %w", err)
	}

	if err := bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(req.Password)); err != nil {
		log.Printf("getUserData: incorrect password for %s", req.Email)
		return UserData{}, errors.New("getUserData: incorrect password")
	}

	log.Printf("getUserData: success for %s, cities=%v", req.Email, cities)
	return UserData{
		Email:  req.Email,
		Cities: cities,
	}, nil
}

func deleteUser(r *http.Request) error {
	var req UserData
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("deleteUser: decode error: %v", err)
		return fmt.Errorf("deleteUser: decode error: %w", err)
	}
	log.Printf("deleteUser: request for %s", req.Email)

	if req.Email == "" || req.Password == "" {
		return errors.New("deleteUser: email and password are required")
	}

	var storedHash string
	err := DB.QueryRow("SELECT password FROM users WHERE email=$1", req.Email).Scan(&storedHash)
	if err == sql.ErrNoRows {
		log.Printf("deleteUser: user %s not found", req.Email)
		return errors.New("deleteUser: user not found")
	}
	if err != nil {
		log.Printf("deleteUser: select error: %v", err)
		return fmt.Errorf("deleteUser: select error: %w", err)
	}

	if err := bcrypt.CompareHashAndPassword([]byte(storedHash), []byte(req.Password)); err != nil {
		log.Printf("deleteUser: incorrect password for %s", req.Email)
		return errors.New("deleteUser: incorrect password")
	}

	_, err = DB.Exec("DELETE FROM users WHERE email=$1", req.Email)
	if err != nil {
		log.Printf("deleteUser: delete error: %v", err)
		return fmt.Errorf("deleteUser: delete error: %w", err)
	}

	log.Printf("deleteUser: user %s deleted", req.Email)
	return nil
}

func sendWeatherEmails() error {
	for k, v := range mapOfCities {
		log.Printf("sendWeatherEmails: city %s => %+v", k, v)
	}
	log.Println("sendWeatherEmails: start")

	rows, err := DB.Query("SELECT email, cities FROM users")
	if err != nil {
		log.Printf("sendWeatherEmails: select error: %v", err)
		return fmt.Errorf("sendWeatherEmails: select error: %w", err)
	}
	defer rows.Close()

	mapOfCityWeatherForecast := make(map[string][]forecastAPIResp)

	rows.Columns()

	for rows.Next() {
		var email string
		var cities []string
		if err := rows.Scan(&email, pq.Array(&cities)); err != nil {
			log.Printf("sendWeatherEmails: row scan error: %v", err)
			continue
		}

		log.Printf("sendWeatherEmails: processing user %s with cities %v", email, cities)

		var forecastParts [][]forecastAPIResp

		for _, city := range cities {
			if val, ok := mapOfCityWeatherForecast[city]; !ok {
				cityData, ok := mapOfCities[city]
				if !ok {
					log.Printf("sendWeatherEmails: city %s not found in mapOfCities", city)
					continue
				}
				forecast, err := getWeatherForecast(cityData)
				if err != nil {
					log.Printf("sendWeatherEmails: getWeatherForecast error for city %s: %v", city, err)
					continue
				}
				mapOfCityWeatherForecast[city] = forecast
				forecastParts = append(forecastParts, forecast)
			} else {
				forecastParts = append(forecastParts, val)
			}
		}

		if len(forecastParts) == 0 {
			log.Printf("sendWeatherEmails: no valid cities for user %s", email)
			continue
		}

		body, err := createEmailBody(forecastParts, cities)
		if err != nil {
			log.Printf("sendWeatherEmails: createEmailBody error for %s: %v", email, err)
			continue
		}

		task := EmailTask{
			To:      email,
			Subject: "Ежедневный прогноз погоды",
			Body:    body,
			Type:    "daily_forecast",
			Meta:    map[string]interface{}{"sent_by": "weather_service"},
		}

		ctxPub, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := publishEmailTask(ctxPub, task); err != nil {
			log.Printf("sendWeatherEmails: publish error for %s: %v", email, err)
			continue
		}

		log.Printf("sendWeatherEmails: email task published for %s", email)
	}

	return nil
}

func startPeriodicEmailSending(intervalSeconds int) {
	log.Println("start_periodic_email_sending")

	go func() {
		ticker := time.NewTicker(time.Duration(intervalSeconds) * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			if err := sendWeatherEmails(); err != nil {
				log.Printf("Periodic email sending error: %v", err)
			} else {
				log.Println("Periodic email sending: Emails sent successfully")
			}
		}
	}()
}

func publishWelcomeEmail(ctx context.Context, userEmail string) error {
	body := `<html>
		<body>
			<h1>Добро пожаловать в WeatherService!</h1>
			<p>Спасибо за регистрацию — мы будем присылать обновления по погоде в выбранных тобой городах.</p>
		</body>
	</html>`
	task := EmailTask{
		To:      userEmail,
		Subject: "Добро пожаловать в WeatherService!",
		Body:    body,
		Type:    "welcome",
		Meta:    map[string]interface{}{"sent_by": "weather_service"},
	}

	ctxPub, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := publishEmailTask(ctxPub, task); err != nil {
		return fmt.Errorf("PublishWelcomeEmail: publish error: %w", err)
	}

	return nil
}

func createEmailBody(forecastParts [][]forecastAPIResp, cities []string) (string, error) {
	body := `<html>
        <body>
            <h1>Привет!</h1>
            <p>Вот твой ежедневный прогноз погоды:</p>`

	for i, forecast := range forecastParts {
		if len(forecast) == 0 {
			continue
		}

		log.Printf("createEmailBody: forecast for city %s with %d entries", cities[i], len(forecast))
		body += fmt.Sprintf("<h2><b>%s</b></h2>", cities[i])
		body += "<ul>"

		for _, entry := range forecast {
			t := time.Unix(entry.Dt, 0).Format("02 Jan 15:04")
			desc := "N/A"
			if len(entry.Weather) > 0 {
				desc = entry.Weather[0].Description
			}
			body += fmt.Sprintf(
				"<li>%s: %.1f°C (ощущается как %.1f°C), давление %.1f мм.рт.ст, ветер %.1f м/с, %s</li>",
				t, entry.Main.Temp, entry.Main.FeelsLike, float32(entry.Main.Pressure)*0.75, entry.Wind.Speed, desc,
			)
		}
		body += "</ul>"
	}

	body += `
            <p>Спасибо, что используешь наш сервис!</p>
        </body>
    </html>`

	return body, nil
}
