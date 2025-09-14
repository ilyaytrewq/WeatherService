package weatherservice

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

const (
	apiWeatherURL     = "https://pro.openweathermap.org/data/2.5/weather"
	apiCoordinatesURL = "http://api.openweathermap.org/geo/1.0/direct"
)

var (
	apiKey = os.Getenv("API_WEATHER_KEY")
)

type weatherAPIResp struct {
	Dt   int64 `json:"dt"`
	Main struct {
		Temp      float32 `json:"temp"`
		FeelsLike float32 `json:"feels_like"`
		Pressure  int16   `json:"pressure"`
	} `json:"main"`
	Wind struct {
		Speed float32 `json:"speed"`
		Deg   int16   `json:"deg"`
	} `json:"wind"`
	City struct {
		Name string `json:"name"`
	}
}

type forecastAPIResp struct {
	Dt   int64 `json:"dt"`
	Main struct {
		Temp      float32 `json:"temp"`
		FeelsLike float32 `json:"feels_like"`
		Pressure  int16   `json:"pressure"`
	} `json:"main"`
	Wind struct {
		Speed float32 `json:"speed"`
	} `json:"wind"`
	Weather []struct {
		Description string `json:"description"`
	} `json:"weather"`
}

type listForecastAPIResp struct {
	Cnt  int `json:"cnt"`
	List []forecastAPIResp `json:"list"`
	City struct {
		Name string `json:"name"`
	}
}

func getCoordinates(cityName string) (CityType, error) {
	url := fmt.Sprintf("%s?q=%s&limit=1&appid=%s", apiCoordinatesURL, cityName, apiKey)
	log.Printf("getCoordinates: URL=%s", strings.Replace(url, apiKey, "***", 1))

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("getCoordinates: request error: %v", err)
		return CityType{}, fmt.Errorf("getCoordinates: request error: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("getCoordinates: status=%s", resp.Status)

	if resp.StatusCode != http.StatusOK {
		return CityType{}, errors.New("getCoordinates: non-200 response from API")
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return CityType{}, fmt.Errorf("getCoordinates: read body error: %w", err)
	}

	var cities []CityType
	if err := json.Unmarshal(data, &cities); err != nil {
		log.Printf("getCoordinates: decode error: %v", err)
		return CityType{}, fmt.Errorf("getCoordinates: decode error: %w", err)
	}
	if len(cities) == 0 {
		log.Printf("getCoordinates: no results for city %s", cityName)
		return CityType{}, fmt.Errorf("getCoordinates: no results for city %s", cityName)
	}

	if len(data) > 0 {
		sample := string(data)
		if len(sample) > 200 {
			sample = sample[:200] + "..."
		}
		log.Printf("getCoordinates: response sample=%s", sample)
	}

	return cities[0], nil
}

func getWeather(city CityType) (weatherAPIResp, error) {
	url := fmt.Sprintf("%s?lat=%f&lon=%f&appid=%s&units=metric", apiWeatherURL, city.Lat, city.Lon, apiKey)
	log.Printf("getWeather: URL=%s", strings.Replace(url, apiKey, "***", 1))

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("getWeather: request error: %v", err)
		return weatherAPIResp{}, fmt.Errorf("getWeather: request error: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("getWeather: status=%s", resp.Status)

	if resp.StatusCode != http.StatusOK {
		return weatherAPIResp{}, errors.New("getWeather: non-200 response from API")
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return weatherAPIResp{}, fmt.Errorf("getWeather: read body error: %w", err)
	}

	if len(data) > 0 {
		sample := string(data)
		if len(sample) > 200 {
			sample = sample[:200] + "..."
		}
		log.Printf("getWeather: response sample=%s", sample)
	}

	var weatherResp weatherAPIResp
	if err := json.Unmarshal(data, &weatherResp); err != nil {
		log.Printf("getWeather: decode error: %v", err)
		return weatherAPIResp{}, fmt.Errorf("getWeather: decode error: %w", err)
	}

	return weatherResp, nil
}

func getWeatherForecast(city CityType) ([]forecastAPIResp, error) {
	url := fmt.Sprintf("https://pro.openweathermap.org/data/2.5/forecast/hourly?lat=%f&lon=%f&appid=%s&units=metric", city.Lat, city.Lon, apiKey)
	log.Printf("getWeatherForecast: URL=%s", strings.Replace(url, apiKey, "***", 1))

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("getWeatherForecast: request error: %v", err)
		return []forecastAPIResp{}, fmt.Errorf("getWeatherForecast: request error: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("getWeatherForecast: status=%s", resp.Status)

	if resp.StatusCode != http.StatusOK {
		return []forecastAPIResp{}, errors.New("getWeatherForecast: non-200 response from API")
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return []forecastAPIResp{}, fmt.Errorf("getWeatherForecast: read body error: %w", err)
	}

	if len(data) > 0 {
		sample := string(data)
		if len(sample) > 200 {
			sample = sample[:200] + "..."
		}
		log.Printf("getWeatherForecast: response sample=%s", sample)
	}

	var forecastResp listForecastAPIResp
	if err := json.Unmarshal(data, &forecastResp); err != nil {
		log.Printf("getWeatherForecast: decode error: %v", err)
		return []forecastAPIResp{}, fmt.Errorf("getWeatherForecast: decode error: %w", err)
	}

	if forecastResp.Cnt == 0 || len(forecastResp.List) == 0 {
		return []forecastAPIResp{}, errors.New("getWeatherForecast: empty forecast data")
	}

	// Return the first day's forecast as a sample
	return forecastResp.List[0:24], nil
}
