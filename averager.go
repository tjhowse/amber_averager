package main

import (
	"encoding/json"
	"fmt"

	client "github.com/influxdata/influxdb1-client/v2"
)

func getAggregatedMetric(c client.Client, metric, timeframe string) (float64, error) {
	query := `
	SELECT %s("value")
	FROM "mqtt_consumer"
	WHERE ("topic" = '6hull/power_price/import/5m_bid') and time > now() - %s
	`
	query = fmt.Sprintf(query, metric, timeframe)
	q := client.NewQuery(query, "telegraf", "")
	response, err := c.Query(q)
	if err == nil && response.Error() == nil {
		if len(response.Results) == 0 {
			return 0, fmt.Errorf("No results to query")
		}
		if len(response.Results[0].Series) == 0 {
			return 0, fmt.Errorf("No series in results")
		}
		if len(response.Results[0].Series[0].Values) == 0 {
			return 0, fmt.Errorf("No values in series")
		}
		for _, v := range response.Results[0].Series[0].Values {
			return v[1].(json.Number).Float64()
		}
	}
	return 0, err
}

func getStdDev(c client.Client, timeframe string) (float64, error) {
	return getAggregatedMetric(c, "stddev", timeframe)
}

func insertCalculatedValue(c client.Client)
{

}

func main() {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}
	defer c.Close()
	stdDev, err := getAggregatedMetric(c, "stddev", "1w")
	if err != nil {
		fmt.Printf("Couldn't get stdDev: %v", err)
	} else {
		fmt.Printf("stdDev: %v\n", stdDev)
	}
	mean, err := getAggregatedMetric(c, "mean", "1w")
	if err != nil {
		fmt.Printf("Couldn't get mean: %v", err)
	} else {
		fmt.Printf("mean: %v\n", mean)
	}
	current, err := getAggregatedMetric(c, "mean", "9m")
	if err != nil {
		fmt.Printf("Couldn't get a current value: %v", err)
	} else {
		fmt.Printf("current: %v\n", current)
	}

	sigma := (current - mean) / stdDev

	fmt.Printf("%v\n", sigma)
}
