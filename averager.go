package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"../mosquittoscope/mosquittoscope"
	client "github.com/influxdata/influxdb1-client/v2"
)

func getAggregatedMetric(c client.Client, metric, timeframe string) (float64, error) {
	query := `
	SELECT %s("value")
	FROM "mqtt_consumer"
	WHERE ("topic" = '6hull/power_price/import/5m_bid') and %s
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
			if v[1] == nil {
				return 0, fmt.Errorf("No value in... value")
			}
			return v[1].(json.Number).Float64()
		}
	}
	return 0, err
}

func getStdDev(c client.Client, timeframe string) (float64, error) {
	return getAggregatedMetric(c, "stddev", timeframe)
}

// func insertCalculatedValue(c client.Client) {
// 	query := `
// 	INSERT derived_5m_bid_sigma`
// 	query = fmt.Sprintf(query, metric, timeframe)
// 	q := client.NewQuery(query, "telegraf", "")
// 	response, err := c.Query(q)
// }

func calcSigmaForTimepoint(c client.Client, timepoint string) (float64, error) {
	t, err := time.Parse(time.RFC3339, timepoint)
	if err != nil {
		fmt.Printf("Failed to parse time: %v\n", err)
		os.Exit(1)
	}
	timeNS := t.UnixNano()
	timeFilter := fmt.Sprintf("time < %v and time > %v - ", timeNS, timeNS)
	// fmt.Println(timeFilter + "1w")
	stdDev, err := getAggregatedMetric(c, "stddev", timeFilter+"1w")
	if err != nil {
		return 0, fmt.Errorf("Couldn't get stdDev: %v", err)
	}
	mean, err := getAggregatedMetric(c, "mean", timeFilter+"1w")
	if err != nil {
		return 0, fmt.Errorf("Couldn't get mean: %v", err)
	}
	current, err := getAggregatedMetric(c, "mean", timeFilter+"9m")
	if err != nil {
		return 0, fmt.Errorf("Couldn't get a current value: %v", err)
	}
	// fmt.Printf("%f %f %f\n", stdDev, mean, current)
	return (current - mean) / stdDev, nil
}

// func calcSigmaForTimepointWithCurrentValue(c client.Client, timepoint string, current float64) (float64, error) {

// }

func getTimesForCalculations(c client.Client) ([]string, error) {
	query := `
	SELECT "value"
	FROM "mqtt_consumer"
	WHERE ("topic" = '6hull/power_price/import/5m_bid') and time > 1593406358906 + 2w
	`
	// WHERE ("topic" = '6hull/power_price/import/5m_bid') and time > now() - 1h
	result := []string{}
	q := client.NewQuery(query, "telegraf", "")
	response, err := c.Query(q)
	if err == nil && response.Error() == nil {
		if len(response.Results) == 0 {
			return result, fmt.Errorf("No results to query")
		}
		if len(response.Results[0].Series) == 0 {
			return result, fmt.Errorf("No series in results")
		}
		if len(response.Results[0].Series[0].Values) == 0 {
			return result, fmt.Errorf("No values in series")
		}
		for _, v := range response.Results[0].Series[0].Values {
			result = append(result, v[0].(string))
		}
	}
	return result, nil

}

func backfillHistorical5mBidSigma() {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
	})
	times, err := getTimesForCalculations(c)
	if err != nil {
		log.Fatal(err)
	}
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  "telegraf",
		Precision: "ns",
	})

	for _, v := range times {
		sigma, err := calcSigmaForTimepoint(c, v)
		if err != nil {
			// fmt.Printf("Shit! %v\n", err)
			continue
		}
		fields := map[string]interface{}{"5m_bid_sigma": sigma}
		t, err := time.Parse(time.RFC3339, v)
		t = t.Add(-time.Duration(5 * time.Minute))
		if err != nil {
			fmt.Printf("Failed to parse time: %v\n", err)
			os.Exit(1)
		}
		p, _ := client.NewPoint("calculated_values", nil, fields, t)
		bp.AddPoint(p)
	}
	if err := c.Write(bp); err != nil {
		log.Fatal(err)
	}

}

func main() {
	s := mosquittoscope.NewSettings("./defaults.yaml")

	a := mosquittoscope.NewMQTTMonitor(s)
	c, err := a.SubscribeAndGetChannel("6hull/power_price/import/5m_bid")
	if err != nil {
		log.Fatal(err)
	}

}
