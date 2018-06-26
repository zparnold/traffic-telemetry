package main

import (
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
	"golang.org/x/net/context"
	"encoding/json"
	"net/http"
	"io/ioutil"
	"os"
	"google.golang.org/api/option"
)


type TrafficData struct {
	Direction        string `json:"_direction"`
	FromStreet       string `json:"_fromst"`
	LastUpdated      string `json:"_last_updt"`
	Length           string `json:"_length"`
	StartLatitude    string `json:"_lif_lat"`
	EndLatitude      string `json:"_lit_lat"`
	EndLongitude     string `json:"_lit_lon"`
	Heading          string `json:"_strheading"`
	ToStreet         string `json:"_tost"`
	Speed            string `json:"_traffic"`
	SegmentId        string `json:"segmentid"`
	StartLongitude   string `json:"start_lon"`
	Street           string `json:"street"`
}


func main()  {
	ctx := context.Background()
	fmt.Println("Loading CTA Data")
	trafficPayload := getApiData()

	// Sets your Google Cloud Platform project ID.
	projectID := os.Getenv("GCLOUD_PROJECT_ID")
	topicID := os.Getenv("PUBSUB_TOPIC_ID")

	// Creates a client.
	fmt.Println("Creating PubSub Client")
	client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsFile("credentials.json"))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	fmt.Println("Publishing messages")
	for _, value := range trafficPayload {
		fmt.Println(value)
		sendMessage(client, ctx, topicID, value)
	}
}

func getApiData() []TrafficData {
	url := "https://data.cityofchicago.org/resource/8v9j-bter.json"
	ctaDataHttpClient := http.Client{}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Fatal(err)
	}

	res, getErr := ctaDataHttpClient.Do(req)
	if getErr != nil {
		log.Fatal(getErr)
	}

	body, readErr := ioutil.ReadAll(res.Body)
	if readErr != nil {
		log.Fatal(readErr)
	}
	tData := []TrafficData{}
	jsonErr := json.Unmarshal(body, &tData)
	if jsonErr != nil {
		log.Fatal(jsonErr)
	}
	return tData
}

func sendMessage(psClient *pubsub.Client, ctx context.Context, topicId string, data TrafficData) {
	//convert traffic data to json
	tData, err := json.Marshal(data)
	if err != nil {
		fmt.Println(err)
		return
	}
	//get topic from client
	t := psClient.Topic(topicId)
	result := t.Publish(ctx, &pubsub.Message{
		Data: []byte(tData),
	})
	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	id, err := result.Get(ctx)
	if err != nil {
		log.Fatalf("Error publishing message: ", err)
	}
	fmt.Printf("Published a message; msg ID: %v\n", id)
}