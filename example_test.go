package patroni_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"

	patroni "github.com/pgsty/go-patroni"
)

func ExampleClient_GetPatroni() {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(writer, `{"state":"running","role":"primary","patroni":{"version":"4.1.3","scope":"demo","name":"node-1"}}`)
	}))
	defer server.Close()

	client, err := patroni.NewClient(patroni.ClientOptions{})
	if err != nil {
		panic(err)
	}
	response, err := client.GetPatroni(context.Background(), server.URL)
	if err != nil {
		panic(err)
	}
	fmt.Println(response.Data.Patroni.Name, response.Data.Role, response.Data.Patroni.Version)
	// Output: node-1 primary 4.1.3
}
