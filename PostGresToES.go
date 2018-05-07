package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"log"
	"time"
	"github.com/lib/pq"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
)

var (
	indexURL = "http://localhost:9200/aboro/aboro"
	conninfo string = "dbname=aboro user=postgres password=aboro sslmode=disable"
	verbose = true
	inserts, deletes int64
	idRef string
)

type Message struct {
	Table  string   		`json:"table"`
	Id     int       		`json:"id"`
	Action string		    `json:"action"`
	Data   *json.RawMessage `json:"data"`
}

func main() {
	_, err := sql.Open("postgres", conninfo)
	if err != nil {
		panic(err)
	}

	reportProblems := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf(err.Error())
		}
	}

	listener := pq.NewListener(conninfo, 10*time.Second, time.Minute, reportProblems)
	err = listener.Listen("events")
	if err != nil {
		panic(err)
	}
	log.Printf("On ecoute PostgreSQL...")
	for {
		getNotification(listener)
	}
}

func getNotification(l *pq.Listener) {
	for {
		select {
		case n := <-l.Notify:
			if verbose {
				log.Printf("Données entrante")
			}
			var prettyJSON bytes.Buffer
			err := json.Indent(&prettyJSON, []byte(n.Extra), "", "\t")
			if err != nil {
				log.Printf("Erreur dans le JSON: ", err)
				return
			}
			if verbose {
				log.Printf("Message brut : ")
				log.Printf(string(prettyJSON.Bytes()))
			}

			var message Message
            bytes := []byte(string(prettyJSON.Bytes()))
			err2 := json.Unmarshal(bytes, &message)
			if err2 != nil {
				log.Printf("Erreur dans la construction de l'obejt JSON: ", err2)
				return
			}

			fmt.Println("avant")
			s := []string{message.Table, strconv.Itoa(message.Id)}
			r := strings.Join(s, "_")
	        fmt.Println(r)
			fmt.Println("Suite")
			writeChangesEs(message)
			return
		case <-time.After(90 * time.Second):
			log.Printf("Rien depuis 90 secondes, check connexion")
			go func() {
				l.Ping()
				}()
				return
			}
		}
	}


	func writeChangesEs(message Message) {

		table := message.Table
		if verbose {
			log.Printf("table : %s", table)
			fmt.Println(reflect.TypeOf(table))
		}

		action := message.Action
		if verbose {
			log.Printf("action : %s", action)
			fmt.Println(reflect.TypeOf(action))
		}

		idRef := message.Id
		if verbose {
			log.Printf("id : %s", strconv.Itoa(idRef))
			fmt.Println(reflect.TypeOf(action))
		}

		s := []string{message.Table, strconv.Itoa(message.Id)}
		tableAndId := strings.Join(s, "_")

		if action == "DELETE" {
			if verbose {
				log.Printf("DELETE %s", tableAndId)
			}
			if !elasticReq("DELETE", tableAndId, nil) {
				log.Printf("Failed to delete %s",tableAndId)
			}
			} else {
				if verbose {
					log.Printf("INDEX  %s", tableAndId)
				}
				r := bytes.NewReader([]byte(*message.Data))
				if !elasticReq("PUT", tableAndId , r) {
					log.Printf("Failed to index %s:\n%s", tableAndId, string(*message.Data))
				}
			}
		}

		// Make a request to Elasticsearch and return the result
		func elasticReq(method, id string, reader io.Reader) bool {
			resp := httpReq(method, indexURL+"/"+id, reader)
			if resp == nil {
				return false
			}
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			return true
		}

		func httpReq(method, url string, reader io.Reader) *http.Response {
			req, err := http.NewRequest(method, url, reader)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json")
			if err != nil {
				log.Fatal("HTTP request build failed: ", method, " ", url, ": ", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Fatal("HTTP request failed: ", method, " ", url, ": ", err)
			}
			if isErrorHTTPCode(resp) {
				body, _ := ioutil.ReadAll(resp.Body)
				resp.Body.Close()
				log.Print("HTTP error: ", resp.Status, ": ", string(body))
				return nil
			}
			return resp
		}

		func isErrorHTTPCode(resp *http.Response) bool {
			return resp.StatusCode < 200 || resp.StatusCode >= 300
		}
