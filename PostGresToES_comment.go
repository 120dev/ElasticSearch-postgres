package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

var (
	indexURL         = "http://localhost:9200/aboro/aboro"
	conninfo         = "dbname=aboro user=postgres password=aboro sslmode=disable"
	verbose          = true
	inserts, deletes int64
	idRef            string
)

// Message : struncture d'un message qui arrive de postgres
type Message struct {
	Table  string           `json:"table"`
	ID     int              `json:"id"`
	Action string           `json:"action"`
	Data   *json.RawMessage `json:"data"`
}

// Main : Fonction principale
func main() {
	//ouverture de la connection avec postgres
	_, err := sql.Open("postgres", conninfo)
	if err != nil {
		panic(err)
	}

	//log des erreurs
	reportProblems := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Printf(err.Error())
		}
	}

	// declaration du listener
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

// getNotification : Fonction appelé par main dès qu'il y a un evenement dans postgres
func getNotification(l *pq.Listener) {
	for {
		select {
		case n := <-l.Notify:
			if verbose {
				log.Printf("Données entrante")
			}
			// On converti en Json la data entrante
			var prettyJSON bytes.Buffer
			err := json.Indent(&prettyJSON, []byte(n.Extra), "", "\t")
			if err != nil {
				log.Println("Erreur dans le JSON: ", err)
				return
			}
			if verbose {
				log.Printf("Message brut : ")
				log.Printf(string(prettyJSON.Bytes()))
			}

			// On cast en struct
			var message Message
			bytes := []byte(string(prettyJSON.Bytes()))
			err2 := json.Unmarshal(bytes, &message)
			if err2 != nil {
				log.Println("Erreur dans la construction de l'obejt JSON: ", err2)
				return
			}
			// on envoie dans la fonction d'écriture
			writeChangesEs(message)
			return
		case <-time.After(90 * time.Second):
			// Si rien depuis 90 secondes, on check la connexion
			log.Printf("Rien depuis 90 secondes, check connexion")
			go func() {
				l.Ping()
			}()
			return
		}
	}
}

// writeChangesEs : Fonction appelé par getNotification pour envoyer le message a ES
func writeChangesEs(message Message) {
	s := []string{message.Table, strconv.Itoa(message.ID)}
	tableAndID := strings.Join(s, "_")

	if message.Action == "DELETE" {
		if verbose {
			log.Printf("DELETE %s", tableAndID)
		}
		//if DELETE, on delete le message d'ES
		if !elasticReq("DELETE", tableAndID, nil) {
			log.Printf("Failed to delete %s", tableAndID)
		}
	} else {
		if verbose {
			log.Printf("INDEX  %s", tableAndID)
		}
		//on put le message dans ES
		r := bytes.NewReader([]byte(*message.Data))
		if !elasticReq("PUT", tableAndID, r) {
			log.Printf("Failed to index %s:\n%s", tableAndID, string(*message.Data))
		}
	}
}

// elasticReq : fait une request à Elasticsearch et retourne le result
func elasticReq(method, id string, reader io.Reader) bool {
	resp := httpReq(method, indexURL+"/"+id, reader)
	if resp == nil {
		return false
	}
	ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	return true
}

// httpReq : partie http de la request
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

// isErrorHTTPCode : partie gestion des erreurs http
func isErrorHTTPCode(resp *http.Response) bool {
	return resp.StatusCode < 200 || resp.StatusCode >= 300
}
