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
	"gopkg.in/yaml.v2"
)

var (
	indexURL = "http://elasticsearch:9200"
	conninfo = "host=postgres port=5432 dbname=<DB> user=<DB_LOGIN> password=<DB_PASS> sslmode=disable"
	verbose          = false
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

// Configuration : struncture du fichier de configuration
type Configuration struct {
	APIUser             string `yaml:"api_user"`
	APIUserHeader       string `yaml:"api_user_header"`
	Key                 string `yaml:"key"`
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
	var c Configuration
	var url, header string

	c.getConf()

	switch message.Table {
  	case `users`:
  		url = c.APIUser
  		header = c.APIUserHeader
  	default:
  		log.Printf("Table < %s > pas connu", message.Table)
	}

	urlf := []string{url, strconv.Itoa(message.ID)}
	urlFinal := strings.Join(urlf, "")

	if verbose {
		log.Printf("url %s", url)
		log.Printf("urlFinal %s", urlFinal)
		log.Printf("header %s", header)
		log.Printf("Key %s", c.Key)
	}

	resp := httpReqAPI(urlFinal, header, c.Key)
	if resp == nil {
		log.Printf("Failed to get data from API %s", urlFinal)
	}
	body, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		panic(readErr.Error())
	}
	var prettyCandidateJSON bytes.Buffer
	err2 := json.Indent(&prettyCandidateJSON, []byte(body), "", "\t")
	if err2 != nil {
		log.Println("Erreur dans le JSON: ", err2)
	}

	if verbose {
		log.Printf("prettyCandidateJSON %s", string(prettyCandidateJSON.Bytes()))
	}

	if message.Action == "DELETE" {

		log.Printf("DELETE %s", tableAndID)
		//if DELETE, on delete le message d'ES
		if !elasticReq("DELETE", message.Table, tableAndID, nil) {
			log.Printf("Failed to delete %s", tableAndID)
		}
	} else {
		log.Printf("ADD/UPDATE ID %s", tableAndID)
		//on put le message dans ES
		r := bytes.NewReader([]byte(string(prettyCandidateJSON.Bytes())))
		if !elasticReq("PUT", message.Table, tableAndID, r) {
			log.Printf("Failed to index %s:\n%s", tableAndID, string(prettyCandidateJSON.Bytes()))
		}
	}
}

// elasticReq : fait une request à Elasticsearch et retourne le result
func elasticReq(method, table string, id string, reader io.Reader) bool {
	resp := httpReq(method, indexURL+"/"+table+"/data/"+id, reader)
	if resp == nil {
		return false
	}
	ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	return true
}

// httpReq : foncton qui écrit dans ES
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

// httpReqApi : fonction qui appel la bonne API et qui renvoi la réponse
func httpReqAPI(url string, header string, key string) *http.Response {
	req, err := http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", header)
	req.Header.Set("gateway", key)
	if err != nil {
		log.Fatal("HTTP request build failed: ", "GET", " ", url, ": ", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal("HTTP request failed: ", "GET", " ", url, ": ", err)
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

// getConf : Récupére l'url des APIs depuis le fichier de conf
func (c *Configuration) getConf() *Configuration {
	yamlFile, err := ioutil.ReadFile("/home/go/conf.yaml")
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	return c
}
