package main

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/thedevsaddam/gojsonq"
)

var s, path string
var hosts []string

func failOnError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func failOnError2(err error, msg string) {
	if err != nil {
		fmt.Println(msg)
		log.Fatal(err)
	}
}

func compress(str string) string {
	result := str
	strToReplace := [...]string{"\n", "\t", "\r", "\\"}
	for _, v := range strToReplace {
		result = strings.Replace(result, v, "", -1)
	}
	return result
}

func injectID(rawJSON string, id string) string {
	injectStr := fmt.Sprintf("\"_id\":\"%s\",", id)
	injectStrWithOpenBraces := fmt.Sprintf("{%s", injectStr)
	result := strings.Replace(rawJSON, "{", injectStrWithOpenBraces, 1)
	return result
}

func isJSON(s string) bool {
	var js map[string]interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Running hosts:  %v", hosts)
}

func handlerJoin(w http.ResponseWriter, r *http.Request) {
	hostsStr := strings.Join(hosts, ",")
	fmt.Fprintf(w, "%s", hostsStr)
}

func handlerUpdateHosts(w http.ResponseWriter, r *http.Request) {
	bodyIo, _ := ioutil.ReadAll(r.Body)
	hostsStr := string(bodyIo)
	fmt.Printf("\n updated hosts to %s\n", hostsStr)
	hosts = strings.Split(hostsStr, ",")
	fmt.Fprintf(w, "OK:%v", r.Body)
}

func handlerCreate(w http.ResponseWriter, r *http.Request) {
	bodyIo, _ := ioutil.ReadAll(r.Body)
	bodyStr := compress(string(bodyIo))
	if isJSON(bodyStr) {
		t := time.Now()
		h := md5.New()
		io.WriteString(h, bodyStr)
		docID := fmt.Sprintf("%s_%x", t.Format("20060102150405"), h.Sum(nil))
		bodyStr = injectID(bodyStr, docID)
		filename := fmt.Sprintf("/%s.json", docID)
		err := ioutil.WriteFile(path+filename, []byte(bodyStr), 0644)
		if err != nil {
			fmt.Fprintf(w, "Create not success")
		} else {
			fmt.Fprintf(w, "Create %s", docID)
		}
	} else {
		fmt.Fprintf(w, "Input is not valid json")
	}
}

func handlerReadFromAnotherHost(w http.ResponseWriter, r *http.Request) {
	ID := strings.TrimPrefix(r.URL.Path, "/private/read/")
	readPath := fmt.Sprintf("%s/%s.json", path, ID)
	dat, err := ioutil.ReadFile(readPath)
	if err != nil {
		fmt.Fprintf(w, "Not Found")
	} else {
		fmt.Fprintf(w, "%s", string(dat))
	}
}

func handlerUpdateFromAnotherHost(w http.ResponseWriter, r *http.Request) {
	docID := strings.TrimPrefix(r.URL.Path, "/private/update/")

	bodyIo, _ := ioutil.ReadAll(r.Body)
	filename := fmt.Sprintf("/%s.json", docID)
	err := ioutil.WriteFile(path+filename, bodyIo, 0644)
	if err != nil {
		fmt.Fprintf(w, "Update not success")
	} else {
		fmt.Fprintf(w, "Updated")
	}
}

func handlerDeleteFromAnotherHost(w http.ResponseWriter, r *http.Request) {
	ID := strings.TrimPrefix(r.URL.Path, "/private/delete/")
	readPath := fmt.Sprintf("%s/%s.json", path, ID)
	err := os.Remove(readPath)
	if err != nil {
		fmt.Fprintf(w, "Not Found")
	} else {
		fmt.Fprintf(w, "Deleted")
	}
}

func findDocFromHosts(ID string) (string, error) {
	// broadcast
	for _, host := range hosts {
		endPointRead := fmt.Sprintf("http://localhost%s/private/read/%s", host, ID)
		fmt.Println(endPointRead)
		resp, err := http.Get(endPointRead)
		failOnError(err)
		data, _ := ioutil.ReadAll(resp.Body)
		dataStr := string(data)
		if dataStr != "Not Found" {
			return dataStr, nil
		}
	}
	return "", errors.New("Not Found")
}

func updateDocFromHosts(ID string, newData string) error {
	//broadcast
	for _, host := range hosts {
		endPointUpdate := fmt.Sprintf("http://localhost%s/private/update/%s", host, ID)
		resp, err := http.Post(endPointUpdate, "raw", strings.NewReader(newData))
		if err != nil {
			fmt.Println(err)
		} else {
			data, _ := ioutil.ReadAll(resp.Body)
			dataStr := string(data)
			fmt.Println("updateDocFromHosts.datastr", dataStr)
			if dataStr == "Updated" {
				return nil
			}
		}
	}
	return errors.New("Cannot update doc(not exists in cluster)")
}

func deleteDocFromHosts(ID string) error {
	//broadcast
	for _, host := range hosts {
		endPointDelete := fmt.Sprintf("http://localhost%s/private/delete/%s", host, ID)
		resp, err := http.Get(endPointDelete)
		if err != nil {
			fmt.Println(err)
		} else {
			data, _ := ioutil.ReadAll(resp.Body)
			dataStr := string(data)
			fmt.Println("updateDocFromHosts.datastr", dataStr)
			if dataStr == "Deleted" {
				return nil
			}
		}
	}
	return errors.New("Cannot delete doc(not exists in cluster)")
}

func handlerRead(w http.ResponseWriter, r *http.Request) {
	ID := strings.TrimPrefix(r.URL.Path, "/read/")
	data, err := findDocFromHosts(ID)
	if err != nil {
		fmt.Fprintf(w, "Not found in cluster")
	} else {
		fmt.Fprintf(w, data)
	}
}

func handlerReadAllFromAnotherHosts(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	failOnError(err)
	var filterMap map[string]interface{}
	err = json.Unmarshal(body, &filterMap)
	failOnError(err)
	files, err := ioutil.ReadDir(path)
	failOnError(err)

	result := []string{}
	for _, f := range files {
		filePath := fmt.Sprintf("%s/%s", path, f.Name())
		data, err := ioutil.ReadFile(filePath)
		failOnError(err)
		dataStr := string(data)
		matched := true
		for k, v := range filterMap {
			fieldValue := gojsonq.New().JSONString(dataStr).Find(k)
			if fieldValue != v {
				matched = false
			}
		}
		if matched {
			result = append(result, dataStr)
		}
	}
	jsonResult, err := json.Marshal(result)
	fmt.Fprintf(w, string(jsonResult))
}

func findAllDocsFromHosts(filterJSON string) ([]string, error) {
	result := []string{}
	// broadcast
	for _, host := range hosts {
		endPointRead := fmt.Sprintf("http://localhost%s/private/readall/", host)
		fmt.Println("Endpoint:", endPointRead)
		fmt.Println("Filter:", filterJSON)
		resp, err := http.Post(endPointRead, "raw", strings.NewReader(filterJSON))
		failOnError(err)
		data, err := ioutil.ReadAll(resp.Body)
		failOnError(err)
		dataStr := string(data)
		dataSlice := []string{}
		err = json.Unmarshal([]byte(dataStr), &dataSlice)
		failOnError(err)
		result = append(result, dataSlice...)
	}

	return result, nil
}

func handlerReadAll(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	failOnError(err)
	bodyStr := string(body)

	var filterJSON string
	if bodyStr == "" {
		filterJSON = `{}`
	} else {
		filterJSON = compress(bodyStr)
	}

	if isJSON(filterJSON) {
		data, err := findAllDocsFromHosts(filterJSON)
		if err != nil {
			fmt.Fprintf(w, "Not found in cluster")
		} else {
			// var rawResult []interface{}
			// for _, v := range data {
			// 	var objmap map[string]interface{}
			// 	_ = json.Unmarshal([]byte(v), &objmap)
			// 	rawResult = append(rawResult, objmap)
			// }
			// jsonResult, err := json.Marshal(rawResult)
			jsonResult, err := json.Marshal(data)
			failOnError(err)
			fmt.Fprintf(w, string(jsonResult))
		}
	} else {
		fmt.Fprintf(w, "Not valid json")
	}
}

func handlerUpdate(w http.ResponseWriter, r *http.Request) {
	ID := strings.TrimPrefix(r.URL.Path, "/update/")
	fmt.Println("handlerUpdate", ID)
	bodyIo, _ := ioutil.ReadAll(r.Body)
	err := updateDocFromHosts(ID, string(bodyIo))
	if err != nil {
		fmt.Fprintf(w, "Not found for update")
	} else {
		fmt.Fprintf(w, "Updated")
	}
}

func handlerDelete(w http.ResponseWriter, r *http.Request) {
	ID := strings.TrimPrefix(r.URL.Path, "/delete/")
	fmt.Println("handlerDelete", ID)
	err := deleteDocFromHosts(ID)
	if err != nil {
		fmt.Fprintf(w, "Not found for delete")
	} else {
		fmt.Fprintf(w, "Deleted: %s", ID)
	}
}

func updateHostsToAll() {
	for _, host := range hosts[0 : len(hosts)-1] {
		endPointUpdate := fmt.Sprintf("http://localhost%s/updateHosts", host)
		fmt.Println(endPointUpdate)
		hostsStr := strings.Join(hosts, ",")
		_, err := http.Post(endPointUpdate, "raw", strings.NewReader(hostsStr))
		failOnError(err)
	}
}

func joinCluster(joinTo string) string {
	fmt.Printf("Calling /join to %s\n", joinTo)
	endPoint := fmt.Sprintf("http://localhost:%s/join", joinTo)
	resp, err := http.Get(endPoint)
	if err != nil {
		fmt.Println("Get err", err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Println("response body:")
	bodyStr := string(body)
	fmt.Println(bodyStr)
	hosts = strings.Split(bodyStr, ",")
	newPort := ":500" + strconv.Itoa(len(hosts))
	return newPort
}

func main() {
	modeFlag := flag.String("mode", "", "mode: eg. create, join")
	joinToFlag := flag.String("join", "", "join: 5000")
	pathFlag := flag.String("path", "", "eg ./tmp/something/")
	flag.Parse()
	mode := *modeFlag
	joinTo := *joinToFlag
	path = *pathFlag
	fmt.Println(mode)
	fmt.Println(joinTo)
	port := "0"
	s = "xxxx"

	if mode == "join" {
		port = joinCluster(joinTo)
		hosts = append(hosts, port)
		updateHostsToAll()
	} else {
		port = ":5000"
		hosts = []string{":5000"}
	}

	http.HandleFunc("/join", handlerJoin)
	http.HandleFunc("/updateHosts", handlerUpdateHosts)
	http.HandleFunc("/create", handlerCreate)
	http.HandleFunc("/read/", handlerRead)
	http.HandleFunc("/private/read/", handlerReadFromAnotherHost)
	http.HandleFunc("/readall/", handlerReadAll)
	http.HandleFunc("/private/readall/", handlerReadAllFromAnotherHosts)
	http.HandleFunc("/update/", handlerUpdate)
	http.HandleFunc("/private/update/", handlerUpdateFromAnotherHost)
	http.HandleFunc("/delete/", handlerDelete)
	http.HandleFunc("/private/delete/", handlerDeleteFromAnotherHost)
	http.HandleFunc("/", handler)
	fmt.Printf("Process started on port: %s\n", port)
	fmt.Printf("Hosts: %+v\n", hosts)
	log.Fatal(http.ListenAndServe(port, nil))
}
