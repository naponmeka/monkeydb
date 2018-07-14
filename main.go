package main

import (
	"crypto/md5"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var s, path string
var hosts []string

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
	t := time.Now()
	h := md5.New()
	io.WriteString(h, string(bodyIo))
	docID := fmt.Sprintf("%s_%x", t.Format("20060102150405"), h.Sum(nil))
	filename := fmt.Sprintf("/%s.json", docID)
	err := ioutil.WriteFile(path+filename, bodyIo, 0644)
	if err != nil {
		fmt.Fprintf(w, "Create not success")
	} else {
		fmt.Fprintf(w, "Create %s", docID)
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

func findDocFromHosts(ID string) (string, error) {
	// broadcast
	for _, host := range hosts {
		endPointRead := fmt.Sprintf("http://localhost%s/private/read/%s", host, ID)
		fmt.Println(endPointRead)
		resp, err := http.Get(endPointRead)
		if err != nil {
			fmt.Println(err)
		} else {
			data, _ := ioutil.ReadAll(resp.Body)
			dataStr := string(data)
			if dataStr != "Not Found" {
				return dataStr, nil
			}
		}
	}
	return "", errors.New("Not Found")
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

func updateHostsToAll() {
	for _, host := range hosts[0 : len(hosts)-1] {
		endPointUpdate := fmt.Sprintf("http://localhost%s/updateHosts", host)
		fmt.Println(endPointUpdate)
		hostsStr := strings.Join(hosts, ",")
		_, err := http.Post(endPointUpdate, "raw", strings.NewReader(hostsStr))
		if err != nil {
			fmt.Println(err)
		}
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
	joinToFlag := flag.String("join", "", "join: :5000")
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
	http.HandleFunc("/", handler)
	fmt.Printf("Process started on port: %s", port)
	fmt.Printf("Hosts: %+v", hosts)
	log.Fatal(http.ListenAndServe(port, nil))
}
