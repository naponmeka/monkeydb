package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
)

var s string
var hosts []string

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Running hosts:  %v", hosts)
}

func handlerJoin(w http.ResponseWriter, r *http.Request) {
	hostsStr := strings.Join(hosts, ",")
	fmt.Fprintf(w, "%s", hostsStr)
}

func handlerUpdateHosts(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "OK:%v", r.Body)
	bodyIo, _ := ioutil.ReadAll(r.Body)
	hostsStr := string(bodyIo)
	fmt.Printf("\n updated hosts to %s\n", hostsStr)
	hosts = strings.Split(hostsStr, ",")
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
	flag.Parse()
	mode := *modeFlag
	joinTo := *joinToFlag
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
	http.HandleFunc("/", handler)
	fmt.Printf("Process started on port: %s", port)
	fmt.Printf("Hosts: %+v", hosts)
	log.Fatal(http.ListenAndServe(port, nil))
}
