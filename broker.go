package main

import (
	"errors"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	paramForPUT = "PUT"
	paramForGET = "GET"

	defaultTimeout = 5 // таймаут по умолчанию, если не передан другой
)

type brocker struct {
	queues      map[string][]string
	queuesMutex *sync.Mutex
}

type parametersPUT struct {
	nameQueue string
	message   string
}

type parametersGET struct {
	nameQueue string
	timeout   string
}

func newBrocker() *brocker {
	q := make(map[string][]string)
	qM := &sync.Mutex{}
	return &brocker{queues: q, queuesMutex: qM}
}

func main() {
	b := newBrocker()

	http.HandleFunc("/", b.handler)
	log.Fatal(http.ListenAndServe("localhost:8181", nil))
}

func (b *brocker) handler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		b.putMessage(w, r)
	case http.MethodGet:
		b.getMessage(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (b *brocker) insertQueue(nameQueue string, updateQ []string) {
	b.queuesMutex.Lock()
	b.queues[nameQueue] = updateQ
	defer b.queuesMutex.Unlock()
}

func (b *brocker) readQueue(nameQueue string) ([]string, bool) {
	q, ok := b.queues[nameQueue]
	return q, ok
}

func (b *brocker) putMessage(w http.ResponseWriter, r *http.Request) {
	p, err := parseURL(r.URL.String(), paramForPUT)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	pu, ok := p.(*parametersPUT)
	if ok {
		b.insertMessage(pu)
	} else {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (b *brocker) insertMessage(p *parametersPUT) {
	b.queuesMutex.Lock()
	defer b.queuesMutex.Unlock()
	q, ok := b.queues[p.nameQueue]
	if !ok {
		q := make([]string, 0)
		q = append(q, p.message)
		b.queues[p.nameQueue] = q
		return
	}
	q = append(q, p.message)
	b.queues[p.nameQueue] = q
}

//парсим url, чтобы вытащить имя очереди, сообщение, таймаут
func parseURL(u string, paramMethod string) (interface{}, error) {
	parts, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	switch paramMethod {
	case "PUT":
		name, err := getPartURL(parts.Path, true)
		if err != nil {
			return nil, err
		}

		newMessage, err := getPartURL(parts.RawQuery, false)
		if err != nil {
			return nil, err
		}
		pu := &parametersPUT{nameQueue: name, message: newMessage}
		return pu, nil
	case "GET":
		name, err := getPartURL(parts.Path, true)
		if err != nil {
			return nil, err
		}

		time, err := getPartURL(parts.RawQuery, false) // возвращаем  ошибку empty body при отсутствии передаваемого параметра
		if err != nil {
			if err.Error() == "empty body" {
				return &parametersGET{nameQueue: name, timeout: "0"}, nil
			}
			return nil, err
		}
		pg := &parametersGET{nameQueue: name, timeout: time}
		return pg, nil
	}
	return nil, errors.New("bad parametr")
}

func getPartURL(data string, parseQueue bool) (string, error) {
	var spl []string
	if parseQueue {
		spl = strings.Split(data, "/")
		if len(spl) == 0 && len(spl) > 2 {
			return "", errors.New("bad parametr")
		}
	} else {
		if data == "" {
			return "", errors.New("empty body")
		}
		spl = strings.Split(data, "=")
		if len(spl) > 3 || spl[len(spl)-1] == "" {
			return "", errors.New("bad parametr")
		}
	}
	return spl[len(spl)-1], nil
}

func (b *brocker) getMessage(w http.ResponseWriter, r *http.Request) {
	p, err := parseURL(r.URL.String(), paramForGET)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	message := make(chan string)

	pg, _ := p.(*parametersGET)

	timeout, err := strconv.Atoi(pg.timeout)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if timeout == 0 {
		timeout = defaultTimeout
	}

	var q []string
	go func() {
		q = b.pullMessage(message, pg.nameQueue)
	}()

	var msg string

	select {
	case msg = <-message:
		b.insertQueue(pg.nameQueue, q)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(msg))
		return
	case <-time.After(time.Duration(timeout) * time.Second):
		w.WriteHeader(http.StatusNotFound)
		return
	}
}

func (b *brocker) pullMessage(mC chan string, nameQ string) []string {
	flag := true
	var m string
	var q []string
	for flag {
		q, m = b.tryingTakeMessage(nameQ)
		if m != "" {
			mC <- m
			flag = false
		}
	}
	return q
}

func (b *brocker) tryingTakeMessage(nameQueue string) ([]string, string) {
	b.queuesMutex.Lock()
	defer b.queuesMutex.Unlock()
	q, ok := b.queues[nameQueue]
	if !ok {
		return nil, ""
	}

	var message string

	if len(q) > 0 {
		message = q[0]
		q = q[1:]
	}
	return q, message
}
