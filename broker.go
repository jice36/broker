package main

import (
	"errors"
	"fmt"
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
	queuesMutex *sync.RWMutex
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
	qM := &sync.RWMutex{}
	return &brocker{queues: q, queuesMutex: qM}
}

func main() {
	fmt.Print("введите порт:")
	var port string
	fmt.Scan(&port)

	b := newBrocker()

	http.HandleFunc("/", b.handler)

	log.Fatal(http.ListenAndServe("localhost:"+port, nil))
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
	b.queuesMutex.Unlock()
}

func (b *brocker) readQueue(nameQueue string) ([]string, bool) {
	b.queuesMutex.RLock()
	q, ok := b.queues[nameQueue]
	b.queuesMutex.RUnlock()
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
		err = b.insertMessage(pu)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	} else {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
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

func (b *brocker) insertMessage(p *parametersPUT) error {
	q, ok := b.readQueue(p.nameQueue)
	if !ok {
		q := make([]string, 0)
		q = append(q, p.message)
		b.insertQueue(p.nameQueue, q)
	}
	q = append(q, p.message)
	b.insertQueue(p.nameQueue, q)
	return nil
}

func (b *brocker) getMessage(w http.ResponseWriter, r *http.Request) {
	p, err := parseURL(r.URL.String(), paramForGET)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	message := make(chan string)

	pg, ok := p.(*parametersGET)
	if ok {
		go b.pullMessage(message, pg.nameQueue)
	}

	timeout, err := strconv.Atoi(pg.timeout)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if timeout == 0 {
		timeout = defaultTimeout
	}

	var msg string

	select {
	case msg = <-message:
		w.Write([]byte(msg))
		return
	case <-time.After(time.Duration(timeout) * time.Second):
		w.WriteHeader(http.StatusNotFound)
		return
	}
}

func (b *brocker) pullMessage(mC chan string, nameQ string) {
	flag := true
	var m string
	for flag {
		m = b.tryingTakeMessage(nameQ)
		if m != "" {
			mC <- m
			flag = false
		}
	}
}

func (b *brocker) tryingTakeMessage(nameQueue string) string {
	q, ok := b.readQueue(nameQueue)
	if !ok {
		return ""
	}

	var message string

	if len(q) > 0 {
		message = q[0]
		q = q[1:]
		b.insertQueue(nameQueue, q)
	}
	return message
}
