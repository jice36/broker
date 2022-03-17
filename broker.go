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

/*


curl -D - -XPUT localhost:8181/a?v=1

curl -D - -XGET http://127.0.0.1:8181/a?timeout=1

ab -n 10000 -c 100 -m PUT http://127.0.0.1:8181/a?v=11


ab -n 10000 -c 100 -m  GET http://127.0.0.1:8181/a


*/

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
	//var port string

	// fmt.Print("Введите порт:")
	// fmt.Scan(&port)
	b := newBrocker()

	http.HandleFunc("/", b.handler)
	//log.Fatal(http.ListenAndServe("localhost:"+port, nil))
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
	return
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
	qC := make(chan []string)
	go func() {
		b.pullMessage(qC, pg.nameQueue)
	}()

	select {
	case q = <-qC:
		b.queuesMutex.Lock()
		message := q[0]
		q = q[1:]
		b.queues[pg.nameQueue] = q
		b.queuesMutex.Unlock()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(message))
		return
	case <-time.After(time.Duration(timeout) * time.Second):
		w.WriteHeader(http.StatusNotFound)
		return
	}
}

func (b *brocker) pullMessage(qC chan []string, nameQ string) {
	for {
		b.queuesMutex.RLock()
		q, ok := b.queues[nameQ]
		b.queuesMutex.RUnlock()
		if ok {
			if len(q) > 0 {
				qC <- q
				return
			}
		}
	}
}
