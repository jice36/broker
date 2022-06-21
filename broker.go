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

type broker struct {
	queues      map[string]channels
	queuesMutex sync.Mutex
}

type channels struct{
	in chan <- string
	out <- chan string
}

type parametersPUT struct {
	nameQueue string
	message   string
}

type parametersGET struct {
	nameQueue string
	timeout   string
}

func newBrocker() *broker {
	q := make(map[string]channels)
	qM := sync.Mutex{}
	return &broker{queues: q, queuesMutex: qM}
}

func main() {
	//var port string
	//
	//fmt.Print("Введите порт:")
	//fmt.Scan(&port)
	b := newBrocker()

	http.HandleFunc("/", b.handler)
	log.Fatal(http.ListenAndServe("localhost:8181", nil))
}

func (b *broker) handler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		b.putMessage(w, r)
	case http.MethodGet:
		b.getMessage(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (b *broker) putMessage(w http.ResponseWriter, r *http.Request) {
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

func (b *broker) insertMessage(p *parametersPUT) {
	b.queuesMutex.Lock()
	defer b.queuesMutex.Unlock()

	channel, ok := b.queues[p.nameQueue]
	if !ok {
		in, out := dynamicChannel(5)
		channel = channels{
			in:  in,
			out: out,
		}
		in <- p.message
		b.queues[p.nameQueue] = channel
		return
	}
	channel.in <- p.message
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
	return nil, errors.New("bad parameter")
}

func getPartURL(data string, parseQueue bool) (string, error) {
	var spl []string
	if parseQueue {
		spl = strings.Split(data, "/")
		if len(spl) == 0 && len(spl) > 2 {
			return "", errors.New("bad parameter")
		}
	} else {
		if data == "" {
			return "", errors.New("empty body")
		}
		spl = strings.Split(data, "=")
		if len(spl) > 3 || spl[len(spl)-1] == "" {
			return "", errors.New("bad parameter")
		}
	}
	return spl[len(spl)-1], nil
}

func (b *broker) getMessage(w http.ResponseWriter, r *http.Request) {
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

	channel , ok := b.queues[pg.nameQueue]
	if !ok{
		w.WriteHeader(http.StatusNotFound)
		return
	}

	select {
	case message := <-channel.out:
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(message))
		return
	case <-time.After(time.Duration(timeout) * time.Second):
		w.WriteHeader(http.StatusNotFound)
		return
	}
}

func dynamicChannel(initial int) (chan <- string, <- chan string) {
	in := make(chan string, initial)
	out := make(chan string, initial)
	go func () {
		defer close(out)
		buffer := make([]string, 0, initial)
	loop:
		for {
			packet, ok := <- in
			if !ok {
				break loop
			}
			select {
			case out <- packet:
				continue
			default:
			}
			buffer = append(buffer, packet)
			for len(buffer) > 0 {
				select {
				case packet, ok := <-in:
					if !ok {
						break loop
					}
					buffer = append(buffer, packet)

				case out <- buffer[0]:
					buffer = buffer[1:]
				}
			}
		}
		for len(buffer) > 0 {
			out <- buffer[0]
			buffer = buffer[1:]
		}
	} ()

	return in, out
}
