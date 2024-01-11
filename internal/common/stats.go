package common

import (
	"fmt"
	"io"
	"net"
	"time"
)

type StatsdSender struct {
	queue chan string
	uri   string
}

var sender *StatsdSender

func (s *StatsdSender) begin() {
	s.queue = make(chan string, 100)
	if s.uri == "" {
		// TODO: Read from config or env
		host := "statsd.beam"
		port := 8125

		s.uri = fmt.Sprintf("%s:%v", host, port)
	}

	go s.process()
}

func (s *StatsdSender) count(metric string, value int) {
	s.queue <- fmt.Sprintf("%s:%d|c", metric, value)
}

func (s *StatsdSender) time(metric string, value time.Duration) {
	s.queue <- fmt.Sprintf("%s:%d|ms", metric, value/1e6)
}

func (s *StatsdSender) gauge(metric string, value int) {
	s.queue <- fmt.Sprintf("%s:%d|g", metric, value)
}

func (s *StatsdSender) process() {
	for msg := range s.queue {
		if conn, err := net.Dial("udp", s.uri); err == nil {
			io.WriteString(conn, msg)
			conn.Close()
		}
	}
}

func GetStatSender() *StatsdSender {
	if sender == nil {
		return InitStatsdSender("")
	}
	return sender
}

func InitStatsdSender(uri string) *StatsdSender {
	if sender != nil {
		return sender
	}

	sender := &StatsdSender{uri: uri}
	sender.begin()
	return sender
}

func (s *StatsdSender) StatCount(metric string, value int) {
	s.count(metric, value)
}

func (s *StatsdSender) StatCountTags(metric string, value int, tags map[string]string) {
	s.count(ConcatTags(metric, &tags), value)
}

func (s *StatsdSender) StatTime(metric string, value time.Duration) {
	s.time(metric, value)
}

func (s *StatsdSender) StatTimeTags(metric string, value time.Duration, tags map[string]string) {
	s.time(ConcatTags(metric, &tags), value)
}

func (s *StatsdSender) StatGauge(metric string, value int) {
	s.gauge(metric, value)
}

func (s *StatsdSender) StatGaugeTags(metric string, value int, tags map[string]string) {
	s.gauge(ConcatTags(metric, &tags), value)
}

func ConcatTags(metric string, tags *map[string]string) string {
	if tags == nil {
		return metric
	}

	tagsString := ""

	for k, v := range *tags {
		tagsString += fmt.Sprintf("%s=%s;", k, v)
	}

	// Remove the last semicolon
	tagsString = tagsString[:len(tagsString)-1]

	return fmt.Sprintf("%s;%s", metric, tagsString)
}
