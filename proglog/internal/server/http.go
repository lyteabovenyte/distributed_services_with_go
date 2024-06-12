package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gorilla/mux"
)

type httpServer struct {
	Log *Log
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

// implementing our server's handlers
func (s *httpServer) handleProduce(rw http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	encErr := json.NewDecoder(r.Body).Decode(&req)
	if encErr != nil {
		http.Error(rw, encErr.Error(), http.StatusBadRequest)
		return
	}
	off, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ProduceResponse{Offset: off}
	decErr := json.NewEncoder(rw).Encode(res)
	if decErr != nil {
		http.Error(rw, decErr.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *httpServer) handleConsume(rw http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	decErr := json.NewDecoder(r.Body).Decode(&req)
	if decErr != nil {
		http.Error(rw, decErr.Error(), http.StatusBadRequest)
		return
	}
	rec, err := s.Log.Read(req.Offset)
	if errors.Is(err, ErrOffsetNotFound) {
		http.Error(rw, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ConsumeResponse{Record: rec}
	encErr := json.NewEncoder(rw).Encode(res)
	if encErr != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
}
