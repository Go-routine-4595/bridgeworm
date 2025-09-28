package usecase

import "github.com/Go-routine-4595/bridgeworm/service"

type Worker struct {
	srv *service.Service
}

func NewWorker() *Worker {
	return &Worker{
		srv: service.NewService(),
	}
}
