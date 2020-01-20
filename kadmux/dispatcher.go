package kadmux

type Dispatcher struct {
	newWork           chan WorkRequest
	workers           chan chan WorkRequest
	exit              chan bool
	registeredWorkers []*Worker
}

func NewDispatcher(maxWorkers int) *Dispatcher {
	d := &Dispatcher{
		newWork:           make(chan WorkRequest),
		workers:           make(chan chan WorkRequest, maxWorkers),
		exit:              make(chan bool),
		registeredWorkers: make([]*Worker, 0),
	}

	return d
}

func (d *Dispatcher) Start() {
	queue := make([]*WorkRequest, 0)
	for {

		var next *WorkRequest
		var workers chan chan WorkRequest
		if len(queue) > 0 {
			next = queue[0]
			workers = d.workers
		}

		select {
		case <-d.exit:
			return
		case work := <-d.newWork:
			queue = append(queue, &work)

		case worker := <-workers:
			worker <- *next
			queue = queue[1:]
		}
	}
}

func (d *Dispatcher) Stop() {
	for _, w := range d.registeredWorkers {
		w.Stop()
	}

	d.exit <- true
}

func (d *Dispatcher) Dispatch(w *Worker) {
	d.registeredWorkers = append(d.registeredWorkers, w)
	go w.Start(d.workers)
}

func (d *Dispatcher) QueueWork() chan<- WorkRequest {
	return d.newWork
}
