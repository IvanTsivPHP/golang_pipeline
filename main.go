package main

import (
	"bufio"
	"container/ring"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	Stage1Delay   = 1 * time.Second
	Stage2Delay   = 2 * time.Second
	BufferSize    = 10
	FlushInterval = 30 * time.Second
)

type PipeLineInt struct {
	stages []func(<-chan int) <-chan int
}

func (p *PipeLineInt) Run(source <-chan int, workerCounts []int, initDone chan struct{}) <-chan int {
	var c <-chan int = source
	for i, stage := range p.stages {
		c = runStageWithWorkers(stage, c, workerCounts[i])
	}
	close(initDone)
	return c
}

func runStageWithWorkers(stage func(<-chan int) <-chan int, source <-chan int, workerCount int) <-chan int {
	out := make(chan int)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for v := range stage(source) {
				out <- v
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func stage1(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "Starting worker instance for stage 1")
		defer close(out)
		for v := range in {
			time.Sleep(Stage1Delay)
			if v >= 0 {
				fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "Number", v, "passed negative filter")
				out <- v
			} else {
				fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "Number", v, "filtered out by negative filter")
			}
		}
	}()
	return out
}

func stage2(in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "Starting worker instance for stage 2")
		defer close(out)
		for v := range in {
			time.Sleep(Stage2Delay)
			if v%3 != 0 {
				out <- v
				fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "Number", v, "passed multiple_of_3 filter")
			} else {
				fmt.Println(time.Now().Format("2006-01-02 15:04:05"), "Number", v, "filtered out by multiple_of_3 filter")
			}
		}
	}()
	return out
}

func readInput(source chan<- int) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("Enter an integer or 'q' to quit:")
		if !scanner.Scan() {
			break
		}
		input := scanner.Text()
		if input == "q" {
			break
		}
		value, err := strconv.Atoi(input)
		if err != nil {
			fmt.Println("Invalid input. Please enter an integer.")
			continue
		}
		source <- value
	}
	close(source)
}

func flushBuffer(buffer *ring.Ring, start time.Time) {
	fmt.Println("Buffered results:")
	buffer.Do(func(p interface{}) {
		if p != nil {
			elapsed := time.Since(start)
			fmt.Printf("Value: %d, Time Elapsed: %v\n", p.(int), elapsed)
		}
	})
	fmt.Println("End of buffered results")
}

func main() {
	pipeline := PipeLineInt{
		stages: []func(<-chan int) <-chan int{
			stage1,
			stage2,
		},
	}

	source := make(chan int)
	initDone := make(chan struct{})
	workerCounts := []int{2, 2}
	start := time.Now()
	result := pipeline.Run(source, workerCounts, initDone)

	go func() {
		<-initDone
		go readInput(source)
	}()

	buffer := ring.New(BufferSize)
	ticker := time.NewTicker(FlushInterval)

	go func() {
		for range ticker.C {
			flushBuffer(buffer, start)
			buffer = ring.New(BufferSize)
		}
	}()

	for v := range result {
		buffer.Value = v
		buffer = buffer.Next()
	}

	flushBuffer(buffer, start)

}
