package network

import "fmt"

func (r *Receiver) Stop() {
	r.Stopped = true
	if r.Conn != nil {
		r.Conn.Close()
	}
}

var Verbose bool = true

func Logf(format string, a ...interface{}) {
	if Verbose {
		fmt.Printf(format, a...)
	}
}

func Logln(a ...interface{}) {
	if Verbose {
		fmt.Println(a...)
	}
}
