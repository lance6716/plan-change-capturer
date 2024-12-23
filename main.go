package main

import "github.com/lance6716/plan-change-capturer/cmd"

func main() {
	err := cmd.Execute()
	if err != nil {
		panic(err)
	}
}
