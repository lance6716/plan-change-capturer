package main

import "github.com/lance6716/plan-change-capturer/cmd"

func main() {
	// TODO(lance6716): signal handling
	err := cmd.Execute()
	if err != nil {
		panic(err)
	}
}
