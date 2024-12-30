package report

import (
	"html/template"
	"os"
)

var t = template.Must(template.New("report").Parse(tpl))

type Report struct {
	TaskInfoItems      [][2]string // [key, value]
	WorkloadInfoItems  [][2]string
	ExecutionInfoItems [][2]string
	Summary            Summary
	TopSQLs            Table
	Details            []Details
}

type Summary struct {
	Overall     ChangeCount
	Improved    ChangeCount
	Unchanged   ChangeCount
	MayDegraded ChangeCount
	Errors      ChangeCount
	Unsupported ChangeCount
}

type ChangeCount struct {
	SQL  int
	Plan int
}

type Table struct {
	Header []string
	Data   [][]string
}

type Details struct {
	Header string
	Labels [][2]string
	Source *Plan
	Target *Plan
}

type Plan struct {
	Labels [][2]string
	Text   string
}

func Render(r *Report, outFilename string) error {
	file, err := os.Create(outFilename)
	if err != nil {
		return err
	}
	defer file.Close()

	return render(r, file)
}

func render(r *Report, outFile *os.File) error {
	return t.Execute(outFile, r)
}
