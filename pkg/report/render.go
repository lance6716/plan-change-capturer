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
