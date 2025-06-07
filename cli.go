package dbtestify

import (
	"fmt"
	"strings"

	"github.com/fatih/color"
)

var okC = color.New(color.FgGreen).SprintFunc()
var pkeyL = color.New(color.FgBlue, color.Bold, color.Underline).SprintfFunc()
var pkeyV = color.New(color.FgBlue, color.Underline).SprintfFunc()
var actualLC = color.New(color.FgRed).SprintfFunc()
var actualTC = color.New(color.BgRed, color.FgBlack).SprintfFunc()
var expectLC = color.New(color.FgGreen).SprintfFunc()
var expectTC = color.New(color.BgGreen, color.FgBlack).SprintfFunc()
var nameC = color.New(color.FgBlue, color.Bold).SprintfFunc()

func DumpDiffCLICallback(showTableName, quiet bool) func(result AssertTableResult) {
	return func(result AssertTableResult) {
		if showTableName {
			fmt.Print(nameC("Table: %s\n", result.Name))
		}
		if result.Status == Match {
			if !quiet {
				fmt.Printf(" %s\n", okC("OK"))
			}
		} else {
			fmt.Print(expectLC("- Expected\n"))
			fmt.Print(actualLC("+ Actual\n"))

			for _, r := range result.Rows {
				for i := range result.PrimaryKeys {
					fmt.Print(pkeyL("%s", r.Fields[i].Key))
					if r.Status == OnlyOnActual {
						fmt.Print(pkeyV(": %v", r.Fields[i].Actual))
					} else {
						fmt.Print(pkeyV(": %v", r.Fields[i].Expect))
					}
					if i+1 == len(result.PrimaryKeys) {
						fmt.Print("\n")
					} else {
						fmt.Print(pkeyV(", "))
					}
				}
				switch r.Status {
				case Match:
					fmt.Printf("  ")
					for i, f := range r.Fields {
						if i < len(result.PrimaryKeys) {
							continue
						}
						if i != len(result.PrimaryKeys) {
							fmt.Printf(", ")
						}
						fmt.Printf("%s: %v", f.Key, f.Expect)
					}
				case NotMatch:
					fmt.Print(expectTC("+") + " ")
					for i, f := range r.Fields {
						if i < len(result.PrimaryKeys) {
							continue
						}
						if i != len(result.PrimaryKeys) {
							fmt.Print(expectLC(", "))
						}
						if f.Status == Match {
							fmt.Print(expectLC("%s: %v", f.Key, f.Expect))
						} else {
							fmt.Print(expectLC("%s: ", f.Key))
							e := fmt.Sprintf("%v", f.Expect)
							a := fmt.Sprintf("%v", f.Actual)
							fmt.Print(expectTC(e))
							fmt.Print(strings.Repeat(" ", max(len(a)-len(e), 0)))
						}
					}
					fmt.Print("\n" + actualTC("-") + " ")
					for i, f := range r.Fields {
						if i < len(result.PrimaryKeys) {
							continue
						}
						if i != len(result.PrimaryKeys) {
							fmt.Print(actualLC(", "))
						}
						if f.Status == Match {
							fmt.Print(actualLC("%s: %v", f.Key, f.Actual))
						} else {
							fmt.Print(actualLC("%s: ", f.Key))
							e := fmt.Sprintf("%v", f.Expect)
							a := fmt.Sprintf("%v", f.Actual)
							fmt.Print(actualTC("%v", f.Actual))
							fmt.Print(strings.Repeat(" ", max(len(e)-len(a), 0)))
						}
					}
				case OnlyOnExpect:
					fmt.Print(expectTC("+") + " ")
					for i, f := range r.Fields {
						if i < len(result.PrimaryKeys) {
							continue
						}
						if i != len(result.PrimaryKeys) {
							fmt.Print(expectLC(", "))
						}
						fmt.Print(expectTC("%s: %v", f.Key, f.Expect))
					}
				case OnlyOnActual:
					fmt.Print(actualTC("-") + " ")
					for i, f := range r.Fields {
						if i < len(result.PrimaryKeys) {
							continue
						}
						if i != len(result.PrimaryKeys) {
							fmt.Print(actualLC(", "))
						}
						fmt.Print(actualTC("%s: %v", f.Key, f.Actual))
					}
				}
				fmt.Print("\n")
			}
			fmt.Print("\n")
		}
	}
}
