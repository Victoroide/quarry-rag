// Command testreport reads the JSON events of a `go test` invocation from
// standard input and reports on them. It writes a Markdown summary to
// standard output and package progress and failure diagnostics to standard
// error. The packages observed or named as arguments must all have a result.
//
// The command exits with status 1 when a package failed or did not build,
// when a package has no result, or when a result came from the test
// cache. The status of `go test` itself is not hidden: run the pipeline with
// pipefail so that either failure fails the step.
package main

import (
	"flag"
	"io"
	"os"
)

func main() {
	os.Exit(execute(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func execute(args []string, in io.Reader, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("testreport", flag.ContinueOnError)
	flags.SetOutput(stderr)
	o := options{}
	flags.IntVar(&o.slowest, "slowest", 10, "number of slowest tests to list")
	flags.StringVar(&o.moduleCache, "module-cache", "", "state of the module cache, shown in the summary")
	flags.StringVar(&o.buildCache, "build-cache", "", "state of the build cache, shown in the summary")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	o.expectedList = flags.Args()

	r := newRun()
	printedBuilds := map[string]bool{}
	err := r.decode(in, func(p *packageResult) {
		progress(stderr, r, p)
		if p.failedBuild != "" && p.outcome == "fail" && !p.noTests {
			printedBuilds[p.failedBuild] = true
		}
	})
	for _, name := range r.order {
		p := r.packages[name]
		if p.outcome != "" {
			continue
		}
		if p.failedBuild == "" {
			printf(stderr, "INCOMPLETE\t%s\n%s", p.name, r.failureDetails(p))
		} else if !printedBuilds[p.failedBuild] {
			progress(stderr, r, p)
			printedBuilds[p.failedBuild] = true
		}
	}
	if err != nil {
		printf(stderr, "%v\n", err)
		return 1
	}
	r.summary(stdout, o)
	problems := r.problems(o.expectedList)
	for _, problem := range problems {
		printf(stderr, "testreport: %s\n", problem)
	}
	if len(problems) > 0 {
		return 1
	}
	return 0
}

// progress prints a line for an ended package, and what failed in it.
func progress(w io.Writer, r *run, p *packageResult) {
	switch {
	case p.noTests:
		return
	case p.outcome == "pass" || p.outcome == "skip":
		printf(w, "ok   \t%s\t%ss\n", p.name, seconds(p.elapsed))
	default:
		printf(w, "FAIL \t%s\t%ss\n%s", p.name, seconds(p.elapsed), r.failureDetails(p))
	}
}
