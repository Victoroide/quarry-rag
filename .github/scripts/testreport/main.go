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
	diagnostics := &diagnosticWriter{Writer: stderr}
	err := r.decode(in, func(p *packageResult) {
		if !progress(diagnostics, p) {
			return
		}
		if p.failedBuild == "" {
			printf(diagnostics, "%s", r.failureDetails(p))
			return
		}
		b := r.builds[p.failedBuild]
		if b.failed {
			b.writeRemaining(diagnostics)
		}
	})
	r.resolveBuildFailures()
	r.writeFallbacks(diagnostics)
	if err != nil {
		diagnostics.startLine()
		printf(diagnostics, "%v\n", err)
		return 1
	}
	problems := r.problems(o.expectedList)
	r.summary(stdout, o, problems)
	for _, problem := range problems {
		diagnostics.startLine()
		printf(diagnostics, "testreport: %s\n", problem)
	}
	if len(problems) > 0 {
		return 1
	}
	return 0
}

// diagnosticWriter keeps reporter framing on its own line without changing
// the bytes between fragments in a compiler diagnostic.
type diagnosticWriter struct {
	io.Writer
	partialLine bool
}

func (w *diagnosticWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if n > 0 {
		w.partialLine = p[n-1] != '\n'
	}
	return n, err
}

func (w *diagnosticWriter) WriteString(s string) (int, error) {
	n, err := io.WriteString(w.Writer, s)
	if n > 0 {
		w.partialLine = s[n-1] != '\n'
	}
	return n, err
}

func (w *diagnosticWriter) startLine() {
	if w.partialLine {
		_, _ = w.WriteString("\n")
	}
}

// progress prints the package header and reports whether it printed a failure.
func progress(w *diagnosticWriter, p *packageResult) bool {
	if p.noTests {
		return false
	}
	w.startLine()
	switch p.outcome {
	case "pass", "skip":
		printf(w, "ok   \t%s\t%ss\n", p.name, seconds(p.elapsed))
		return false
	default:
		printf(w, "FAIL \t%s\t%ss\n", p.name, seconds(p.elapsed))
		return true
	}
}

func (r *run) writeFallbacks(w *diagnosticWriter) {
	for _, name := range r.order {
		p := r.packages[name]
		if !p.reportable() || p.outcome != "" {
			continue
		}
		switch {
		case p.standaloneBuild != "":
			progress(w, p)
		case !p.didNotBuild():
			w.startLine()
			printf(w, "INCOMPLETE\t%s\n%s", p.name, r.failureDetails(p))
		}
	}
	for _, id := range r.buildOrder {
		b := r.builds[id]
		name := buildPackage(id)
		standaloneVariant := b.failed && id != name && r.packages[name].standaloneBuild == id
		if b.hasPendingOutput() || r.independentBuilds[id] || standaloneVariant {
			w.startLine()
			printf(w, "BUILD FAIL\t%s\n", id)
			b.writeRemaining(w)
		}
	}
}

func (b *buildResult) hasPendingOutput() bool {
	return (b.failed || b.referenced) && b.nextFragment < len(b.fragments)
}

func (b *buildResult) writeRemaining(w io.Writer) {
	for _, fragment := range b.fragments[b.nextFragment:] {
		_, _ = io.WriteString(w, fragment)
	}
	b.nextFragment = len(b.fragments)
}
