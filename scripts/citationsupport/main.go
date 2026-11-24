// Command citationsupport reports whether held-out calibration meets the live gate.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/Victoroide/quarry-rag/internal/citationsupport"
)

func main() {
	path := flag.String("input", "", "path to the local citation calibration file")
	flag.Parse()
	if err := report(*path, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func report(path string, out io.Writer) error {
	if path == "" {
		return fmt.Errorf("-input is required")
	}
	calibration, err := citationsupport.ReadCalibration(path)
	if err != nil {
		return err
	}
	pairs, err := calibration.Evaluate()
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(struct {
		Model string                       `json:"model"`
		Pairs []citationsupport.PairReport `json:"pairs"`
	}{calibration.Model, pairs})
}
