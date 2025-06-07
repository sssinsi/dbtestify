package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
)

type ListResult struct {
	DataSets []string `json:"datasets"`
}

func dumpDataSetList(useJson bool, w io.Writer, root fs.FS, port uint16) {
	if useJson {
		result := ListResult{
			DataSets: getTestList(root),
		}
		e := json.NewEncoder(w)
		e.Encode(&result)
	} else {
		for _, ds := range getTestList(root) {
			fmt.Fprintf(w, "* %s\n", ds)
			fmt.Fprintf(w, "    * Seed:   curl -X POST http://localhost:%d/api/seed/%s\n", port, ds)
			fmt.Fprintf(w, "    * Assert: curl http://localhost:%d/api/assert/%s\n", port, ds)
		}
	}
}

func getTestList(root fs.FS) []string {
	var result []string
	fs.WalkDir(root, ".", func(path string, d fs.DirEntry, err error) error {
		ext := filepath.Ext(path)
		if !d.IsDir() && (ext == ".yaml" || ext == ".yml") {
			result = append(result, path)
		}
		return nil
	})
	return result
}
