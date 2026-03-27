package dlq

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type record struct {
	Timestamp string      `json:"timestamp"`
	Source    string      `json:"source"`
	Error     string      `json:"error"`
	Data      interface{} `json:"data"`
}

func Write(source string, data interface{}, err error) error {
	if mkErr := os.MkdirAll("logs/dlq", 0755); mkErr != nil {
		return fmt.Errorf("mkdir dlq: %w", mkErr)
	}

	f, openErr := os.OpenFile(filepath.Join("logs", "dlq", source+".jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if openErr != nil {
		return fmt.Errorf("open dlq file: %w", openErr)
	}
	defer f.Close()

	entry := record{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Source:    source,
		Data:      data,
	}
	if err != nil {
		entry.Error = err.Error()
	}

	b, marshalErr := json.Marshal(entry)
	if marshalErr != nil {
		return fmt.Errorf("marshal dlq record: %w", marshalErr)
	}
	if _, writeErr := f.Write(append(b, '\n')); writeErr != nil {
		return fmt.Errorf("write dlq record: %w", writeErr)
	}
	return nil
}
