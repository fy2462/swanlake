package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/google/uuid"
)

type TestCase struct {
	kind     string // "statement" or "query"
	sql      string
	types    string     // for query
	expected [][]string // for query
}

type Runner struct {
	endpoint string
	testFile string
	testDir  string
	conn     adbc.Connection
}

func (r *Runner) initTempDir() error {
	testFileDir, err := filepath.Abs(filepath.Dir(r.testFile))
	if err != nil {
		return err
	}
	tempDir := filepath.Join(testFileDir, uuid.New().String())
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp dir: %w", err)
	}
	r.testDir = tempDir
	return nil
}

type Parser struct {
	testDir string
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	// Parse args: endpoint and test file
	if len(os.Args) < 3 {
		return fmt.Errorf("usage: %s <endpoint> <test_file>", os.Args[0])
	}
	endpoint := os.Args[1]
	testFile := os.Args[2]

	runner := &Runner{
		endpoint: endpoint,
		testFile: testFile,
	}
	if err := runner.initTempDir(); err != nil {
		return err
	}
	return runner.Run()
}

func (r *Runner) Run() error {
	ctx := context.Background()

	fmt.Printf("Connecting to %s...\n", r.endpoint)

	if err := r.connect(ctx); err != nil {
		return err
	}
	defer r.conn.Close()

	fmt.Println("Connected successfully!")

	// Parse test file
	parser := &Parser{testDir: r.testDir}
	testCases, err := parser.ParseTestFile(r.testFile)
	if err != nil {
		return fmt.Errorf("failed to parse test file: %w", err)
	}

	// Execute test cases
	return r.executeTests(ctx, testCases)
}

func (r *Runner) connect(ctx context.Context) error {
	drv := flightsql.NewDriver(nil)
	db, err := drv.NewDatabase(map[string]string{
		adbc.OptionKeyURI:             r.endpoint,
		flightsql.OptionSSLSkipVerify: adbc.OptionValueEnabled,
	})
	if err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}
	defer db.Close()

	conn, err := db.Open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}
	r.conn = conn
	return nil
}

func (r *Runner) executeTests(ctx context.Context, testCases []TestCase) error {
	for i, tc := range testCases {
		fmt.Printf("Running test %d: %s\n", i+1, tc.kind)
		if err := r.executeTestCase(ctx, tc); err != nil {
			return fmt.Errorf("test %d, sql %s failed: %w", i+1, tc.sql, err)
		}
		fmt.Printf("Test %d passed\n", i+1)
	}

	fmt.Println("All tests passed!")

	// Cleanup only on success
	if err := r.cleanup(); err != nil {
		fmt.Printf("Warning: cleanup failed: %v\n", err)
	}

	return nil
}

func (r *Runner) executeTestCase(ctx context.Context, tc TestCase) error {
	switch tc.kind {
	case "statement":
		return r.executeStatement(ctx, tc.sql)
	case "query":
		return r.executeQuery(ctx, tc.sql, tc.expected)
	default:
		return fmt.Errorf("unknown test kind: %s", tc.kind)
	}
}

func (r *Runner) executeStatement(ctx context.Context, sql string) error {
	stmt, err := r.conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(ctx)
	return err
}

func (r *Runner) executeQuery(ctx context.Context, sql string, expected [][]string) error {
	stmt, err := r.conn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(sql); err != nil {
		return err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return err
	}
	defer reader.Release()

	var actual [][]string
	for reader.Next() {
		record := reader.Record()
		for i := 0; i < int(record.NumRows()); i++ {
			var row []string
			for j := 0; j < int(record.NumCols()); j++ {
				col := record.Column(j)
				row = append(row, valueToString(col, i))
			}
			actual = append(actual, row)
		}
	}

	if err := reader.Err(); err != nil {
		return err
	}

	// Compare
	if len(actual) != len(expected) {
		return fmt.Errorf("row count mismatch: got %d, expected %d", len(actual), len(expected))
	}

	for i, row := range actual {
		if len(row) != len(expected[i]) {
			return fmt.Errorf("column count mismatch in row %d: got %d, expected %d", i, len(row), len(expected[i]))
		}
		for j, val := range row {
			if val != expected[i][j] {
				return fmt.Errorf("value mismatch at row %d col %d: got %q, expected %q", i, j, val, expected[i][j])
			}
		}
	}

	return nil
}

func (p *Parser) ParseTestFile(path string) ([]TestCase, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var testCases []TestCase
	scanner := bufio.NewScanner(file)

	var current TestCase
	var inQuery bool
	var inExpected bool

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}

		if strings.HasPrefix(line, "require ") {
			// Skip requires
			continue
		}

		if strings.HasPrefix(line, "statement ok") {
			if current.kind != "" {
				testCases = append(testCases, current)
			}
			current = TestCase{kind: "statement"}
			inQuery = false
			inExpected = false
			continue
		}

		if strings.HasPrefix(line, "query ") {
			if current.kind != "" {
				testCases = append(testCases, current)
			}
			parts := strings.Fields(line)
			if len(parts) < 2 {
				return nil, fmt.Errorf("invalid query line: %s", line)
			}
			current = TestCase{kind: "query", types: parts[1]}
			inQuery = true
			inExpected = false
			continue
		}

		if inQuery {
			if line == "----" {
				inExpected = true
				current.expected = [][]string{}
				continue
			}
			if !inExpected {
				current.sql += line + "\n"
			} else {
				if line == "" {
					continue
				}
				row := strings.Split(line, "\t")
				current.expected = append(current.expected, row)
			}
		} else if current.kind == "statement" {
			current.sql += line + "\n"
		}
	}

	if current.kind != "" {
		testCases = append(testCases, current)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Apply substitutions
	for i := range testCases {
		testCases[i].sql = strings.ReplaceAll(testCases[i].sql, "__TEST_DIR__", p.testDir)
	}

	return testCases, nil
}

func (r *Runner) cleanup() error {
	return os.RemoveAll(r.testDir)
}

func valueToString(col arrow.Array, row int) string {
	if col.IsNull(row) {
		return "NULL"
	}

	switch arr := col.(type) {
	case *array.Int8:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Int16:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Int32:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Int64:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Uint8:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Uint16:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Uint32:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Uint64:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Float32:
		return fmt.Sprintf("%g", arr.Value(row))
	case *array.Float64:
		return fmt.Sprintf("%g", arr.Value(row))
	case *array.String:
		return arr.Value(row)
	case *array.Boolean:
		return fmt.Sprintf("%t", arr.Value(row))
	default:
		return fmt.Sprintf("%v", col.ValueStr(row))
	}
}
