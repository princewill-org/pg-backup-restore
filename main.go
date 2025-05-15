package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/manifoldco/promptui"
)

const (
	defaultBackupDir = "./backups"
	defaultHost      = "localhost"
	defaultPort      = 5432
	defaultUser      = "postgres"
)

type Config struct {
	host      string
	port      int
	user      string
	password  string
	dbname    string
	backupDir string
	schema    string // Empty means all schemas
	sslmode   string // SSL mode (e.g., "require", "verify-full", "disable")
}

func main() {
	config, err := promptConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get configuration: %v\n", err)
		os.Exit(1)
	}

	action, err := promptAction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get action: %v\n", err)
		os.Exit(1)
	}

	switch action {
	case "dump":
		if err := dumpDatabase(config); err != nil {
			fmt.Fprintf(os.Stderr, "Dump failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Database dump completed successfully")
	case "restore":
		if err := restoreDatabase(config); err != nil {
			fmt.Fprintf(os.Stderr, "Restore failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Database restore completed successfully")
	default:
		fmt.Fprintf(os.Stderr, "Invalid action selected\n")
		os.Exit(1)
	}
}

func promptAction() (string, error) {
	prompt := promptui.Select{
		Label: "Select Action",
		Items: []string{"dump", "restore"},
	}

	_, result, err := prompt.Run()
	if err != nil {
		return "", fmt.Errorf("prompt failed: %v", err)
	}
	return result, nil
}

func promptConfig() (Config, error) {
	config := Config{
		backupDir: defaultBackupDir,
		sslmode:   "require", // Default to require for Neon
	}

	// Prompt for database name
	prompt := promptui.Prompt{
		Label: "Database name",
		Validate: func(input string) error {
			if input == "" {
				return fmt.Errorf("database name is required")
			}
			return nil
		},
	}
	dbname, err := prompt.Run()
	if err != nil {
		return config, fmt.Errorf("prompt failed for database name: %v", err)
	}
	config.dbname = dbname

	// Prompt for host
	prompt = promptui.Prompt{
		Label:   "Database host",
		Default: defaultHost,
	}
	host, err := prompt.Run()
	if err != nil {
		return config, fmt.Errorf("prompt failed for host: %v", err)
	}
	config.host = host

	// Prompt for port
	prompt = promptui.Prompt{
		Label:   "Database port",
		Default: strconv.Itoa(defaultPort),
		Validate: func(input string) error {
			port, err := strconv.Atoi(input)
			if err != nil || port < 1 || port > 65535 {
				return fmt.Errorf("invalid port number")
			}
			return nil
		},
	}
	portStr, err := prompt.Run()
	if err != nil {
		return config, fmt.Errorf("prompt failed for port: %v", err)
	}
	port, _ := strconv.Atoi(portStr)
	config.port = port

	// Prompt for user
	prompt = promptui.Prompt{
		Label:   "Database user",
		Default: defaultUser,
	}
	user, err := prompt.Run()
	if err != nil {
		return config, fmt.Errorf("prompt failed for user: %v", err)
	}
	config.user = user

	// Prompt for password
	prompt = promptui.Prompt{
		Label: "Database password",
		Mask:  '*',
	}
	password, err := prompt.Run()
	if err != nil {
		return config, fmt.Errorf("prompt failed for password: %v", err)
	}
	config.password = password

	// Prompt for schema (optional)
	prompt = promptui.Prompt{
		Label:   "Schema name (leave empty for all schemas)",
		Default: "",
	}
	schema, err := prompt.Run()
	if err != nil {
		return config, fmt.Errorf("prompt failed for schema: %v", err)
	}
	config.schema = schema

	// Prompt for backup directory
	prompt = promptui.Prompt{
		Label:   "Backup directory",
		Default: defaultBackupDir,
	}
	backupDir, err := prompt.Run()
	if err != nil {
		return config, fmt.Errorf("prompt failed for backup directory: %v", err)
	}
	config.backupDir = backupDir

	// Prompt for SSL mode
	prompt = promptui.Prompt{
		Label:   "SSL mode (require, verify-full, disable)",
		Default: config.sslmode,
		Validate: func(input string) error {
			switch input {
			case "require", "verify-full", "disable":
				return nil
			default:
				return fmt.Errorf("invalid SSL mode; use require, verify-full, or disable")
			}
		},
	}
	sslmode, err := prompt.Run()
	if err != nil {
		return config, fmt.Errorf("prompt failed for SSL mode: %v", err)
	}
	config.sslmode = sslmode

	return config, nil
}

func getConnString(config Config) string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.host, config.port, config.user, config.password, config.dbname, config.sslmode)
}

func dumpDatabase(config Config) error {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, getConnString(config))
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}
	defer conn.Close(ctx)

	// Create backup directory if it doesn't exist
	if err := os.MkdirAll(config.backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %v", err)
	}

	// Generate backup filename
	backupFile := filepath.Join(config.backupDir,
		fmt.Sprintf("%s_%s.sql", config.dbname, time.Now().Format("20060102_150405")))
	f, err := os.Create(backupFile)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %v", err)
	}
	defer f.Close()

	// Dump schemas and their contents
	if err := dumpSchemas(ctx, conn, f, config.schema); err != nil {
		return err
	}

	return nil
}

func dumpSchemas(ctx context.Context, conn *pgx.Conn, w io.Writer, targetSchema string) error {
	// Get schema names
	schemaQuery := `
		SELECT schema_name 
		FROM information_schema.schemata 
		WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema'
	`
	if targetSchema != "" {
		schemaQuery += fmt.Sprintf(" AND schema_name = '%s'", targetSchema)
	}

	rows, err := conn.Query(ctx, schemaQuery)
	if err != nil {
		return fmt.Errorf("failed to query schemas: %v", err)
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var schemaName string
		if err := rows.Scan(&schemaName); err != nil {
			return fmt.Errorf("failed to scan schema name: %v", err)
		}
		schemas = append(schemas, schemaName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("schema query error: %v", err)
	}

	if len(schemas) == 0 {
		return fmt.Errorf("no schemas found")
	}

	for _, schema := range schemas {
		// Write CREATE SCHEMA statement
		if _, err := fmt.Fprintf(w, "CREATE SCHEMA IF NOT EXISTS %s;\n\n", schema); err != nil {
			return fmt.Errorf("failed to write schema creation for %s: %v", schema, err)
		}

		// Dump schema contents (tables)
		if err := dumpSchemaTables(ctx, conn, w, schema); err != nil {
			return fmt.Errorf("failed to dump tables for schema %s: %v", schema, err)
		}
	}

	return nil
}

func dumpSchemaTables(ctx context.Context, conn *pgx.Conn, w io.Writer, schema string) error {
	log.Printf("Dumping tables for schema %s", schema)
	rows, err := conn.Query(ctx, `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = $1
    `, schema)
	if err != nil {
		return fmt.Errorf("failed to query table names for schema %s: %v", schema, err)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name for schema %s: %v", schema, err)
		}
		tableNames = append(tableNames, tableName)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("table names query error for schema %s: %v", schema, err)
	}

	for _, tableName := range tableNames {
		ddl, err := generateTableDDL(ctx, conn, schema, tableName)
		if err != nil {
			return fmt.Errorf("failed to generate table definition for %s.%s: %v", schema, tableName, err)
		}
		if _, err := fmt.Fprintf(w, "%s;\n\n", ddl); err != nil {
			return fmt.Errorf("failed to write table definition for %s.%s: %v", schema, tableName, err)
		}

		if err := dumpTableData(ctx, conn, w, schema, tableName); err != nil {
			return fmt.Errorf("failed to dump data for %s.%s: %v", schema, tableName, err)
		}
	}

	return nil
}

func dumpTableData(ctx context.Context, conn *pgx.Conn, w io.Writer, schema, tableName string) error {
	qualifiedTableName := fmt.Sprintf("%s.%s", schema, tableName)

	// Disable triggers
	if _, err := fmt.Fprintf(w, "ALTER TABLE %s DISABLE TRIGGER ALL;\n", qualifiedTableName); err != nil {
		return fmt.Errorf("failed to write disable trigger for %s: %v", qualifiedTableName, err)
	}

	// Get column names and types
	colRows, err := conn.Query(ctx, `
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
    `, schema, tableName)
	if err != nil {
		return fmt.Errorf("failed to get columns for %s: %v", qualifiedTableName, err)
	}
	defer colRows.Close()

	var columns []string
	var colTypes []string
	for colRows.Next() {
		var colName, colType string
		if err := colRows.Scan(&colName, &colType); err != nil {
			return fmt.Errorf("failed to scan column info for %s: %v", qualifiedTableName, err)
		}
		columns = append(columns, colName)
		colTypes = append(colTypes, colType)
	}

	// Dump table data
	dataRows, err := conn.Query(ctx, fmt.Sprintf("SELECT * FROM %s", qualifiedTableName))
	if err != nil {
		return fmt.Errorf("failed to query data for %s: %v", qualifiedTableName, err)
	}
	defer dataRows.Close()

	// Get type information for proper encoding
	oidMap := dataRows.FieldDescriptions()
	typeMap := conn.TypeMap()

	for dataRows.Next() {
		values, err := dataRows.Values()
		if err != nil {
			return fmt.Errorf("failed to get row values for %s: %v", qualifiedTableName, err)
		}

		var formattedValues []string
		for i, v := range values {
			if v == nil {
				formattedValues = append(formattedValues, "NULL")
				continue
			}

			// Get the OID for the column
			oid := oidMap[i].DataTypeOID
			pgType, ok := typeMap.TypeForOID(oid)
			if !ok {
				return fmt.Errorf("unknown type OID %d for column %s in table %s", oid, columns[i], qualifiedTableName)
			}

			// Format the value according to its PostgreSQL type
			var formatted string
			switch pgType.Name {
			case "text", "varchar", "char":
				str, ok := v.(string)
				if !ok {
					return fmt.Errorf("expected string for column %s in table %s, got %T", columns[i], qualifiedTableName, v)
				}
				formatted = fmt.Sprintf("'%s'", strings.ReplaceAll(str, "'", "''"))
			case "integer", "bigint", "smallint", "numeric", "real", "double precision":
				formatted = fmt.Sprintf("%v", v)
			case "boolean":
				b, ok := v.(bool)
				if !ok {
					return fmt.Errorf("expected boolean for column %s in table %s, got %T", columns[i], qualifiedTableName, v)
				}
				formatted = fmt.Sprintf("%t", b)
			case "timestamp", "timestamptz", "date":
				var t time.Time
				switch v := v.(type) {
				case time.Time:
					t = v
				case pgtype.Timestamp:
					if v.Valid {
						t = v.Time
					} else {
						formatted = "NULL"
						continue
					}
				default:
					return fmt.Errorf("expected timestamp for column %s in table %s, got %T", columns[i], qualifiedTableName, v)
				}
				formatted = fmt.Sprintf("'%s'", t.Format("2006-01-02 15:04:05.999999-07:00"))
			case "json", "jsonb":
				var jsonData []byte
				switch v := v.(type) {
				case []byte:
					jsonData = v
				case string:
					jsonData = []byte(v)
				case nil:
					formatted = "NULL"
					continue
				default:
					return fmt.Errorf("expected JSON for column %s in table %s, got %T", columns[i], qualifiedTableName, v)
				}
				if len(jsonData) == 0 {
					formatted = "NULL"
				} else {
					formatted = fmt.Sprintf("'%s'", strings.ReplaceAll(string(jsonData), "'", "''"))
				}
			case "array":
				formatted, err = formatArray(ctx, conn, v, pgType, columns[i], qualifiedTableName)
				if err != nil {
					return err
				}
			default:
				// Fallback for other types
				formatted = fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%v", v), "'", "''"))
			}
			formattedValues = append(formattedValues, formatted)
		}

		// Write INSERT statement
		if _, err := fmt.Fprintf(w, "INSERT INTO %s (%s) VALUES (%s);\n",
			qualifiedTableName,
			strings.Join(columns, ", "),
			strings.Join(formattedValues, ", ")); err != nil {
			return fmt.Errorf("failed to write insert for %s: %v", qualifiedTableName, err)
		}
	}

	// Enable triggers
	if _, err := fmt.Fprintf(w, "ALTER TABLE %s ENABLE TRIGGER ALL;\n\n", qualifiedTableName); err != nil {
		return fmt.Errorf("failed to write enable trigger for %s: %v", qualifiedTableName, err)
	}

	return nil
}

func formatArray(ctx context.Context, conn *pgx.Conn, v interface{}, pgType *pgtype.Type, columnName, tableName string) (string, error) {
	if v == nil {
		return "NULL", nil
	}

	val := reflect.ValueOf(v)
	if val.Kind() != reflect.Slice && val.Kind() != reflect.Array {
		return "", fmt.Errorf("expected array for column %s in table %s, got %T", columnName, tableName, v)
	}

	elements := make([]interface{}, val.Len())
	for i := 0; i < val.Len(); i++ {
		elements[i] = val.Index(i).Interface()
	}

	// Derive the element type from the schema
	parts := strings.Split(tableName, ".")
	schema, table := parts[0], parts[1]
	elemTypeName, err := getArrayElementType(ctx, conn, schema, table, columnName)
	if err != nil {
		log.Printf("Failed to get array element type, falling back to %s: %v", pgType.Name, err)
		elemTypeName = pgType.Name
	}

	nestedType := &pgtype.Type{
		Name:  elemTypeName,
		OID:   pgtype.UnknownOID,
		Codec: pgType.Codec,
	}

	return formatGenericArray(elements, nestedType, columnName, tableName)
}

// func formatStringArray(arr []string) string {
// 	elems := make([]string, len(arr))
// 	for i, elem := range arr {
// 		if elem == "" {
// 			elems[i] = `""`
// 		} else {
// 			elems[i] = fmt.Sprintf(`"%s"`, strings.ReplaceAll(elem, `"`, `\"`))
// 		}
// 	}
// 	return fmt.Sprintf("ARRAY[%s]", strings.Join(elems, ","))
// }

// func formatIntArray(arr interface{}) string {
// 	var elems []string
// 	switch v := arr.(type) {
// 	case []int32:
// 		elems = make([]string, len(v))
// 		for i, elem := range v {
// 			elems[i] = fmt.Sprintf("%d", elem)
// 		}
// 	case []int64:
// 		elems = make([]string, len(v))
// 		for i, elem := range v {
// 			elems[i] = fmt.Sprintf("%d", elem)
// 		}
// 	}
// 	return fmt.Sprintf("ARRAY[%s]", strings.Join(elems, ","))
// }

// func formatFloatArray(arr interface{}) string {
// 	var elems []string
// 	switch v := arr.(type) {
// 	case []float32:
// 		elems = make([]string, len(v))
// 		for i, elem := range v {
// 			elems[i] = fmt.Sprintf("%g", elem)
// 		}
// 	case []float64:
// 		elems = make([]string, len(v))
// 		for i, elem := range v {
// 			elems[i] = fmt.Sprintf("%g", elem)
// 		}
// 	}
// 	return fmt.Sprintf("ARRAY[%s]", strings.Join(elems, ","))
// }

// func formatBoolArray(arr []bool) string {
// 	elems := make([]string, len(arr))
// 	for i, elem := range arr {
// 		elems[i] = fmt.Sprintf("%t", elem)
// 	}
// 	return fmt.Sprintf("ARRAY[%s]", strings.Join(elems, ","))
// }

// func formatTimestampArray(arr []time.Time) string {
// 	elems := make([]string, len(arr))
// 	for i, elem := range arr {
// 		elems[i] = fmt.Sprintf("'%s'", elem.Format("2006-01-02 15:04:05.999999-07:00"))
// 	}
// 	return fmt.Sprintf("ARRAY[%s]", strings.Join(elems, ","))
// }

// func formatNestedStringArray(arr [][]string) string {
// 	elems := make([]string, len(arr))
// 	for i, subArr := range arr {
// 		elems[i] = formatStringArray(subArr)
// 	}
// 	return fmt.Sprintf("ARRAY[%s]", strings.Join(elems, ","))
// }

// func formatNestedIntArray(arr interface{}) string {
// 	var elems []string
// 	switch v := arr.(type) {
// 	case [][]int32:
// 		elems = make([]string, len(v))
// 		for i, subArr := range v {
// 			elems[i] = formatIntArray(subArr)
// 		}
// 	case [][]int64:
// 		elems = make([]string, len(v))
// 		for i, subArr := range v {
// 			elems[i] = formatIntArray(subArr)
// 		}
// 	}
// 	return fmt.Sprintf("ARRAY[%s]", strings.Join(elems, ","))
// }

func formatGenericArray(elements []interface{}, elemType *pgtype.Type, columnName, tableName string) (string, error) {
	elems := make([]string, len(elements))
	for i, elem := range elements {
		if elem == nil {
			elems[i] = "NULL"
			continue
		}

		switch elemType.Name {
		case "text", "varchar", "char":
			str, ok := elem.(string)
			if !ok {
				return "", fmt.Errorf("expected string element in array for column %s in table %s, got %T", columnName, tableName, elem)
			}
			if str == "" {
				elems[i] = `""`
			} else {
				elems[i] = fmt.Sprintf(`"%s"`, strings.ReplaceAll(str, `"`, `\"`))
			}
		case "integer", "bigint", "smallint":
			switch num := elem.(type) {
			case int32:
				elems[i] = fmt.Sprintf("%d", num)
			case int64:
				elems[i] = fmt.Sprintf("%d", num)
			default:
				return "", fmt.Errorf("expected integer element in array for column %s in table %s, got %T", columnName, tableName, elem)
			}
		case "real", "double precision":
			switch f := elem.(type) {
			case float32:
				elems[i] = fmt.Sprintf("%g", f)
			case float64:
				elems[i] = fmt.Sprintf("%g", f)
			default:
				return "", fmt.Errorf("expected float element in array for column %s in table %s, got %T", columnName, tableName, elem)
			}
		case "boolean":
			b, ok := elem.(bool)
			if !ok {
				return "", fmt.Errorf("expected boolean element in array for column %s in table %s, got %T", columnName, tableName, elem)
			}
			elems[i] = fmt.Sprintf("%t", b)
		case "timestamp", "timestamptz", "date":
			var t time.Time
			switch v := elem.(type) {
			case time.Time:
				t = v
			case pgtype.Timestamp:
				if v.Valid {
					t = v.Time
				} else {
					elems[i] = "NULL"
					continue
				}
			default:
				return "", fmt.Errorf("expected timestamp element in array for column %s in table %s, got %T", columnName, tableName, elem)
			}
			elems[i] = fmt.Sprintf("'%s'", t.Format("2006-01-02 15:04:05.999999-07:00"))
		case "json", "jsonb":
			var jsonData []byte
			switch v := elem.(type) {
			case []byte:
				jsonData = v
			case string:
				jsonData = []byte(v)
			default:
				return "", fmt.Errorf("expected JSON element in array for column %s in table %s, got %T", columnName, tableName, elem)
			}
			if len(jsonData) == 0 {
				elems[i] = "NULL"
			} else {
				elems[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(string(jsonData), "'", "''"))
			}
		case "array":
			val := reflect.ValueOf(elem)
			if val.Kind() != reflect.Slice && val.Kind() != reflect.Array {
				return "", fmt.Errorf("expected nested array element in array for column %s in table %s, got %T", columnName, tableName, elem)
			}
			nestedElements := make([]interface{}, val.Len())
			for j := 0; j < val.Len(); j++ {
				nestedElements[j] = val.Index(j).Interface()
			}
			// Use a new pgtype.Type for the nested array's element type
			nestedType := &pgtype.Type{
				Name:  elemType.Name, // Fallback to same type
				OID:   pgtype.UnknownOID,
				Codec: elemType.Codec,
			}
			if val.Len() > 0 && nestedElements[0] != nil {
				// Infer the nested type from the first non-nil element
				switch v := nestedElements[0].(type) {
				case string:
					// Distinguish between text and json based on parent type or context
					if strings.Contains(elemType.Name, "json") {
						nestedType.Name = "json"
					} else {
						nestedType.Name = "text"
					}
				case int32, int64:
					nestedType.Name = "integer"
				case float32, float64:
					nestedType.Name = "double precision"
				case bool:
					nestedType.Name = "boolean"
				case time.Time:
					nestedType.Name = "timestamp"
				case []byte:
					nestedType.Name = "json" // JSON often comes as []byte
				default:
					// Fallback to reflection-based type inference
					nestedType.Name = reflect.TypeOf(v).String()
				}
			}
			nested, err := formatGenericArray(nestedElements, nestedType, columnName, tableName)
			if err != nil {
				return "", err
			}
			elems[i] = nested
		default:
			// Fallback for unknown types
			elems[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(fmt.Sprintf("%v", elem), "'", "''"))
		}
	}
	return fmt.Sprintf("ARRAY[%s]", strings.Join(elems, ",")), nil
}

func restoreDatabase(config Config) error {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, getConnString(config))
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}
	defer conn.Close(ctx)

	// Find the latest backup file
	backupFile, err := findLatestBackup(config.backupDir, config.dbname)
	if err != nil {
		return fmt.Errorf("failed to find backup file: %v", err)
	}

	// Read backup file
	sql, err := os.ReadFile(backupFile)
	if err != nil {
		return fmt.Errorf("failed to read backup file: %v", err)
	}

	// If a specific schema is targeted, filter SQL commands for that schema
	var sqlToExecute string
	if config.schema != "" {
		lines := strings.Split(string(sql), "\n")
		var filteredLines []string
		for _, line := range lines {
			// Include schema creation and commands for the target schema
			if strings.Contains(line, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", config.schema)) ||
				strings.Contains(line, fmt.Sprintf("%s.", config.schema)) ||
				!strings.Contains(line, ".") { // Include non-schema-specific commands
				filteredLines = append(filteredLines, line)
			}
		}
		sqlToExecute = strings.Join(filteredLines, "\n")
	} else {
		sqlToExecute = string(sql)
	}

	// Execute SQL commands
	_, err = conn.Exec(ctx, sqlToExecute)
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			return fmt.Errorf("failed to execute SQL: %v (code: %s, detail: %s)",
				err, pgErr.Code, pgErr.Detail)
		}
		return fmt.Errorf("failed to execute SQL: %v", err)
	}

	return nil
}

func findLatestBackup(backupDir, dbname string) (string, error) {
	files, err := os.ReadDir(backupDir)
	if err != nil {
		return "", fmt.Errorf("failed to read backup directory: %v", err)
	}

	var latestFile string
	var latestTime time.Time

	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), dbname) && strings.HasSuffix(file.Name(), ".sql") {
			// Extract timestamp from filename
			parts := strings.Split(file.Name(), "_")
			if len(parts) < 2 {
				continue
			}
			t, err := time.Parse("20060102_150405", parts[1][:len(parts[1])-4])
			if err != nil {
				continue
			}

			if latestFile == "" || t.After(latestTime) {
				latestFile = filepath.Join(backupDir, file.Name())
				latestTime = t
			}
		}
	}

	if latestFile == "" {
		return "", fmt.Errorf("no backup files found for database %s", dbname)
	}

	return latestFile, nil
}

func getArrayElementType(ctx context.Context, conn *pgx.Conn, schema, tableName, columnName string) (string, error) {
	var dataType string
	err := conn.QueryRow(ctx, `
        SELECT udt_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
    `, schema, tableName, columnName).Scan(&dataType)
	if err != nil {
		return "", fmt.Errorf("failed to get type for column %s in table %s: %v", columnName, tableName, err)
	}
	// Strip "_array" suffix and map to pgtype.Type.Name
	if strings.HasSuffix(dataType, "_array") {
		dataType = strings.TrimSuffix(dataType, "_array")
	}
	switch dataType {
	case "text", "varchar", "char":
		return "text", nil
	case "integer", "bigint", "smallint":
		return "integer", nil
	case "real", "double precision":
		return "double precision", nil
	case "boolean":
		return "boolean", nil
	case "timestamp", "timestamptz", "date":
		return "timestamp", nil
	case "json", "jsonb":
		return "json", nil
	default:
		return dataType, nil
	}
}

func generateTableDDL(ctx context.Context, conn *pgx.Conn, schema, tableName string) (string, error) {
	qualifiedTableName := fmt.Sprintf("%s.%s", schema, tableName)
	log.Printf("Generating DDL for table %s", qualifiedTableName)

	// Get columns and primary key information
	rows, err := conn.Query(ctx, `
        SELECT 
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.is_nullable,
            c.column_default,
            CASE 
                WHEN pc.contype = 'p' THEN pg_catalog.pg_get_constraintdef(pc.oid, true)
                ELSE NULL 
            END AS primary_key
        FROM information_schema.columns c
        JOIN pg_catalog.pg_class cl ON cl.relname = c.table_name
        JOIN pg_catalog.pg_namespace n ON n.oid = cl.relnamespace AND n.nspname = c.table_schema
        LEFT JOIN pg_catalog.pg_constraint pc ON pc.conrelid = cl.oid 
            AND pc.contype = 'p' 
            AND pc.conkey @> ARRAY[c.ordinal_position::smallint]
        WHERE c.table_schema = $1 AND c.table_name = $2
        ORDER BY c.ordinal_position
    `, schema, tableName)
	log.Printf("Executing column query for %s", qualifiedTableName)
	if err != nil {
		log.Printf("Failed to query columns for %s: %v", qualifiedTableName, err)
		return "", fmt.Errorf("failed to query columns for %s: %v", qualifiedTableName, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var colName, dataType string
		var charMaxLength pgtype.Int4
		var isNullable, columnDefault, primaryKey pgtype.Text
		if err := rows.Scan(&colName, &dataType, &charMaxLength, &isNullable, &columnDefault, &primaryKey); err != nil {
			log.Printf("Failed to scan column info for %s: %v", qualifiedTableName, err)
			return "", fmt.Errorf("failed to scan column info for %s: %v", qualifiedTableName, err)
		}
		log.Printf("Column %s: type=%s, nullable=%s, default=%v, primaryKey=%v", colName, dataType, isNullable.String, columnDefault.String, primaryKey.String)

		// Format column definition
		colDef := fmt.Sprintf("    %s %s", colName, dataType)
		if charMaxLength.Valid {
			colDef += fmt.Sprintf("(%d)", charMaxLength.Int32)
		}
		if columnDefault.Valid {
			colDef += fmt.Sprintf(" DEFAULT %s", columnDefault.String)
		}
		if isNullable.String == "NO" {
			colDef += " NOT NULL"
		}
		if primaryKey.Valid {
			colDef += " PRIMARY KEY"
		}
		columns = append(columns, colDef)
	}
	if err := rows.Err(); err != nil {
		log.Printf("Column query error for %s: %v", qualifiedTableName, err)
		return "", fmt.Errorf("column query error for %s: %v", qualifiedTableName, err)
	}

	if len(columns) == 0 {
		return "", fmt.Errorf("no columns found for %s", qualifiedTableName)
	}

	// Add other constraints (foreign keys, unique)
	rows, err = conn.Query(ctx, `
        SELECT pg_catalog.pg_get_constraintdef(r.oid, true)
        FROM pg_catalog.pg_constraint r
        JOIN pg_catalog.pg_class c ON c.oid = r.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2 AND r.contype IN ('f', 'u')
    `, schema, tableName)
	if err != nil {
		log.Printf("Failed to query additional constraints for %s: %v", qualifiedTableName, err)
		return "", fmt.Errorf("failed to query additional constraints for %s: %v", qualifiedTableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var constraintDef string
		if err := rows.Scan(&constraintDef); err != nil {
			log.Printf("Failed to scan additional constraint for %s: %v", qualifiedTableName, err)
			return "", fmt.Errorf("failed to scan additional constraint for %s: %v", qualifiedTableName, err)
		}
		columns = append(columns, fmt.Sprintf("    %s", constraintDef))
	}

	// Combine columns and close the statement
	ddl := fmt.Sprintf("CREATE TABLE %s (\n", qualifiedTableName)
	ddl += strings.Join(columns, ",\n")
	ddl += "\n);"

	// Add indexes
	rows, err = conn.Query(ctx, `
        SELECT pg_catalog.pg_get_indexdef(i.indexrelid, 0, true)
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2 AND NOT i.indisprimary
    `, schema, tableName)
	if err != nil {
		log.Printf("Failed to query indexes for %s: %v", qualifiedTableName, err)
		return "", fmt.Errorf("failed to query indexes for %s: %v", qualifiedTableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var indexDef string
		if err := rows.Scan(&indexDef); err != nil {
			log.Printf("Failed to scan index for %s: %v", qualifiedTableName, err)
			return "", fmt.Errorf("failed to scan index for %s: %v", qualifiedTableName, err)
		}
		ddl += fmt.Sprintf("\n%s;", indexDef)
	}

	return ddl, nil
}
