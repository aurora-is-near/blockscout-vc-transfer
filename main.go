package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type Config struct {
	Source      DB     `yaml:"source"`
	Destination DB     `yaml:"destination"`
	Condition   string `yaml:"condition"`
}

type DB struct {
	DB    string `yaml:"db"`
	Table string `yaml:"table"`
}

var config Config

func main() {
	cobra.OnInitialize(initConfig)
	rootCmd.AddCommand(testCmd)
	rootCmd.AddCommand(dumpCmd)
	rootCmd.AddCommand(loadCmd)
	rootCmd.AddCommand(transferCmd)
	rootCmd.AddCommand(transferNamesCmd)
	cobra.CheckErr(rootCmd.Execute())
}

func initConfig() {
	viper.AddConfigPath("config")
	viper.SetConfigName("local")
	viper.SetConfigType("yaml")
	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&config)
	if err != nil {
		return
	}
}

var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dump data from source database to a file",
	Run: func(cmd *cobra.Command, args []string) {
		sourceConn := openDB(config.Source)
		defer sourceConn.Close(context.Background())
		filename := fmt.Sprintf("dump-%s.txt", config.Source.Table)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		absPath, err := filepath.Abs(file.Name())
		if err != nil {
			fmt.Println("Error getting absolute path:", err)
			return
		}

		dump(sourceConn, config.Source.Table, absPath, config.Condition)
		fmt.Printf("Data dumped to %s\n", absPath)
	},
}

var loadCmd = &cobra.Command{
	Use:   "load",
	Short: "Load data from a file to destination database",
	Run: func(cmd *cobra.Command, args []string) {
		destConn := openDB(config.Destination)
		defer destConn.Close(context.Background())
		filename := fmt.Sprintf("dump-%s.txt", config.Destination.Table)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		absPath, err := filepath.Abs(file.Name())
		if err != nil {
			fmt.Println("Error getting absolute path:", err)
			return
		}
		load(destConn, config.Destination.Table, absPath)
		fmt.Printf("Data loaded from %s\n", absPath)
	},
}

// Define a new command called "transfer"
var testCmd = &cobra.Command{
	Use:   "test",
	Short: "Check if database connection is working",
	Run: func(cmd *cobra.Command, args []string) {
		sourceConn := openDB(config.Source)
		destConn := openDB(config.Destination)
		defer sourceConn.Close(context.Background())
		defer destConn.Close(context.Background())
	},
}

var transferCmd = &cobra.Command{
	Use:   "transfer",
	Short: "Transfer data from source database to destination database",
	Run: func(cmd *cobra.Command, args []string) {
		sourceConn := openDB(config.Source)
		destConn := openDB(config.Destination)
		fmt.Println("Database connection is working")
		defer sourceConn.Close(context.Background())
		defer destConn.Close(context.Background())

		filename := fmt.Sprintf("dump-%s.txt", config.Source.Table)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		absPath, err := filepath.Abs(file.Name())
		if err != nil {
			fmt.Println("Error getting absolute path:", err)
			return
		}

		dump(sourceConn, config.Source.Table, absPath, config.Condition)
		load(destConn, config.Destination.Table, absPath)

		fmt.Println("Data transfered successfully")
	},
}

var transferNamesCmd = &cobra.Command{
	Use:   "transfer-names",
	Short: "Transfer names from source database to destination database",
	Run: func(cmd *cobra.Command, args []string) {
		sourceConn := openDB(config.Source)
		destConn := openDB(config.Destination)
		fmt.Println("Database connection is working")
		defer sourceConn.Close(context.Background())
		defer destConn.Close(context.Background())

		rows, err := readFromSourceTable(sourceConn)
		if err != nil {
			log.Fatalf("Error reading from source table: %v", err)
		}

		for _, row := range rows {
			err = writeToTargetTable(destConn, row)
			if err != nil {
				log.Printf("Error writing to target table: %v", err)
			}
		}

		fmt.Println("Data transfered successfully")
	},
}

var rootCmd = &cobra.Command{
	Use:   "blockchain-vc-transfer",
	Short: "Tool to transfer verified contracts data between 2 instance of blockscout dbs",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help() // Trigger help text
		os.Exit(0) // Exit after displaying help
	},
}

func tableExists(conn *pgx.Conn, table string) bool {
	var exists bool
	err := conn.QueryRow(context.Background(), "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = $1)", table).Scan(&exists)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	return exists
}

func openDB(dbConfig DB) *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), dbConfig.DB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	if !tableExists(conn, dbConfig.Table) {
		fmt.Println("Table does not exist")
		os.Exit(1)
	}
	return conn
}

func dump(conn *pgx.Conn, table string, filename string, condition string) {
	if config.Condition != "" {
		table = fmt.Sprintf("%s WHERE %s", table, config.Condition)
	}
	copyToQuery := fmt.Sprintf("COPY (SELECT * FROM %s) TO '%s' WITH BINARY", table, filename)
	_, err := conn.Exec(context.Background(), copyToQuery)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
}

func load(conn *pgx.Conn, table string, filename string) {
	copyFromQuery := fmt.Sprintf("COPY %s FROM '%s' WITH BINARY", table, filename)
	_, err := conn.Exec(context.Background(), copyFromQuery)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
}

func readFromSourceTable(conn *pgx.Conn) ([]map[string]interface{}, error) {
	query := "SELECT address_hash, name, \"primary\", inserted_at, updated_at, metadata, id FROM address_names"
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("query failed: %v", err)
	}
	defer rows.Close()

	var results []map[string]interface{}
	for rows.Next() {
		var addressHash []byte
		var name string
		var primary bool
		var insertedAt, updatedAt time.Time
		var metadata map[string]interface{}
		var id int

		err = rows.Scan(&addressHash, &name, &primary, &insertedAt, &updatedAt, &metadata, &id)
		if err != nil {
			return nil, fmt.Errorf("row scan failed: %v", err)
		}

		row := map[string]interface{}{
			"address_hash": addressHash,
			"name":         name,
			"primary":      primary,
			"inserted_at":  insertedAt,
			"updated_at":   updatedAt,
			"metadata":     metadata,
			"id":           id,
		}
		results = append(results, row)
	}

	return results, nil
}

func writeToTargetTable(conn *pgx.Conn, row map[string]interface{}) error {
	// Check if the address_hash already exists in the target table
	var exists bool
	checkQuery := "SELECT EXISTS(SELECT 1 FROM address_names WHERE address_hash = $1)"
	err := conn.QueryRow(context.Background(), checkQuery, row["address_hash"]).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check query failed: %v", err)
	}

	if exists {
		return nil // If the address_hash already exists, do nothing
	}

	fmt.Printf("Inserting row with address_hash: %x\n", row["address_hash"])
	// Insert the row into the target table
	insertQuery := `INSERT INTO address_names (address_hash, name, "primary", inserted_at, updated_at, metadata, id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)`
	_, err = conn.Exec(context.Background(), insertQuery,
		row["address_hash"], row["name"], row["primary"], row["inserted_at"], row["updated_at"], row["metadata"], row["id"])
	if err != nil {
		return fmt.Errorf("insert query failed: %v", err)
	}

	return nil
}
