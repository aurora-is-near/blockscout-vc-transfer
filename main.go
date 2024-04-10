package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

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
	Use:   "blockchain-vc-transfer",
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
