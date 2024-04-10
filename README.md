# Blockchain Verified Contracts Transfer Tool

This tool facilitates the transfer of verified contracts data between two separate instances of blockscout databases. It allows for the dumping of data from a source database, loading data into a destination database, and directly transferring data between databases based on specified conditions.

## Features

- **Test Database Connection:** Verify that connections to both the source and destination databases can be established.
- **Dump Data:** Export data from the source database to a local file.
- **Load Data:** Import data from a local file into the destination database.
- **Transfer Data:** Directly transfer data from the source to the destination database, leveraging local file storage as an intermediary step.

## Configuration

The tool relies on a `local.yaml` configuration file located in the `config` directory. This file should specify the source and destination database connections, along with any conditions to apply to the data selection process.

### Configuration Structure

```yaml
source:
  db: "source_database_connection_string"
  table: "source_table_name"
destination:
  db: "destination_database_connection_string"
  table: "destination_table_name"
condition: "SQL_condition"
```

## Prerequisites

- Go 1.15 or newer
- Access to source and destination PostgreSQL databases
- `local.yaml` configuration file set up in the `config` directory

## Getting Started

1. Clone the repository to your local machine.
2. Ensure you have a `local.yaml` configuration file within the `config` directory, structured according to the above specifications.
3. Build the tool using Go:

```sh
go build .
```

4. Run the tool using one of the available commands:

```sh
./blockchain-vc-transfer test
./blockchain-vc-transfer dump
./blockchain-vc-transfer load
./blockchain-vc-transfer transfer
```

## Commands

- `test`: Checks the database connections to both the source and destination.
- `dump`: Exports data from the source database based on the specified condition.
- `load`: Imports data into the destination database from a file.
- `transfer`: Transfers data directly from the source to the destination database.

## Development

This project uses the following key technologies:

- Go programming language
- [pgx](https://github.com/jackc/pgx) for PostgreSQL database connection and operations
- [Cobra](https://github.com/spf13/cobra) for command-line interface creation
- [Viper](https://github.com/spf13/viper) for configuration management

