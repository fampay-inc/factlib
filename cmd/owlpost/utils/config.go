package utils

import (
	"fmt"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	Name        string `env:"APP_NAME" envDefault:"owlpost"`
	SrcSvc      string `env:"SRC_SERVICE_NAME" envDefault:"westeros"`
	WalPrefix   string `env:"WAL_PREFIX" envDefault:"westeros"`
	MasterDbURL string `env:"MASTER_DATABASE_URL" envDefault:"postgres://postgres:postgres@localhost:5432/westeros"`

	KafkaBrokers  []string `env:"KAFKA_BROKERS" envDefault:"localhost:9093"`
	KafkaUsername string   `env:"KAFKA_USERNAME" envDefault:""`
	KafkaPassword string   `env:"KAFKA_PASSWORD" envDefault:""`
	KafkaSSL      bool     `env:"KAFKA_SSL" envDefault:"false"`

	WALHealthPort       int    `env:"WAL_HEALTH_PORT" envDefault:"3001"`
	ReplicationSlotName string `env:"REPLICATION_SLOT_NAME" envDefault:"factlib_slot"`
	PublicationName     string `env:"PUBLICATION_NAME" envDefault:"facts"`
}

var appConfig *Config

// GetConfig creates a new Config struct.
func GetConfig() *Config {
	if appConfig != nil {
		return appConfig
	} else {
		err := godotenv.Load(".env")
		if err != nil {
			fmt.Println("Unable to load .env file. Continuing without loading it...")
		}
		appConfig = &Config{}
		if err = env.Parse(appConfig); err != nil {
			panic(err)
		}
		return appConfig
	}
}
