package config

import (
	"log"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Common struct {
	Logger  string   `env:"LOGGER"           envDefault:"DEBUG"` // DEBUG | PROD
	Brokers []string `env:"REDPANDA_BROKERS" envSeparator:"," envDefault:"127.0.0.1:9092"`
	Topic   string   `env:"REDPANDA_TOPIC" envDefault:"wiki.recentchange.proto"`
}
type Producer struct {
	Common
	Port        int    `env:"PRODUCER_PORT" envDefault:"7002"`
	URL         string `env:"WIKI_URL"        envDefault:"https://stream.wikimedia.org/v2/stream/recentchange"`
	UserAgent   string `env:"WIKI_USER_AGENT" envDefault:"wiki-stream-producer/1.0 (https://github.com/you/wiki-stream)"`
	Accept      string `env:"WIKI_ACCEPT" envDefault:"text/event-stream"`
	ContentType string `env:"CONTENT_TYPE" envDefault:"application/x-protobuf"`
	ProtoType   string `env:"PROTO_TYPE" envDefault:"wikirecent.v1.WikiEvent"`
}

type Consumer struct {
	Common
	Port               int           `env:"CONSUMER_PORT"        envDefault:"7001"`
	Group              string        `env:"REDPANDA_GROUP"       envDefault:"wiki-stats"`
	DBBackend          string        `env:"DB_BACKEND"           envDefault:"in-memory"` // cassandra | in-memory
	StatsFlushInterval time.Duration `env:"STATS_FLUSH_INTERVAL" envDefault:"10s"`

	CassandraHosts       []string      `env:"CASSANDRA_HOSTS" envSeparator:"," envDefault:"127.0.0.1"`
	CassandraKeyspace    string        `env:"CASSANDRA_KEYSPACE"    envDefault:"wikistream"`
	CassandraConsistency string        `env:"CASSANDRA_CONSISTENCY" envDefault:"QUORUM"`
	CassandraTimeout     time.Duration `env:"CASSANDRA_TIMEOUT"     envDefault:"5s"`

	JWTSecret      string        `env:"JWT_SECRET,required"`
	JWTIssuer      string        `env:"JWT_ISSUER"       envDefault:"wiki-stream-go"`
	AccessTokenTTL time.Duration `env:"ACCESS_TOKEN_TTL" envDefault:"1h"`
	BcryptCost     int           `env:"BCRYPT_COST"      envDefault:"12"`
}

func LoadProducer() (Producer, error) {
	loadDotEnv()
	return env.ParseAs[Producer]()
}

func LoadConsumer() (Consumer, error) {
	loadDotEnv()
	return env.ParseAs[Consumer]()
}

// loadDotEnv reads the .env file into the process environment, applying each
// envDefault where a variable is missing. Real environment variables win over
// .env, and a missing .env is fine: in Docker the values come from compose.
func loadDotEnv() {
	if err := godotenv.Load(); err != nil {
		log.Printf("config: no .env loaded (%v); using environment and defaults", err)
	}
}
