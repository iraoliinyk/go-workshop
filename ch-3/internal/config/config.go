package config

import (
	"log"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	Port               int           `env:"CONSUMER_PORT"       envDefault:"7001"`
	URL                string        `env:"WIKI_URL"        envDefault:"https://stream.wikimedia.org/v2/stream/recentchange"`
	UserAgent          string        `env:"WIKI_USER_AGENT" envDefault:"wiki-stream-consumer/1.0 (https://github.com/you/wiki-stream)"`
	Accept             string        `env:"WIKI_ACCEPT"     envDefault:"application/json"`
	DBBackend          string        `env:"DB_BACKEND"     envDefault:"in-memory"` // cassandra | in-memory
	StatsFlushInterval time.Duration `env:"STATS_FLUSH_INTERVAL"     envDefault:"10s"`
	Logger             string        `env:"LOGGER" envDefault:"DEBUG"` // DEBUG | PROD

	CassandraHosts       []string      `env:"CASSANDRA_HOSTS" envSeparator:"," envDefault:"127.0.0.1"`
	CassandraKeyspace    string        `env:"CASSANDRA_KEYSPACE" envDefault:"wikistream"`
	CassandraConsistency string        `env:"CASSANDRA_CONSISTENCY" envDefault:"QUORUM"`
	CassandraTimeout     time.Duration `env:"CASSANDRA_TIMEOUT" envDefault:"5s"`

	JWTSecret      string        `env:"JWT_SECRET,required"`
	JWTIssuer      string        `env:"JWT_ISSUER"       envDefault:"wiki-stream-go"`
	AccessTokenTTL time.Duration `env:"ACCESS_TOKEN_TTL" envDefault:"1h"`
	BcryptCost     int           `env:"BCRYPT_COST"      envDefault:"12"`
}

// Load reads the .env file into the process environment and then parses that
// environment into a Config, applying each envDefault where a variable is missing.
// Real environment variables win over .env, a missing .env is fine, and this should
// be called once at startup.
func Load() (Config, error) {
	if err := godotenv.Load(); err != nil {
		log.Printf("config: no .env loaded (%v); using environment and defaults", err)
	}
	return env.ParseAs[Config]()
}
