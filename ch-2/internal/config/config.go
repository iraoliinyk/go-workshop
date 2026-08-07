package config

import (
	"log"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Config struct {
	Port      int    `env:"CONSUMER_PORT"       envDefault:"7001"`
	URL       string `env:"WIKI_URL"        envDefault:"https://stream.wikimedia.org/v2/stream/recentchange"`
	UserAgent string `env:"WIKI_USER_AGENT" envDefault:"wiki-stream-consumer/1.0 (https://github.com/you/wiki-stream)"`
	Accept    string `env:"WIKI_ACCEPT"     envDefault:"application/json"`
}

// Load builds the Config from the environment.
//
// It first loads the .env file in the current working directory (via godotenv)
// into the process environment, then parses that environment into Config (via
// caarlos0/env), applying each field's envDefault when the variable is unset.
// Variables already set in the real environment take precedence over .env, and
// a missing .env is not an error — real environment variables and defaults win.
// Call this exactly once, at startup.
//
// caarlos0/env and godotenv are used instead of github.com/kelseyhightower/envconfig
// because they are actively maintained and rely on explicit struct tags.
func Load() (Config, error) {
	if err := godotenv.Load(); err != nil {
		log.Printf("config: no .env loaded (%v); using environment and defaults", err)
	}
	return env.ParseAs[Config]()
}
