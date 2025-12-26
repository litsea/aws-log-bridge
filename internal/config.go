package internal

import (
	"time"
)

const (
	LogTypeFlink = "flink"
)

type Config struct {
	Env            string        `yaml:"env"`
	PollInterval   time.Duration `yaml:"poll_interval"`
	SentryDSN      string        `yaml:"sentry_dsn"`
	VectorEndpoint string        `yaml:"vector_endpoint"`
	VectorAuth     VectorAuth    `yaml:"vector_auth"`
	LogGroups      []GroupConfig `yaml:"log_groups"`
}

type VectorAuth struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type GroupConfig struct {
	Name          string `yaml:"name"`
	Type          string `yaml:"type"`
	Service       string `yaml:"service"`
	SentryEnabled bool   `yaml:"sentry_enabled"`
	VectorEnabled bool   `yaml:"vector_enabled"`
}
