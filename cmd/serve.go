package cmd

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/getsentry/sentry-go"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/litsea/aws-log-bridge/internal"
)

var ServeCmd = &cobra.Command{
	Use:   "serve",
	Short: "serve",
	Run: func(cmd *cobra.Command, args []string) {
		confData, err := os.ReadFile("config.yml")
		if err != nil {
			log.Fatalf("Failed to read config: %v", err)
		}
		var conf internal.Config
		if err := yaml.Unmarshal(confData, &conf); err != nil {
			log.Fatalf("Failed to parse config: %v", err)
		}

		if err := sentry.Init(sentry.ClientOptions{
			Dsn:         conf.SentryDSN,
			Environment: conf.Env,
		}); err != nil {
			log.Fatalf("Sentry init failed: %v", err)
		}
		defer sentry.Flush(2 * time.Second)

		ctx := context.TODO()
		awsCfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			log.Fatalf("AWS SDK init failed: %v", err)
		}
		cwClient := cloudwatchlogs.NewFromConfig(awsCfg)

		for _, gc := range conf.LogGroups {
			go internal.StartLogWatcher(ctx, cwClient, gc, conf)
		}

		log.Println("LogBridge started.")
		select {}
	},
}

func init() {
	RootCmd.AddCommand(ServeCmd)
}
