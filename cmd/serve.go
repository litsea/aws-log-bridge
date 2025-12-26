package cmd

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/getsentry/sentry-go"
	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v3"

	"github.com/litsea/aws-log-bridge/internal"
)

var ServeCmd = &cobra.Command{
	Use:   "serve",
	Short: "serve",
	Run: func(cmd *cobra.Command, args []string) {
		log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

		confData, err := os.ReadFile("config.yml")
		if err != nil {
			log.Fatalf("Failed to read config: %v", err)
		}
		var conf internal.Config
		if err := yaml.Unmarshal(confData, &conf); err != nil {
			log.Fatalf("Failed to parse config: %v", err)
		}

		if len(conf.LogGroups) == 0 {
			log.Fatalf("No log groups found in config")
		}

		if err := sentry.Init(sentry.ClientOptions{
			Dsn:         conf.SentryDSN,
			Environment: conf.Env,
		}); err != nil {
			log.Fatalf("Sentry init failed: %v", err)
		}
		defer sentry.Flush(2 * time.Second)

		ctx, cancel := context.WithCancel(context.Background())

		awsCfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			log.Fatalf("AWS SDK init failed: %v", err)
		}
		cwClient := cloudwatchlogs.NewFromConfig(awsCfg)

		cm := internal.NewCheckpointManager()

		for _, gc := range conf.LogGroups {
			go internal.StartLogWatcher(ctx, cm, cwClient, gc, conf)
		}

		log.Println("LogBridge started.")

		waitForShutdown(cancel)

		log.Println("Waiting 5s for final cleanups...")
		time.Sleep(5 * time.Second)
		log.Println("LogBridge stopped.")
	},
}

func waitForShutdown(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	log.Printf("Signal %v received.", sig)

	cancel() // Trigger ctx.Done() in all goroutines
}

func init() {
	RootCmd.AddCommand(ServeCmd)
}
