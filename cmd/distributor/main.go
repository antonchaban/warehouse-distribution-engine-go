package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/antonchaban/warehouse-distribution-engine-go/config"
	"github.com/antonchaban/warehouse-distribution-engine-go/internal/adapter/grpc"
	"github.com/antonchaban/warehouse-distribution-engine-go/internal/adapter/postgres"
	"github.com/antonchaban/warehouse-distribution-engine-go/internal/adapter/rabbitmq"
	"github.com/antonchaban/warehouse-distribution-engine-go/internal/algorithm"
	"github.com/antonchaban/warehouse-distribution-engine-go/internal/app"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	// 1. Initialize Logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("initializing distribution engine...")

	// 2. Load Configuration (Updated)
	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	// 3. Setup Context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 4. Infrastructure: Database
	dbPool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer dbPool.Close()
	logger.Info("connected to database")

	// 5. Infrastructure: gRPC Client
	// Note: For local docker-compose testing without a real Java server,
	// this might fail if we don't mock it.
	// But for production code, we keep it strict.
	grpcClient, err := grpc.NewClient(cfg.JavaServiceAddr)
	if err != nil {
		logger.Error("failed to create grpc client", "error", err)
		// We won't Exit(1) here just to allow local testing if Java service is missing,
		// but in prod, you might want to exit.
	} else {
		defer func(grpcClient *grpc.Client) {
			err := grpcClient.Close()
			if err != nil {
				logger.Error("failed to close grpc client", "error", err)
			}
		}(grpcClient)
		logger.Info("connected to java service", "addr", cfg.JavaServiceAddr)
	}

	// 6. Assemble Layers
	repo := postgres.NewRepository(dbPool)
	algo := algorithm.NewWFDAlgorithm()

	// Ensure grpcClient is not nil (if connection failed above)
	// In a real scenario, we'd handle this better.
	var sender app.ResultSender

	if cfg.MockOutput {
		logger.Info("using MOCK sender (results will be logged, not sent via network)")
		sender = &noopSender{l: logger}
	} else {
		// Only try to connect to gRPC if NOT in mock mode
		grpcClient, err := grpc.NewClient(cfg.JavaServiceAddr)
		if err != nil {
			logger.Error("failed to create grpc client", "error", err)
			// In prod, maybe exit. In dev, fallback.
		} else {
			// defer grpcClient.Close() // Careful with defer inside if/else block scope!
			// Better to handle cleanup differently or accept connection stays open until main exit.
		}

		if grpcClient != nil {
			sender = grpcClient
		} else {
			// Fallback if connection failed
			logger.Warn("gRPC client creation failed, falling back to noopSender")
			sender = &noopSender{l: logger}
		}
	}

	distributionService := app.NewService(repo, algo, sender, logger)

	// 7. Start RabbitMQ Consumer
	consumer := rabbitmq.NewConsumer(
		cfg.RabbitMQURL,
		cfg.QueueName,
		distributionService,
		logger,
	)

	go func() {
		logger.Info("starting rabbitmq consumer", "queue", cfg.QueueName)
		if err := consumer.Start(ctx); err != nil {
			logger.Error("consumer stopped", "error", err)
			cancel()
		}
	}()

	logger.Info("service is running")

	// 8. Graceful Shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Info("shutdown signal received")
	cancel()
	time.Sleep(1 * time.Second)
	logger.Info("service stopped")
}

// Temporary mock for local testing if Java service is down
type noopSender struct {
	l *slog.Logger
}

func (n *noopSender) SendPlan(ctx context.Context, plan algorithm.DistributionPlan) error {
	n.l.Info("mock sender: plan calculated", "moves", len(plan.Moves))
	return nil
}
