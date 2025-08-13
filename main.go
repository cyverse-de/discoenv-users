package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	_ "expvar"

	"github.com/cyverse-de/go-mod/cfg"
	"github.com/cyverse-de/go-mod/logging"
	"github.com/cyverse-de/go-mod/otelutils"
	"github.com/cyverse-de/go-mod/protobufjson"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"github.com/uptrace/opentelemetry-go-extra/otelsqlx"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
)

const serviceName = "discoenv-users"

var log = logging.Log.WithFields(logrus.Fields{"service": serviceName})

func main() {
	var (
		err    error
		dbconn *sqlx.DB

		configPath    = flag.String("config", cfg.DefaultConfigPath, "The path to the config file")
		dotEnvPath    = flag.String("dotenv-path", cfg.DefaultDotEnvPath, "The path to the env file to load")
		maxDBConns    = flag.Int("max-db-conns", 10, "Sets the maximum number of open database connections")
		tlsCert       = flag.String("tlscert", "/etc/nats/tls/tls.crt", "Path to the TLS cert used for the NATS connection")
		tlsKey        = flag.String("tlskey", "/etc/nats/tls/tls.key", "Path to the TLS key used for the NATS connection")
		caCert        = flag.String("tlsca", "/etc/nats/tls/ca.crt", "Path to the TLS CA cert used for the NATS connection")
		credsPath     = flag.String("creds", "/etc/nats/creds/services.creds", "Path to the NATS user creds file used for authn with NATS")
		maxReconnects = flag.Int("max-reconnects", 10, "The number of reconnection attempts the NATS client will make if the server does not respond")
		reconnectWait = flag.Int("reconnect-wait", 1, "The number of seconds to wait between reconnection attempts")
		natsSubject   = flag.String("subject", "cyverse.discoenv.users.>", "The NATS subject to subscribe to")
		natsQueue     = flag.String("queue", "discoenv_users_service", "The NATS queue name for this instance. Joins to a queue group by default")
		varsPort      = flag.Int("vars-port", 60000, "The port to listen on for requests to /debug/vars")
		logLevel      = flag.String("log-level", "info", "One of trace, debug, info, warn, error, fatal, or panic.")
		envPrefix     = flag.String("env-prefix", cfg.DefaultEnvPrefix, "The prefix to look for when setting configuration setting in environment variables")
	)

	flag.Parse()
	logging.SetupLogging(*logLevel)

	tracerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shutdown := otelutils.TracerProviderFromEnv(tracerCtx, serviceName, func(e error) { log.Fatal(e) })
	defer shutdown()

	nats.RegisterEncoder("protojson", &protobufjson.Codec{})

	k, err := cfg.Init(&cfg.Settings{
		EnvPrefix:   *envPrefix,
		ConfigPath:  *configPath,
		DotEnvPath:  *dotEnvPath,
		StrictMerge: false,
		FileType:    cfg.YAML,
	})
	if err != nil {
		log.Fatal(err)
	}

	dbURI := k.String("db.uri")
	if dbURI == "" {
		log.Fatal("db.uri must be set in the configuration file")
	}

	log.Info("connecting to the database")
	dbconn = otelsqlx.MustConnect(
		"postgres",
		dbURI,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL),
	)
	dbconn.SetMaxOpenConns(*maxDBConns)
	log.Info("done connecting to the database")

	natsURL := k.String("nats.cluster")
	if natsURL == "" {
		log.Fatal("nats.cluster must be set in the configuration file or in the env vars as DISCOENV_NATS_CLUSTER")
	}

	log.Infof("NATS URL is %s", natsURL)
	log.Infof("NATS TLS cert file is %s", *tlsCert)
	log.Infof("NATS TLS key file is %s", *tlsKey)
	log.Infof("NATS CA cert file is %s", *caCert)
	log.Infof("NATS creds file is %s", *credsPath)
	log.Infof("NATS subject is %s", *natsSubject)
	log.Infof("NATS queue is %s", *natsQueue)

	nc, err := nats.Connect(
		natsURL,
		nats.UserCredentials(*credsPath),
		nats.RootCAs(*caCert),
		nats.ClientCert(*tlsCert, *tlsKey),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(*maxReconnects),
		nats.ReconnectWait(time.Duration(*reconnectWait)*time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				log.Errorf("disconnected from nats: %s", err.Error())
			} else {
				log.Error("disconnected from nats")
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Infof("reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Errorf("connection closed: %s", nc.LastError().Error())
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("configured servers: %s", strings.Join(nc.Servers(), " "))
	log.Infof("connected to NATS host: %s", nc.ConnectedServerName())

	conn, err := nats.NewEncodedConn(nc, "protojson")
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("set up encoded connection to NATS")

	if _, err = conn.QueueSubscribe(*natsSubject, *natsQueue, getHandler(conn, dbconn)); err != nil {
		log.Fatal(err)
	}

	log.Infof("subscribed to NATS queue")

	portStr := fmt.Sprintf(":%d", *varsPort)
	if err = http.ListenAndServe(portStr, nil); err != nil {
		log.Fatal(err)
	}
}
