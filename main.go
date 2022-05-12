package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"time"

	_ "expvar"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/go-mod/logging"
	"github.com/cyverse-de/go-mod/otelutils"
	"github.com/cyverse-de/go-mod/protobufjson"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"github.com/uptrace/opentelemetry-go-extra/otelsqlx"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
)

const serviceName = "discoenv-users"

var log = logging.Log.WithFields(logrus.Fields{"service": serviceName})

func main() {
	var (
		err    error
		config *viper.Viper
		dbconn *sqlx.DB

		natsURL       = flag.String("nats", "tls://nats:4222", "NATS connection URL")
		configPath    = flag.String("config", "/etc/iplant/de/jobservices.yml", "The path to the config file")
		maxDBConns    = flag.Int("max-db-conns", 10, "Sets the maximum number of open database connections")
		tlsCert       = flag.String("tlscert", "/etc/nats/tls/tls.crt", "Path to the TLS cert used for the NATS connection")
		tlsKey        = flag.String("tlskey", "/etc/nats/tls/tls.key", "Path to the TLS key used for the NATS connection")
		caCert        = flag.String("tlsca", "/etc/nats/tls/ca.crt", "Path to the TLS CA cert used for the NATS connection")
		credsPath     = flag.String("creds", "/etc/nats/creds/service.creds", "Path to the NATS user creds file used for authn with NATS")
		maxReconnects = flag.Int("max-reconnects", 10, "The number of reconnection attempts the NATS client will make if the server does not respond")
		reconnectWait = flag.Int("reconnect-wait", 1, "The number of seconds to wait between reconnection attempts")
		natsSubject   = flag.String("subject", "cyverse.discoenv.users.>", "The NATS subject to subscribe to")
		natsQueue     = flag.String("queue", "discoenv_users_service", "The NATS queue name for this instance. Joins to a queue group by default")
		varsPort      = flag.Int("vars-port", 60000, "The port to listen on for requests to /debug/vars")
		logLevel      = flag.String("log-level", "info", "One of trace, debug, info, warn, error, fatal, or panic.")
	)

	flag.Parse()
	logging.SetupLogging(*logLevel)

	tracerCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	shutdown := otelutils.TracerProviderFromEnv(tracerCtx, serviceName, func(e error) { log.Fatal(e) })
	defer shutdown()

	nats.RegisterEncoder("protojson", &protobufjson.Codec{})

	config, err = configurate.Init(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("read configuration from %s", *configPath)

	dbURI := config.GetString("db.uri")
	if dbURI == "" {
		log.Fatal("db.uri must be set in the configuration file")
	}

	dbconn = otelsqlx.MustConnect(
		"postgres",
		dbURI,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL),
	)
	dbconn.SetMaxOpenConns(*maxDBConns)
	log.Info("done connecting to the database")

	log.Infof("NATS URL is %s", *natsURL)
	log.Infof("NATS TLS cert file is %s", *tlsCert)
	log.Infof("NATS TLS key file is %s", *tlsKey)
	log.Infof("NATS CA cert file is %s", *caCert)
	log.Infof("NATS creds file is %s", *credsPath)

	nc, err := nats.Connect(
		*natsURL,
		nats.UserCredentials(*credsPath),
		nats.RootCAs(*caCert),
		nats.ClientCert(*tlsCert, *tlsKey),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(*maxReconnects),
		nats.ReconnectWait(time.Duration(*reconnectWait)*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := nats.NewEncodedConn(nc, "protojson")
	if err != nil {
		log.Fatal(err)
	}

	if _, err = conn.QueueSubscribe(*natsSubject, *natsQueue, getHandler(conn, dbconn)); err != nil {
		log.Fatal(err)
	}

	portStr := fmt.Sprintf(":%d", *varsPort)
	if err = http.ListenAndServe(portStr, nil); err != nil {
		log.Fatal(err)
	}
}
