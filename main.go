package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/go-mod/logging"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/cyverse-de/p/go/user"
	"github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var log = logging.Log.WithFields(logrus.Fields{"service": "discoenv-users-service"})

// ProtobufJSON is a an implmentation of the NATS Encoder interface that
// can serialize/deserialize messages using protojson. Some logic is borrowed
// from the protocol buffer encoder included in NATS.
// See https://github.com/nats-io/nats.go/blob/main/encoders/protobuf/protobuf_enc.go
type ProtobufJSON struct{}

func (p *ProtobufJSON) Encode(subject string, v interface{}) ([]byte, error) {
	msg, ok := v.(proto.Message)
	if !ok {
		return nil, errors.New("invalid protocol buffer message passed to Encode()")
	}
	b, err := protojson.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (p *ProtobufJSON) Decode(subject string, data []byte, vPtr interface{}) error {
	if _, ok := vPtr.(*interface{}); ok {
		return nil
	}

	msg, ok := vPtr.(proto.Message)
	if !ok {
		return errors.New("invalid protocol buffer message passed to Decode()")
	}

	return protojson.Unmarshal(data, msg)

}

type lookupUser struct {
	Username      string `db:"username"`
	UserID        string `db:"id"`
	Preferences   *lookupPreferences
	Logins        []lookupLogin
	SavedSearches []lookupSavedSearches
}

type lookupPreferences struct {
	UUID        string `db:"id"`
	Preferences string `db:"preferences"`
}

type lookupLogin struct {
	IPAddress  string    `db:"ip_address"`
	UserAgent  string    `db:"user_agent"`
	LoginTime  time.Time `db:"login_time"`
	LogoutTime time.Time `db:"logout_time"`
}

func lookupLogins(dbconn *sqlx.DB, userID string) ([]lookupLogin, error) {
	var err error

	usersT := goqu.T("users")
	loginsT := goqu.T("logins")
	loginsQ := goqu.From(loginsT).
		Join(usersT, goqu.On(loginsT.Col("user_id").Eq(usersT.Col("id")))).
		Where(usersT.Col("id").Eq(userID))

	loginsQueryString, _, err := loginsQ.ToSQL()
	if err != nil {
		return nil, err
	}

	rows, err := dbconn.QueryxContext(context.Background(), loginsQueryString)
	if err != nil {
		return nil, err
	}

	logins := []lookupLogin{}

	for rows.Next() {
		login := lookupLogin{}
		if err = rows.StructScan(&login); err != nil {
			return nil, err
		}

		logins = append(logins, login)
	}

	return logins, nil
}

type lookupSavedSearches struct {
	UUID          string `db:"id"`
	SavedSearches string `db:"saved_searches"`
}

func main() {
	var (
		err    error
		config *viper.Viper
		dbconn *sqlx.DB

		natsURL       = flag.String("nats", "tls://nats:4222", "NATS connection URL")
		configPath    = flag.String("config", "/etc/iplant/de/jobservices.yml", "The path to the config file")
		tlsCert       = flag.String("tlscert", "/etc/nats/tls.crt", "Path to the TLS cert used for the NATS connection")
		tlsKey        = flag.String("tlskey", "/etc/nats/tls.key", "Path to the TLS key used for the NATS connection")
		caCert        = flag.String("tlsca", "/etc/nats/ca.crt", "Path to the TLS CA cert used for the NATS connection")
		credsPath     = flag.String("creds", "/etc/nats/service.creds", "Path to the NATS user creds file used for authn with NATS")
		maxReconnects = flag.Int("max-reconnects", 10, "The number of reconnection attempts the NATS client will make if the server does not respond")
		reconnectWait = flag.Int("reconnect-wait", 1, "The number of seconds to wait between reconnection attempts")
		natsSubject   = flag.String("subject", "cyverse.discoenv.users.>", "The NATS subject to subscribe to")
		natsQueue     = flag.String("queue", "discoenv_users_service", "The NATS queue name for this instance. Joins to a queue group by default")
		logLevel      = flag.String("log-level", "info", "One of trace, debug, info, warn, error, fatal, or panic.")
	)

	flag.Parse()
	logging.SetupLogging(*logLevel)

	nats.RegisterEncoder("protojson", &ProtobufJSON{})

	config, err = configurate.Init(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("read configuration from %s", *configPath)

	dbURI := config.GetString("db.uri")
	if dbURI == "" {
		log.Fatal("db.uri must be set in the configuration file")
	}

	dbconn = sqlx.MustConnect("postgres", dbURI)
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

	conn.QueueSubscribe(*natsSubject, *natsQueue, func(subject, reply string, request *user.UserLookupRequest) {
		var svcerr svcerror.Error
		var err error

		usersT := goqu.T("users")
		jobsT := goqu.T("jobs")

		q := goqu.From(usersT)

		switch x := request.LookupIds.(type) {
		case *user.UserLookupRequest_AnalysisId:
			analysisID := request.GetAnalysisId()

			q = q.
				Join(jobsT, goqu.On(usersT.Col("id").Eq(jobsT.Col("user_id")))).
				Where(jobsT.Col("id").Eq(analysisID))

		case *user.UserLookupRequest_Username:
			q = q.Where(usersT.Col("username").Eq(request.GetUsername()))

		case *user.UserLookupRequest_UserId:
			q = q.Where(usersT.Col("id").Eq(request.GetUserId()))

		default:
			svcerr = svcerror.Error{
				ErrorCode: svcerror.Code_BAD_REQUEST,
				Message:   fmt.Sprintf("lookup type %T not known", x),
			}
			log.Error(svcerr)
			conn.Publish(reply, &svcerr)
			return
		}

		q = q.Select(usersT.Col("id"), usersT.Col("username"))

		queryString, _, err := q.ToSQL()
		if err != nil {
			svcerr = svcerror.Error{
				ErrorCode: svcerror.Code_INTERNAL,
				Message:   err.Error(),
			}
			conn.Publish(reply, &svcerr)
			return
		}

		u := lookupUser{}

		// argument handling is done by the goqu library above.
		if err = dbconn.QueryRowxContext(context.Background(), queryString).StructScan(&u); err != nil {
			svcerr = svcerror.Error{
				ErrorCode: svcerror.Code_INTERNAL,
				Message:   err.Error(),
			}
			log.Error(svcerr)
			conn.Publish(reply, &svcerr)
			return
		}

		responseUser := user.User{
			Uuid:     u.UserID,
			Username: u.Username,
		}

		if request.IncludeLogins {
			logins, err := lookupLogins(dbconn, u.UserID)
			if err != nil {
				svcerr = svcerror.Error{
					ErrorCode: svcerror.Code_INTERNAL,
					Message:   err.Error(),
				}
				log.Error(svcerr)
				conn.Publish(reply, &svcerr)
				return
			}

			responseUser.Logins = []*user.User_Login{}

			for _, login := range logins {
				ul := user.User_Login{
					IpAddress:  login.IPAddress,
					UserAgent:  login.UserAgent,
					LoginTime:  timestamppb.New(login.LoginTime),
					LogoutTime: timestamppb.New(login.LogoutTime),
				}
				responseUser.Logins = append(responseUser.Logins, &ul)
			}
		}

		if err = conn.Publish(reply, &responseUser); err != nil {
			log.Error(err)
		}

	})

	select {}
}
