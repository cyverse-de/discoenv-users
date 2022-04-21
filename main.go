package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"time"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/go-mod/logging"
	"github.com/cyverse-de/go-mod/protobufjson"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/cyverse-de/p/go/user"
	"github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var log = logging.Log.WithFields(logrus.Fields{"service": "discoenv-users-service"})

type lookupUser struct {
	Username        string `db:"username"`
	UserID          string `db:"id"`
	LoginCount      uint32 `db:"login_count"`
	Preferences     string `db:"preferences"`
	PreferencesID   string `db:"preferences_id"`
	Logins          []lookupLogin
	SavedSearches   string `db:"saved_searches"`
	SavedSearchesID string `db:"saved_searches_id"`
}

type lookupLogin struct {
	IPAddress  sql.NullString `db:"ip_address"`
	UserAgent  sql.NullString `db:"user_agent"`
	LoginTime  sql.NullTime   `db:"login_time"`
	LogoutTime sql.NullTime   `db:"logout_time"`
}

func lookupLogins(dbconn *sqlx.DB, userID string, limit, offset uint) ([]lookupLogin, error) {
	var err error

	usersT := goqu.T("users")
	loginsT := goqu.T("logins")
	loginsQ := goqu.From(loginsT).
		Join(usersT, goqu.On(loginsT.Col("user_id").Eq(usersT.Col("id")))).
		Where(usersT.Col("id").Eq(userID)).
		Select(
			loginsT.Col("ip_address"),
			loginsT.Col("user_agent"),
			loginsT.Col("login_time"),
			loginsT.Col("logout_time"),
		).
		Order(loginsT.Col("login_time").Desc()).
		Limit(limit).
		Offset(offset)

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

func loginCount(dbconn *sqlx.DB, userID string) (uint, error) {
	var err error

	usersT := goqu.T("users")
	loginsT := goqu.T("logins")
	countQ := goqu.From(loginsT).
		Join(usersT, goqu.On(loginsT.Col("user_id").Eq(usersT.Col("id")))).
		Where(usersT.Col("id").Eq(userID)).
		Select(goqu.COUNT("*"))
	q, _, err := countQ.ToSQL()
	if err != nil {
		return 0, err
	}

	var count uint
	if err = dbconn.QueryRowxContext(context.Background(), q).Scan(&count); err != nil {
		return count, err
	}
	return count, nil
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

	if _, err = conn.QueueSubscribe(*natsSubject, *natsQueue, func(subject, reply string, request *user.UserLookupRequest) {
		var svcerr svcerror.Error
		var err error

		usersT := goqu.T("users")
		jobsT := goqu.T("jobs")
		savedSearchesT := goqu.T("user_saved_searches")
		prefsT := goqu.T("user_preferences")

		q := goqu.From(usersT)

		log.Infof("%+v\n", request)

		if request.IncludeSavedSearches {
			q = q.Join(savedSearchesT, goqu.On(usersT.Col("id").Eq(savedSearchesT.Col("user_id"))))
		}

		if request.IncludePreferences {
			q = q.Join(prefsT, goqu.On(usersT.Col("id").Eq(prefsT.Col("user_id"))))
		}

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
			log.Error(&svcerr)
			if err = conn.Publish(reply, &svcerr); err != nil {
				log.Error(err)
			}
			return
		}

		selectFields := []interface{}{
			usersT.Col("id"),
			usersT.Col("username"),
		}

		if request.IncludeSavedSearches {
			selectFields = append(
				selectFields,
				savedSearchesT.Col("id").As("saved_searches_id"),
				savedSearchesT.Col("saved_searches").As("saved_searches"),
			)
		}

		if request.IncludePreferences {
			selectFields = append(
				selectFields,
				prefsT.Col("id").As("preferences_id"),
				prefsT.Col("preferences").As("preferences"),
			)
		}

		q = q.Select(selectFields...)

		queryString, _, err := q.ToSQL()
		if err != nil {
			svcerr = svcerror.Error{
				ErrorCode: svcerror.Code_INTERNAL,
				Message:   err.Error(),
			}
			if err = conn.Publish(reply, &svcerr); err != nil {
				log.Error(err)
			}
			return
		}

		log.Infof("%s\n", queryString)

		u := lookupUser{}

		// argument handling is done by the goqu library above.
		if err = dbconn.QueryRowxContext(context.Background(), queryString).StructScan(&u); err != nil {
			svcerr = svcerror.Error{
				ErrorCode: svcerror.Code_INTERNAL,
				Message:   err.Error(),
			}
			log.Error(&svcerr)
			if err = conn.Publish(reply, &svcerr); err != nil {
				log.Error(err)
			}
			return
		}

		responseUser := user.User{
			Uuid:     u.UserID,
			Username: u.Username,
			Preferences: &user.User_Preferences{
				Uuid:        u.PreferencesID,
				Preferences: u.Preferences,
			},
			SavedSearches: &user.User_SavedSearches{
				Uuid:          u.SavedSearchesID,
				SavedSearches: u.SavedSearches,
			},
		}

		if request.IncludeLogins {
			logins, err := lookupLogins(
				dbconn,
				u.UserID,
				uint(request.LoginLimit),
				uint(request.LoginOffset),
			)
			if err != nil {
				svcerr = svcerror.Error{
					ErrorCode: svcerror.Code_INTERNAL,
					Message:   err.Error(),
				}
				log.Error(&svcerr)
				if err = conn.Publish(reply, &svcerr); err != nil {
					log.Error(err)
				}
				return
			}

			responseUser.Logins = []*user.User_Login{}

			for _, login := range logins {
				ul := user.User_Login{}
				if login.IPAddress.Valid {
					ul.IpAddress = login.IPAddress.String
				}
				if login.UserAgent.Valid {
					ul.UserAgent = login.UserAgent.String
				}
				if login.LoginTime.Valid {
					ul.LoginTime = timestamppb.New(login.LoginTime.Time)
				}
				if login.LogoutTime.Valid {
					ul.LogoutTime = timestamppb.New(login.LogoutTime.Time)
				}
				responseUser.Logins = append(responseUser.Logins, &ul)
			}

			loginCount, err := loginCount(dbconn, u.UserID)
			if err != nil {
				svcerr = svcerror.Error{
					ErrorCode: svcerror.Code_INTERNAL,
					Message:   err.Error(),
				}
				log.Error(&svcerr)
				if err = conn.Publish(reply, &svcerr); err != nil {
					log.Error(err)
				}
				return
			}

			responseUser.LoginCount = uint32(loginCount)
		}

		if err = conn.Publish(reply, &responseUser); err != nil {
			log.Error(err)
		}

	}); err != nil {
		log.Fatal(err)
	}

	select {}
}
