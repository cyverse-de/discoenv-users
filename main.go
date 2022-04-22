package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"reflect"
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
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var log = logging.Log.WithFields(logrus.Fields{"service": "discoenv-users-service"})

// NATSMessageHandler is the handler type that we support. Should map to the nats.Handler
// function signature func(*nats.Msg).
type NATSMessageHandler func(context.Context, *nats.Msg)

type NATSTextMapCarrier struct {
	nats.Header
}

func (n NATSTextMapCarrier) Keys() []string {
	var keys []string
	for key := range n.Header {
		keys = append(keys, key)
	}
	return keys
}

func ProtoUnmarshal(data []byte, ptr interface{}) error {
	if _, ok := ptr.(*interface{}); ok {
		return nil
	}

	msg, ok := ptr.(proto.Message)
	if !ok {
		return errors.New("invalid protocol buffer message passed to ProtoUnmarshal")
	}

	return protojson.Unmarshal(data, msg)
}

// func ProtoMarshal(ptr interface{}) ([]byte, error) {
// 	msg, ok := ptr.(proto.Message)
// 	if !ok {
// 		return nil, errors.New("invalid protocol buffer message passed to ProtoMarshal")
// 	}
// 	b, err := protojson.Marshal(msg)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return b, nil
// }

type NATSOtelEncodedConnection struct {
	nats.EncodedConn
	OtelInstrumentationName string
	OtelSystem              string
	OtelProtocol            string
	OtelProtocolVersion     string
}

// Middleware adds trace info to the message headers. Only accepts
// nats.Handler of the form func(*nats.Msg) for now. Anything else will
// bypass the telemetry logic.
func (ec *NATSOtelEncodedConnection) Middleware(ctx context.Context, next NATSMessageHandler) NATSMessageHandler {
	return func(ctx context.Context, msg *nats.Msg) {
		headerCarrier := NATSTextMapCarrier{
			msg.Header,
		}
		msgCtx := otel.GetTextMapPropagator().Extract(ctx, headerCarrier)
		tracer := otel.GetTracerProvider().Tracer(ec.OtelInstrumentationName)
		msgCtx, span := tracer.Start(msgCtx, msg.Subject+" process", trace.WithSpanKind(trace.SpanKindConsumer))
		defer span.End()

		span.SetAttributes(
			semconv.MessagingSystemKey.String(ec.OtelSystem),
			semconv.MessagingProtocolKey.String(ec.OtelProtocol),
			semconv.MessagingProtocolVersionKey.String(ec.OtelProtocolVersion),
			semconv.MessagingDestinationKindTopic.Key.String(msg.Subject),
			semconv.MessagingOperationKey.String("process"),
		)
		next(msgCtx, msg)
	}
}

func NewNATSOtelEncodedConnection(encConn *nats.EncodedConn, instr, system, proto, protoVersion string) *NATSOtelEncodedConnection {
	return &NATSOtelEncodedConnection{
		EncodedConn:             *encConn,
		OtelInstrumentationName: instr,
		OtelSystem:              system,
		OtelProtocol:            proto,
		OtelProtocolVersion:     protoVersion,
	}
}

func (ec *NATSOtelEncodedConnection) QueueSubscribeContext(ctx context.Context, subject, queue string, handler NATSMessageHandler) (*nats.Subscription, error) {
	constructedHandler := func(msg *nats.Msg) {
		handler(ctx, msg)
	}
	return ec.QueueSubscribe(subject, queue, constructedHandler)
}

func (ec *NATSOtelEncodedConnection) setupMsg(ctx context.Context, subject string, v interface{}) (*nats.Msg, error) {
	var (
		tracer trace.Tracer
		outMsg *nats.Msg
	)

	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		tracer = span.TracerProvider().Tracer(ec.OtelInstrumentationName)
	} else {
		tracer = otel.GetTracerProvider().Tracer(ec.OtelInstrumentationName)
	}

	ctx, span := tracer.Start(ctx, subject+" send", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()

	span.SetAttributes(
		semconv.MessagingSystemKey.String(ec.OtelSystem),
		semconv.MessagingProtocolKey.String(ec.OtelProtocol),
		semconv.MessagingProtocolVersionKey.String(ec.OtelProtocolVersion),
		semconv.MessagingDestinationKindTopic.Key.String(subject),
	)

	var msgHeaders nats.Header

	otel.GetTextMapPropagator().Inject(ctx, NATSTextMapCarrier{msgHeaders})

	encodedData, err := ec.Enc.Encode(subject, v)
	if err != nil {
		return nil, err
	}

	outMsg = nats.NewMsg(subject)
	outMsg.Header = msgHeaders
	outMsg.Data = encodedData

	return outMsg, nil
}

func (ec *NATSOtelEncodedConnection) PublishContext(ctx context.Context, subject string, v interface{}) error {
	outMsg, err := ec.setupMsg(ctx, subject, v)
	if err != nil {
		return err
	}

	// Use Conn here instead of ec.EncodedConn since we already encoded the message.
	if err = ec.Conn.PublishMsg(outMsg); err != nil {
		return err
	}

	return nil
}

func (ec *NATSOtelEncodedConnection) PublishRequestContext(ctx context.Context, subject, reply string, v interface{}) error {
	outMsg, err := ec.setupMsg(ctx, subject, v)
	if err != nil {
		return err
	}

	outMsg.Reply = reply

	// Use Conn here instead of ec.EncodedConn since we already encoded the message.
	if err = ec.Conn.PublishMsg(outMsg); err != nil {
		return err
	}
	return nil
}

// Parts of this are taken from the NATS source.
var emptyMsgType = reflect.TypeOf(&nats.Msg{})

func (ec *NATSOtelEncodedConnection) RequestContext(ctx context.Context, subject string, v, vPtr interface{}, timeout time.Duration) error {
	outMsg, err := ec.setupMsg(ctx, subject, v)
	if err != nil {
		return err
	}
	inMsg, err := ec.Conn.RequestMsg(outMsg, timeout)
	if err != nil {
		return err
	}
	if reflect.TypeOf(vPtr) == emptyMsgType {
		// If the pointer is to an empty message,
		// return the incoming message without decoding it.
		mPtr := vPtr.(*nats.Msg)
		*mPtr = *inMsg
	} else {
		err = ec.Enc.Decode(inMsg.Subject, inMsg.Data, vPtr)
	}
	return err
}

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

	otconn := NewNATSOtelEncodedConnection(conn, "github.com/cyverse-de/discoenv-users", "NATS", "NATS", "2.8")

	ctx := context.Background()

	handler := otconn.Middleware(ctx, func(ctx context.Context, msg *nats.Msg) {
		var (
			err     error
			request *user.UserLookupRequest
			svcerr  svcerror.Error
		)
		if err = ProtoUnmarshal(msg.Data, request); err != nil {
			log.Error(err)
		}

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
			if err = conn.Publish(msg.Reply, &svcerr); err != nil {
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
			if err = conn.Publish(msg.Reply, &svcerr); err != nil {
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
			if err = conn.Publish(msg.Reply, &svcerr); err != nil {
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
				if err = conn.Publish(msg.Reply, &svcerr); err != nil {
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
				if err = conn.Publish(msg.Reply, &svcerr); err != nil {
					log.Error(err)
				}
				return
			}

			responseUser.LoginCount = uint32(loginCount)
		}

		if err = conn.Publish(msg.Reply, &responseUser); err != nil {
			log.Error(err)
		}
	})

	if _, err = otconn.QueueSubscribeContext(ctx, *natsSubject, *natsQueue, handler); err != nil {
		log.Fatal(err)
	}

	select {}
}
