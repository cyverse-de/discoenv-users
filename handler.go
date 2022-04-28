package main

import (
	"context"
	"fmt"

	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/cyverse-de/p/go/user"
	"github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

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

// addLookupSQL will add the SQL that selects the individual user
// and returns the parameter value passed in that identifies them.
func addLookupSQL(q *goqu.SelectDataset, request *user.UserLookupRequest) (*goqu.SelectDataset, string, error) {
	var (
		err   error
		param string
	)

	usersT := goqu.T("users")
	jobsT := goqu.T("jobs")

	switch x := request.LookupIds.(type) {
	case *user.UserLookupRequest_AnalysisId:
		param = request.GetAnalysisId()

		q = q.
			Join(jobsT, goqu.On(usersT.Col("id").Eq(jobsT.Col("user_id")))).
			Where(jobsT.Col("id").Eq("?"))

	case *user.UserLookupRequest_Username:
		param = request.GetUsername()
		q = q.Where(usersT.Col("username").Eq("?"))

	case *user.UserLookupRequest_UserId:
		param = request.GetUserId()
		q = q.Where(usersT.Col("id").Eq("?"))

	default:
		err = fmt.Errorf("lookup type %T not known", x)
		return nil, "", err
	}

	return q, param, nil
}

// buildQuery returns a query constructed based on the *user.UserLookupRequest passed in and
// returns 3 values, the *goqu.SelectDataset, a list of arguments for the quest, and any errors
// returned
func buildQuery(request *user.UserLookupRequest) (*goqu.SelectDataset, []interface{}, error) {
	var (
		err         error
		userIDParam string
		args        []interface{}
	)

	usersT := goqu.T("users")
	savedSearchesT := goqu.T("user_saved_searches")
	prefsT := goqu.T("user_preferences")

	q := goqu.From(usersT)

	if request.IncludeSavedSearches {
		q = q.Join(savedSearchesT, goqu.On(usersT.Col("id").Eq(savedSearchesT.Col("user_id"))))
	}

	if request.IncludePreferences {
		q = q.Join(prefsT, goqu.On(usersT.Col("id").Eq(prefsT.Col("user_id"))))
	}

	q, userIDParam, err = addLookupSQL(q, request)
	if err != nil {
		return nil, nil, err
	}

	args = append(args, userIDParam)

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

	return q, args, nil
}

// getHandler returns a nats.Handler that will look up user information
// based on the incoming UserLookupRequest and will send a response
// in the form of a user.User message on the reply subject.
func getHandler(conn *nats.EncodedConn, dbconn *sqlx.DB) nats.Handler {
	return func(subject, reply string, request *user.UserLookupRequest) {
		var (
			err  error
			args []interface{}
			q    *goqu.SelectDataset
		)

		// Set up telemetry tracking
		carrier := gotelnats.PBTextMapCarrier{
			Header: request.Header,
		}

		ctx, span := gotelnats.StartSpan(&carrier, subject, gotelnats.Process)
		defer span.End()

		log.Infof("%+v\n", request)

		// Build the query
		q, args, err = buildQuery(request)
		if err != nil {
			handleError(ctx, err, svcerror.Code_BAD_REQUEST, reply, conn)
			return
		}

		queryString, _, err := q.ToSQL()
		if err != nil {
			handleError(ctx, err, svcerror.Code_INTERNAL, reply, conn)
		}

		log.Infof("%s\n", queryString)

		// Execute the query
		u := lookupUser{}

		if err = dbconn.QueryRowxContext(ctx, queryString, args...).StructScan(&u); err != nil {
			handleError(ctx, err, svcerror.Code_INTERNAL, reply, conn)
		}

		// Build up the response data
		responseUser := &user.User{
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
			responseUser, err = addLogins(ctx, responseUser, request, dbconn)
			if err != nil {
				handleError(ctx, err, svcerror.Code_INTERNAL, reply, conn)
			}
		}

		// Publish the response
		if err = publishResponse(ctx, conn, reply, responseUser); err != nil {
			log.Error(err)
		}
	}
}

func publishResponse(ctx context.Context, conn *nats.EncodedConn, reply string, responseUser *user.User) error {
	carrier := gotelnats.PBTextMapCarrier{
		Header: responseUser.Header,
	}

	_, span := gotelnats.InjectSpan(ctx, &carrier, reply, gotelnats.Send)
	defer span.End()

	return conn.Publish(reply, &responseUser)
}

func handleError(ctx context.Context, err error, code svcerror.Code, reply string, conn *nats.EncodedConn) {
	span := trace.SpanFromContext(ctx) // or pass the span into handleError
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())

	svcerr := svcerror.Error{
		ErrorCode: code,
		Message:   err.Error(),
	}

	log.Error(&svcerr)

	carrier := gotelnats.PBTextMapCarrier{
		Header: svcerr.Header,
	}

	_, span = gotelnats.InjectSpan(ctx, &carrier, reply, gotelnats.Send)
	defer span.End()

	if err = conn.Publish(reply, &svcerr); err != nil {
		log.Error(err)
	}
}
