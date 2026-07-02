package main

import (
	"context"
	"database/sql"
	"errors"

	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/p/go/ptypes"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/cyverse-de/p/go/user"
	"github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
	"github.com/nats-io/nats.go"
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

type lookupLogin struct {
	IPAddress  sql.NullString `db:"ip_address"`
	UserAgent  sql.NullString `db:"user_agent"`
	LoginTime  sql.NullTime   `db:"login_time"`
	LogoutTime sql.NullTime   `db:"logout_time"`
}

func lookupLogins(ctx context.Context, dbconn *sqlx.DB, userID string, limit, offset uint) ([]lookupLogin, error) {
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

	rows, err := dbconn.QueryxContext(ctx, loginsQueryString)
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

func loginCount(ctx context.Context, dbconn *sqlx.DB, userID string) (uint, error) {
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
	if err = dbconn.QueryRowxContext(ctx, q).Scan(&count); err != nil {
		return count, err
	}
	return count, nil
}

//nolint:staticcheck // EncodedConn retirement is a planned follow-up to the protobuf removal
func getHandler(conn *nats.EncodedConn, dbconn *sqlx.DB) nats.Handler {
	return func(subject, reply string, request *user.UserLookupRequest) {
		var err error

		log.Debugf("request received: %+v\n", request)

		// Set this up early so that potential errors can be returned easily.
		responseUser := &user.LookupResponse{
			Header: gotelnats.NewHeader(),
		}

		if request.Header == nil {
			request.Header = gotelnats.NewHeader()
		}

		carrier := gotelnats.PBTextMapCarrier{
			Header: request.Header,
		}

		ctx, span := gotelnats.StartSpan(&carrier, subject, gotelnats.Process)
		defer span.End()

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

		// The old protojson decoder rejected requests setting more than one
		// oneof member; re-establish that exclusivity for the flattened fields.
		idsSet := 0
		for _, id := range []*string{request.AnalysisId, request.Username, request.UserId} {
			if id != nil {
				idsSet++
			}
		}
		if idsSet > 1 {
			lookupErr := errors.New("only one of analysisId, username, and userId may be set")
			responseUser.Error = gotelnats.InitServiceError(ctx, lookupErr, &gotelnats.ErrorOptions{
				ErrorCode: svcerror.ErrorCode_BAD_REQUEST,
			})
			if err = gotelnats.PublishResponse(ctx, conn, reply, responseUser); err != nil {
				log.Error(err)
			}
			return
		}

		switch {
		case request.AnalysisId != nil:
			q = q.
				Join(jobsT, goqu.On(usersT.Col("id").Eq(jobsT.Col("user_id")))).
				Where(jobsT.Col("id").Eq(request.GetAnalysisId()))

		case request.Username != nil:
			q = q.Where(usersT.Col("username").Eq(request.GetUsername()))

		case request.UserId != nil:
			q = q.Where(usersT.Col("id").Eq(request.GetUserId()))

		default:
			lookupErr := errors.New("no lookup id was set in the request")
			responseUser.Error = gotelnats.InitServiceError(ctx, lookupErr, &gotelnats.ErrorOptions{
				ErrorCode: svcerror.ErrorCode_BAD_REQUEST,
			})
			if err = gotelnats.PublishResponse(ctx, conn, reply, responseUser); err != nil {
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
			responseUser.Error = gotelnats.InitServiceError(ctx, err, &gotelnats.ErrorOptions{
				ErrorCode: svcerror.ErrorCode_INTERNAL,
			})
			if err = gotelnats.PublishResponse(ctx, conn, reply, responseUser); err != nil {
				log.Error(err)
			}
			return
		}

		log.Debugf("%s\n", queryString)

		u := lookupUser{}

		if err = dbconn.QueryRowxContext(ctx, queryString).StructScan(&u); err != nil {
			errCode := svcerror.ErrorCode_INTERNAL

			if err == sql.ErrNoRows {
				errCode = svcerror.ErrorCode_NOT_FOUND
			}

			responseUser.Error = gotelnats.InitServiceError(ctx, err, &gotelnats.ErrorOptions{
				ErrorCode: errCode,
			})

			if err = gotelnats.PublishResponse(ctx, conn, reply, responseUser); err != nil {
				log.Error(err)
			}

			return
		}

		responseUser.Uuid = u.UserID
		responseUser.Username = u.Username
		responseUser.Preferences = &user.Preferences{
			Uuid:        u.PreferencesID,
			Preferences: u.Preferences,
		}
		responseUser.SavedSearches = &user.SavedSearches{
			Uuid:          u.SavedSearchesID,
			SavedSearches: u.SavedSearches,
		}

		if request.IncludeLogins {
			logins, err := lookupLogins(
				ctx,
				dbconn,
				u.UserID,
				uint(request.LoginLimit),
				uint(request.LoginOffset),
			)
			if err != nil {
				responseUser.Error = gotelnats.InitServiceError(ctx, err, &gotelnats.ErrorOptions{
					ErrorCode: svcerror.ErrorCode_INTERNAL,
				})
				if err = gotelnats.PublishResponse(ctx, conn, reply, responseUser); err != nil {
					log.Error(err)
				}
				return
			}

			responseUser.Logins = make([]*user.Login, 0, len(logins))

			for _, login := range logins {
				ul := user.Login{}
				if login.IPAddress.Valid {
					ul.IpAddress = login.IPAddress.String
				}
				if login.UserAgent.Valid {
					ul.UserAgent = login.UserAgent.String
				}
				if login.LoginTime.Valid {
					ul.LoginTime = ptypes.New(login.LoginTime.Time)
				}
				if login.LogoutTime.Valid {
					ul.LogoutTime = ptypes.New(login.LogoutTime.Time)
				}
				responseUser.Logins = append(responseUser.Logins, &ul)
			}

			loginCount, err := loginCount(ctx, dbconn, u.UserID)
			if err != nil {
				responseUser.Error = gotelnats.InitServiceError(ctx, err, &gotelnats.ErrorOptions{
					ErrorCode: svcerror.ErrorCode_INTERNAL,
				})
				if err = gotelnats.PublishResponse(ctx, conn, reply, responseUser); err != nil {
					log.Error(err)
				}
				return
			}

			responseUser.LoginCount = uint32(loginCount)
		}

		if err = gotelnats.PublishResponse(ctx, conn, reply, responseUser); err != nil {
			log.Error(err)
		}
	}
}
