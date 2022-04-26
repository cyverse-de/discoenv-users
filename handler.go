package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/p/go/svcerror"
	"github.com/cyverse-de/p/go/user"
	"github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func getHandler(conn *nats.EncodedConn, dbconn *sqlx.DB) nats.Handler {
	return func(subject, reply string, request *user.UserLookupRequest) {
		var err error

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
			lookupErr := fmt.Errorf("lookup type %T not known", x)
			handleError(ctx, lookupErr, svcerror.Code_BAD_REQUEST, reply, conn)
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
			handleError(ctx, err, svcerror.Code_INTERNAL, reply, conn)
		}

		log.Infof("%s\n", queryString)

		u := lookupUser{}

		// argument handling is done by the goqu library above.
		if err = dbconn.QueryRowxContext(context.Background(), queryString).StructScan(&u); err != nil {
			handleError(ctx, err, svcerror.Code_INTERNAL, reply, conn)
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
				handleError(ctx, err, svcerror.Code_INTERNAL, reply, conn)
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
				handleError(ctx, err, svcerror.Code_INTERNAL, reply, conn)
			}

			responseUser.LoginCount = uint32(loginCount)
		}

		if err = publishResponse(ctx, conn, reply, &responseUser); err != nil {
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
	svcerr := svcerror.Error{
		ErrorCode: code,
		Message:   err.Error(),
	}

	log.Error(&svcerr)

	carrier := gotelnats.PBTextMapCarrier{
		Header: svcerr.Header,
	}

	_, span := gotelnats.InjectSpan(ctx, &carrier, reply, gotelnats.Send)
	defer span.End()

	if err = conn.Publish(reply, &svcerr); err != nil {
		log.Error(err)
	}
}
