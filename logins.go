package main

import (
	"context"
	"database/sql"

	"github.com/cyverse-de/p/go/user"
	"github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type lookupLogin struct {
	IPAddress  sql.NullString `db:"ip_address"`
	UserAgent  sql.NullString `db:"user_agent"`
	LoginTime  sql.NullTime   `db:"login_time"`
	LogoutTime sql.NullTime   `db:"logout_time"`
}

func addLogins(ctx context.Context, responseUser *user.User, request *user.UserLookupRequest, dbconn *sqlx.DB) (*user.User, error) {
	logins, err := lookupLogins(
		ctx,
		dbconn,
		responseUser.Uuid,
		uint(request.LoginLimit),
		uint(request.LoginOffset),
	)
	if err != nil {
		return nil, err

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

	loginCount, err := loginCount(ctx, dbconn, responseUser.Uuid)
	if err != nil {
		return nil, err
	}

	responseUser.LoginCount = uint32(loginCount)

	return responseUser, nil
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
