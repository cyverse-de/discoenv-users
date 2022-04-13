# discoenv-users

discoenv-users is a microservice for the CyVerse Discovery Environment which
allows users to look up information about a user. It uses NATS for
communication.

It currently supports looking up a user's:

- Username
- UserID
- Logins
- Preferences
- Saved searches

User-related lookups are keyed off of their username, user ID, or analysis ID.
Only one of those is allowed in each request.

## Requests

Requests are JSON-encoded messages sent to the `cyverse.discoenv.users.>`
subject in NATS. The request format is available in the
[user/requests.proto](https://github.com/cyverse-de/p/blob/main/protos/user/requests.proto)
file of the [`p`](https://github.com/cyverse-de/p/) repository.

Responses will be in the format detailed in the
[user/user.proto](https://github.com/cyverse-de/p/blob/main/protos/user/user.proto)
file of the [`p`](https://github.com/cyverse-de/p/) repository.

A Go module is automatically generated from the above files and is available at
[github.com/cyverse-de/p/go/user](https://github.com/cyverse-de/p/tree/main/go/user).
Make sure to check the tags to figure out the versioning for the Go module.

### Example

This example shows how to make a request on the command-line. Note the usage of
the `login_limit` and `login_offset` fields. Also, the saved searches and
preferences are JSON encoded themselves, so are returned as strings. They are
not parsed.

```Shell

> nats request --tlscert /etc/nats/tls.crt --tlskey /etc/nats/tls.key --tlsca /etc/nats/ca.crt --creds /etc/nats/user.creds cyverse.discoenv.users.lookup '{"username":"example@iplantcollaborative.org", "include_saved_searches":true, "include_preferences":true, "include_logins":true, "login_limit":5, "login_offset":0}' -s tls://nats:4222 --raw | jq
{
  "uuid": "6be9d7fe-854a-11e4-b1aa-bb594900dd6f",
  "username": "example@iplantcollaborative.org",
  "preferences": {
    "uuid": "98fab7cc-bb52-11ec-bb2b-62d47aced14b",
    "preferences": "{\"default_output_folder\":{\"id\":\"/cyverse/home/example/analyses_qa\",\"path\":\"/cyverse/home/example/analyses\"},\"system_default_output_dir\":{\"id\":\"/cyverse/home/example/analyses\",\"path\":\"/cyverse/home/example/analyses\"}}"
  },
  "logins": [
    {
      "userAgent": "axios/0.21.4",
      "loginTime": "2021-04-12T10:53:16.242350Z"
    },
    {
      "userAgent": "axios/0.21.4",
      "loginTime": "2021-04-11T12:55:32.520684Z"
    },
    {
      "userAgent": "axios/0.21.4",
      "loginTime": "2021-04-11T12:55:16.933512Z"
    },
    {
      "userAgent": "axios/0.21.4",
      "loginTime": "2021-04-01T16:43:41.253016Z"
    },
    {
      "userAgent": "axios/0.21.4",
      "loginTime": "2021-04-01T16:43:32.593088Z"
    }
  ],
  "loginCount": 1558,
  "savedSearches": {
    "uuid": "3227ee06-97c7-11e7-8248-008cfa5ae621",
    "savedSearches": "[{\"total\":72,\"fileQuery\":\"index.html\",\"label\":\"index.html\",\"path\":\"/savedFilters/\",\"files\":[],\"folders\":[],\"tagQuery\":[],\"execution-time\":\"7675\",\"createdWithin\":{},\"modifiedWithin\":{},\"fileSizeRange\":{\"maxUnit\":{\"unit\":1,\"label\":\"KB\"},\"minUnit\":{\"unit\":1,\"label\":\"KB\"}}}]"
  }
}
```
