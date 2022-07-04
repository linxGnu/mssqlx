// Copy from: https://github.com/lib/pq//conn.go

package mssqlx

import (
	"fmt"
	"unicode"

	"github.com/go-sql-driver/mysql"
)

// pqScanner implements a tokenizer for libpq-style option strings.
type pqScanner struct {
	s []rune
	i int
}

// newPQScanner returns a new scanner initialized with the option string s.
func newPQScanner(s string) *pqScanner {
	return &pqScanner{[]rune(s), 0}
}

// Next returns the next rune.
// It returns 0, false if the end of the text has been reached.
func (s *pqScanner) Next() (rune, bool) {
	if s.i >= len(s.s) {
		return 0, false
	}
	r := s.s[s.i]
	s.i++
	return r, true
}

// SkipSpaces returns the next non-whitespace rune.
// It returns 0, false if the end of the text has been reached.
func (s *pqScanner) SkipSpaces() (rune, bool) {
	r, ok := s.Next()
	for unicode.IsSpace(r) && ok {
		r, ok = s.Next()
	}
	return r, ok
}

// parses the postgres-options from name and adds them to the values.
//
// The parsing code is based on conninfo_parse from libpq's fe-connect.c
func parsePQOpts(name string, o map[string]string) error {
	s := newPQScanner(name)

	for {
		var (
			keyRunes, valRunes []rune
			r                  rune
			ok                 bool
		)

		if r, ok = s.SkipSpaces(); !ok {
			break
		}

		// Scan the key
		for !unicode.IsSpace(r) && r != '=' {
			keyRunes = append(keyRunes, r)
			if r, ok = s.Next(); !ok {
				break
			}
		}

		// Skip any whitespace if we're not at the = yet
		if r != '=' {
			r, ok = s.SkipSpaces()
		}

		// The current character should be =
		if r != '=' || !ok {
			return fmt.Errorf(`missing "=" after %q in connection info string"`, string(keyRunes))
		}

		// Skip any whitespace after the =
		if r, ok = s.SkipSpaces(); !ok {
			// If we reach the end here, the last value is just an empty string as per libpq.
			o[string(keyRunes)] = ""
			break
		}

		if r != '\'' {
			for !unicode.IsSpace(r) {
				if r == '\\' {
					if r, ok = s.Next(); !ok {
						return fmt.Errorf(`missing character after backslash`)
					}
				}
				valRunes = append(valRunes, r)

				if r, ok = s.Next(); !ok {
					break
				}
			}
		} else {
		quote:
			for {
				if r, ok = s.Next(); !ok {
					return fmt.Errorf(`unterminated quoted string literal in connection string`)
				}
				switch r {
				case '\'':
					break quote
				case '\\':
					r, _ = s.Next()
					fallthrough
				default:
					valRunes = append(valRunes, r)
				}
			}
		}

		o[string(keyRunes)] = string(valRunes)
	}

	return nil
}

// converts a url to a connection string for driver.Open.
// Example:
//
//	"postgres://bob:secret@1.2.3.4:5432/mydb?sslmode=verify-full"
//
// converts to:
//
//	"user=bob password=secret host=1.2.3.4 port=5432 dbname=mydb sslmode=verify-full"
//
// A minimal example:
//
//	"postgres://"
//
// This will be blank, causing driver.Open to use all of the defaults
func parsePostgresDSN(dsn string) (string, error) {
	meta := make(map[string]string)

	if err := parsePQOpts(dsn, meta); err != nil {
		return "", err
	}

	host, port := meta["host"], meta["port"]
	if host == "" && port == "" {
		return "", nil
	}

	return fmt.Sprintf("%s:%s", host, port), nil
}

func hostnameFromDSN(driverName, dsn string) string {
	switch driverName {
	case "mysql":
		if cf, err := mysql.ParseDSN(dsn); err == nil {
			return fmt.Sprintf("%s(%s)", cf.Net, cf.Addr)
		}

	case "postgres":
		if host, err := parsePostgresDSN(dsn); err == nil {
			return host
		}
	}

	return ""
}
