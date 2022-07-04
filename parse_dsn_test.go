package mssqlx

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDSN(t *testing.T) {
	require.Equal(t, "127.0.0.1:5555",
		hostnameFromDSN("postgres", "host=127.0.0.1 port=5555 user=root password=password dbname=testdb sslmode=disable"))

	require.Equal(t, "",
		hostnameFromDSN("postgres", "a=b c=d"))

	require.Equal(t, "",
		hostnameFromDSN("postgres", "a:b"))

	require.Equal(t, "tcp(172.17.0.2:3306)",
		hostnameFromDSN("mysql", "user:password@(172.17.0.2:3306)/practice?charset=utf8mb4&interpolateParams=true"))

	require.Equal(t, "udp(172.17.0.2:3306)",
		hostnameFromDSN("mysql", "user:password@udp(172.17.0.2:3306)/practice?charset=utf8mb4&interpolateParams=true"))

	require.Equal(t, "",
		hostnameFromDSN("mysql", "a=b c=d"))

}
