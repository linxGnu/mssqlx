package mssqlx

import (
	"fmt"
	"testing"
)

func TestErrors(t *testing.T) {
	if isWsrepNotReady(nil) || isWsrepNotReady(fmt.Errorf("ERRor ")) || isWsrepNotReady(fmt.Errorf("ERRor 1047: asd")) {
		t.Fatal()
	}
	if !isWsrepNotReady(fmt.Errorf("Error 1047:ab")) || !isWsrepNotReady(fmt.Errorf("ERROR 1047:cd")) {
		t.Fatal()
	}
}
