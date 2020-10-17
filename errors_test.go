package mssqlx

import (
	"fmt"
	"testing"
)

func TestErrors(t *testing.T) {
	if IsWsrepNotReady(nil) || IsWsrepNotReady(fmt.Errorf("ERRor ")) || IsWsrepNotReady(fmt.Errorf("ERRor 1047: asd")) {
		t.Fatal()
	}
	if !IsWsrepNotReady(fmt.Errorf("Error 1047:ab")) || !IsWsrepNotReady(fmt.Errorf("ERROR 1047:cd")) {
		t.Fatal()
	}
}
