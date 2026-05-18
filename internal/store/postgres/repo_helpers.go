package postgres

import "time"

func nullTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t
}

func nullString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
