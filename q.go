package sqlhelper

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"

	"github.com/lib/pq"
)

func StructToKVs(v interface{}) (keys []string, values []interface{}) {
	rv := reflect.Indirect(reflect.ValueOf(v))
	if rv.Kind() != reflect.Struct {
		panic(fmt.Sprintf("%#+v is not a struct", v))
	}

	t := rv.Type()
	keys = make([]string, 0, rv.NumField())
	values = make([]interface{}, 0, cap(keys))
	for i := 0; i < cap(keys); i++ {
		name := getName(t.Field(i))
		if name == "" {
			continue
		}
		keys = append(keys, name)
		values = append(values, rv.Field(i).Interface())
	}
	return
}

func getName(t reflect.StructField) string {
	if t.Name == "" || unicode.IsLower(rune(t.Name[0])) {
		return ""
	}

	if n := t.Tag.Get("sql"); n != "" {
		if p := strings.Split(n, ","); len(p) > 0 {
			return p[0]
		}
	}

	if n := t.Tag.Get("json"); n != "" {
		if p := strings.Split(n, ","); len(p) > 0 {
			return p[0]
		}
	}
	return t.Name
}

type Syntax uint8

const (
	PSQL Syntax = iota
	MySQL
)

func NewQuery(syn Syntax) *Query {
	return &Query{Syntax: syn}
}

type Query struct {
	sb strings.Builder
	Syntax
}

func (q *Query) InsertInto(tbl string, keys ...string) *Query {
	if len(keys) == 0 {
		panic("need at least one key")
	}

	q.sb.Grow(512)
	q.sb.WriteString("INSERT INTO ")
	q.sb.WriteString(tbl)

	q.sb.WriteString(" (")
	for i, k := range keys {
		if i > 0 {
			q.sb.WriteByte(',')
		}
		q.sb.WriteString(k)
	}
	q.sb.WriteString(") VALUES (")

	for i := 0; i < len(keys); i++ {
		if i > 0 {
			q.sb.WriteByte(',')
		}

		switch q.Syntax {
		case 0:
			q.sb.WriteString("$" + strconv.Itoa(i+1))
		default:
			q.sb.WriteByte('?')
		}
	}
	q.sb.WriteByte(')')

	switch q.Syntax {
	case 0:
		q.sb.WriteString(" RETURNING id")
	}

	return q
}

func (q *Query) ExecInsert(ins interface{}, ctx context.Context, values ...interface{}) (id string, err error) {
	switch t := ins.(type) {
	case *sql.DB:
		err = t.QueryRowContext(ctx, q.String(), values...).Scan(&id)
	case *sql.Tx:
		err = t.QueryRowContext(ctx, q.String(), values...).Scan(&id)
	case *sql.Stmt:
		_, err = t.ExecContext(ctx, values...)
	default:
		panic(fmt.Sprintf("invalid type: %T", ins))
	}

	return
}

func (q *Query) String() string { return q.sb.String() }

func (q *Query) Copy() *Query {
	nq := NewQuery(q.Syntax)
	nq.sb.Grow(q.sb.Cap())
	nq.sb.WriteString(q.String())
	return nq
}

func WrapCopyIn(ctx context.Context, db *sql.DB, table string, fields []string, fn func(stmt *sql.Stmt) error) (err error) {
	var (
		tx *sql.Tx
		st *sql.Stmt
	)

	if tx, err = db.Begin(); err != nil {
		return
	}

	defer func() {
		if st != nil {
			st.Close()
		}

		if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}

	}()

	if st, err = tx.PrepareContext(ctx, pq.CopyIn(table, fields...)); err != nil {
		return
	}

	if err = fn(st); err != nil {
		return
	}

	_, err = st.Exec()
	return
}
