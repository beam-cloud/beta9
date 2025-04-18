package common

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/jmoiron/sqlx"
)

type CursorPaginationInfo[DBType any] struct {
	Next string   `json:"next" serializer:"next"`
	Data []DBType `json:"data" serializer:"data"`
}

const CursorTimestampFormat = "2006-01-02 15:04:05.999999 -0700 MST"

func StructToMap(obj interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	val := reflect.ValueOf(obj)
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		// Check if the field is an embedded struct
		if fieldType.Anonymous {
			embeddedFields := StructToMap(field.Interface())
			for k, v := range embeddedFields {
				result[k] = v
			}
		} else {
			tag := fieldType.Tag.Get("db")
			if tag != "" {
				result[tag] = field.Interface()
			} else {
				fieldName := fieldType.Name
				result[fieldName] = field.Interface()
			}
		}
	}

	return result
}

type DatetimeCursor struct {
	Value string `json:"value"`
	Id    uint   `json:"id"`
}

type SquirrelCursorPaginator[DBType any] struct {
	Client          *sqlx.DB
	SelectBuilder   squirrel.SelectBuilder
	SortOrder       string
	SortColumn      string
	SortQueryPrefix string
	PageSize        int
}

func getOperator(sortOrder string) string {
	lowercaseSortOrder := strings.ToLower(sortOrder)
	if lowercaseSortOrder == "asc" {
		return ">="
	}
	return "<="
}

func EncodeCursor(cursor DatetimeCursor) string {
	serializedCursor, err := json.Marshal(cursor)
	if err != nil {
		return ""
	}
	encodedCursor := base64.StdEncoding.EncodeToString(serializedCursor)
	return encodedCursor
}

func DecodeCursor(cursor string) (*DatetimeCursor, error) {
	if cursor == "" {
		return nil, nil
	}

	decodedCursor, err := base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return nil, err
	}

	var cur DatetimeCursor
	if err := json.Unmarshal(decodedCursor, &cur); err != nil {
		return nil, err
	}
	return &cur, nil
}

func Paginate[DBType any](settings SquirrelCursorPaginator[DBType], cursorString string) (*CursorPaginationInfo[DBType], error) {
	if settings.PageSize <= 0 {
		settings.PageSize = 10
	}

	cursor, err := DecodeCursor(cursorString)
	if err != nil {
		return nil, err
	}

	settings.SelectBuilder = settings.SelectBuilder.OrderBy(settings.SortColumn + " " + settings.SortOrder)
	settings.SelectBuilder = settings.SelectBuilder.Limit(uint64(settings.PageSize + 1))

	if cursor != nil {
		sortColumnName := settings.SortQueryPrefix + "." + settings.SortColumn
		sortIdColumnName := settings.SortQueryPrefix + ".id"
		timeValue, err := time.Parse(CursorTimestampFormat, cursor.Value)
		if err != nil {
			return nil, err
		}

		operator := getOperator(settings.SortOrder)
		whereExp := fmt.Sprintf("(%s %s ? OR (%s = ? AND %s %s ?))", sortColumnName, operator, sortColumnName, sortIdColumnName, operator)
		settings.SelectBuilder = settings.SelectBuilder.Where(squirrel.Expr(whereExp, timeValue, timeValue, cursor.Id))
	}

	sql, args, err := settings.SelectBuilder.ToSql()
	if err != nil {
		return nil, err
	}

	log.Printf("sql: %s", sql)

	rows := []DBType{}
	err = settings.Client.Select(&rows, sql, args...)
	if err != nil {
		return nil, err
	}

	var nextCursor string
	pageReturnLength := len(rows)

	if pageReturnLength > settings.PageSize {
		pageReturnLength = settings.PageSize
		lastRow := StructToMap(rows[len(rows)-1])
		cursor := DatetimeCursor{
			Value: lastRow[settings.SortColumn].(types.Time).Format(CursorTimestampFormat),
			Id:    lastRow["id"].(uint),
		}

		nextCursor = EncodeCursor(cursor)
	}

	log.Printf("nextCursor: %s", nextCursor)
	log.Printf("rows: %v, pageReturnLength: %d", rows, pageReturnLength)

	return &CursorPaginationInfo[DBType]{
		Next: nextCursor,
		Data: rows[:pageReturnLength],
	}, nil
}
