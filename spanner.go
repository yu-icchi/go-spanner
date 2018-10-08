package spanner

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	proto3 "github.com/golang/protobuf/ptypes/struct"
	"google.golang.org/grpc/codes"
)

var jst = time.FixedZone("JST", 9*60*60)

var fieldsCache = sync.Map{}

const (
	timezoneJST timezone = iota + 1

	spannerTag    = "spanner"
	spannerOptTag = "spannerOpt"
)

type timezone int

type fields struct {
	data map[string]*field
}

func newFields() *fields {
	return &fields{
		data: map[string]*field{},
	}
}

func (f *fields) set(field *field) {
	f.data[field.name] = field
}

func (f *fields) get(name string) *field {
	field, ok := f.data[name]
	if !ok {
		return nil
	}
	return field
}

type field struct {
	name  string
	index []int
	opt   *opt
}

type opt struct {
	timezone timezone
}

func ToStruct(row *spanner.Row, ptr interface{}) error {
	t := reflect.TypeOf(ptr)
	if t == nil || t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
		return spannerErrorf(codes.InvalidArgument, "ToStruct(): type %T is not a valid pointer to Go struct", ptr)
	}

	v := reflect.ValueOf(ptr).Elem()
	t = t.Elem()

	var f *fields
	cache, ok := fieldsCache.Load(t)
	if !ok {
		f = getFields(t)
		fieldsCache.Store(t, f)
	} else {
		f = cache.(*fields)
	}

	seen := map[string]struct{}{}
	for i, name := range row.ColumnNames() {
		if name == "" {
			return spannerErrorf(codes.InvalidArgument, "unnamed field %v", i)
		}
		field := f.get(name)
		if field == nil {
			continue
		}
		value := v.FieldByIndex(field.index)
		if !value.IsValid() {
			continue
		}
		if _, ok := seen[name]; ok {
			return spannerErrorf(codes.InvalidArgument, "duplicated field name %q", name)
		}
		col := &spanner.GenericColumnValue{}
		if err := row.Column(i, col); err != nil {
			return err
		}
		if isNull(col.Value) {
			continue
		}
		if err := col.Decode(value.Addr().Interface()); err != nil {
			return err
		}
		if field.opt != nil {
			if field.opt.timezone == timezoneJST {
				t, ok := value.Interface().(time.Time)
				if !ok {
					return spannerErrorf(codes.InvalidArgument, "invalid timestamp")
				}
				value.Set(reflect.ValueOf(t.In(jst)))
			}
		}
		seen[name] = struct{}{}
	}
	return nil
}

func isNull(value *proto3.Value) bool {
	_, ok := value.Kind.(*proto3.Value_NullValue)
	return ok
}

func spannerErrorf(code codes.Code, format string, args ...interface{}) error {
	return &spanner.Error{
		Code: code,
		Desc: fmt.Sprintf(format, args...),
	}
}

type fieldScan struct {
	typ   reflect.Type
	index []int
}

func getFields(t reflect.Type) *fields {
	var current []fieldScan
	next := []fieldScan{{typ: t}}

	visited := map[reflect.Type]struct{}{}
	fields := newFields()

	for len(next) > 0 {
		current, next = next, next[:0]

		for _, scan := range current {
			t := scan.typ
			if _, ok := visited[t]; ok {
				continue
			}
			visited[t] = struct{}{}
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)

				tag := parseTag(f.Tag)
				if tag == nil {
					continue
				}

				var ntyp reflect.Type
				if f.Anonymous {
					ntyp = f.Type
					if ntyp.Kind() == reflect.Ptr {
						ntyp = ntyp.Elem()
					}
				}

				if tag.name != "" || ntyp == nil || ntyp.Kind() != reflect.Struct {
					field := newField(f, tag, scan.index, f.Index)
					fields.set(field)
					continue
				}

				var index []int
				index = append(index, scan.index...)
				index = append(index, f.Index...)
				next = append(next, fieldScan{typ: ntyp, index: index})
			}
		}
	}
	return fields
}

func newField(f reflect.StructField, tag *tag, index, idx []int) *field {
	name := tag.name
	if name == "" {
		name = f.Name
	}
	field := &field{
		name: name,
		opt:  tag.opt,
	}
	field.index = append(field.index, index...)
	field.index = append(field.index, idx...)
	return field
}

type tag struct {
	name string
	opt  *opt
}

func parseTag(t reflect.StructTag) *tag {
	tagName := t.Get(spannerTag)
	if tagName == "-" {
		return nil
	}

	tag := &tag{
		name: tagName,
	}

	tagOption := t.Get(spannerOptTag)
	if tagOption == "" {
		return tag
	}

	opt := &opt{}
	for _, tmp := range strings.Split(tagOption, ",") {
		switch strings.ToLower(tmp) {
		case "jst":
			opt.timezone = timezoneJST
		}
	}
	tag.opt = opt

	return tag
}
