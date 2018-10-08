package spanner

import (
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/k0kubun/pp"
	"github.com/stretchr/testify/require"
)

type Sample struct {
	Hoge
	Test
	Foo string `spanner:"foo"`
}

type Hoge struct {
	ID string
}

type Test struct {
	Num       int64     `spanner:"num"`
	CreatedAt time.Time `spanner:"created_at" spannerOpt:"jst"`
}

func TestToStruct(t *testing.T) {
	s := &Sample{}
	now := time.Date(2018, 10, 7, 10, 30, 40, 0, jst)

	row, err := spanner.NewRow(
		[]string{
			"ID",
			"foo",
			"num",
			"created_at",
		},
		[]interface{}{
			"test",
			"hoge",
			100,
			now,
		},
	)
	require.NoError(t, err)

	err = row.ToStruct(s)
	require.NoError(t, err)
	pp.Println(s)

	s2 := &Sample{}
	err = ToStruct(row, s2)
	require.NoError(t, err)
	pp.Println(s2)
	//assert.Equal(t, &Sample{
	//	Hoge: Hoge{
	//		ID: "test",
	//	},
	//	Test: Test{
	//		Num:       100,
	//		CreatedAt: now,
	//	},
	//	Foo: "hoge",
	//}, s)
}

func BenchmarkNativeToStruct(b *testing.B) {
	now := time.Date(2018, 10, 7, 10, 30, 40, 0, jst)
	row, err := spanner.NewRow(
		[]string{
			"ID",
			"foo",
			"num",
			"created_at",
		},
		[]interface{}{
			"test",
			"hoge",
			100,
			now,
		},
	)
	if err != nil {
		b.FailNow()
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := &Sample{}
		row.ToStruct(s)
	}
}

func BenchmarkToStruct(b *testing.B) {
	now := time.Date(2018, 10, 7, 10, 30, 40, 0, jst)
	row, err := spanner.NewRow(
		[]string{
			"ID",
			"foo",
			"num",
			"created_at",
		},
		[]interface{}{
			"test",
			"hoge",
			100,
			now,
		},
	)
	if err != nil {
		b.FailNow()
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := &Sample{}
		ToStruct(row, s)
	}
}
