package util

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestReadStrRowsByColumnName(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("select 1").WillReturnRows(
		sqlmock.NewRows([]string{"a"}).AddRow("1"),
	)
	rows, err := db.Query("select 1")
	require.NoError(t, err)
	defer rows.Close()

	got, allFound, err := ReadStrRowsByColumnName(rows, []string{"a"})
	require.NoError(t, err)
	require.True(t, allFound)
	require.Equal(t, [][]string{{"1"}}, got)

	mock.ExpectQuery("select 2").WillReturnRows(
		sqlmock.NewRows([]string{"a"}).AddRow("2"),
	)
	rows2, err2 := db.Query("select 2")
	require.NoError(t, err2)
	defer rows2.Close()

	got2, allFound2, err2 := ReadStrRowsByColumnName(rows2, []string{"b"})
	require.NoError(t, err2)
	require.False(t, allFound2)
	require.Nil(t, got2)
	got2, allFound2, err2 = ReadStrRowsByColumnName(rows2, []string{"a"})
	require.NoError(t, err2)
	require.True(t, allFound2)
	require.Equal(t, [][]string{{"2"}}, got2)

	mock.ExpectQuery("select 3").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}).AddRow("3", "4").AddRow("5", "6"),
	)
	rows3, err3 := db.Query("select 3")
	require.NoError(t, err3)
	defer rows3.Close()

	got3, allFound3, err3 := ReadStrRowsByColumnName(rows3, []string{"a", "b", "c"})
	require.NoError(t, err3)
	require.False(t, allFound3)
	require.Nil(t, got3)

	got3, allFound3, err3 = ReadStrRowsByColumnName(rows3, []string{"b", "a"})
	require.NoError(t, err3)
	require.True(t, allFound3)
	require.Equal(t, [][]string{{"4", "3"}, {"6", "5"}}, got3)

	mock.ExpectQuery("select 4").WillReturnRows(
		sqlmock.NewRows([]string{"a", "b"}).AddRow("7", "8").AddRow("9", "10"),
	)
	rows4, err4 := db.Query("select 4")
	require.NoError(t, err4)
	defer rows4.Close()

	got4, allFound4, err4 := ReadStrRowsByColumnName(rows4, []string{"a"})
	require.NoError(t, err4)
	require.True(t, allFound4)
	require.Equal(t, [][]string{{"7"}, {"9"}}, got4)
}
