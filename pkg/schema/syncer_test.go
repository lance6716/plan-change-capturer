package schema

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestConcurrentSyncDBSucc(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	seed := time.Now().UnixNano()
	s := rand.NewSource(seed)
	t.Logf("seed: %d", seed)

	taskNum := 100
	dbNames := make([]string, taskNum)
	dedup := map[string]struct{}{}
	for i := range taskNum {
		dbNames[i] = "db" + strconv.Itoa(int(s.Int63()%10))
		dedup[dbNames[i]] = struct{}{}
	}

	for range dedup {
		mock.ExpectExec(".*").
			WillDelayFor(time.Duration(s.Int63()%100) * time.Microsecond).
			WillReturnResult(sqlmock.NewResult(0, 0))
	}
	wg := sync.WaitGroup{}
	ctx := context.Background()
	syncer := NewSyncer(db)

	wg.Add(taskNum)
	for _, dbName := range dbNames {
		go func() {
			defer wg.Done()
			err := syncer.CreateDatabase(ctx, dbName, "test")
			require.NoError(t, err)
		}()
	}
	wg.Wait()
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConcurrentSyncDBFail(t *testing.T) {
	errMock := errors.New("mock error")
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.MatchExpectationsInOrder(false)

	seed := time.Now().UnixNano()
	s := rand.NewSource(seed)
	t.Logf("seed: %d", seed)

	taskNum := 100
	dbNames := make([]string, taskNum)
	dedup := map[string]struct{}{}
	for i := range taskNum {
		dbNames[i] = "db" + strconv.Itoa(int(s.Int63()%10))
		dedup[dbNames[i]] = struct{}{}
	}

	for range dedup {
		mock.ExpectExec(".*").
			WillDelayFor(time.Duration(s.Int63()%100) * time.Microsecond).
			WillReturnError(errMock)
		// when CREATE DATABASE meet error, syncer will query SHOW CREATE DATABASE...
		mock.ExpectQuery(".*").
			WillDelayFor(time.Duration(s.Int63()%100) * time.Microsecond).
			WillReturnError(errMock)
	}
	wg := sync.WaitGroup{}
	ctx := context.Background()
	syncer := NewSyncer(db)

	wg.Add(taskNum)
	for _, dbName := range dbNames {
		go func() {
			defer wg.Done()
			err := syncer.CreateDatabase(ctx, dbName, "test")
			require.ErrorIs(t, err, errMock)
		}()
	}
	wg.Wait()
	require.NoError(t, mock.ExpectationsWereMet())
}
