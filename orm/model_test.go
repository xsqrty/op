package orm

import (
	"fmt"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type MockCompany struct {
	ID   string  `op:"id,primary"`
	Name string  `op:"name"`
	Date *string `op:"date"`
}

type MockModel struct {
	ID      string       `op:"id,primary"`
	Name    string       `op:"name"`
	Date    *time.Time   `op:"date"`
	Count   int          `op:"count,aggregated"`
	Company *MockCompany `op:"companies,nested"`
	NoTags  string
}

func TestGetModelDetails(t *testing.T) {
	for _, table := range []string{"users", "interventions"} {
		details, err := getModelDetails(table, &MockModel{})

		fullIdColumn := fmt.Sprintf("%s.%s", table, "id")
		fullNameColumn := fmt.Sprintf("%s.%s", table, "name")
		fullNameDate := fmt.Sprintf("%s.%s", table, "date")

		require.NoError(t, err)
		require.Equal(t, fullIdColumn, details.primary)
		require.Equal(t, "id", details.primaryAsTag)

		require.Equal(t, map[string]modelSetters{
			fullIdColumn:     {path: []int{0}},
			fullNameColumn:   {path: []int{1}},
			fullNameDate:     {path: []int{2}},
			"count":          {path: []int{3}},
			"companies.id":   {path: []int{4, 0}},
			"companies.name": {path: []int{4, 1}},
			"companies.date": {path: []int{4, 2}},
		}, details.setters)

		require.Equal(t, map[string]map[string]string{
			table: {
				"id":    fullIdColumn,
				"name":  fullNameColumn,
				"date":  fullNameDate,
				"count": "count",
			},
			"companies": {
				"id":   "companies.id",
				"name": "companies.name",
				"date": "companies.date",
			},
		}, details.mapping)

		require.Equal(t, map[string][]string{
			"companies": {
				"companies.id",
				"companies.name",
				"companies.date",
			},
			table: {
				fullIdColumn,
				fullNameColumn,
				fullNameDate,
				"count",
			},
		}, details.fields)

		require.Equal(t, map[string][]string{
			"companies": {
				"id",
				"name",
				"date",
			},
			table: {
				"id",
				"name",
				"date",
				"count",
			},
		}, details.tags)

		require.Equal(t, map[string]map[string]*tagDetails{
			"companies": {
				"id":   &tagDetails{isAggregated: false},
				"name": &tagDetails{isAggregated: false},
				"date": &tagDetails{isAggregated: false},
			},
			table: {
				"id":    &tagDetails{isAggregated: false},
				"name":  &tagDetails{isAggregated: false},
				"date":  &tagDetails{isAggregated: false},
				"count": &tagDetails{isAggregated: true},
			},
		}, details.tagsDetails)
	}

	var mp *MockModel
	details, err := getModelDetails("users", mp)
	require.Nil(t, details)
	require.EqualError(t, err, ErrTargetIsNil.Error())

	var m MockModel
	details, err = getModelDetails("users", m)
	require.Nil(t, details)
	require.EqualError(t, err, ErrTargetNotStructPointer.Error())

	s := ""
	ms := &s
	details, err = getModelDetails("users", ms)
	require.Nil(t, details)
	require.EqualError(t, err, ErrTargetNotStructPointer.Error())
}

func TestGetSettersKeysByTags(t *testing.T) {
	table := "users"
	details, _ := getModelDetails(table, &MockModel{})
	setters, err := getSettersKeysByTags(details, table, []string{"id", "name"})

	require.NoError(t, err)
	require.Equal(t, map[string]modelSetters{
		"id":   {path: []int{0}},
		"name": {path: []int{1}},
	}, setters)

	setters, err = getSettersKeysByTags(details, table, []string{"undefined"})
	require.Nil(t, setters)
	require.EqualError(t, err, `tag "undefined" does not exist in the setters list`)
}

func TestGetSettersKeysByFields(t *testing.T) {
	table := "users"
	model := &MockModel{}

	details, _ := getModelDetails(table, model)
	pointers, err := getPointersByModelSetters(model, details.setters, []string{"users.id", "users.name", "users.date", "companies.id", "companies.name", "companies.date"})

	require.NoError(t, err)
	require.Equal(t, []any{
		&model.ID,
		&model.Name,
		model.Date,
		&model.Company.ID,
		&model.Company.Name,
		model.Company.Date,
	}, pointers)

	now := time.Now()
	name := gofakeit.Name()

	*(pointers[2].(*time.Time)) = now
	*(pointers[4].(*string)) = name

	require.Equal(t, now.Unix(), model.Date.Unix())
	require.Equal(t, name, model.Company.Name)

	var mp *MockModel
	pointers, err = getPointersByModelSetters(mp, details.setters, []string{"users.id", "users.name"})
	require.Nil(t, pointers)
	require.EqualError(t, err, ErrTargetIsNil.Error())

	var m MockModel
	pointers, err = getPointersByModelSetters(m, details.setters, []string{"users.id", "users.name"})
	require.Nil(t, pointers)
	require.EqualError(t, err, ErrTargetNotStructPointer.Error())

	details, _ = getModelDetails(table, model)
	pointers, err = getPointersByModelSetters(model, details.setters, []string{"undefined"})
	require.Nil(t, pointers)
	require.EqualError(t, err, `key "undefined" is not described in *orm.MockModel`)
}
