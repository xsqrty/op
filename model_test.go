package op

import (
	"fmt"
	"github.com/stretchr/testify/assert"
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
	Company *MockCompany `op:"companies,nested"`
	NoTags  string
}

func TestGetModelDetails(t *testing.T) {
	for _, table := range []string{"users", "interventions"} {
		details, err := getModelDetails(table, &MockModel{})

		fullIdColumn := fmt.Sprintf("%s.%s", table, "id")
		fullNameColumn := fmt.Sprintf("%s.%s", table, "name")
		fullNameDate := fmt.Sprintf("%s.%s", table, "date")

		assert.NoError(t, err)
		assert.Equal(t, fullIdColumn, details.primary)
		assert.Equal(t, "id", details.primaryAsTag)

		assert.Equal(t, map[string]modelSetters{
			fullIdColumn:     {path: []int{0}},
			fullNameColumn:   {path: []int{1}},
			fullNameDate:     {path: []int{2}},
			"companies.id":   {path: []int{3, 0}},
			"companies.name": {path: []int{3, 1}},
			"companies.date": {path: []int{3, 2}},
		}, details.setters)

		assert.Equal(t, map[string]map[string]string{
			table: {
				"id":   fullIdColumn,
				"name": fullNameColumn,
				"date": fullNameDate,
			},
			"companies": {
				"id":   "companies.id",
				"name": "companies.name",
				"date": "companies.date",
			},
		}, details.mapping)

		assert.Equal(t, map[string][]string{
			"companies": {
				"companies.id",
				"companies.name",
				"companies.date",
			},
			table: {
				fullIdColumn,
				fullNameColumn,
				fullNameDate,
			},
		}, details.fields)

		assert.Equal(t, map[string][]string{
			"companies": {
				"id",
				"name",
				"date",
			},
			table: {
				"id",
				"name",
				"date",
			},
		}, details.tags)
	}

	var mp *MockModel
	details, err := getModelDetails("users", mp)
	assert.Nil(t, details)
	assert.EqualError(t, err, ErrTargetIsNil.Error())

	var m MockModel
	details, err = getModelDetails("users", m)
	assert.Nil(t, details)
	assert.EqualError(t, err, ErrTargetNotStructPointer.Error())

	s := ""
	ms := &s
	details, err = getModelDetails("users", ms)
	assert.Nil(t, details)
	assert.EqualError(t, err, ErrTargetNotStructPointer.Error())
}

func TestGetSettersKeysByTags(t *testing.T) {
	table := "users"
	details, _ := getModelDetails(table, &MockModel{})
	setters, err := getSettersKeysByTags(details, table, []string{"id", "name"})

	assert.NoError(t, err)
	assert.Equal(t, map[string]modelSetters{
		"id":   {path: []int{0}},
		"name": {path: []int{1}},
	}, setters)

	setters, err = getSettersKeysByTags(details, table, []string{"undefined"})
	assert.Nil(t, setters)
	assert.EqualError(t, err, `tag "undefined" does not exist in the setters list`)
}

func TestGetSettersKeysByFields(t *testing.T) {
	table := "users"
	model := &MockModel{}

	details, _ := getModelDetails(table, model)
	pointers, err := getPointersByModelSetters(model, details.setters, []string{"users.id", "users.name", "users.date", "companies.id", "companies.name", "companies.date"})

	assert.NoError(t, err)
	assert.Equal(t, []any{
		&model.ID,
		&model.Name,
		model.Date,
		&model.Company.ID,
		&model.Company.Name,
		model.Company.Date,
	}, pointers)

	now := time.Now()
	*(pointers[2].(*time.Time)) = now

	assert.Equal(t, now.Unix(), model.Date.Unix())

	var mp *MockModel
	pointers, err = getPointersByModelSetters(mp, details.setters, []string{"users.id", "users.name"})
	assert.Nil(t, pointers)
	assert.EqualError(t, err, ErrTargetIsNil.Error())

	var m MockModel
	pointers, err = getPointersByModelSetters(m, details.setters, []string{"users.id", "users.name"})
	assert.Nil(t, pointers)
	assert.EqualError(t, err, ErrTargetNotStructPointer.Error())

	details, _ = getModelDetails(table, model)
	pointers, err = getPointersByModelSetters(model, details.setters, []string{"undefined"})
	assert.Nil(t, pointers)
	assert.EqualError(t, err, `key "undefined" is not described in *op.MockModel`)
}
