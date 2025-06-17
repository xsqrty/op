package op

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

const opTag = "op"

var (
	ErrTargetNotPointer = errors.New("target must be a pointer to a struct")
	ErrTargetIsNil      = errors.New("target must not be nil")
	ErrAmbiguousField   = errors.New("target is ambiguous")
	ErrFieldNotDescribe = errors.New("target is not described in the struct")
)

var modelsCache sync.Map

type modelSetters struct {
	path []int
}

type modelDetails struct {
	primary      string
	primaryAsTag string
	setters      map[string]modelSetters
	mapping      map[string]map[string]string
	fields       map[string][]string
	tags         map[string][]string
}

func findAliasFullName[T any](g *query[T], data *modelDetails, shortValue string) []string {
	var aliases []string

	for _, table := range g.usingTables {
		if _, ok := data.mapping[table]; ok {
			if v, ok := data.mapping[table][shortValue]; ok {
				aliases = append(aliases, v)
			}
		}
	}

	return aliases
}

func getSettersKeysByTags(md *modelDetails, table string, tags []string) (map[string]modelSetters, error) {
	setters := make(map[string]modelSetters, len(md.tags))
	for _, tag := range tags {
		path := md.mapping[table][tag]
		setter, ok := md.setters[path]
		if !ok {
			return nil, fmt.Errorf("tag %s does not exist in the setters list", tag)
		}

		setters[tag] = setter
	}

	return setters, nil
}

func getModelDetails(table string, target any) (*modelDetails, error) {
	val := reflect.ValueOf(target)
	kind := val.Kind()

	if kind != reflect.Ptr {
		return nil, ErrTargetNotPointer
	}

	if val.IsNil() {
		return nil, ErrTargetIsNil
	}

	val = val.Elem()
	typ := val.Type()
	if val.Kind() != reflect.Struct {
		return nil, ErrTargetNotPointer
	}

	if cached, ok := modelsCache.Load(typ); ok {
		return cached.(*modelDetails), nil
	}

	result := &modelDetails{
		setters: make(map[string]modelSetters),
		mapping: make(map[string]map[string]string),
		fields:  make(map[string][]string),
		tags:    make(map[string][]string),
	}

	err := forEachModel(table, val, nil, result)
	if err != nil {
		return nil, err
	}

	modelsCache.Store(typ, result)
	return result, nil
}

func forEachModel(table string, val reflect.Value, path []int, result *modelDetails) error {
	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		fieldVal := val.Field(i)
		fieldTyp := typ.Field(i)

		tagValue := fieldTyp.Tag.Get(opTag)
		tags := strings.Split(tagValue, ",")
		if len(tags) == 0 || tags[0] == "" {
			continue
		}

		tag := tags[0]
		isPrimary := path == nil && strings.Contains(tagValue, ",primary")
		isAggregated := strings.Contains(tagValue, ",aggregated")
		isNested := strings.Contains(tagValue, ",nested")

		if fieldVal.Kind() == reflect.Ptr {
			if fieldVal.IsNil() {
				fieldVal.Set(reflect.New(fieldVal.Type().Elem()))
			}

			fieldVal = fieldVal.Elem()
		}

		if fieldVal.Kind() == reflect.Struct && isNested {
			err := forEachModel(tag, fieldVal, append(path, i), result)
			if err != nil {
				return err
			}

			continue
		}

		pathName := table + "." + tag
		if isAggregated {
			pathName = tag
		}

		result.setters[pathName] = modelSetters{path: append(path, i)}
		if _, ok := result.mapping[table]; !ok {
			result.mapping[table] = make(map[string]string)
		}

		result.mapping[table][tag] = pathName
		result.tags[table] = append(result.tags[table], tag)
		result.fields[table] = append(result.fields[table], pathName)
		if isPrimary {
			result.primary = pathName
			result.primaryAsTag = tag
		}
	}

	return nil
}

func getPointersByModelSetters(target any, setters map[string]modelSetters, keys []string) ([]any, error) {
	valueOf := reflect.ValueOf(target)
	if valueOf.Kind() != reflect.Ptr {
		return nil, ErrTargetNotPointer
	}

	if valueOf.IsNil() {
		return nil, ErrTargetIsNil
	}

	valueOf = valueOf.Elem()
	result := make([]any, len(keys))
	for i, key := range keys {
		if setter, ok := setters[key]; ok {
			field := valueOf
			for _, pathIndex := range setter.path {
				field = field.Field(pathIndex)
				if field.Kind() == reflect.Ptr {
					if field.IsNil() {
						field.Set(reflect.New(field.Type().Elem()))
					}
					field = field.Elem()
				}
			}

			if field.Kind() == reflect.Ptr {
				if field.IsNil() {
					field.Set(reflect.New(field.Type().Elem()))
				}

				result[i] = field.Interface()
				continue
			} else {
				result[i] = field.Addr().Interface()
				continue
			}
		}

		return nil, fmt.Errorf("target %s not found", key)
	}

	return result, nil
}

func prepareModelQuery[T any](q *query[T], target *T) (*modelDetails, []string, error) {
	data, err := getModelDetails(q.with, target)
	if err != nil {
		return nil, nil, err
	}

	var keys []string
	retAliases := q.ret.GetReturning()
	if len(retAliases) == 0 {
		var fields []any
		for _, table := range q.usingTables {
			for _, v := range data.fields[table] {
				fields = append(fields, v)
				keys = append(keys, v)
			}
		}

		err := q.ret.SetReturning(fields)
		if err != nil {
			return nil, nil, err
		}
	} else {
		for i := 0; i < len(retAliases); i++ {
			aliasValue := retAliases[i].Alias()

			if _, ok := data.setters[aliasValue]; !ok {
				aliases := findAliasFullName(q, data, aliasValue)
				if len(aliases) == 0 {
					return nil, nil, fmt.Errorf("%q: %w %T", aliasValue, ErrFieldNotDescribe, target)
				} else if len(aliases) > 1 {
					return nil, nil, fmt.Errorf("%q: %w", aliasValue, ErrAmbiguousField)
				}

				aliasValue = aliases[0]
				if retAliases[i].IsPure() {
					retAliases[i].Rename(aliases[0])
				}
			}

			keys = append(keys, aliasValue)
		}
	}

	return data, keys, nil
}
