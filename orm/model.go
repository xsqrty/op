package orm

import (
	"errors"
	"fmt"
	"github.com/xsqrty/op"
	"reflect"
	"strings"
	"sync"
)

const opTag = "op"

var (
	ErrTargetNotStructPointer = errors.New("target must be a pointer to a struct")
	ErrTargetIsNil            = errors.New("target must not be nil")
	ErrAmbiguousField         = errors.New("target is ambiguous")
	ErrFieldNotDescribe       = errors.New("target is not described in the struct")
)

var modelsCache sync.Map

type modelSetters struct {
	path []int
}

type tagDetails struct {
	isAggregated bool
}

type modelDetails struct {
	primary      string
	primaryAsTag string
	setters      map[string]modelSetters
	mapping      map[string]map[string]string
	fields       map[string][]string
	tags         map[string][]string
	tagsDetails  map[string]map[string]*tagDetails
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

func getSettersByTags(md *modelDetails, table string, tags []string) (map[string]modelSetters, error) {
	setters := make(map[string]modelSetters, len(md.tags))
	for _, tag := range tags {
		path := md.mapping[table][tag]
		setter, ok := md.setters[path]
		if !ok {
			return nil, fmt.Errorf("tag %q does not exist in the setters list", tag)
		}

		setters[tag] = setter
	}

	return setters, nil
}

func getModelDetails(table string, target any) (*modelDetails, error) {
	val := reflect.ValueOf(target)
	kind := val.Kind()

	if kind != reflect.Ptr {
		return nil, ErrTargetNotStructPointer
	}

	if val.IsNil() {
		return nil, ErrTargetIsNil
	}

	val = val.Elem()
	typ := val.Type()
	if val.Kind() != reflect.Struct {
		return nil, ErrTargetNotStructPointer
	}

	if cachedMap, ok := modelsCache.Load(table); ok {
		if cache, ok := cachedMap.(*sync.Map).Load(typ); ok {
			return cache.(*modelDetails), nil
		}
	}

	result := &modelDetails{
		setters:     make(map[string]modelSetters),
		mapping:     make(map[string]map[string]string),
		fields:      make(map[string][]string),
		tags:        make(map[string][]string),
		tagsDetails: make(map[string]map[string]*tagDetails),
	}

	collectModelDetails(table, val, nil, result)
	inner, _ := modelsCache.LoadOrStore(table, &sync.Map{})
	inner.(*sync.Map).Store(typ, result)

	return result, nil
}

func collectModelDetails(table string, val reflect.Value, path []int, result *modelDetails) {
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

		if isNested {
			if fieldVal.Kind() == reflect.Struct {
				collectModelDetails(tag, fieldVal, append(path, i), result)
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

		if _, ok := result.tagsDetails[table]; !ok {
			result.tagsDetails[table] = make(map[string]*tagDetails)
		}

		result.mapping[table][tag] = pathName
		result.tags[table] = append(result.tags[table], tag)
		result.tagsDetails[table][tag] = &tagDetails{isAggregated: isAggregated}
		result.fields[table] = append(result.fields[table], pathName)

		if isPrimary {
			result.primary = pathName
			result.primaryAsTag = tag
		}
	}
}

func getKeysPointers(target any, setters map[string]modelSetters, keys []string) ([]any, error) {
	valueOf := reflect.ValueOf(target)
	if valueOf.Kind() != reflect.Ptr {
		return nil, ErrTargetNotStructPointer
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

			result[i] = field.Addr().Interface()
			continue
		}

		return nil, fmt.Errorf("key %q is not described in %T", key, target)
	}

	return result, nil
}

func setQueryReturning[T any](q *query[T], target *T) (*modelDetails, []string, error) {
	data, err := getModelDetails(q.with, target)
	if err != nil {
		return nil, nil, err
	}

	var keys []string
	retAliases := q.ret.GetReturning()
	if len(retAliases) == 0 {
		for _, table := range q.usingTables {
			for _, v := range data.fields[table] {
				retAliases = append(retAliases, op.ColumnAlias(op.Column(v)))
				keys = append(keys, v)
			}
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
				if retAliases[i].IsPureColumn() {
					retAliases[i].Rename(aliases[0])
				}
			}

			keys = append(keys, aliasValue)
		}
	}

	q.ret.SetReturning(retAliases)
	return data, keys, nil
}
