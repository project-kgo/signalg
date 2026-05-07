package signalg

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

var (
	ErrMethodNotFound      = errors.New("signalg: method not found")
	errNilDispatcherTarget = errors.New("signalg: nil dispatcher target")
)

var (
	contextType = reflect.TypeFor[context.Context]()
	errorType   = reflect.TypeFor[error]()
)

type hubDispatcher struct {
	routes map[string]hubRoute
}

type hubRoute struct {
	method  reflect.Method
	reqType reflect.Type
}

var dispatcherCache sync.Map

func dispatcherFor(hub Hub) (*hubDispatcher, error) {
	if hub == nil {
		return nil, errNilDispatcherTarget
	}
	hubType := reflect.TypeOf(hub)
	if cached, ok := dispatcherCache.Load(hubType); ok {
		return cached.(*hubDispatcher), nil
	}

	dispatcher := buildDispatcher(hubType)
	actual, _ := dispatcherCache.LoadOrStore(hubType, dispatcher)
	return actual.(*hubDispatcher), nil
}

func buildDispatcher(hubType reflect.Type) *hubDispatcher {
	dispatcher := &hubDispatcher{
		routes: make(map[string]hubRoute),
	}
	for method := range hubType.Methods() {
		methodType := method.Type
		if methodType.NumIn() != 3 || methodType.NumOut() != 2 {
			continue
		}
		if methodType.In(1) != contextType {
			continue
		}
		reqType := methodType.In(2)
		if reqType.Kind() != reflect.Pointer {
			continue
		}
		if !methodType.Out(1).Implements(errorType) {
			continue
		}

		dispatcher.routes[method.Name] = hubRoute{
			method:  method,
			reqType: reqType,
		}
	}
	return dispatcher
}

func (d *hubDispatcher) dispatch(ctx context.Context, hub Hub, msg Message) (any, error) {
	if d == nil {
		return nil, ErrMethodNotFound
	}
	route, ok := d.routes[msg.Method]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMethodNotFound, msg.Method)
	}

	req := reflect.New(route.reqType.Elem())
	if err := msg.Decode(req.Interface()); err != nil {
		return nil, err
	}

	results := route.method.Func.Call([]reflect.Value{
		reflect.ValueOf(hub),
		reflect.ValueOf(ctx),
		req,
	})
	if errValue := results[1]; !errValue.IsNil() {
		return nil, errValue.Interface().(error)
	}
	if isNilValue(results[0]) {
		return nil, nil
	}
	return results[0].Interface(), nil
}

func isNilValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
