package goes

import (
	"fmt"
	"reflect"
)

func Handler[TEvent any](eventHandler func(event TEvent)) handler {
	handler := handler{
		name: reflect.TypeOf(*new(TEvent)).Name(),
		action: func(event any) error {

			switch e := event.(type) {
			case TEvent:
				eventHandler(e)
			case *TEvent:
				eventHandler(*e)
			default:
				return fmt.Errorf("unable to handle %T", e)
			}

			return nil
		},
	}

	eventFactory[handler.name] = func() any {
		return new(TEvent)
	}

	return handler
}

type handler struct {
	name   string
	action func(event any) error
}
