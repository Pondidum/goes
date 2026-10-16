package goes

import "context"

type Projection interface {
	Project(ctx context.Context, unit UnitOfWork, pendingEvents []EventDescriptor) error
}
