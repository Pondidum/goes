package goes

import "context"

type Projection interface {
	Reset(ctx context.Context, unit UnitOfWork) error
	Project(ctx context.Context, unit UnitOfWork, pendingEvents []EventDescriptor) error
}
