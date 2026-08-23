package api

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"github.com/meteron/meteron/internal/rbac"
)

type ctxKey int

const identityKey ctxKey = 1

func withIdentity(ctx context.Context, ident *rbac.Identity) context.Context {
	return context.WithValue(ctx, identityKey, ident)
}

func identityOf(r *http.Request) *rbac.Identity {
	ident, _ := r.Context().Value(identityKey).(*rbac.Identity)
	return ident
}

// customerFilter 客户级密钥强制数据隔离：平台密钥可传 ?customer_id=，客户密钥只看自己。
func customerFilter(r *http.Request, ident *rbac.Identity) (*uuid.UUID, error) {
	q := r.URL.Query().Get("customer_id")
	if ident.CustomerID != uuid.Nil {
		return &ident.CustomerID, nil // 客户级密钥：强制本客户
	}
	if q == "" {
		return nil, nil
	}
	id, err := uuid.Parse(q)
	if err != nil {
		return nil, errBadRequest("invalid customer_id")
	}
	return &id, nil
}
