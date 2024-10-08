package auth

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Authorize, defers to Casbin’s Enforce function
type Authorizer struct {
	enforcer *casbin.Enforcer
}

// New function’s model and policy arguments are paths
// to the files where you’ve defined the model
// (which will configure Casbin’s authorization mechanism—which for us will be ACL)
// and the policy (which is a CSV file containing our ACL table)
func New(model, policy string) *Authorizer {
	enforcer := casbin.NewEnforcer(model, policy)
	return &Authorizer{
		enforcer: enforcer,
	}
}

// Authorize returns whether the given subject is permitted to
// run the given action on the given object based on the model
// and policy you configure Casbin with
func (a *Authorizer) Authorize(subject, object, action string) error {
	if !a.enforcer.Enforce(subject, object, action) {
		msg := fmt.Sprintf(
			"%s not permitted to %s to %s",
			subject,
			object,
			action,
		)
		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}
	return nil
}
