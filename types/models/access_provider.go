package models

//go:generate go run github.com/raito-io/enumer -type=AccessProviderState -json -trimprefix=AccessProviderState
type AccessProviderState int

const (
	AccessProviderStateActive AccessProviderState = iota
	AccessProviderStateInactive
	AccessProviderStateDeleted
)

//go:generate go run github.com/raito-io/enumer -type=AccessProviderAction -json -trimprefix=AccessProviderAction
type AccessProviderAction int

const (
	AccessProviderActionPromise AccessProviderAction = iota // Deprecated promises are set on who item
	AccessProviderActionGrant
	AccessProviderActionDeny
	AccessProviderActionMask
	AccessProviderActionFiltered
	AccessProviderActionPurpose
)
