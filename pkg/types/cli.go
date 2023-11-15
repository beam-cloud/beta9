package types

type ValidationErrorTrialEnded struct {
}

func (e *ValidationErrorTrialEnded) Error() string {
	return "trial_expired"
}

type ValidationErrorReachedTrialAppLimit struct {
}

func (e *ValidationErrorReachedTrialAppLimit) Error() string {
	return "exceeded_trial_app_limit"
}

type CredentialsErrorInvalidProfile struct {
}

func (e *CredentialsErrorInvalidProfile) Error() string {
	return "invalid_credentials"
}

type SubscriptionNotActiveError struct {
}

func (e *SubscriptionNotActiveError) Error() string {
	return "subscription_not_active"
}

type ConcurrenyLimitReachedError struct {
}

func (e *ConcurrenyLimitReachedError) Error() string {
	return "concurrency_limit_reached"
}
