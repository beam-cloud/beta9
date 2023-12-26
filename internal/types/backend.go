package types

import "time"

type Identity struct {
	ID         uint      `db:"id"`
	ExternalId string    `db:"external_id"`
	CreatedAt  time.Time `db:"created_at"`
}

type IdentityToken struct {
	ID         uint   `db:"id"`
	ExternalId string `db:"external_id"`
}
