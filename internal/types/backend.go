package types

import "time"

// User represents the user model
type User struct {
    ID        int       `db:"id"`
    Username  string    `db:"username"`
    Email     string    `db:"email"`
    CreatedAt time.Time `db:"created_at"`
}

// Post represents the post model with a foreign key to User
type Post struct {
    ID        int       `db:"id"`
    UserID    int       `db:"user_id"`
    Title     string    `db:"title"`
    Content   string    `db:"content"`
    CreatedAt time.Time `db:"created_at"`
}