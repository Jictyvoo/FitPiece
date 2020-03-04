module fit_piece_models_value

struct User {
	access_level int = 0
	username string = ""
	password string = ""
	user_id string = ""
}

pub fn(user User) get_username() string {
	return user.username
}

pub fn(user User) get_access_level() int {
	return user.access_level
}

pub fn(user User) get_user_id() string {
	return user.user_id
}
