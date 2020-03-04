struct Page {
	file_location string
	page_name string
	layout string
	headers string
}

struct AccessLevel {
	dashboard string
	main_page string
	redirect_pages []string
}

struct PageInfo {
	pages map[string]Page
	access_levels []AccessLevel
}
