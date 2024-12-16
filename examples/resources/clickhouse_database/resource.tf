provider "clickhouseops" {}

resource "clickhouseops_database" "test" {
  name    = "test"
  comment = "test comment"
}