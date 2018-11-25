package postgresql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccPostgresqlTable_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlTableDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccPostgresqlTableConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlTableExists("postgresql_table.mytable"),
				),
			},
		},
	})
}

func testAccCheckPostgresqlTableDestroy(s *terraform.State) error {
	return nil //XXX
}

func testAccCheckPostgresqlTableExists(n string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]
		if !ok {
			return fmt.Errorf("Resource not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No ID is set")
		}

		client := testAccProvider.Meta().(*Client)
		exists, err := checkTableExists(client, rs.Primary.ID)

		if err != nil {
			return fmt.Errorf("Error checking table %s", err)
		}

		if !exists {
			return fmt.Errorf("Table not found")
		}

		return nil
	}
}

//XXX database name argument.
func checkTableExists(client *Client, tableName string) (bool, error) {
	var _rez int
	err := client.DB().QueryRow("SELECT 1 from pg_tables d WHERE tablename=$1", tableName).Scan(&_rez)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about table: %s", err)
	}

	return true, nil
}

var testAccPostgresqlTableConfig = `
resource "postgresql_table" "mytable" {
	name = "mytable"
}
`
