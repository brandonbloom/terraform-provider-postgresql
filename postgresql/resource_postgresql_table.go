package postgresql

import (
	"database/sql"

	"github.com/hashicorp/terraform/helper/schema"
)

const (
	tableNameAttr = "name"
)

func resourcePostgreSQLTable() *schema.Resource {
	return &schema.Resource{
		Create: resourcePostgresqlSQLTableCreate,
		Read:   resourcePostgresqlSQLTableRead,
		Update: resourcePostgresqlSQLTableUpdate,
		Delete: resourcePostgresqlSQLTableDelete,
		Exists: resourcePostgresqlSQLTableExists,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			tableNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The name of the table",
			},
		},
	}
}

func resourcePostgreSQLTableCreate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	//XXX

	return resourcePostgreSQLTableReadImpl(d, meta)
}

func resourcePostgreSQLTableDelete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	txn, err := c.DB().Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()

	tableName := d.Get(tableNameAttr).(string)

	//XXX

	d.SetId("")

	return nil
}

func resourcePostgreSQLTableExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	c := meta.(*Client)
	c.catalogLock.RLock()
	defer c.catalogLock.RUnlock()

	//XXX databaseName
	var tableName string
	err := c.DB().QueryRow("SELECT rolname FROM pg_catalog.pg_tables WHERE tablename=$1", d.Id()).Scan(&tableName)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func resourcePostgreSQLTableRead(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.RLock()
	defer c.catalogLock.RUnlock()

	return resourcePostgreSQLTableReadImpl(d, meta)
}

func resourcePostgreSQLTableReadImpl(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)

	tableId := d.Id()
	//XXX
	//var roleSuperuser, roleInherit, roleCreateRole, roleCreateDB, roleCanLogin, roleReplication bool
	//var roleConnLimit int
	//var roleName, roleValidUntil string

	//columns := []string{
	//	"rolname",
	//	"rolsuper",
	//	"rolinherit",
	//	"rolcreaterole",
	//	"rolcreatedb",
	//	"rolcanlogin",
	//	"rolreplication",
	//	"rolconnlimit",
	//	`COALESCE(rolvaliduntil::TEXT, 'infinity')`,
	//}

	//roleSQL := fmt.Sprintf("SELECT %s FROM pg_catalog.pg_roles WHERE rolname=$1", strings.Join(columns, ", "))
	//err := c.DB().QueryRow(roleSQL, tableId).Scan(
	//	&roleName,
	//	&roleSuperuser,
	//	&roleInherit,
	//	&roleCreateRole,
	//	&roleCreateDB,
	//	&roleCanLogin,
	//	&roleReplication,
	//	&roleConnLimit,
	//	&roleValidUntil,
	//)
	//switch {
	//case err == sql.ErrNoRows:
	//	log.Printf("[WARN] PostgreSQL TABLE (%s) not found", tableId)
	//	d.SetId("")
	//	return nil
	//case err != nil:
	//	return errwrap.Wrapf("Error reading TABLE: {{err}}", err)
	//}

	//d.Set(roleNameAttr, roleName)
	//d.Set(roleConnLimitAttr, roleConnLimit)
	//d.Set(roleCreateDBAttr, roleCreateDB)
	//d.Set(roleCreateRoleAttr, roleCreateRole)
	//d.Set(roleEncryptedPassAttr, true)
	//d.Set(roleInheritAttr, roleInherit)
	//d.Set(roleLoginAttr, roleCanLogin)
	//d.Set(roleReplicationAttr, roleReplication)
	//d.Set(roleSkipDropRoleAttr, d.Get(roleSkipDropRoleAttr).(bool))
	//d.Set(roleSkipReassignOwnedAttr, d.Get(roleSkipReassignOwnedAttr).(bool))
	//d.Set(roleSuperuserAttr, roleSuperuser)
	//d.Set(roleValidUntilAttr, roleValidUntil)

	d.SetId(tableName)

	return nil
}

func resourcePostgreSQLTableUpdate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	db := c.DB()

	//XXX

	return resourcePostgreSQLTableReadImpl(d, meta)
}
