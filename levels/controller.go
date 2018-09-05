package levels

import "storage-db/types"

type Controller struct {
	versionTable *types.AtomicInt64
	tables map[int64]*Table
}

func NewController() *Controller {
	return &Controller{
		versionTable: types.NewAtomicInt64(2),
		tables:make(map[int64]*Table),
	}
}

func (c *Controller) GetVersionTable() *types.AtomicInt64 {
	return c.versionTable
}

func (c *Controller) AddTable(table *Table) {
	c.tables[table.id] = table
}