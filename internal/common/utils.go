package common

import (
	"bytes"
	"database/sql"
	"html/template"
)

func RenderTemplate(queryTemplate string, input any) (*string, error) {
	tpl, err := template.New("input").Parse(queryTemplate)
	if err != nil {
		return nil, err
	}
	var tplBuffer bytes.Buffer
	if err := tpl.Execute(&tplBuffer, input); err != nil {
		return nil, err
	}
	query := tplBuffer.String()
	return &query, nil
}

func ReadCluster(db *sql.DB, uuid string) (*string, error) {
	var cluster string
	err := db.QueryRow("SELECT `cluster` FROM system.distributed_ddl_queue where query like ?", "%"+uuid+"%").Scan(&cluster)
	if err != nil {
		return nil, err
	}
	return &cluster, nil
}