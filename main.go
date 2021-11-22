package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net"
	"strings"
	"time"

	lineprotocol "github.com/influxdata/line-protocol"
	"github.com/lib/pq"
	pqoid "github.com/lib/pq/oid"
	"github.com/robfig/cron/v3"
)

const (
	pgStatActivitySql = "SELECT datid, datname, pid, leader_pid, usesysid, usename, application_name, client_addr, client_hostname, client_port, backend_start, xact_start, query_start, state_change, wait_event_type, wait_event, state, backend_xid, backend_xmin, query, backend_type FROM pg_stat_activity"

	pg13StatStatementsSql = "SELECT userid, dbid, queryid, query, plans, total_plan_time, min_plan_time, max_plan_time, mean_plan_time, stddev_plan_time, calls, total_exec_time, min_exec_time, max_exec_time, mean_exec_time, stddev_exec_time, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written, blk_read_time, blk_write_time, wal_records, wal_fpi, wal_bytes FROM pg_stat_statements"

	pg11StatStatementsSql = "SELECT userid, dbid, queryid, query, calls, total_time, min_time, max_time, mean_time, stddev_time, rows, shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written, local_blks_hit, local_blks_read, local_blks_dirtied, local_blks_written, temp_blks_read, temp_blks_written, blk_read_time, blk_write_time FROM pg_stat_statements"

	pgStatStatementsExtensionCheckSql = "SELECT exists(SELECT * FROM pg_extension WHERE extname = 'pg_stat_statements');"

	pgCheckVersion = "SELECT lower(version());"
)

var shutdownCh chan (struct{})

var databaseUrl string
var ilpServer string
var every string

func init() {
	flag.StringVar(&databaseUrl, "dsn", "host=localhost user=postgres dbname=postgres sslmode=disable", "")
	flag.StringVar(&ilpServer, "influx", "localhost:9009", "")
	flag.StringVar(&every, "every", "30s", "How frequently to get stats, use s - seconds, m -  minutes, h - hour")
}

type pgStatActivityRow struct {
	Datid           sql.NullString
	Datname         sql.NullString
	Pid             sql.NullInt64
	LeaderPID       sql.NullInt64
	Usesysid        sql.NullString
	Usename         sql.NullString
	ApplicationName sql.NullString
	ClientAddr      sql.NullString
	ClientHostname  sql.NullString
	ClientPort      sql.NullInt64
	BackendStart    pq.NullTime
	XactStart       pq.NullTime
	QueryStart      pq.NullTime
	StateChange     pq.NullTime
	WaitEventType   sql.NullString
	WaitEvent       sql.NullString
	State           sql.NullString
	BackendXID      sql.NullString
	BackendXmin     sql.NullString
	Query           sql.NullString
	BackendType     sql.NullString
}

// pg11StatStatementsRow a Postgres 11 compatible struct for pg_stat_statements row
type pg11StatStatementsRow struct {
	userid              pqoid.Oid       //	pg_authid.oid 	OID of user who executed the statement
	dbid                pqoid.Oid       //	pg_database.oid 	OID of database in which the statement was executed
	queryid             sql.NullInt64   //		 	  	Internal hash code, computed from the statement's parse tree
	query               sql.NullString  //	 	  	Text of a representative statement
	calls               sql.NullInt64   //	 	  	Number of times executed
	total_time          sql.NullFloat64 //		 precision 	  	Total time spent in the statement, in milliseconds
	min_time            sql.NullFloat64 //		 precision 	  	Minimum time spent in the statement, in milliseconds
	max_time            sql.NullFloat64 //		 precision 	  	Maximum time spent in the statement, in milliseconds
	mean_time           sql.NullFloat64 //		 precision 	  	Mean time spent in the statement, in milliseconds
	stddev_time         sql.NullFloat64 //			 precision 	  	Population standard deviation of time spent in the statement, in milliseconds
	rows                sql.NullInt64   //	 	  	Total number of rows retrieved or affected by the statement
	shared_blks_hit     sql.NullInt64   //				 	  	Total number of shared block cache hits by the statement
	shared_blks_read    sql.NullInt64   //				 	  	Total number of shared blocks read by the statement
	shared_blks_dirtied sql.NullInt64   //					 	  	Total number of shared blocks dirtied by the statement
	shared_blks_written sql.NullInt64   //					 	  	Total number of shared blocks written by the statement
	local_blks_hit      sql.NullInt64   //			 	  	Total number of local block cache hits by the statement
	local_blks_read     sql.NullInt64   //				 	  	Total number of local blocks read by the statement
	local_blks_dirtied  sql.NullInt64   //				 	  	Total number of local blocks dirtied by the statement
	local_blks_written  sql.NullInt64   //				 	  	Total number of local blocks written by the statement
	temp_blks_read      sql.NullInt64   //			 	  	Total number of temp blocks read by the statement
	temp_blks_written   sql.NullInt64   //				 	  	Total number of temp blocks written by the statement
	blk_read_time       sql.NullFloat64 //			 precision 	  	Total time the statement spent reading blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)
	blk_write_time      sql.NullFloat64 //			 precision 	  	Total time the statement spent writing blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)
}

func (p *pg11StatStatementsRow) Tags() map[string]string {
	tags := make(map[string]string)
	tags["userid"] = fmt.Sprintf("%d", p.userid)
	tags["dbid"] = fmt.Sprintf("%d", p.dbid)
	tags["queryid"] = fmt.Sprintf("%d", p.queryid.Int64)
	addNullStringTag(tags, "query", p.query)

	return tags
}

func (p *pg11StatStatementsRow) Fields() map[string]interface{} {
	fields := make(map[string]interface{})
	addNullInt64Field(fields, "calls", p.calls)
	addNullFloat64Field(fields, "total_time", p.total_time)
	addNullFloat64Field(fields, "min_time", p.min_time)
	addNullFloat64Field(fields, "max_time", p.max_time)
	addNullFloat64Field(fields, "mean_time", p.mean_time)
	addNullFloat64Field(fields, "stddev_time", p.stddev_time)
	addNullInt64Field(fields, "rows", p.rows)
	addNullInt64Field(fields, "shared_blks_hit", p.shared_blks_hit)
	addNullInt64Field(fields, "shared_blks_read", p.shared_blks_read)
	addNullInt64Field(fields, "shared_blks_dirtied", p.shared_blks_dirtied)
	addNullInt64Field(fields, "shared_blks_written", p.shared_blks_written)
	addNullInt64Field(fields, "local_blks_hit", p.local_blks_hit)
	addNullInt64Field(fields, "local_blks_read", p.local_blks_read)
	addNullInt64Field(fields, "local_blks_dirtied", p.local_blks_dirtied)
	addNullInt64Field(fields, "local_blks_written", p.local_blks_written)
	addNullInt64Field(fields, "temp_blks_read", p.temp_blks_read)
	addNullInt64Field(fields, "temp_blks_written", p.temp_blks_written)
	addNullFloat64Field(fields, "blk_read_time", p.blk_read_time)
	addNullFloat64Field(fields, "blk_write_time", p.blk_write_time)

	return fields
}

// pg13StatStatementsRow a Postgres 13+ compatible struct for pg_stat_statements row
type pg13StatStatementsRow struct {
	userid              pqoid.Oid       // oid 	pg_authid.oid 	OID of user who executed the statement
	dbid                pqoid.Oid       // oid 	pg_database.oid 	OID of database in which the statement was executed
	queryid             sql.NullInt64   // sql.NullInt64 //  	  	Internal hash code, computed from the statement's parse tree
	query               sql.NullString  // text 	  	Text of a representative statement
	plans               sql.NullInt64   // Number of times the statement was planned (if pg_stat_statements.track_planning is enabled, otherwise zero)
	total_plan_time     sql.NullFloat64 // Total time spent planning the statement, in milliseconds (if pg_stat_statements.track_planning is enabled, otherwise zero)
	min_plan_time       sql.NullFloat64 // Minimum time spent planning the statement, in milliseconds (if pg_stat_statements.track_planning is enabled, otherwise zero)
	max_plan_time       sql.NullFloat64 // Maximum time spent planning the statement, in milliseconds (if pg_stat_statements.track_planning is enabled, otherwise zero)
	mean_plan_time      sql.NullFloat64 // Mean time spent planning the statement, in milliseconds (if pg_stat_statements.track_planning is enabled, otherwise zero)
	stddev_plan_time    sql.NullFloat64 // Population standard deviation of time spent planning the statement, in milliseconds (if pg_stat_statements.track_planning is enabled, otherwise zero)
	calls               sql.NullInt64   // Number of times the statement was executed
	total_exec_time     sql.NullFloat64 // Total time spent executing the statement, in milliseconds
	min_exec_time       sql.NullFloat64 // Minimum time spent executing the statement, in milliseconds
	max_exec_time       sql.NullFloat64 // Maximum time spent executing the statement, in milliseconds
	mean_exec_time      sql.NullFloat64 // Mean time spent executing the statement, in milliseconds
	stddev_exec_time    sql.NullFloat64 // Population standard deviation of time spent executing the statement, in milliseconds
	rows                sql.NullInt64   // Total number of rows retrieved or affected by the statement
	shared_blks_hit     sql.NullInt64   // Total number of shared block cache hits by the statement
	shared_blks_read    sql.NullInt64   // Total number of shared blocks read by the statement
	shared_blks_dirtied sql.NullInt64   // Total number of shared blocks dirtied by the statement
	shared_blks_written sql.NullInt64   // Total number of shared blocks written by the statement
	local_blks_hit      sql.NullInt64   // Total number of local block cache hits by the statement
	local_blks_read     sql.NullInt64   // Total number of local blocks read by the statement
	local_blks_dirtied  sql.NullInt64   // Total number of local blocks dirtied by the statement
	local_blks_written  sql.NullInt64   // Total number of local blocks written by the statement
	temp_blks_read      sql.NullInt64   // Total number of temp blocks read by the statement
	temp_blks_written   sql.NullInt64   // Total number of temp blocks written by the statement
	blk_read_time       sql.NullFloat64 // Total time the statement spent reading blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)
	blk_write_time      sql.NullFloat64 // Total time the statement spent writing blocks, in milliseconds (if track_io_timing is enabled, otherwise zero)
	wal_records         sql.NullInt64   // Total number of WAL records generated by the statement
	wal_fpi             sql.NullInt64   // Total number of WAL full page images generated by the statement
	wal_bytes           sql.NullInt64   // Total amount of WAL generated by the statement in bytes
}

func (p *pg13StatStatementsRow) Tags() map[string]string {
	tags := make(map[string]string)
	tags["userid"] = fmt.Sprintf("%d", p.userid)
	tags["dbid"] = fmt.Sprintf("%d", p.dbid)
	tags["queryid"] = fmt.Sprintf("%d", p.queryid.Int64)
	addNullStringTag(tags, "query", p.query)

	return tags
}

func (p *pg13StatStatementsRow) Fields() map[string]interface{} {
	fields := make(map[string]interface{})
	addNullInt64Field(fields, "plans", p.plans)
	addNullFloat64Field(fields, "total_plan_time", p.total_plan_time)
	addNullFloat64Field(fields, "min_plan_time", p.min_plan_time)
	addNullFloat64Field(fields, "max_plan_time", p.max_plan_time)
	addNullFloat64Field(fields, "mean_plan_time", p.mean_plan_time)
	addNullFloat64Field(fields, "stddev_plan_time", p.stddev_plan_time)
	addNullInt64Field(fields, "calls", p.calls)
	addNullFloat64Field(fields, "total_exec_time", p.total_exec_time)
	addNullFloat64Field(fields, "min_exec_time", p.min_exec_time)
	addNullFloat64Field(fields, "max_exec_time", p.max_exec_time)
	addNullFloat64Field(fields, "mean_exec_time", p.mean_exec_time)
	addNullFloat64Field(fields, "stddev_exec_time", p.stddev_exec_time)
	addNullInt64Field(fields, "rows", p.rows)
	addNullInt64Field(fields, "shared_blks_hit", p.shared_blks_hit)
	addNullInt64Field(fields, "shared_blks_read", p.shared_blks_read)
	addNullInt64Field(fields, "shared_blks_dirtied", p.shared_blks_dirtied)
	addNullInt64Field(fields, "shared_blks_written", p.shared_blks_written)
	addNullInt64Field(fields, "local_blks_hit", p.local_blks_hit)
	addNullInt64Field(fields, "local_blks_read", p.local_blks_read)
	addNullInt64Field(fields, "local_blks_dirtied", p.local_blks_dirtied)
	addNullInt64Field(fields, "local_blks_written", p.local_blks_written)
	addNullInt64Field(fields, "temp_blks_read", p.temp_blks_read)
	addNullInt64Field(fields, "temp_blks_written", p.temp_blks_written)
	addNullFloat64Field(fields, "blk_read_time", p.blk_read_time)
	addNullFloat64Field(fields, "blk_write_time", p.blk_write_time)
	addNullInt64Field(fields, "wal_records", p.wal_records)
	addNullInt64Field(fields, "wal_fpi", p.wal_fpi)
	addNullInt64Field(fields, "wal_bytes", p.wal_bytes)
	return fields
}

func (p *pgStatActivityRow) Tags() map[string]string {
	tags := make(map[string]string, 10)
	addNullStringTag(tags, "datid", p.Datid)
	addNullStringTag(tags, "datname", p.Datname)
	addNullStringTag(tags, "usesysid", p.Usesysid)
	addNullStringTag(tags, "usename", p.Usename)
	addNullStringTag(tags, "application_name", p.ApplicationName)
	addNullStringTag(tags, "client_addr", p.ClientAddr)
	addNullStringTag(tags, "client_hostname", p.ClientHostname)
	addNullStringTag(tags, "wait_event_type", p.WaitEventType)
	addNullStringTag(tags, "wait_event", p.WaitEvent)
	addNullStringTag(tags, "state", p.State)
	addNullStringTag(tags, "backend_xid", p.BackendXID)
	addNullStringTag(tags, "backend_xmin", p.BackendXmin)
	addNullStringTag(tags, "query", p.Query)
	addNullStringTag(tags, "backend_type", p.BackendType)

	return tags
}

func (p *pgStatActivityRow) Fields() map[string]interface{} {
	fields := make(map[string]interface{})
	addNullInt64Field(fields, "client_port", p.ClientPort)
	addNullInt64Field(fields, "pid", p.Pid)
	addNullInt64Field(fields, "leader_pid", p.LeaderPID)
	addNullTimeField(fields, "backend_start", p.BackendStart)
	addNullTimeField(fields, "backend_xact_start", p.XactStart)
	addNullTimeField(fields, "backend_xact_start", p.XactStart)

	return fields
}

func main() {
	flag.Parse()

	if every == "" {
		every = "30s"
	}

	db1, err := sql.Open("postgres", databaseUrl)
	if err != nil {
		panic(err)
	}
	defer db1.Close()

	conn1, err := net.Dial("tcp", ilpServer)
	if err != nil {
		panic(err)
	}
	defer conn1.Close()

	serializer := lineprotocol.NewEncoder(conn1)
	serializer.SetMaxLineBytes(1024)
	serializer.SetFieldTypeSupport(lineprotocol.UintSupport)

	// send on startup
	sendPgStatActivityMetrics(db1, serializer)
	sendPgStatStatementsMetrics(db1, serializer)

	c := cron.New()

	c.AddFunc(fmt.Sprintf("@every %s", strings.TrimSpace(every)), func() {
		db, err := sql.Open("postgres", databaseUrl)
		if err != nil {
			panic(err)
		}
		defer db.Close()

		conn, err := net.Dial("tcp", ilpServer)
		if err != nil {
			panic(err)
		}
		defer conn.Close()

		serializer := lineprotocol.NewEncoder(conn)
		serializer.SetMaxLineBytes(1024)
		serializer.SetFieldTypeSupport(lineprotocol.UintSupport)

		sendPgStatActivityMetrics(db, serializer)
		sendPgStatStatementsMetrics(db, serializer)
	})

	c.Start()

	select {
	// TODO: handle signals case s := <-signalCh:
	case <-shutdownCh:
		fmt.Println("Shutting down...")
	}
}

func sendPgStatActivityMetrics(db *sql.DB, serializer *lineprotocol.Encoder) {
	rows, err := db.Query(pgStatActivitySql)

	if err != nil {
		panic(rows.Err())
	}
	defer rows.Close()
	for rows.Next() {
		statRow := &pgStatActivityRow{}
		err = rows.Scan(
			&statRow.Datid,
			&statRow.Datname,
			&statRow.Pid,
			&statRow.LeaderPID,
			&statRow.Usesysid,
			&statRow.Usename,
			&statRow.ApplicationName,
			&statRow.ClientAddr,
			&statRow.ClientHostname,
			&statRow.ClientPort,
			&statRow.BackendStart,
			&statRow.XactStart,
			&statRow.QueryStart,
			&statRow.StateChange,
			&statRow.WaitEventType,
			&statRow.WaitEvent,
			&statRow.State,
			&statRow.BackendXID,
			&statRow.BackendXmin,
			&statRow.Query,
			&statRow.BackendType,
		)
		if err != nil {
			panic(err)
		}
		event, err := lineprotocol.New("pg_stat_activity", statRow.Tags(), statRow.Fields(), statRow.QueryStart.Time)
		if err != nil {
			panic(err)
		}
		serializer.Encode(event)
	}
}

func sendPgStatStatementsMetrics(db *sql.DB, serializer *lineprotocol.Encoder) {
	var pgStatStementsExtensionExists bool
	err := db.QueryRow(pgStatStatementsExtensionCheckSql).Scan(&pgStatStementsExtensionExists)
	if err != nil {
		panic(err)
	}
	if !pgStatStementsExtensionExists {
		fmt.Println("pg_stat_statements extension not available on the database server")
		return
	}
	var postgresqlVersion string
	err = db.QueryRow(pgCheckVersion).Scan(&postgresqlVersion)
	if err != nil {
		return
	}

	if strings.Contains(postgresqlVersion, "postgres 11") {
		sendPg11StatStatementsMetrics(db, serializer)
	}
	if strings.Contains(postgresqlVersion, "postgres 13") {
		sendPg13StatStatementsMetrics(db, serializer)
	}
}

func sendPg11StatStatementsMetrics(db *sql.DB, serializer *lineprotocol.Encoder) {
	rows, err := db.Query(pg11StatStatementsSql)
	if err != nil {
		fmt.Printf("Failed to read rows from pg_stat_statements. Got %v\n", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		r := &pg11StatStatementsRow{}
		err = rows.Scan(
			&r.userid,
			&r.dbid,
			&r.queryid,
			&r.query,
			&r.calls,
			&r.total_time,
			&r.min_time,
			&r.max_time,
			&r.mean_time,
			&r.stddev_time,
			&r.rows,
			&r.shared_blks_hit,
			&r.shared_blks_read,
			&r.shared_blks_dirtied,
			&r.shared_blks_written,
			&r.local_blks_hit,
			&r.local_blks_read,
			&r.local_blks_dirtied,
			&r.local_blks_written,
			&r.temp_blks_read,
			&r.temp_blks_written,
			&r.blk_read_time,
			&r.blk_write_time,
		)
		if err != nil {
			panic(err)
		}
		event, err := lineprotocol.New("pg_stat_statements", r.Tags(), r.Fields(), time.Now())
		if err != nil {
			panic(err)
		}
		serializer.Encode(event)
	}
}

func sendPg13StatStatementsMetrics(db *sql.DB, serializer *lineprotocol.Encoder) {
	rows, err := db.Query(pg13StatStatementsSql)
	if err != nil {
		fmt.Printf("Failed to read rows from pg_stat_statements. Got %v\n", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		r := &pg13StatStatementsRow{}
		err = rows.Scan(
			&r.userid,
			&r.dbid,
			&r.queryid,
			&r.query,
			&r.plans,
			&r.total_plan_time,
			&r.min_plan_time,
			&r.max_plan_time,
			&r.mean_plan_time,
			&r.stddev_plan_time,
			&r.calls,
			&r.total_exec_time,
			&r.min_exec_time,
			&r.max_exec_time,
			&r.mean_exec_time,
			&r.stddev_exec_time,
			&r.rows,
			&r.shared_blks_hit,
			&r.shared_blks_read,
			&r.shared_blks_dirtied,
			&r.shared_blks_written,
			&r.local_blks_hit,
			&r.local_blks_read,
			&r.local_blks_dirtied,
			&r.local_blks_written,
			&r.temp_blks_read,
			&r.temp_blks_written,
			&r.blk_read_time,
			&r.blk_write_time,
			&r.wal_records,
			&r.wal_fpi,
			&r.wal_bytes,
		)
		if err != nil {
			panic(err)
		}
		event, err := lineprotocol.New("pg_stat_statements", r.Tags(), r.Fields(), time.Now())
		if err != nil {
			panic(err)
		}
		serializer.Encode(event)
	}
}

func addNullStringTag(m map[string]string, fieldName string, value sql.NullString) {
	if value.Valid {
		m[fieldName] = value.String
	}
}

func addNullInt64Field(m map[string]interface{}, fieldName string, value sql.NullInt64) {
	if value.Valid {
		m[fieldName] = value.Int64
	}
}

func addNullFloat64Field(m map[string]interface{}, fieldName string, value sql.NullFloat64) {
	if value.Valid {
		m[fieldName] = value.Float64
	}
}

func addNullTimeField(m map[string]interface{}, fieldName string, value pq.NullTime) {
	if value.Valid {
		m[fieldName] = value.Time.UnixNano()
	}
}
