pgstat2ilp
==========

`pgstat2ilp` is a command-line program for exporting output from `pg_stat_activity` and `pg_stat_statements` (if the extension is installed/enabled) periodically  from Postgresql databases into time-series databases that support the Influx Line Protocol (ILP).

Currently, you will have to build it from source, binary Releases may be made available soon.

```sh
$ git clone https://github.com/zikani03/pgstat2ilp.git
$ cd pgstat2ilp
$ go build
```

## Usage

By default, pgstat2ilp will export data to the time-series database every thirty seconds (30s), you can customize the time with `-every` argument e.g. `-every 1h`

```sh
$ ./pgstat2ilp -dsn "postgres://postgres:password@localhost:5432/postgres?sslmode=disable" -influx "localhost:9009"
```

## License

MIT License

---

Copyright (c) 2021, Zikani Nyirenda Mwase, NNDI 
