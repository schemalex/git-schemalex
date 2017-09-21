# git-schemalex

database migration tool for mysql schema is managed via git.

## DOWNLOAD

```
$ go get github.com/schemalex/git-schemalex/cmd/git-schemalex
```

## USAGE

```
$ git schemalex -schema path/to/schema -dsn "$root:$passowrd@/$database" -workspace /path/to/git/repository -deploy
```

### DSN

see [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)

## SEE ALSO

* https://github.com/schemalex/schemalex
* https://github.com/typester/GitDDL

## LICENSE

MIT
