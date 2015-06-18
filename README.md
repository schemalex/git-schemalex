# git-schemalex

database migration tool for mysql schema is managed via git.

## DOWNLOAD

```
$ go get github.com/soh335/git-schemalex/cmd/git-schemalex
```

## USAGE

```
$ git schemalex -schema path/to/schema -dns "$root:$passowrd@/$database" -workspace /path/to/git/repository -deploy
```

### DNS

see [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)

## SEE ALSO

* https://github.com/soh335/schemalex
* https://github.com/typester/GitDDL

## LICENSE

MIT
