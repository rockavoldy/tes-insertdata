# Test insert 1 millions data

Trying with golang, using goroutine, channel, and waitgroup to insert 1 millions row data to mongodb collection.

Download [majestic_million.csv](http://downloads.majestic.com/majestic_million.csv) first, then run

```shell
go run main.go
```