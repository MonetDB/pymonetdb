# Tests

```test
PARSE monetdb://dbhost:12345/mydb
EXPECT host=dbhost
EXPECT port=12345
EXPECT database=mydb
EXPECT NO user
EXPECT NO password
EXPECT NO sock
```


```test
PARSE monetdb:///my%41db
EXPECT NO host
EXPECT NO port
EXPECT database=myAdb
EXPECT NO user
EXPECT NO password
EXPECT NO sock
```

```test
REJECT monetdb:///m%xxbad
```

```test
PARSE monetdb://dbhost:12345/mydb?user=claude&password=m%26ms
EXPECT host=dbhost
EXPECT port=12345
EXPECT database=mydb
EXPECT user=claude
EXPECT password=m&ms
EXPECT NO sock
```

```test
PARSE monetdb://dbhost:12345/mydb?user=claude&password=m%26ms
EXPECT host=dbhost
EXPECT port=12345
EXPECT database=mydb
EXPECT user=claude
EXPECT password=m&ms
EXPECT NO sock
```

```test
PARSE monetdb://banana/
EXPECT NO port
EXPECT host=banana
EXPECT NO database
```

```test
PARSE monetdb://banana
EXPECT NO port
EXPECT host=banana
EXPECT NO database
```


```test
PARSE monetdb://[2001::2a]
EXPECT NO port
EXPECT host=2001::2a
EXPECT NO database
```

Python does not reject this:
maybe they're right, the square brackets
have a tricky meaning.
what does java do?

```skiptest
REJECT monetdb://[2001::%2a]
EXPECT NO port
EXPECT NO host
EXPECT NO database
```

```test
SET user=alan
SET password=turing
PARSE monetdbs:///
EXPECT user=alan
EXPECT password=turing
```

```test
SET user=alan
SET password=turing
PARSE monetdbs:///?user=alan
EXPECT user=alan
EXPECT password=turing
```

```test
SET user=alan
SET password=turing
PARSE monetdbs:///?user=mathison
EXPECT user=mathison
EXPECT NO password
```

```test
PARSE monetdb:///
EXPECT NO binary
```

```test
PARSE monetdb://?binary=0
EXPECT binary=0
```

```test
PARSE monetdb://?binary=01
EXPECT binary=1
```

```test
PARSE monetdb://?binary=0100
EXPECT binary=100
```


```test
PARSE monetdb://?binary=true
EXPECT binary=true
```

```test
PARSE monetdb://?binary=yEs
EXPECT binary=true
```

```test
PARSE monetdb://?binary=on
EXPECT binary=true
```

```test
PARSE monetdb://?binary=fAlse
EXPECT binary=false
```

```test
PARSE monetdb://?binary=no
EXPECT binary=false
```

```test
PARSE monetdb://?binary=off
EXPECT binary=false
```

```test
REJECT monetdb://?binary=banana
```

```test
PARSE monetdb://?replysize=100&fetchsize=200&replysize=300
EXPECT replysize=300
```
