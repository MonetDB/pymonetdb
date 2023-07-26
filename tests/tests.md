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
EXPECT effective_binary!=0
```

```test
PARSE monetdb://?binary=0
EXPECT binary=0
EXPECT effective_binary=0
```

```test
PARSE monetdb://?binary=01
EXPECT binary=1
EXPECT effective_binary=1
```

```test
PARSE monetdb://?binary=0100
EXPECT binary=100
EXPECT effective_binary=100
```

```test
PARSE monetdb://?binary=true
EXPECT binary=true
EXPECT effective_binary!=0
```

```test
PARSE monetdb://?binary=yEs
EXPECT binary=true
EXPECT effective_binary!=0
```

```test
PARSE monetdb://?binary=on
EXPECT binary=true
EXPECT effective_binary!=0
```

```test
PARSE monetdb://?binary=fAlse
EXPECT binary=false
EXPECT effective_binary=0
```

```test
PARSE monetdb://?binary=no
EXPECT binary=false
EXPECT effective_binary=0
```

```test
PARSE monetdb://?binary=off
EXPECT binary=false
EXPECT effective_binary=0
```

```test
REJECT monetdb://?binary=banana
```

```test
PARSE monetdb://?replysize=100&fetchsize=200&replysize=300
EXPECT replysize=300
```


```test
EXPECT valid=true
```

```test
SET host=banana
SET port=1234
EXPECT valid=true
```

```test
SET port=1234
EXPECT valid=true
```

```test
SET host=banana
SET port=0
EXPECT valid=false
```

```test
SET host=banana
SET port=65536
EXPECT valid=false
```

```test
SET host=banana
SET port=-1
EXPECT valid=false
```

```test
SET password=banana
EXPECT valid=false
SET user=banana
EXPECT valid=true
```

```test
EXPECT valid=true
EXPECT NO use_tls
EXPECT effective_use_tls=false
SET use_tls=on
EXPECT effective_use_tls=true
SET use_tls=off
EXPECT effective_use_tls=false
```

```test
EXPECT NO host
EXPECT NO sock
EXPECT effective_tcp_host=localhost
SET sock=/tmp/sok
EXPECT NO effective_tcp_host
```


```test
EXPECT effective_tcp_host=localhost
EXPECT effective_port=50000
EXPECT effective_unix_sock=/tmp/.s.monetdb.50000
```


```test
SET use_tls=on
EXPECT effective_tcp_host=localhost
EXPECT effective_port=50000
EXPECT NO effective_unix_sock
```


```test
PARSE monetdb:///
EXPECT effective_tcp_host=localhost
EXPECT effective_port=50000
EXPECT effective_unix_sock=/tmp/.s.monetdb.50000
```

```test
PARSE monetdb://dbhost/
EXPECT effective_tcp_host=dbhost
EXPECT effective_port=50000
EXPECT NO effective_unix_sock
```

```skiptest
PARSE monetdb://:12345/
EXPECT effective_tcp_host=localhost
EXPECT effective_port=50000
EXPECT effective_unix_sock=/tmp/.s.monetdb.50000
```

```test
PARSE monetdb://dbhost:12345
EXPECT effective_tcp_host=dbhost
EXPECT effective_port=12345
EXPECT NO effective_unix_sock
```

```test
EXPECT effective_tcp_host=localhost
EXPECT effective_port=50000
EXPECT effective_unix_sock=/tmp/.s.monetdb.50000
```

```test
REJECT monetdb://dbhost:12345?port=3456
```

## Interaction between replysize and fetchsize

```test
PARSE monetdb:?
```
