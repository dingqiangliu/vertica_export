# Export data out of Vertica in parallel

This is a Vertica User Defined Functions (UDF) to export data out of Vertica in parallel to local or shared filesystem, ftp, hdfs and other destinations.

![vertica_export](./imgs/vertica_export.png)

Following types are supported:

- BOOLEAN
- INTEGER
- FLOAT/DOUBLE PRECISION
- NUMERIC/DECIMAL/NUMBER/MONEY
- DATE
- DATETIME/TIMESTAMP
- TIMESTAMPTZ
- TIME
- TIMETZ
- INTERVAL DAY TO SECOND/INTERVAL YEAR TO MONTH
- BINARY/VARBINARY/LONG VARBINARY
- UUID
- CHAR/VARCHAR/LONG VARCHAR

**Note**: for unsupported type, please using columnName::varchar to convert it to text manually.

## Syntax

EXPORTDATA (column1 [, column2, ...] [using parameters path=':path', cmd=':cmd', buffersize=:buffersize, separator=':separator', fromcharset=':fromcharset', 'tocharset=:tocharset'] ) over(partition auto)

Parameters:

- columN: output columns.
- path: data file path, must be writeable for each node Optional paramter, default value is '', means output to stdout.
  - If path includes ${nodeName}, each node will replace ${nodeName} with currentNodeName.
  - If path includes ${hostName}, each node will replace ${hostName} with current hostname.
  - cmd: data saving commands, eg:
    - saving data to a file: ```cat - > /tmp/test.tx```
    - saving data to HDFS: ```cmd='hdfs dfs -put - /data/exp-$'||'{HOSTNAME} 2>&1 > /dev/null'```
    - saving data to FTP: ```cmd='curl -X PUT -T - ftp://username:password@ftpserver/data/exp-$'||'{HOSTNAME} 2>&1 > /dev/null'```
- buffersize: writing buffer size(bytes). Optional paramter, default value is 1024.
- separator: separator string for concatenating. Optional paramter, default value is '|'.
- fromcharset: source encoding. Optional paramter, default value is ''.
- tocharset: target encoding. Optional paramter, default value is ''.
- (return): statistics, such as output row numbers per node.

## Examples

export to file on each node:

```SQL
select exportdata(*
  sing parameters path='/data/export-utf8.txt.${nodeName}'
) over (partition auto)
from exportdataTEST.NUMBERS;
```

export to file on each node, with gb18030 encoding ...

```SQL
select exportdata(*
  using parameters path='/data/export-gbk.txt.${nodeName}', separator=','
  , fromcharset='utf8', tocharset='gb18030'
  ) over (partition auto)
from exportdataTEST.NUMBERS;
```

export to ftp server:

```SQL
select exportdata(*
  using parameters cmd='curl -X PUT -T - ftp://username:password@ftpserver/data/exp-${hostName} 2>&1 > /dev/null'
  , separator=',', fromcharset='utf8', tocharset='gb18030'
  ) over (partition auto)
from exportdataTEST.NUMBERS;
```

## Install, test and uninstall

Befoe build and install, g++ should be available(yum -y groupinstall "Development tools" && yum -y groupinstall "Additional Development" can help on this).

- Build: make
- Install: make install
- Test: make run
- Uninstall make uninstall
