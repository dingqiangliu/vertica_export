<html lang="zn_CN"> <head> <meta charset='utf-8'> <title>Export data out of Vertica in parallel</title> </head> <body>

Export data out of Vertica in parallel
==========
This is a Vertica User Defined Functions (UDF) to export data out of Vertica in parallel to local or shared filesystem, ftp, hdfs and other destinations.

![vertica_export](./imgs/vertica_export.png)

**Note**: This version only support string type values since Vertica6. 
Although columnName::varchar can help on this, but you have no chance convert the value type if you have many columns and want to use "select * ...".
[A new version ParallelExport for Vertica7+ (https://marketplace.saas.hpe.com/big-data/content/parallel-export)](https://marketplace.saas.hpe.com/big-data/content/parallel-export) may be helpful on this.

Syntax:
----------

EXPORTDATA (column1 [, column2, ...] [using parameters path=':path', cmd=':cmd', buffersize=:buffersize, separator=':separator', fromcharset=':fromcharset', 'tocharset=:tocharset'] ) over(partition auto)

Parameters:

 * columN: output columns.
 * path: data file path, must be writeable for each node Optional paramter, default value is '', means output to stdout.
   - If path includes ${nodeName}, each node will replace ${nodeName} with currentNodeName .
   - If path includes ${hostName}, each node will replace ${hostName} with current hostname .
 * cmd: data saving commands, eg:
   - saving data to a file: <code>cat - > /tmp/test.tx</code>t
   - saving data to HDFS: <code>cmd='hdfs dfs -put - /data/exp-$'||'{HOSTNAME} 2>&1 > /dev/null'</code> 
   - saving data to FTP: <code>cmd='curl -X PUT -T - ftp://username:password@ftpserver/data/exp-$'||'{HOSTNAME} 2>&1 > /dev/null'</code> 
 * buffersize: writing buffer size(bytes). Optional paramter, default value is 1024.
 * separator: separator string for concatenating. Optional paramter, default value is '|'.
 * fromcharset: source encoding. Optional paramter, default value is ''.
 * tocharset: target encoding. Optional paramter, default value is ''.
 * (return): statistics, such as output row numbers per node. 

Examples:
----------
export to file on each node:
<code><pre>
	select exportdata(ID, Name, Descpt
		using parameters path='/data/export-utf8.txt.${nodeName}'
	) over (partition auto) 
	from exportdataTEST.NUMBERS;
</code></pre>

export to file on each node, with gb18030 encoding ...
<code><pre>
	select exportdata(ID, Name, Descpt 
		using parameters path='/data/export-gbk.txt.${nodeName}', separator=','
		  , fromcharset='utf8', tocharset='gb18030'
		) over (partition auto) 
	from exportdataTEST.NUMBERS;
</code></pre>

export to ftp server:
<code><pre>
	select exportdata(ID, Name, Descpt 
		using parameters cmd='curl -X PUT -T - ftp://username:password@ftpserver/data/exp-${hostName} 2>&1 > /dev/null'
		  , separator=',', fromcharset='utf8', tocharset='gb18030'
		) over (partition auto) 
	from exportdataTEST.NUMBERS;
</code></pre>


Install, test and uninstall:
----------
Befoe build and install, g++ should be available(yum -y groupinstall "Development tools" && yum -y groupinstall "Additional Development" can help on this).

 * Build: make
 * Install: make install
 * Test: make run
 * Uninstall make uninstall

</body> </html>



