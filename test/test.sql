/*****************************
 * Vertica Analytic Database
 *
 * exportdata User Defined Functions
 *
 * Copyright HP Vertica, 2013
 */



\o /dev/null
create schema if not exists exportdataTEST;
set search_path = public,exportdataTEST;

\set PWD '\''`pwd`'\''

create table if not exists exportdataTEST.NUMBERS(
  ID int
  , Name CHAR(20)
  , Descpt VARCHAR(20)
  , binValue VARBINARY(20) default E'\0x24'
  , bValue boolean default 'true'
  , dValue Date default sysdate
  , dtValue DateTime default sysdate
  , fValue FLOAT default 1
  , iValue int default 1
  , nValue NUMERIC(8,2) default 1
)
segmented by hash(ID) all nodes
;

truncate table exportdataTEST.NUMBERS;

copy exportdataTEST.NUMBERS(ID, Name, Descpt) from stdin delimiter ',' direct;
1,One,一
2,Two,二
3,Three,三
4,Four,四
5,Five,五
6,Six,六
7,Seven,七
8,Eight,八
9,Nigh,九
10,Ten,十
\.


\o

\echo export to file on each node ...
--select exportdata(ID, Name, Descpt
--	using parameters path=:PWD||'/test/export-utf8.txt.${nodeName}'
--	) over (partition auto) 
--  from exportdataTEST.NUMBERS;
select exportdata(ID, Name, Descpt, bValue, dValue, dtValue, fValue, iValue, nValue --, binValue
	using parameters path=:PWD||'/test/export-utf8.txt.${nodeName}'
	) over (partition auto) 
  from exportdataTEST.NUMBERS;


\echo export to file on each node, with gb18030 encoding ...
select exportdata(ID, Name, Descpt 
	using parameters path=:PWD||'/test/export-gbk.txt.${hostName}', separator=',', fromcharset='utf8', tocharset='gb18030'
	) over (partition auto) 
  from exportdataTEST.NUMBERS;

\echo export to saving cmd on each node, with gb18030 encoding ...
select exportdata(ID, Name, Descpt 
	using parameters cmd='cat - > '||:PWD||'/test/test.txt', separator=',', fromcharset='utf8', tocharset='gb18030'
	) over (partition auto) 
  from exportdataTEST.NUMBERS;

--\echo export to HADOOP/HDFS with gb18030 encoding ...
--select exportdata(ID, Name, Descpt 
--	using parameters cmd='
--(
--URL="http://v001:50070/webhdfs/v1/test.txt-`hostname`"; 
--USER="root"; 
--curl -X DELETE "$URL?op=DELETE&user.name=$USER"; 
--MSG=`curl -i -X PUT "$URL?op=CREATE&overwrite=true&user.name=$USER" 2>/dev/null | grep "Set-Cookie:\|Location:" | tr ''\r'' ''\n''`; 
--Ck=`echo $MSG | awk -F ''Location:'' ''{print $1}'' | awk -F ''Set-Cookie:'' ''{print $2}''| tr -d ''[:blank:]\r\n''`; 
--Loc=`echo $MSG | awk -F ''Location:'' ''{print $2}''| tr -d ''[:blank:]\r\n''`; 
--curl -b "$Ck" -X PUT -T - "$Loc";
--) 2>&1 > /dev/null
--	', separator=',', fromcharset='utf8', tocharset='gb18030'
--	) over (partition auto) 
--  from exportdataTEST.NUMBERS;

--\echo export to ftp with gb18030 encoding ...
--select exportdata(ID, Name, Descpt 
--	using parameters cmd='curl -X PUT -T - ftp://dbadmin:vertica@v001//data/export/test.txt-`hostname` 2>&1 > /dev/null'
--  , separator=',', fromcharset='utf8', tocharset='gb18030'
--	) over (partition auto) 
--  from exportdataTEST.NUMBERS;


drop schema exportdataTEST cascade;
