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

create table if not exists exportdataTEST.TYPTES(
  ID int
  , Name CHAR(20)
  , Descpt VARCHAR(20)
  , boolValue BOOLEAN default 'true'
  , intValue INT default 1234567890123456789
  , floatValue FLOAT default 1.23
  , numericValue NUMERIC(18,2) default 1234567890123456.90
  , lnumericValue NUMERIC(37,2) default 123456789012345678901234567890.12
  , dateValue DATE default '2012-01-23'
  , datetimeValue DATETIME default '2012-01-23 01:23:45.0678'
  , timestamptimetzValue TIMESTAMPTZ default '2012-01-23 01:23:45.0678+08'
  , timeValue TIME default '01:23:45.0678'
  , timetzValue TIMETZ default '01:23:45.0678+08'
  , intervald2sValue INTERVAL DAY TO SECOND default '10 days 23:34:56.0789'
  , intervaly2mValue INTERVAL YEAR TO MONTH default '3 years 2 months'
  , binaryValue VARBINARY(20) default E'1234567890\tA-Za-z'
  , uuidValue UUID default '6bbf0744-74b4-46b9-bb05-53905d4538e7'
)
segmented by hash(ID) all nodes
;

truncate table exportdataTEST.TYPTES;

copy exportdataTEST.TYPTES(ID, Name, Descpt) from stdin delimiter ',' direct;
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
select exportdata(* 
  using parameters path='/tmp/export.txt.${nodeName}'
  ) over (partition auto) 
from exportdataTEST.TYPTES;

\echo export to file on each node, with gb18030 encoding ...
select exportdata(ID, Name, Descpt 
  using parameters path='/tmp/export-gbk.txt.${hostName}', separator=',', fromcharset='utf8', tocharset='gb18030'
  ) over (partition auto) 
from exportdataTEST.TYPTES;

\echo export to saving cmd on each node, with gb18030 encoding ...
select exportdata(ID, Name, Descpt 
  using parameters cmd='cat - > /tmp/export-utf8.txt.${nodeName}', separator=',', fromcharset='utf8', tocharset='utf8'
  ) over (partition auto) 
from exportdataTEST.TYPTES;

--\echo export to HADOOP/HDFS with gb18030 encoding ...
--select exportdata(ID, Name, Descpt 
--  using parameters cmd='
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
--from exportdataTEST.TYPTES;

--\echo export to ftp with gb18030 encoding ...
--select exportdata(ID, Name, Descpt 
--  using parameters cmd='curl -X PUT -T - ftp://dbadmin:vertica@v001//data/export/test.txt-`hostname` 2>&1 > /dev/null'
--  , separator=',', fromcharset='utf8', tocharset='gb18030'
--	) over (partition auto) 
--from exportdataTEST.TYPTES;


drop schema exportdataTEST cascade;
