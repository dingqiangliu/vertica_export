/*****************************
 * Vertica Analytic Database
 *
 * exportdata User Defined Functions
 *
 * Copyright HP Vertica, 2013
 */

-- Step 1: Create LIBRARY 
\set libfile '\''`pwd`'/lib/exportdata.so\'';
CREATE LIBRARY exportdata AS :libfile;

-- Step 2: Create cube/rollup Factory
\set tmpfile '/tmp/exportdatainstall.sql'
\! cat /dev/null > :tmpfile

\t
\o :tmpfile
select 'CREATE TRANSFORM FUNCTION exportdata AS LANGUAGE ''C++'' NAME '''||obj_name||''' LIBRARY exportdata;' from user_library_manifest where lib_name='exportdata' and obj_name ilike 'ExportdataFactory%';

\o
\t

\i :tmpfile
\! rm -f :tmpfile
