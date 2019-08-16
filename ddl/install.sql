/*****************************
 * Vertica Analytic Database
 *
 * exportdata User Defined Functions
 *
 * Copyright HP Vertica, 2013
 */

-- Step 1: Create LIBRARY 
\set libname ParallelExport
\set strlibname '''':libname''''
\set language '''C++'''

--\set isfenced 'not fenced'
\set isfenced 'fenced'


\set strlibfile '\''`pwd`'/lib/ParallelExport.so\'';
CREATE OR REPLACE LIBRARY :libname AS :strlibfile ;

-- Step 2: Create cube/rollup Factory
\set tmpfile '/tmp/exportdatainstall.sql'
\! cat /dev/null > :tmpfile

\t
\o :tmpfile
select 'CREATE OR REPLACE TRANSFORM FUNCTION '||replace(obj_name, 'Factory', '')||' AS LANGUAGE '''||:language||''' NAME '''||obj_name||''' LIBRARY :libname :isfenced ;' from user_library_manifest where lib_name=:strlibname and obj_name ilike 'ExportdataFactory%';
select 'GRANT EXECUTE ON TRANSFORM FUNCTION '||lower(replace(split_part(obj_name, '.', 1+regexp_count(obj_name, '\.')), 'Factory', ''))||' (' || arg_types || ') to PUBLIC;' from user_library_manifest where lib_name=:strlibname and obj_type='Transform Function';

select 'CREATE OR REPLACE TRANSFORM FUNCTION ParallelExport AS LANGUAGE '''||:language||''' NAME '''||obj_name||''' LIBRARY :libname :isfenced ;' from user_library_manifest where lib_name=:strlibname and obj_name ilike 'ExportdataFactory%';
select 'GRANT EXECUTE ON TRANSFORM FUNCTION ParallelExport (' || arg_types || ') to PUBLIC;' from user_library_manifest where lib_name=:strlibname and obj_type='Transform Function';

\o
\t

\i :tmpfile
\! rm -f :tmpfile
