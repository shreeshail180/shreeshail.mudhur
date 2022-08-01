CREATE OR REPLACE PROCEDURE "MERGE_INCREMENTAL_SP_DIGITAL_BULK_V2"("OPTIONS" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS ' 

/*
* Stored procedure to generate merge statement based upon the metadata
* and execute the merge to integrate the changes from a multi tables to target table
*
* @Project : Harmony
*/

var result = '''';

try
{

var src_json = JSON.parse(OPTIONS);
  var src_table_name = src_json.source_tables.toUpperCase() ;
  var target_tables = src_json.target_tables.toUpperCase() ;
  var metadata_db_schema = src_json.metadata.toUpperCase() ;
  var log_db_nm = src_json.log_db.toUpperCase() ;
  var ins_rec_count = 0 ;
  var upd_rec_count = 0 ;
  var main_query_id = '''';
  var aud_files_names= '''';
  var delta_type= src_json.deltatype.toUpperCase();
  
  
  //return src_table_name+target_tables+metadata;
  //return src_table_name;
  
  


// get source and target db details
var sql_db_name_str = "SELECT distinct sdb.db_name source_db_name,source_schema,target_schema, \\n"
                     +" tdb.db_name target_db_name,target_schema \\n"
                     +" FROM "+ metadata_db_schema +"."+"source_to_target_entity_mapping e \\n"  
                     +" JOIN table("+ metadata_db_schema +".db_name_fn()) sdb \\n"
                     +" ON e.source_db_key = sdb.db_key \\n"
                     +" JOIN table("+ metadata_db_schema +".db_name_fn()) tdb \\n"
                     +" ON e.target_db_key = tdb.db_key  \\n"
                     +" WHERE UPPER(source_entity_name) in ("+ src_table_name +" ) \\n"
                     +" AND UPPER(target_entity_name) = UPPER(''"+ target_tables +"'');"

var env_db_sql_stmt = snowflake.createStatement(
{
sqlText: sql_db_name_str
}
);
var env_db_sql_rs= env_db_sql_stmt.execute();
env_db_sql_rs.next(); 
var    src_db_name = env_db_sql_rs.getColumnValue(1);
var src_schema_name = env_db_sql_rs.getColumnValue(2);
var tgt_schema_name = env_db_sql_rs.getColumnValue(3);
var    tgt_db_name = env_db_sql_rs.getColumnValue(4);
var tgt_schema_name = env_db_sql_rs.getColumnValue(5);

result = result + " 1.source and target database are fetched for "+target_tables+ "\\n"

var start_time = "select current_timestamp";

result_start_time = snowflake.execute(
{
sqlText: start_time
});

';