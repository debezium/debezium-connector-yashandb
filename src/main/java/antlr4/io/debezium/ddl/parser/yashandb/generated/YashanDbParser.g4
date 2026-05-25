// $antlr-format alignTrailingComments true, columnLimit 150, minEmptyLines 1, maxEmptyLinesToKeep 1, reflowComments false, useTab false
// $antlr-format allowShortRulesOnASingleLine false, allowShortBlocksOnASingleLine true, alignSemicolons hanging, alignColons hanging

parser grammar YashanDbParser;

options {
    tokenVocab = YashanDbLexer;
    // superClass = PlSqlParserBase;
}

sql_script
    : unit_statement (SEMICOLON '/'? unit_statement)* SEMICOLON? '/'? EOF
    ;

unit_statement
    : data_manipulation_language_statements
    | data_definition_language_statements
    ;

data_manipulation_language_statements
    : select_statement
    | insert_statement
    | update_statement
    | delete_statement
    | merge_statement
    ;

data_definition_language_statements
    : create_table_statement
    | create_index_statement
    | create_sequence_statement
    | create_view_statement
    | create_table_as_statement
    | create_database_statement
    | create_materialized_view_statement
    | drop_table_statement
    | drop_index_statement
    | drop_view_statement
    | drop_sequence_statement
    | drop_database_statement
    | drop_materialized_view_statement
    | alter_table_statement
    | alter_index_statement
    | alter_sequence_statement
    | alter_database_statement
    | alter_tablespace_statement
    | alter_materialized_view_statement
    | create_user_statement
    | alter_user_statement
    | drop_user_statement
    | create_role_statement
    | drop_role_statement
    | create_profile_statement
    | alter_profile_statement
    | drop_profile_statement
    | create_tablespace_statement
    | drop_tablespace_statement
    | create_synonym_statement
    | drop_synonym_statement
    | create_directory_statement
    | drop_directory_statement
    | rename_statement
    | comment_statement
    | truncate_table_statement
    | grant_statement
    | revoke_statement
    | audit_statement
    | noaudit_statement
    | backup_database_statement
    | restore_database_statement
    | recover_database_statement
    | create_pluggable_database_statement
    | drop_pluggable_database_statement
    | alter_pluggable_database_statement
    | create_tablespace_set_statement
    | drop_tablespace_set_statement
    | alter_tablespace_set_statement
    | create_database_link_statement
    | drop_database_link_statement
    | alter_database_link_statement
    | create_context_statement
    | drop_context_statement
    | create_outline_statement
    | drop_outline_statement
    | alter_outline_statement
    | drop_procedure_statement
    | drop_function_statement
    | drop_package_statement
    | drop_trigger_statement
    | drop_type_statement
    | analyze_statement
    | commit_statement
    | rollback_statement
    | savepoint_statement
    | explain_statement
    | set_transaction_statement
    | lock_table_statement
    | purge_statement
    | use_statement
    | flashback_statement
    | alter_session_statement
    | alter_system_statement
    | create_sqlmap_statement
    | drop_sqlmap_statement
    | create_audit_policy_statement
    | drop_audit_policy_statement
    | alter_audit_policy_statement
    | alter_procedure_statement
    | alter_function_statement
    | alter_package_statement
    | alter_trigger_statement
    | alter_type_statement
    | drop_type_body_statement
    | backup_archivelog_statement
    | restore_archivelog_statement
    | release_savepoint_statement
    | shutdown_statement
    | administer_key_management_statement
    | create_restore_point_statement
    | drop_restore_point_statement
    ;

create_table_statement
    : CREATE TABLE if_not_exists? table_name '(' relation_properties ')' table_options? partition_options?
    ;

create_index_statement
    : CREATE UNIQUE? INDEX index_name index_type? ON table_name '(' index_expr (',' index_expr)* ')' index_option* index_type?
    ;

create_sequence_statement
    : CREATE SEQUENCE sequence_name (sequence_option)*
    ;

sequence_name
    : identifier
    | schema '.' identifier
    ;

sequence_option
    : INCREMENT BY UNSIGNED_INTEGER
    | START WITH UNSIGNED_INTEGER
    | MAXVALUE UNSIGNED_INTEGER
    | NOMAXVALUE
    | MINVALUE UNSIGNED_INTEGER
    | NOMINVALUE
    | CYCLE
    | NOCYCLE
    | ORDER
    | NOORDER
    | CACHE UNSIGNED_INTEGER
    | NOCACHE
    ;

create_view_statement
    : CREATE VIEW identifier AS select_statement with_check_option?
    | CREATE VIEW schema '.' identifier AS select_statement with_check_option?
    | CREATE OR REPLACE VIEW identifier AS select_statement with_check_option?
    | CREATE OR REPLACE VIEW schema '.' identifier AS select_statement with_check_option?
    ;

with_check_option
    : WITH (CASCADED | LOCAL)? CHECK OPTION
    ;

create_table_as_statement
    : CREATE (GLOBAL | PRIVATE)? TEMPORARY? TABLE table_name AS select_statement
    | CREATE (GLOBAL | PRIVATE)? TEMPORARY? TABLE schema '.' table_name AS select_statement
    ;

create_database_statement
    : CREATE DATABASE if_not_exists? database_name (DEFAULT? CHARACTER SET charset_name (DEFAULT? COLLATE collation_name)?)?
    ;

database_name
    : identifier
    ;

drop_table_statement
    : DROP TABLE if_exists? table_name CASCADE? CONSTRAINTS? PURGE?
    | DROP TABLE if_exists? schema '.' table_name CASCADE? CONSTRAINTS? PURGE?
    ;

drop_index_statement
    : DROP INDEX index_name ON table_name
    | DROP INDEX index_name ON schema '.' table_name
    ;

drop_view_statement
    : DROP VIEW if_exists? identifier
    | DROP VIEW if_exists? schema '.' identifier
    ;

drop_sequence_statement
    : DROP SEQUENCE sequence_name
    | DROP SEQUENCE schema '.' sequence_name
    ;

drop_database_statement
    : DROP DATABASE if_exists? database_name
    ;

alter_sequence_statement
    : ALTER SEQUENCE sequence_name sequence_option+
    | ALTER SEQUENCE schema '.' sequence_name sequence_option+
    ;

alter_database_statement
    : ALTER DATABASE database_name DEFAULT? CHARACTER SET charset_name
    ;

// Materialized View Statements
create_materialized_view_statement
    : CREATE MATERIALIZED VIEW if_not_exists? identifier
      ( '(' column_name (',' column_name)* ')' )?
      (TABLESPACE tablespace_name)?
      (BUILD (IMMEDIATE | DEFERRED))?
      create_mv_refresh_clause?
      query_rewrite_clause?
      AS select_statement
    ;

create_mv_refresh_clause
    : REFRESH (COMPLETE | FORCE | FAST | NEVER)?
      (ON DEMAND | ON COMMIT)?
      (START WITH expression | NEXT expression)*
    ;

query_rewrite_clause
    : ENABLE? DISABLE? QUERY REWRITE
    ;

drop_materialized_view_statement
    : DROP MATERIALIZED VIEW if_exists? identifier
    ;

materialized_view_name
    : identifier
    ;

alter_materialized_view_statement
    : ALTER MATERIALIZED VIEW materialized_view_name (alter_mv_refresh_clause | alter_mv_compose | alter_mv_parallel)?
    ;

alter_mv_refresh_clause
    : REFRESH (COMPLETE | FORCE | FAST | NEVER)
    | START WITH expression
    | NEXT expression
    ;

alter_mv_compose
    : COMPOSE
    ;

alter_mv_parallel
    : PARALLEL UNSIGNED_INTEGER
    ;

// Backup and Restore Statements
backup_database_statement
    : BACKUP DATABASE (FULL | INCREMENTAL LEVEL UNSIGNED_INTEGER)? (FORCE)? (TAG identifier)?
    | BACKUP DATABASE DELETE BACKUPSET (IF EXISTS)? (TAG identifier)?
    | BACKUP DATABASE CANCEL
    ;

restore_database_statement
    : RESTORE DATABASE (FROM TAG identifier)?
    ;

recover_database_statement
    : RECOVER DATABASE
    | RECOVER DATABASE UNTIL (TIME expression | SCN expression | CANCEL)
    | RECOVER DATABASE FROM SEQUENCE expression
    ;

// Pluggable Database Statements
create_pluggable_database_statement
    : CREATE PLUGGABLE DATABASE identifier (COMPAT_MODE '=' (YASHAN | MYSQL))?
    ;

drop_pluggable_database_statement
    : DROP PLUGGABLE DATABASE identifier
    | DROP PLUGGABLE DATABASE ALL
    | DROP PLUGGABLE DATABASE identifier INCLUDING ARCHIVELOG
    | DROP PLUGGABLE DATABASE identifier KEEP LOGFILE
    | DROP PLUGGABLE DATABASE identifier KEEP METADATA
    ;

alter_pluggable_database_statement
    : ALTER PLUGGABLE DATABASE (identifier (',' identifier)* | ALL)
      (NOMOUNT | MOUNT | OPEN | CLOSE | STARTUP | SHUTDOWN)
    ;

// Tablespace Set Statements
create_tablespace_set_statement
    : CREATE TABLESPACE SET identifier ON identifier
    ;

drop_tablespace_set_statement
    : DROP TABLESPACE SET identifier
    ;

alter_tablespace_set_statement
    : ALTER TABLESPACE SET identifier
    ;

// Database Link Statements
create_database_link_statement
    : CREATE PUBLIC? DATABASE LINK identifier CONNECT TO identifier IDENTIFIED BY expression USING expression
    ;

drop_database_link_statement
    : DROP DATABASE LINK identifier
    ;

alter_database_link_statement
    : ALTER DATABASE LINK identifier
    ;

// Context Statements
create_context_statement
    : CREATE OR REPLACE? CONTEXT identifier USING identifier
    ;

drop_context_statement
    : DROP CONTEXT identifier
    ;

// Outline Statements
create_outline_statement
    : CREATE OR REPLACE? PUBLIC? OUTLINE identifier ON expression
    | CREATE OR REPLACE? PUBLIC? OUTLINE identifier FROM PUBLIC? identifier FOR CATEGORY identifier ON expression
    ;

drop_outline_statement
    : DROP OUTLINE identifier
    ;

alter_outline_statement
    : ALTER OUTLINE identifier
    ;

// DROP PROCEDURE/FUNCTION/PACKAGE/TRIGGER/TYPE
drop_procedure_statement
    : DROP PROCEDURE if_exists? schema '.' identifier
    | DROP PROCEDURE if_exists? identifier
    ;

drop_function_statement
    : DROP FUNCTION if_exists? schema '.' identifier
    | DROP FUNCTION if_exists? identifier
    ;

drop_package_statement
    : DROP PACKAGE if_exists? schema '.' identifier
    | DROP PACKAGE if_exists? identifier
    ;

drop_trigger_statement
    : DROP TRIGGER if_exists? schema '.' identifier
    | DROP TRIGGER if_exists? identifier
    ;

drop_type_statement
    : DROP TYPE if_exists? schema '.' identifier
    | DROP TYPE if_exists? identifier
    ;

// ANALYZE Statement
analyze_statement
    : ANALYZE TABLE table_name
    | ANALYZE TABLE table_name ESTIMATE PERCENT UNSIGNED_INTEGER
    | ANALYZE TABLE table_name COMPUTE STATISTICS
    | ANALYZE SCHEMA identifier
    | ANALYZE DATABASE
    ;

// Transaction Control Statements
commit_statement
    : COMMIT WORK?
    ;

rollback_statement
    : ROLLBACK WORK?
    | ROLLBACK WORK? TO SAVEPOINT identifier
    ;

savepoint_statement
    : SAVEPOINT identifier
    ;

// EXPLAIN Statement
explain_statement
    : EXPLAIN PLAN FOR select_statement
    | EXPLAIN select_statement
    ;

// SET TRANSACTION Statement
set_transaction_statement
    : SET TRANSACTION READ ONLY
    | SET TRANSACTION READ WRITE
    ;

// LOCK TABLE Statement
lock_table_statement
    : LOCK TABLE table_name IN (SHARE | EXCLUSIVE) MODE
    ;

// PURGE Statement
purge_statement
    : PURGE (TABLE table_name | INDEX index_name | RECYCLEBIN)
    ;

// USE Statement
use_statement
    : USE database_name
    ;

// FLASHBACK Statement
flashback_statement
    : FLASHBACK TABLE table_name TO TIMESTAMP expression
    | FLASHBACK TABLE table_name TO BEFORE DROP
    ;

// ALTER SESSION Statement
alter_session_statement
    : ALTER SESSION SET identifier '=' expression
    ;

// ALTER SYSTEM Statement
alter_system_statement
    : ALTER SYSTEM SET identifier '=' expression
    | ALTER SYSTEM SWITCH LOGFILE
    | ALTER SYSTEM CHECKPOINT
    ;

// SQLMAP Statements
create_sqlmap_statement
    : CREATE SQLMAP identifier
    ;

drop_sqlmap_statement
    : DROP SQLMAP identifier
    ;

// AUDIT POLICY Statements
create_audit_policy_statement
    : CREATE AUDIT POLICY identifier
    ;

drop_audit_policy_statement
    : DROP AUDIT POLICY identifier
    ;

alter_audit_policy_statement
    : ALTER AUDIT POLICY identifier
    ;

// ALTER PROCEDURE/FUNCTION/PACKAGE/TRIGGER/TYPE
alter_procedure_statement
    : ALTER PROCEDURE identifier COMPILE
    ;

alter_function_statement
    : ALTER FUNCTION identifier COMPILE
    ;

alter_package_statement
    : ALTER PACKAGE identifier COMPILE
    ;

alter_trigger_statement
    : ALTER TRIGGER identifier ENABLE
    | ALTER TRIGGER identifier DISABLE
    | ALTER TRIGGER identifier COMPILE
    ;

alter_type_statement
    : ALTER TYPE identifier COMPILE
    ;

drop_type_body_statement
    : DROP TYPE BODY identifier
    ;

// BACKUP/RESTORE ARCHIVELOG
backup_archivelog_statement
    : BACKUP ARCHIVELOG ALL
    | BACKUP ARCHIVELOG FROM SEQUENCE UNSIGNED_INTEGER
    ;

restore_archivelog_statement
    : RESTORE ARCHIVELOG ALL
    | RESTORE ARCHIVELOG FROM SEQUENCE UNSIGNED_INTEGER
    ;

// RELEASE SAVEPOINT
release_savepoint_statement
    : RELEASE SAVEPOINT identifier
    ;

// SHUTDOWN
shutdown_statement
    : SHUTDOWN IMMEDIATE
    | SHUTDOWN ABORT
    | SHUTDOWN TRANSACTIONAL
    ;

// ADMINISTER KEY MANAGEMENT
administer_key_management_statement
    : ADMINISTER KEY MANAGEMENT SET KEY IDENTIFIED BY expression
    | ADMINISTER KEY MANAGEMENT CREATE KEY IDENTIFIED BY expression
    ;

// RESTORE POINT
create_restore_point_statement
    : CREATE RESTORE POINT identifier
    ;

drop_restore_point_statement
    : DROP RESTORE POINT identifier
    ;

alter_table_statement
    : ALTER TABLE table_name alter_table_action
    | ALTER TABLE schema '.' table_name alter_table_action
    ;

alter_table_action
    : rename_clause
    | alter_column_clause
    | alter_index_clause
    | alter_constraint_clause
    | add_table_partition
    | drop_table_partition
    ;

rename_clause
    : RENAME TO table_name
    | RENAME AS table_name
    ;

alter_column_clause
    : ADD COLUMN? column_definition
    | MODIFY COLUMN? column_definition
    | CHANGE COLUMN? column_name column_name datatype column_constraint*
    | RENAME COLUMN column_name TO column_name
    | DROP COLUMN? '(' column_name (',' column_name)* ')'
    ;

alter_index_clause
    : ADD (UNIQUE INDEX | UNIQUE KEY | PRIMARY KEY | INDEX | KEY) index_name? '(' index_expr (',' index_expr)* ')' index_type?
    | DROP INDEX index_name
    | DROP PRIMARY KEY
    | DROP (KEY | INDEX) index_name
    ;

alter_constraint_clause
    : ADD CONSTRAINT constraint_name (UNIQUE '(' column_name (',' column_name)* ')'
        | PRIMARY KEY '(' column_name (',' column_name)* ')'
        | FOREIGN KEY '(' column_name (',' column_name)* ')' references_clause)
    | DROP PRIMARY KEY
    | DROP FOREIGN KEY constraint_name
    | DROP CONSTRAINT constraint_name
    ;

references_clause
    : REFERENCES table_name ('(' column_name ')')?
    ;

constraint_name
    : identifier
    ;

add_table_partition
    : ADD PARTITION (PARTITION partition_name? range_values_clause partition_storage_clause?
        | PARTITION partition_name? list_values_clause partition_storage_clause?
        | PARTITION partition_name? '(' partition_name ')' partition_storage_clause?
        | partition_name? range_values_clause partition_storage_clause?
        | partition_name? list_values_clause partition_storage_clause?
        | partition_name? '(' partition_name ')' partition_storage_clause?)
    ;

drop_table_partition
    : DROP PARTITION partition_name (',' partition_name)* (UPDATE | INVALIDATE)? GLOBAL? INDEXES?
    ;

alter_index_statement
    : ALTER INDEX index_name alter_index_action
    | ALTER INDEX schema '.' index_name alter_index_action
    ;

alter_index_action
    : INITRANS UNSIGNED_INTEGER
    | VISIBLE
    | INVISIBLE
    | UNUSABLE
    | COALESCE CLEANUP?
    | NOPARALLEL
    | PARALLEL UNSIGNED_INTEGER
    | NOLOGGING
    | LOGGING
    | modify_partition
    | modify_subpartition
    | rebuild_clause
    | RENAME TO index_name
    | reclaim_index_clause
    | COMPUTE STATISTICS
    ;

modify_partition
    : MODIFY PARTITION partition_name (INITRANS UNSIGNED_INTEGER | UNUSABLE | COALESCE CLEANUP?)
    ;

modify_subpartition
    : MODIFY SUBPARTITION subpartition_name (UNUSABLE | COALESCE CLEANUP?)
    ;

rebuild_clause
    : REBUILD (PARTITION partition_name | SUBPARTITION subpartition_name | NOREVERSE | REVERSE)?
      (TABLESPACE tablespace_name | INITRANS UNSIGNED_INTEGER | PCTFREE UNSIGNED_INTEGER | ONLINE | COMPRESS UNSIGNED_INTEGER? | NOCOMPRESS | LOGGING | NOLOGGING | NOPARALLEL | PARALLEL UNSIGNED_INTEGER | COMPUTE STATISTICS)*
    ;

reclaim_index_clause
    : RECLAIM SEGMENT (PARTITION partition_name)? TABLESPACE tablespace_name
    ;

subpartition_name
    : identifier
    ;

if_exists
    : IF EXISTS
    ;

index_expr
    : column_name (ASC | DESC)?
    | expression (ASC | DESC)?
    ;

if_not_exists
    : IF NOT EXISTS
    ;

table_name
    : identifier
    | schema '.' identifier
    ;

schema
    : identifier
    ;

relation_properties
    : column_definition (',' (column_definition | index_definition))*
    ;

column_definition
    : column_name datatype column_constraint*
    ;

column_constraint
    : CHARACTER SET charset_name
    | COLLATE collation_name
    | BINARY
    | COMMENT quoted_string
    | DEFAULT default_expr
    | ON UPDATE CURRENT_TIMESTAMP
    | inline_constraint
    | AUTO_INCREMENT
    ;

inline_constraint
    : PRIMARY KEY
    | UNIQUE
    | NOT NULL
    | NULL
    | CHECK '(' condition ')'
    | REFERENCES table_name ('(' column_name ')')?
    ;

index_definition
    : (INDEX | KEY) index_name? index_type? '(' column_name (',' column_name)* ')' index_option*
    | (INDEX | KEY) index_name? index_option* '(' column_name (',' column_name)* ')' index_type?
    | PRIMARY KEY '(' column_name (',' column_name)* ')'
    | UNIQUE (INDEX | KEY)? index_name? '(' column_name (',' column_name)* ')'
    ;

index_name
    : identifier
    ;

index_type
    : USING (BTREE | HASH)
    ;

index_option
    : KEY_BLOCK_SIZE '='? UNSIGNED_INTEGER
    | ALGORITHM '='? (DEFAULT | INPLACE | COPY)
    | LOCK '='? (DEFAULT | NONE | SHARED | EXCLUSIVE)
    | COMMENT quoted_string
    | WITH PARSER parser_name
    ;

parser_name
    : identifier
    ;

charset_name
    : identifier
    ;

collation_name
    : identifier
    ;

default_expr
    : expression
    ;

table_options
    : table_option+
    ;

table_option
    : ENGINE '='? identifier
    | (DEFAULT)? CHARACTER SET '='? charset_name
    | (DEFAULT)? COLLATE '='? collation_name
    | ROW_FORMAT '='? (DEFAULT | DYNAMIC | FIXED | COMPRESSED | REDUNDANT | COMPACT)
    | AUTO_INCREMENT '='? UNSIGNED_INTEGER
    | MAX_ROWS '='? UNSIGNED_INTEGER
    | MIN_ROWS '='? UNSIGNED_INTEGER
    ;

partition_options
    : range_partitions
    | list_partitions
    | hash_partitions
    | range_columns_partitions
    | list_columns_partitions
    | linear_hash_partitions
    | linear_key_partitions
    | composite_range_partitions
    | composite_list_partitions
    ;

range_partitions
    : PARTITION BY RANGE COLUMNS? '(' column_name (',' column_name)* ')'
      '(' partition (',' partition)* ')'
    ;

list_partitions
    : PARTITION BY COLUMNS? LIST '(' column_name (',' column_name)* ')'
      '(' partition (',' partition)* ')'
    ;

hash_partitions
    : PARTITION BY LINEAR? HASH '(' column_name (',' column_name)* ')'
      (individual_partition_clause | hash_partitions_by_quantity)
    ;

range_columns_partitions
    : PARTITION BY RANGE COLUMNS '(' column_name (',' column_name)* ')'
      '(' partition (',' partition)* ')'
    ;

list_columns_partitions
    : PARTITION BY LIST COLUMNS '(' column_name (',' column_name)* ')'
      '(' partition (',' partition)* ')'
    ;

linear_hash_partitions
    : PARTITION BY LINEAR HASH '(' column_name (',' column_name)* ')'
      (individual_partition_clause | hash_partitions_by_quantity)
    ;

linear_key_partitions
    : PARTITION BY LINEAR KEY '(' column_name (',' column_name)* ')'
      (individual_partition_clause | hash_partitions_by_quantity)
    ;

composite_range_partitions
    : PARTITION BY RANGE '(' column_name (',' column_name)* ')'
      (subpartition_by_hash | subpartition_by_key)
    ;

composite_list_partitions
    : PARTITION BY RANGE '(' column_name (',' column_name)* ')'
      (subpartition_by_hash | subpartition_by_key)
    ;

partition
    : PARTITION partition_name range_values_clause? partition_storage_clause?
    ;

partition_name
    : identifier
    ;

range_values_clause
    : VALUES LESS THAN '(' literal (',' literal)* ')'
    | VALUES LESS THAN MAXVALUE
    ;

list_values_clause
    : VALUES IN '(' (DEFAULT | list_values) ')'
    ;

list_values
    : (literal | NULL) (',' (literal | NULL))*
    | '(' (literal | NULL) (',' (literal | NULL))* ')' (',' '(' (literal | NULL) (',' (literal | NULL))* ')' )*
    ;

partition_storage_clause
    : partition
    | '(' partition (',' partition)* ')'
    ;

individual_partition_clause
    : '(' PARTITION partition_name partition_storage_clause?
          (',' PARTITION partition_name partition_storage_clause?)* ')'
    ;

hash_partitions_by_quantity
    : PARTITIONS UNSIGNED_INTEGER (STORE IN '(' tablespace_name (',' tablespace_name)* ')')?
      (OVERFLOW STORE IN '(' tablespace_name (',' tablespace_name)* ')')?
    ;

subpartition_by_hash
    : SUBPARTITION BY HASH '(' column_name (',' column_name)* ')'
      hash_subparts_by_quantity?
    ;

subpartition_by_key
    : SUBPARTITION BY KEY algorithm '=' UNSIGNED_INTEGER '(' column_name (',' column_name)* ')'
      hash_subparts_by_quantity?
    ;

hash_subparts_by_quantity
    : SUBPARTITIONS UNSIGNED_INTEGER subpartition_storage_clause?
    ;

subpartition_storage_clause
    : SUBPARTITION BY KEY algorithm '=' UNSIGNED_INTEGER '(' column_name (',' column_name)* ')'
      hash_subparts_by_quantity?
    ;

algorithm
    : identifier
    ;

tablespace_name
    : identifier
    ;

select_statement
    : subquery for_update_clause?
    ;

insert_statement
    : INSERT (single_table_insert | multi_table_insert)
    ;

update_statement
    : UPDATE general_table_ref update_set_clause where_clause?
    ;

update_set_clause
    : SET (
        column_based_update_set_clause (',' column_based_update_set_clause)*
        | VALUES '(' identifier ')' '=' expression
    )
    ;

delete_statement
    : DELETE FROM? general_table_ref where_clause?
    ;

merge_statement
    : MERGE INTO target_table_clause USING source_table_clause ON '(' condition ')' (
        merge_update_clause merge_insert_clause?
        | merge_insert_clause merge_update_clause?
    )?
    ;

target_table_clause
    : tableview_name t_alias?
    ;

source_table_clause
    : (tableview_name | '(' subquery ')') t_alias?
    ;

merge_update_clause
    : WHEN MATCHED THEN UPDATE SET merge_element (',' merge_element)* where_clause? merge_update_delete_part?
    ;

merge_insert_clause
    : WHEN NOT MATCHED THEN INSERT paren_column_list? values_clause where_clause?
    ;

merge_element
    : column_name '=' expression
    ;

merge_update_delete_part
    : DELETE where_clause
    ;

column_based_update_set_clause
    : column_name '=' expression
    | paren_column_list '=' paren_expressions
    ;

single_table_insert
    : insert_into_clause (values_clause | subquery) (static_returning_clause | on_duplicate_clause)?
    ;

multi_table_insert
    : ALL multi_table_insert_element+ subquery
    ;

multi_table_insert_element
    : insert_into_clause values_clause?
    ;

insert_into_clause
    : INTO general_table_ref paren_column_list?
    ;

static_returning_clause
    : (RETURNING | RETURN) expressions into_clause
    ;

into_clause
    : (BULK)? INTO (general_element | bind_variable) (',' (general_element | bind_variable))*
    ;

on_duplicate_clause
    : ON DUPLICATE KEY UPDATE set_clause (',' set_clause)*
    ;

set_clause
    : column_name '=' VALUES? expression
    ;

general_element
    : general_element_part
    | general_element ('.' general_element_part)+
    | '(' general_element ')'
    ;

general_element_part
    : id_expression dblink? function_argument*
    ;

function_argument
    //    : '(' (argument (',' argument)*)? ')' keep_clause?
    : '(' (argument (',' argument)*)? ')'
    ;

argument
    : (identifier '=' '>')? expression
    ;

//keep_clause
//    : KEEP '(' DENSE_RANK (FIRST | LAST) (query_partition_clause | order_by_clause) ')' over_clause?
//    ;

general_table_ref
    : tableview_name t_alias?
    ;

paren_column_list
    : LEFT_PAREN column_list RIGHT_PAREN
    ;

values_clause
    : VALUES paren_expressions (',' paren_expressions)*
    ;

paren_expressions
    : '(' expressions ')'
    ;

for_update_clause
    : FOR UPDATE for_update_of_part? for_update_options?
    ;

for_update_of_part
    : OF column_list
    ;

column_list
    : column_name (',' column_name)*
    ;

column_name
    : LEVEL
    | CONNECT_BY_ISLEAF
    | CONNECT_BY_ISCYCLE
    | (identifier '.')? pseudo_column
    | (PRIOR|CONNECT_BY_ROOT)? identifier ('.' column_field )* OUTER_JOIN_OPERATOR?
    ;

column_field
    : column_member_func
    | id_expression
    ;


for_update_options
    : SKIP_ LOCKED
    | NOWAIT
    | WAIT expression
    ;

pseudo_column
    : ROWSCN
    | ROWID
    | ROWNUM
    | USER
    ;

subquery
    : subquery_body order_by_clause? pagination_clause?
    | '(' subquery ')'
    ;

pagination_clause
    : row_limiting_clause
    | offset_fetch_clause
    ;

row_limiting_clause
    : LIMIT expression (OFFSET expression)?
    ;

offset_fetch_clause
    : offset_clause? fetch_clause
    ;

offset_clause
    : OFFSET expression (ROW | ROWS)
    ;

fetch_clause
    : FETCH (FIRST | NEXT) expression (ROW | ROWS) ONLY
    ;

subquery_body
    : query_block
    | subquery_body ( UNION | INTERSECT | MINUS | EXCEPT) ALL? subquery_body
    ;

query_block
    : with_clause? SELECT DISTINCT? select_list from_clause where_clause? hierarchical_group_clause?
    ;

hierarchical_group_clause
    : (group_by_clause | having_clause | start_with_clause | connect_by_clause)+ order_by_clause?
    ;

//
//group_by_clause
//    : GROUP BY group_by_right_part having_clause?   # group_by_clause1
//    | having_clause (GROUP BY group_by_right_part)? # group_by_clause2
//    ;

group_by_clause
    : GROUP BY group_by_right_part
    ;

group_by_right_part
    : expression_list
    | rollup_cube_clause
    | grouping_sets_clause
    ;

rollup_cube_clause
    : (ROLLUP | CUBE) '(' expression (',' expression)* ')'
    ;

grouping_sets_clause
    : GROUPING SETS '(' expression (',' expression)* ')'
    ;

having_clause
    : HAVING condition
    ;

from_clause
    : FROM table_clause
    ;

table_clause
    : join_clause
    | table_ref
    ;

table_ref
    : (tableview_name | subquery) sample_clause? flashback_query_clause? t_alias? pivot_clause?
    | '(' table_ref ')'
    ;

flashback_query_clause
    : AS OF (SCN | TIMESTAMP) expression
    ;

pivot_clause
    : PIVOT '(' pivot_element (',' pivot_element)* pivot_for_clause pivot_in_clause ')'
    ;

pivot_element
    : expression column_alias?
    ;

pivot_for_clause
    : FOR column_name
    ;

pivot_in_clause
    : IN '(' pivot_in_clause_element (',' pivot_in_clause_element)* ')'
    ;

pivot_in_clause_element
    : expression column_alias?
    ;

tableview_name
    : (identifier '.')? identifier (
        (dblink? partition_extension_clause? (SLICE '(' expression ')')?)
        | table_collection_expression
        | xmltable
    )
    ;

table_collection_expression
    : TABLE '(' collection_expression ')'
    ;

collection_expression
    : t_alias '.' identifier
    ;

t_alias
    : AS? identifier
    ;

sample_clause
    : SAMPLE '(' expression ')' seed_part?
    ;

seed_part
    : SEED '(' expression ')'
    ;

partition_extension_clause
    : (PARTITION | SUBPARTITION) partition_part
    ;

partition_part
    : '(' partition_name ')'
    | FOR '(' expression_list ')'
    ;

dblink
    : AT_SIGN dblink_name
    ;

dblink_name
    : identifier
    ;

join_clause
    : table_ref (',' table_ref)+ # cartesian_product
    | table_ref join+            # table_join
    ;

join
    : inner_cross_join_clause
    | outer_join_clause
    ;

inner_cross_join_clause
    : INNER? JOIN table_ref ON condition
    | CROSS JOIN table_ref
    ;

outer_join_clause
    : (LEFT | RIGHT | FULL) OUTER? JOIN table_ref (ON condition)?
    ;

where_clause
    : WHERE condition
    ;

with_clause
    : WITH cte_clause_list
    ;

hierarchical_query_clause
    : connect_by_clause start_with_clause? order_by_clause? # connect_by_clause1
    | start_with_clause connect_by_clause order_by_clause?  # connect_by_clause2
    ;

start_with_clause
    : START WITH condition
    ;

connect_by_clause
    : CONNECT BY NOCYCLE? condition
    ;

cte_clause_list
    : cte_clause (',' cte_clause)*
    ;

cte_clause
    : identifier ('(' column_list ')')? AS '(' subquery ')'
    ;

select_list
    : select_list_element (',' select_list_element)*
    ;

select_list_element
    : (identifier '.')? '*'
    | expression_clause column_alias?
    ;

expression_clause
    : subquery
    | case_clause
    | expression
    | column_name
    ;

case_clause
    : CASE case_expression_list else_clause? END
    ;

else_clause
    : ELSE expression
    ;

case_expression_list
    : case_expression+
    ;

case_expression
    : simple_case_expression
    | searched_case_expression
    ;

simple_case_expression
    : expression simple_case_when_list
    ;

searched_case_expression
    : searched_case_when_list
    ;

searched_case_when_list
    : searched_case_when (',' searched_case_when)*
    ;

searched_case_when
    : WHEN condition THEN expression
    ;

simple_case_when_list
    : simple_case_when (',' simple_case_when)*
    ;

simple_case_when
    : WHEN expression THEN expression
    ;

order_by_clause
    : ORDER SIBLINGS? BY order_by_elements (',' order_by_elements)*
    ;

order_by_elements
    : expression (ASC | DESC)? (NULLS (FIRST | LAST))?
    ;

column_alias
    : AS? (identifier | quoted_string)
    | AS
    ;

expression
    : literal
    | constant
    | bind_variable
    | standard_function
    | case_clause
    | subquery
    | expression ('+' | '-' | '*' | '/' | '%' | BAR BAR) expression // TODO: operator precedence needs review
    | '(' expression ')'
    | column_name
    ;

literal
    : CHAR_STRING
    | BIT_STRING_LIT
    | MAXVALUE
    | numeric_literal
    | date_literal
    | timestamp_literal
    | interval_year_to_month_literal
    | interval_day_to_second_literal
    ;

date_literal
    : DATE CHAR_STRING
    ;

timestamp_literal
    : TIMESTAMP CHAR_STRING
    ;

interval_year_to_month_literal
    : INTERVAL CHAR_STRING ((YEAR ('(' UNSIGNED_INTEGER ')')? (TO MONTH)?) | MONTH)
    ;

interval_day_to_second_literal
    : INTERVAL CHAR_STRING (day | hour | minute | second)
    ;

day
    : DAY ('(' UNSIGNED_INTEGER ')')? (TO (HOUR | MINUTE | second))?
    ;

minute
    : MINUTE (TO SECOND)?
    ;

hour
    : HOUR (TO (MINUTE | second))?
    ;

second
    : SECOND ('(' UNSIGNED_INTEGER ')')?
    ;

constant
    : quoted_string
    | NULL
    | TRUE
    | FALSE
    | MINVALUE
    | MAXVALUE
    | DEFAULT
    ;

quoted_string
    : CHAR_STRING
    //| CHAR_STRING_PERL
    | NATIONAL_CHAR_STRING_LIT
    ;

bind_variable
    : (BINDVAR | ':' UNSIGNED_INTEGER)
    ;

regular_id
    : non_reserved_keywords
    | REGULAR_ID
    ;

non_reserved_keywords
    : ABS
    | ACOS
    | ADD_MONTHS
    | AGE
    | ARRAY_APPEND
    | ARRAY_LENGTH
    | ARRAY_NDIMS
    | ARRAY_POSITION
    | ARRAY_REMOVE
    | ARRAY_REPLACE
    | ARRAY_TO_STRING
    | ARRAY_UPPER
    | ASCII
    | ASIN
    | ATAN
    | ATAN2
    | AVG
    | BIN
    | BIN_TO_NUM
    | BITAND
    | BITOR
    | BITXOR
    | BIT_LENGTH
    | CAST
    | CEIL
    | CHARACTER_LENGTH
    | CHAR_LENGTH
    | CHAR_TO_LABEL
    | CHECK_SYS_PRIVILEGE
    | CHR
    | COALESCE
    | CONCAT
    | CONCAT_WS
    | COS
    | COT
    | COUNT
    | CRYPT_ASYM_DECRYPT
    | CRYPT_ASYM_ENCRYPT
    | CRYPT_DECRYPT
    | CRYPT_ENCRYPT
    | CRYPT_HASH
    | CRYPT_HMAC
    | CRYPT_KEY
    | CRYPT_RANDOM
    | CRYPT_SELFTEST
    | CRYPT_SIGN
    | CRYPT_VERIFY
    | CURRENT_TIMESTAMP
    | DATE_ADD
    | DATE_FORMAT
    | DATE_SUB
    | DAYOFWEEK
    | DECODE
    | DECRYPT_AES128
    | DIV
    | EMPTY_BLOB
    | EMPTY_CLOB
    | ENCRYPT_AES128
    | EXP
    | EXTRACT
    | FIND_IN_SET
    | FLOOR
    | FOREIGN
    | GET_TYPE_NAME
    | GREATEST
    | GROUPING
    | GROUPING_ID
    | GROUP_CONCAT
    | GROUP_ID
    | HEXTORAW
    | IFNULL
    | INITCAP
    | INSTR
    | INSTRB
    | ISNULL
    | JSON
    | JSON_ARRAY_GET
    | JSON_ARRAY_LENGTH
    | JSON_EXISTS
    | JSON_FORMAT
    | JSON_PARSE
    | JSON_QUERY
    | JSON_SERIALIZE
    | JSON_VALUE
    | LABEL_TO_CHAR
    | LAST_DAY
    | LAST_INSERT_ID
    | LEAST
    | LEFT
    | LENGTH
    | LENGTH2
    | LENGTHB
    | LISTAGG
    | LN
    | LNNVL
    | LOCALTIME
    | LOCALTIMESTAMP
    | LOG
    | LOWER
    | LPAD
    | LSFA_LISTAGG
    | LTRIM
    | MAX
    | MD5
    | MEDIAN
    | MIN
    | MOD
    | MONTHS_BETWEEN
    | NEXT_DAY
    | NLSSORT
    | NOW
    | NULLIF
    | NUMTODSINTERVAL
    | NUMTOYMINTERVAL
    | NVL
    | NVL2
    | OCTET_LENGTH
    | PERCENTILE_CONT
    | PI
    | POSITION
    | POW
    | POWER
    | PRIMARY
    | PX_CHANNEL
    | PX_OBJ
    | RANDOM
    | REPLACE
    | RIGHT
    | ROUND
    | ROWIDTOCHAR
    | RPAD
    | RTRIM
    | SCN_TO_TIMESTAMP
    | SECURITY_CLEAR_CSP
    | SECURITY_MOD_STATUS
    | SECURITY_MOD_VERSION
    | SESSIONTIMEZONE
    | SIGN
    | SIN
    | SINH
    | SOUNDEX
    | SPLIT
    | SQLCODE
    | SQLERRM
    | SQRT
    | STDDEV
    | STDDEV_POP
    | STDDEV_SAMP
    | STRING_AGG
    | STRING_TO_ARRAY
    | STRPOS
    | SUBSTR
    | SUBSTRB
    | SUBSTRING
    | SUBSTRING_INDEX
    | SUM
    | SYSTIMESTAMP
    | SYS_CONNECT_BY_PATH
    | SYS_CONTEXT
    | SYS_EXTRACT_UTC
    | SYS_GUID
    | TAN
    | TANH
    | TIME
    | TIMEDIFF
    | TIMESTAMP
    | TIMESTAMPDIFF
    | TIMESTAMP_TO_SCN
    | TO_BASE64
    | TO_CHAR
    | TO_DATE
    | TO_DSINTERVAL
    | TO_NUMBER
    | TO_TIMESTAMP
    | TO_YMINTERVAL
    | TRANSLATE
    | TREAT
    | TRIM
    | TRUNC
    | TYPEOF
    | UNISTR
    | UNSUPPORT_ERROR
    | UPPER
    | USERENV
    | UTC_TIMESTAMP
    | VARIANCE
    | VAR_POP
    | VAR_SAMP
    | WM_CONCAT
    | REGEXP_COUNT
    | REGEXP_INSTR
    | REGEXP_LIKE
    | REGEXP_REPLACE
    | REGEXP_SUBSTR
    | RLIKE_FILTER
    | ST_ASTEXT
    | ST_GEOMFROMTEXT
    | ST_GEOMETRYFROMTEXT
    | ST_LINEFROMTEXT
    | ST_GEOMCOLLFROMTEXT
    | ST_ASBINARY
    | ST_ASEWKB
    | ST_ASHEXEWKB
    | ST_GEOMFROMWKB
    | ST_GEOMFROMEWKB
    | ST_SETSRID
    | ST_SRID
    | ST_TRANSFORM
    | ST_INTERSECTS
    | ST_CONTAINS
    | ST_WITHIN
    | ST_OVERLAPS
    | ST_COVEREDBY
    | ST_COVERS
    | ST_CROSSES
    | ST_DISJOINT
    | ST_EQUALS
    | ST_TOUCHES
    | ST_RELATE
    | ST_CONTAINSPROPERLY
    | ST_DWITHIN
    | ST_ASGEOJSON
    | ST_GEOMFROMGEOJSON
    | ST_ASLATLONTEXT
    | ST_BUFFER
    | ST_SIMPLIFY
    | ST_CONCAVEHULL
    | ST_BUILDAREA
    | ST_LINEMERGE
    | ST_POINTONSURFACE
    | ST_DISTANCE
    | ST_CLOSESTPOINT
    | ST_SHORTESTLINE
    | ST_LONGESTLINE
    | ST_MAXDISTANCE
    | ST_PERIMETER
    | ST_AREA
    | ST_LENGTH
    | ST_GEOMETRICMEDIAN
    | ST_CENTROID
    | GEOMETRYTYPE
    | ST_GEOMETRYTYPE
    | ST_BOUNDARY
    | ST_ENVELOPE
    | ST_X
    | ST_Y
    | ST_Z
    | ST_ISEMPTY
    | ST_ISVALID
    | ST_ISSIMPLE
    | ST_ISCLOSED
    | ST_CLIPBYBOX2D
    | ST_DIFFERENCE
    | ST_INTERSECTION
    | ST_UNION
    | ST_SPLIT
    | ST_MAKEPOINT
    | ST_POINT
    | ST_POINTZ
    | ST_MAKELINE
    | ST_POLYGON
    | ST_MAKEENVELOPE
    | INNER_FUNC__MAKE_RTREE_KEY__
    | INNER_FUNC__MAKE_RTREE_KEY3__
    | INNER_FUNC__GEOM_CHECK_MODIFIER__
    | ST_COLLECT
    | ST_EXTENT
    | ST_MULTI
    | ST_COLLECTIONEXTRACT
    | ST_DUMP
    | ST_MAKEVALID
    | YASDECODE
    | DENSE_RANK
    | FIRST_VALUE
    | LAG
    | LAST_VALUE
    | LEAD
    | RANK
    | ROW_NUMBER
    | ABORT
    | ACCOUNT
    | ACTIONS
    | ALL_ROWS
    | APPEND
    | ARCHIVE
    | ARCHIVELOG
    | AT
    | BACKUP
    | BASE
    | BEFORE
    | BEGIN
    | BIGFILE
    | BIGINT
    | BINARY
    | BINARY_BIGINT
    | BINARY_DOUBLE
    | BINARY_FLOAT
    | BINARY_INTEGER
    | BINARY_SMALLINT
    | BINARY_TINYINT
    | BIT
    | BLOB
    | BOOLEAN
    | BOUND
    | BUFFER_POOL
    | BUILD
    | BULK
    | BULKLOAD
    | CACHE
    | CALL
    | CANCEL
    | CASCADE
    | CASE
    | CATEGORY
    | CELL_FLASH_CACHE
    | CHANGE
    | CHAR_CS
    | CHARACTER
    | CHECKPOINT
    | CHOOSE
    | CHUNK
    | CLEAN
    | CLOB
    | CLOSE
    | CLUSTER
    | COLUMNAR
    | COMMIT
    | COMPRESSION
    | CONDITION
    | CONNECT_BY_ISCYCLE
    | CONNECT_BY_ISLEAF
    | CONNECT_BY_ROOT
    | CONTINUE
    | CONTROLFILES
    | CONVERT
    | CROSS
    | CUBE
    | CURRENT_DATE
    | CYCLE
    | DATABASE
    | DATAFILE
    | DATAOID_REUSE
    | DATASPACE
    | DAY
    | DECLARE
    | DECRYPTION
    | DEDUPLICATE
    | DEFINITION
    | DIRECTORY
    | DISABLE
    | DISTRIBUTE
    | DOUBLE
    | DOUBLE_PRECISION
    | DOUBLE_WRITE
    | DUMP
    | DUPLICATE
    | DUPLICATED
    | EDITIONABLE
    | ELSIF
    | ENABLE
    | ENCODING
    | ENCRYPT
    | ENCRYPTION
    | END
    | ENGINE_COL
    | ENGINE_ROW
    | ESCAPE
    | EXCEPTION
    | EXEC
    | EXECUTE
    | EXIT
    | EXPLAIN
    | EXTEND
    | EXTERNAL
    | FAILOVER
    | FETCH
    | FIRST
    | FIRST_ROWS
    | FLASH_CACHE
    | FLASHBACK
    | FLUSH
    | FOLLOWING
    | FORALL
    | FORCE
    | FORMAT
    | FREELIST
    | FREELISTS
    | FULL
    | FUNCTION
    | GLOBAL
    | GOTO
    | HASH_AJ
    | HASH_SJ
    | HEAP
    | HOUR
    | IGNORE
    | INCREMENTAL
    | INDEPEND
    | INDEX_ASC
    | INDEX_DESC
    | INDEX_FFS
    | INDEX_JOIN
    | INDEX_SS
    | INDEX_SS_ASC
    | INDEX_SS_DESC
    | INITIAL
    | INITRANS
    | INMEMORY
    | INNER
    | INSTANCE
    | INSTANCES
    | INT
    | INTERVAL
    | INVALIDATE
    | INVISIBLE
    | JOIN
    | KEEP_DUPLICATES
    | KILL
    | LEADING
    | LIBRARY
    | LIMIT
    | LINK
    | LOAD
    | LOB
    | LOCAL
    | LOCK
    | LOGFILE
    | LOGGING
    | LOGOFF
    | LOGON
    | LOOP
    | LSC
    | MATCHED
    | MATERIALIZED
    | MAX_WORKERS_PER_EXEC
    | MAXDATABUCKETS
    | MAXDATAFILES
    | MAXEXTENTS
    | MAXINSTANCES
    | MAXLOGFILES
    | MAXLOGHISTORY
    | MAXSIZE
    | MAXTRANS
    | MAXVALUE
    | MCOL
    | MERGE
    | MERGE_AJ
    | MERGE_SJ
    | MINEXTENTS
    | MINUTE
    | MINVALUE
    | MONTH
    | MOUNT
    | NATIONAL
    | NATURAL
    | NCHAR
    | NCHAR_CS
    | NCLOB
    | NESTED
    | NEW
    | NEXT
    | NEXTVAL
    | NL_AJ
    | NL_SJ
    | NO
    | NO_INDEX
    | NO_INDEX_FFS
    | NO_INDEX_SS
    | NO_USE_HASH
    | NO_USE_MERGE
    | NO_USE_NL
    | NOAPPEND
    | NOARCHIVELOG
    | NOCACHE
    | NOCYCLE
    | NOLOGGING
    | NOMAXVALUE
    | NOMINVALUE
    | NONEDITIONABLE
    | NOORDER
    | NOPARALLEL
    | NOREVERSE
    | NORMAL
    | NOVALIDATE
    | NULLS
    | NUMERIC
    | NVARCHAR
    | NVARCHAR2
    | OBJNO_REUSE
    | OFFSET
    | ONLY
    | OPEN
    | ORDERED
    | ORGANIZATION
    | OUTER
    | OUTLINE
    | OVER
    | OVERLAPS
    | PACKAGE
    | PARALLEL
    | PARALLELISM
    | PARTITION
    | PASSWORD
    | PCTFREE
    | PCTINCREASE
    | PCTUSED
    | PIPE
    | PIVOT
    | PLS_INTEGER
    | PRAGMA
    | PRECEDING
    | PREPARE
    | PRESERVE
    | PRIVATE
    | PRIVILEGE
    | PRIVILEGES
    | PROCEDURE
    | PROFILE
    | PURGE
    | QUOTA
    | RAISE
    | RANGE
    | READONLY
    | READWRITE
    | REAL
    | REBUILD
    | RECLAIM
    | RECOVER
    | REFERENCES
    | REFRESH
    | REGISTER
    | RELEASE
    | RESETLOGS
    | RESPECT
    | RESTART
    | RESTORE
    | RESTRICT
    | RETURN
    | RETURNING
    | REUSE
    | REVERSE
    | ROLE
    | ROLES
    | ROLLBACK
    | ROLLUP
    | RTREE
    | RULE
    | SAMPLE
    | SAVEPOINT
    | SCHEMA
    | SCN
    | SECOND
    | SECTION
    | SEGMENT
    | SEPARATOR
    | SEQUENCE
    | SETS
    | SHARDED
    | SHARED
    | SHRINK
    | SHUTDOWN
    | SIBLINGS
    | SKIP_
    | SLICE
    | SMALLFILE
    | SOME
    | SQL
    | SQLMAP
    | STABLE
    | STANDBY
    | STORAGE
    | SUBPARTITION
    | SUPPLEMENTAL
    | SWAP
    | SWITCH
    | SWITCHOVER
    | SYS_REFCURSOR
    | SYSAUX
    | SYSTEM
    | TABLESPACE
    | TAC
    | TAG
    | TDE
    | TEMPFILE
    | TEMPORARY
    | TINYINT
    | TRANSACTION
    | TRANSPORT
    | TRUNCATE
    | TTL
    | TYPE
    | UNBOUNDED
    | UNDO
    | UNDO_SEGMENTS
    | UNUSABLE
    | UPGRADE
    | UROWID
    | USABLE
    | USE_HASH
    | USE_MERGE
    | USE_NL
    | USING
    | VISIBLE
    | WAIT
    | WHILE
    | WRAPPED
    | XMLTYPE
    | YEAR
    ;

numeric
    : UNSIGNED_INTEGER '.'?
    | APPROXIMATE_NUM_LIT
    ;

expression_list
    : expressions
    | '(' expression_list ')'
    ;

expressions
    : expression (',' expression)*
    ;

// function

standard_function
    : string_function
    | numeric_function
    | json_function
    | date_function
    | aggregate_function
    | window_function
    | other_function
    | datatype_function
    | dbms_funcion
    | gis_function
    | xml_function
    ;



xml_function
    : XMLTYPE '(' argument ')'
    | (EXISTSNODE|EXTRACTVALUE |XMLEXTRACT) '(' argument ',' argument (',' argument)?  ')'
    | XMLAGG '(' argument order_by_clause? ')'
    | XMLPARSE '(' (DOCUMENT|CONTENT) argument WELLFORMED? ')'
    | xmltable
    ;

xml_namespaces_clause
    : XMLNAMESPACES '(' (expression column_alias)? (',' expression column_alias)* xml_general_default_part? ')'
    ;


xml_general_default_part
    : DEFAULT expression
    ;



xmltable
    : XMLTABLE '(' (xml_namespaces_clause ',')? expression xml_passing_clause? (
        COLUMNS xml_table_column (',' xml_table_column)*
    )? ')' ('.' general_element_part)?
    ;

xml_passing_clause
    : PASSING (BY VALUE)? expression column_alias? (',' expression column_alias?)*
    ;


xml_table_column
    :xml_column_name (FOR ORDINALITY | type_spec (PATH expression)? xml_general_default_part?)
    ;

xml_column_name
    : identifier
    | quoted_string
    ;

string_function
    : (SUBSTR | SUBSTRB) '(' expression ',' expression (',' expression)? ')'
    | SUBSTRING '(' expression (',' | FROM) expression ((',' | FROM) expression)? ')'
    | TO_CHAR '(' (table_element | standard_function | expression) (',' quoted_string)? (
        ',' quoted_string
    )? ')'
    | (DECODE | TO_BASE64 | TO_CHAR) expression_list
    | NVL '(' expression ',' expression ')'
    | TRIM '(' ((LEADING | TRAILING | BOTH)? expression? FROM)? expression ')'
    | (
        BIN
        | CHR
        | CONCAT
        | CONCAT_WS
        | CRYPT_ASYM_DECRYPT
        | CRYPT_ASYM_ENCRYPT
        | CRYPT_ASYM_ENCRYPT
        | CRYPT_ENCRYPT
        | CRYPT_HASH
        | CRYPT_HMAC
        | CRYPT_KEY
        | CRYPT_RANDOM
        | CRYPT_SELFTEST
        | CRYPT_SIGN
        | CRYPT_VERIFY
        | DATE_FORMAT
        | DECRYPT_AES128
        | ENCRYPT_AES128
        | GET_TYPE_NAME
        | INITCAP
        | LEFT
        | LOWER
        | LPAD
        | LSFA_LISTAGG
        | LTRIM
        | MD5
        | REGEXP_SUBSTR
        | RIGHT
        | ROWIDTOCHAR
        | RPAD
        | RTRIM
        | SOUNDEX
        | SPLIT
        | TYPEOF
        | UNISTR
        | SUBSTRING_INDEX
    ) expression_list
    ;

numeric_function
    : LAST_INSERT_ID ('(' expression? ')')?
    | (
        CHAR_LENGTH
        | CHARACTER_LENGTH
        | LENGTH
        | OCTET_LENGTH
        | LENGTHB
        | LENGTH2
        | BIT_LENGTH
        | BITAND
        | BITOR
        | BITXOR
        | BIN_TO_NUM
        | CEIL
        | INSTR
        | ASCII
        | ASIN
        | ATAN
        | ATAN2
        | INSTR
        | INSTRB
        | CHAR_TO_LABEL
        | LN
        | LOG
        | MOD
        | MONTHS_BETWEEN
        | POW
        | POWER
        | SIGN
        | SIN
        | SIN
        | SQRT
        | STRPOS
        | TAN
        | TANH
        | TIMESTAMPDIFF
        | TO_NUMBER
        | ABS
        | ACOS
        | COT
        | DIV
        | EXP
        | FIND_IN_SET
        | FLOOR
    ) expression_list
    | LNNVL '(' condition ')'
    | (PI | RANDOM) none_arguments
    | (POSITION) in_arguments
    | EXTRACT '(' (YEAR | MONTH | DAY | HOUR | MINUTE | SECOND) FROM expression ')'
    ;

json_function
    : JSON '(' expression EXTENDED? ')'
    | ( JSON_ARRAY_GET | JSON_ARRAY_LENGTH | JSON_EXISTS) expression_list
    | JSON_QUERY '(' expression (FORMAT JSON)? ',' quoted_string returning_clause? PRETTY? wrapper_clause? ')'
    | JSON_SERIALIZE '(' expression returning_clause? PRETTY? EXTENDED? ')'
    ;

date_function
    : (
        ADD_MONTHS
        | AGE
        | DATE
        | DAYOFWEEK
        | LAST_DAY
        | NEXT_DAY
        | TIME
        | TIMEDIFF
        | TIMESTAMP
        | TIMESTAMPDIFF
        | TO_DSINTERVAL
        | TO_YMINTERVAL
    ) expression_list
    | (
        CURRENT_TIMESTAMP
        | LOCALTIME
        | LOCALTIMESTAMP
        | NOW
        | SCN_TO_TIMESTAMP
        | SYSDATE
        | SYSTIMESTAMP
        | SYS_EXTRACT_UTC
        | UTC_TIMESTAMP
    ) none_arguments
    | (DATE_ADD | DATE_SUB) '(' expression ',' INTERVAL expression expression ')'
    | NLSSORT '(' expression (',' string_delimiter)? ')'
    | ( NUMTODSINTERVAL | NUMTOYMINTERVAL) '(' expression ',' string_delimiter ')'
    | (TO_DATE | TO_TIMESTAMP) '(' expression (DEFAULT quoted_string ON CONVERSION ERROR)? (
        ',' quoted_string (',' quoted_string)?
    )? ')'
    ;

aggregate_function
    : (AVG | MAX | MIN | SUM) '(' (DISTINCT | ALL)? expression ')' over_clause?
    | COUNT '(' (ASTERISK | ((DISTINCT | ALL)? expression)?) ')' over_clause?
    | STRING_AGG '(' (DISTINCT | ALL)? expression ',' string_delimiter order_by_clause? ')'
    | (VARIANCE | WM_CONCAT) '(' (DISTINCT | ALL)? expression ')'
    | (GROUPING | GROUPING_ID | GROUP_ID) expression_list
    | GROUP_CONCAT '(' DISTINCT? expression (',' expression)? (
        order_by_clause (SEPARATOR quoted_string)?
    )? ')'
    | (LISTAGG | STDDEV) '(' (ALL | DISTINCT)? expression (',' string_delimiter)? listagg_overflow_clause? ')' (
        WITHIN GROUP '(' order_by_clause ')'
    )? over_clause?
    | MEDIAN '(' ALL? expression ')' over_clause?
    | (PERCENTILE_CONT) expression_list within_over_part+
    | (STDDEV_POP | STDDEV_SAMP | VAR_POP | VAR_SAMP) expression_list
    ;

window_function
    : (LAG | LEAD) '(' expression respect_or_ignore_nulls? ',' numeric_literal (',' DEFAULT)? over_clause? ')'
    | FIRST_VALUE '(' expression respect_or_ignore_nulls? ')' over_clause?
    | (RANK | ROW_NUMBER) '(' ')' over_clause
    ;

other_function
    : (
        COALESCE
        | GREATEST
        | HEXTORAW
        | IF
        | IFNULL
        | NULLIF
        | LABEL_TO_CHAR
        | LEAST
        | NVL
        | NVL2
        | PX_CHANNEL
        | PX_OBJ
        | ROUND
        | TRUNC
        | UPPER
        | USERENV
    ) expression_list
    | SYS_CONNECT_BY_PATH '(' identifier ',' quoted_string ')'
    | DENSE_RANK none_arguments over_clause
    | (
        EMPTY_BLOB
        | EMPTY_CLOB
        | SECURITY_CLEAR_CSP
        | SECURITY_MOD_STATUS
        | SECURITY_MOD_VERSION
        | SQLCODE
        | SYS_CONTEXT
        | SYS_GUID
        | UNSUPPORT_ERROR
    ) none_arguments
    | SQLERRM '(' (expression expression)? ')'
    | TRANSLATE '(' expression (USING (CHAR_CS | NCHAR_CS))? (',' expression)* ')'
    ;

dbms_funcion
    : id_expression '.' id_expression '(' expression_list ')'
    ;

datatype_function
    : (CAST) '(' expression AS type_spec (DEFAULT expression ON CONVERSION ERROR)? (
        ',' quoted_string (',' quoted_string)?
    )? ')'
    ;

gis_function
    : (
        GEOMETRYTYPE
        | ST_AREA
        | ST_ASBINARY
        | ST_ASEWKB
        | ST_ASGEOJSON
        | ST_ASHEXEWKB
        | ST_ASLATLONTEXT
        | ST_ASTEXT
        | ST_BOUNDARY
        | ST_BUFFER
        | ST_BUILDAREA
        | ST_CLIPBYBOX2D
        | ST_CLOSESTPOINT
        | ST_COLLECT
        | ST_CONCAVEHULL
        | ST_CONTAINS
        | ST_CONTAINSPROPERLY
        | ST_COVEREDBY
        | ST_COVERS
        | ST_CROSSES
        | ST_DIFFERENCE
        | ST_DISJOINT
        | ST_DISTANCE
        | ST_DUMP
        | ST_DWITHIN
        | ST_ENVELOPE
        | ST_EQUALS
        | ST_EXTENT
        | ST_GEOMCOLLFROMTEXT
        | ST_GEOMETRICMEDIAN
        | ST_GEOMETRYTYPE
        | ST_GEOMFROMEWKB
        | ST_GEOMFROMGEOJSON
        | ST_GEOMFROMTEXT
        | ST_GEOMFROMWKB
        | ST_INTERSECTION
        | ST_INTERSECTS
        | ST_ISCLOSED
        | ST_ISEMPTY
        | ST_ISSIMPLE
        | ST_ISVALID
        | ST_LENGTH
        | ST_LINEFROMTEXT
        | ST_LINEMERGE
        | ST_LONGESTLINE
        | ST_MAKEENVELOPE
        | ST_MAKELINE
        | ST_MAKEPOINT
        | ST_MAXDISTANCE
        | ST_MULTI
        | ST_OVERLAPS
        | ST_PERIMETER
        | ST_POINT
        | ST_POINTZ
        | ST_POLYGON
        | ST_RELATE
        | ST_SETSRID
        | ST_SHORTESTLINE
        | ST_SIMPLIFY
        | ST_SPLIT
        | ST_SRID
        | ST_TOUCHES
        | ST_TRANSFORM
        | ST_UNION
        | ST_WITHIN
        | ST_X
        | ST_Y
        | ST_Z
    ) expression_list
    ;


column_member_func
    : xml_member_func
    ;


xml_member_func
    : ( GETCLOBVAL | GETSTRINGVAL) '('')'
    | ( EXISTSNODE | EXTRACT) '(' (argument (',' argument)?) ')'
    | TRANSFORM '(' ( argument (',' argument) ?) ')'
    ;

respect_or_ignore_nulls
    : (RESPECT | IGNORE) NULLS
    ;

over_clause
    : OVER '(' query_partition_clause? order_by_clause? windowing_clause? ')'
    ;

windowing_clause
    : windowing_type (BETWEEN windowing_elements AND windowing_elements | windowing_elements)
    ;

windowing_type
    : ROWS
    | RANGE
    ;

windowing_elements
    : UNBOUNDED PRECEDING
    | CURRENT ROW
    | expression (PRECEDING | FOLLOWING)
    ;

listagg_overflow_clause
    : ON OVERFLOW (ERROR | TRUNCATE) CHAR_STRING? ((WITH | WITHOUT) COUNT)?
    ;

within_over_part
    : WITHIN GROUP '(' order_by_clause ')' over_clause?
    ;

query_partition_clause
    : PARTITION BY (('(' (subquery | expression_list)? ')') | expression_list)
    ;

wrapper_clause
    : (WITH | WITHOUT) ARRAY? WRAPPER
    ;

returning_clause
    : RETURNING VARCHAR (numeric_literal)
    ;

numeric_literal
    : ('+' | '-')? numeric
    ;

table_element
    : (INTRODUCER char_set_name)? id_expression ('.' id_expression)*
    ;

char_set_name
    : id_expression ('.' id_expression)*
    ;

string_delimiter
    : CHAR_STRING
    | string_function
    | string_delimiter BAR BAR string_delimiter
    | '(' string_delimiter ')'
    | id_expression
    ;

id_expression
    : regular_id
    | DELIMITED_ID
    ;

condition
    : unary_expression
    | compound_expression
    | condition (AND | OR) condition
    | '(' condition ')'
    ;

unary_expression
    : NOT? expression
    ;

compound_expression
    : expression relational_operator (expression | subquery_expression)
    | expression NOT? IN (expression_list | subquery)
    | expression_list NOT? IN (expression_list | subquery)
    | expression NOT? LIKE expression
    | expression NOT? RLIKE expression
    | NOT? EXISTS subquery
    | expression IS NOT? NULL
    | expression NOT? BETWEEN expression AND expression
    ;

relational_operator
    : '='
    | NOT_EQUAL_OP
    | ('<' | '>') '='?
    | OVERLAPS
    ;

subquery_expression
    : subquery_operator? (subquery | expression_list)
    ;

subquery_operator
    : ANY
    | SOME
    | ALL
    ;

none_arguments
    : ('(' expression? ')')?
    ;

in_arguments
    : '(' expression IN expression ')'
    ;

identifier
    : (INTRODUCER char_set_name)? id_expression
    ;

type_spec
    : datatype
    ;

datatype
    : native_datatype_element precision_part?
    | INTERVAL (YEAR | DAY) ('(' expression ')')? TO (MONTH | SECOND) ('(' expression ')')?
    | (YEAR | DAY)
    ;

native_datatype_element
    : BOOLEAN
    | PLS_INTEGER
    | INTEGER
    | BIT
    | CHAR
    | VARCHAR
    | VARCHAR2
    | NCHAR
    | NVARCHAR
    | DATE
    | FLOAT
    | DOUBLE
    | NUMBER
    | TIMESTAMP
    | TINYINT
    | SMALLINT
    | INT
    | BIGINT
    | CLOB
    | NCLOB
    | BLOB
    | XMLTYPE
    | ROWID
    | UROWID
    | JSON
    | RAW
    ;

precision_part
    : '(' (numeric | ASTERISK) (',' (numeric | numeric_negative))? (CHAR)? ')'
    ;

numeric_negative
    : MINUS_SIGN numeric
    ;

//
// User and Role DDL Statements
//

// CREATE USER statement
// Options are mutually exclusive: IDENTIFIED BY / ACCOUNT LOCK|UNLOCK / PASSWORD EXPIRE
create_user_statement
    : CREATE USER identifier IDENTIFIED BY quoted_string
    | CREATE USER identifier ACCOUNT (LOCK | UNLOCK)
    | CREATE USER identifier PASSWORD EXPIRE
    ;

// user_name is used for both ALTER USER and DROP USER
user_name
    : identifier
    | schema '.' identifier
    ;

// ALTER USER statement
// Options are mutually exclusive: IDENTIFIED BY / PASSWORD EXPIRE / ACCOUNT LOCK|UNLOCK
alter_user_statement
    : ALTER USER user_name IDENTIFIED BY quoted_string
    | ALTER USER user_name PASSWORD EXPIRE
    | ALTER USER user_name ACCOUNT (LOCK | UNLOCK)
    ;

// DROP USER statement
drop_user_statement
    : DROP USER user_name CASCADE?
    ;

// CREATE ROLE statement
// CREATE ROLE role [SLOT slot_id] [container "=" (CURRENT|ALL)]
create_role_statement
    : CREATE ROLE role_name
    | CREATE ROLE role_name SLOT UNSIGNED_INTEGER
    | CREATE ROLE role_name CONTAINER '=' (CURRENT | ALL)
    | CREATE ROLE role_name SLOT UNSIGNED_INTEGER CONTAINER '=' (CURRENT | ALL)
    ;

// role_name is used for both CREATE ROLE and DROP ROLE
role_name
    : identifier
    | schema '.' identifier
    ;

// DROP ROLE statement
drop_role_statement
    : DROP ROLE role_name
    ;

// CREATE PROFILE statement
// CREATE PROFILE profile_name LIMIT params [CONTAINER "=" (CURRENT|ALL)]
create_profile_statement
    : CREATE PROFILE identifier LIMIT profile_parameters+ container_clause?
    ;

// profile_parameters: password_parameters | resource_parameters | tcp_ip_parameters
profile_parameters
    : profile_parameter_name (UNSIGNED_INTEGER | UNLIMITED)
    ;

// profile_parameter_name: names of PROFILE parameters
profile_parameter_name
    : FAILED_LOGIN_ATTEMPTS
    | PASSWORD_LIFE_TIME
    | PASSWORD_REUSE_TIME
    | PASSWORD_REUSE_MAX
    | PASSWORD_LOCK_TIME
    | IDLE_TIME
    | CONNECT_TIME
    | CPU_PER_CALL
    | LOGICAL_READS_PER_CALL
    | CPU_PER_SESSION
    | LOGICAL_READS_PER_SESSION
    | PRIVATE_SGA
    | COMPOSITE_LIMIT
    ;

// container_clause: CONTAINER "=" (CURRENT|ALL)
container_clause
    : CONTAINER '=' (CURRENT | ALL)
    ;

// ALTER PROFILE statement
alter_profile_statement
    : ALTER PROFILE identifier LIMIT profile_parameters+ container_clause?
    ;

// DROP PROFILE statement
drop_profile_statement
    : DROP PROFILE identifier CASCADE?
    ;

// size_clause: integer [B|K|M|G|T|P|E]
size_clause
    : UNSIGNED_INTEGER (B | K | M | G | T | P | E)?
    ;

//
// Tablespace DDL Statements
//

// CREATE TABLESPACE statement
// Simplified version: CREATE [BIGFILE|SMALLFILE] TABLESPACE name [datafile_clause] [extent_management_clause]
create_tablespace_statement
    : CREATE (BIGFILE | SMALLFILE)? TABLESPACE identifier datafile_clause? extent_management_clause?
    | CREATE (TEMPORARY | LOCAL TEMPORARY)? TABLESPACE identifier datafile_clause? extent_management_clause?
    | CREATE (SWAP | LOCAL SWAP) TABLESPACE identifier datafile_clause?
    ;

// datafile_clause: DATAFILE|TEMPFILE 'filename' SIZE size_clause [AUTOEXTEND ...]
datafile_clause
    : (DATAFILE | TEMPFILE) file_specification (',' file_specification)*
    ;

// file_specification: 'filename' SIZE size_clause [AUTOEXTEND ...]
file_specification
    : quoted_string SIZE size_clause AUTOEXTEND? (ON | OFF)? (NEXT size_clause)? (MAXSIZE (UNLIMITED | size_clause))?
    ;

// extent_management_clause: EXTENT (AUTOALLOCATE | UNIFORM SIZE size_clause)
extent_management_clause
    : EXTENT (AUTOALLOCATE | UNIFORM SIZE size_clause)
    ;

// DROP TABLESPACE statement
// DROP TABLESPACE [IF EXISTS] name [INCLUDING CONTENTS [(AND|KEEP) DATAFILES] [CASCADE CONSTRAINTS]]
drop_tablespace_statement
    : DROP TABLESPACE if_exists? identifier INCLUDING CONTENTS (AND | KEEP)? DATAFILES? (CASCADE CONSTRAINTS)?
    | DROP TABLESPACE if_exists? identifier (CASCADE CONSTRAINTS)?
    ;

// ALTER TABLESPACE statement
// ALTER TABLESPACE name add_datafile | drop_datafile | shrink | offline | online | rename
alter_tablespace_statement
    : ALTER TABLESPACE identifier ADD (DATAFILE | TEMPFILE) quoted_string
    | ALTER TABLESPACE identifier DROP (DATAFILE | TEMPFILE) quoted_string
    | ALTER TABLESPACE identifier SHRINK
    | ALTER TABLESPACE identifier OFFLINE
    | ALTER TABLESPACE identifier ONLINE
    | ALTER TABLESPACE identifier RENAME TO identifier
    ;

//
// Synonym and Directory DDL Statements
//

// CREATE SYNONYM statement
// CREATE [OR REPLACE] [EDITIONABLE|NONEDITIONABLE] [PUBLIC] SYNONYM [schema "."] synonym FOR object
create_synonym_statement
    : CREATE SYNONYM identifier FOR identifier
    | CREATE OR REPLACE SYNONYM identifier FOR identifier
    | CREATE PUBLIC SYNONYM identifier FOR identifier
    | CREATE OR REPLACE PUBLIC SYNONYM identifier FOR identifier
    ;

// DROP SYNONYM statement
// DROP [PUBLIC] SYNONYM synonym_name
drop_synonym_statement
    : DROP PUBLIC? SYNONYM identifier
    ;

// CREATE DIRECTORY statement
// CREATE [OR REPLACE] DIRECTORY directory_name AS 'path_name'
create_directory_statement
    : CREATE DIRECTORY identifier AS quoted_string
    | CREATE OR REPLACE DIRECTORY identifier AS quoted_string
    ;

// DROP DIRECTORY statement
// DROP DIRECTORY directory_name
drop_directory_statement
    : DROP DIRECTORY identifier
    ;

// RENAME statement
// RENAME TABLE old_table_name TO new_table_name
rename_statement
    : RENAME TABLE identifier TO identifier
    | RENAME TABLE schema '.' identifier TO schema '.' identifier
    ;

// COMMENT statement
// COMMENT ON TABLE|COLUMN ...
comment_statement
    : COMMENT ON TABLE identifier IS quoted_string
    | COMMENT ON TABLE schema '.' identifier IS quoted_string
    | COMMENT ON COLUMN identifier '.' identifier IS quoted_string
    | COMMENT ON COLUMN schema '.' identifier '.' identifier IS quoted_string
    ;

// TRUNCATE TABLE statement
// TRUNCATE TABLE table_name [DROP STORAGE|REUSE STORAGE]
truncate_table_statement
    : TRUNCATE TABLE identifier
    | TRUNCATE TABLE schema '.' identifier
    | TRUNCATE TABLE identifier DROP STORAGE
    | TRUNCATE TABLE schema '.' identifier DROP STORAGE
    | TRUNCATE TABLE identifier REUSE STORAGE
    | TRUNCATE TABLE schema '.' identifier REUSE STORAGE
    ;

//
// Grant and Revoke Statements
//

// GRANT statement
// GRANT privilege TO user [WITH GRANT OPTION]
grant_statement
    : GRANT privilege_name TO user_name
    | GRANT privilege_name TO user_name WITH GRANT OPTION
    | GRANT privilege_name ON table_name TO user_name
    | GRANT privilege_name ON table_name TO user_name WITH GRANT OPTION
    ;

// privilege_name: for privileges like SELECT, INSERT, UPDATE, etc.
privilege_name
    : identifier
    | SELECT
    | INSERT
    | UPDATE
    | DELETE
    | CREATE
    | DROP
    | ALTER
    | INDEX
    | REFERENCES
    ;

// REVOKE statement
// REVOKE privilege FROM user
revoke_statement
    : REVOKE privilege_name FROM user_name
    | REVOKE privilege_name ON table_name FROM user_name
    ;

// AUDIT statement
audit_statement
    : AUDIT privilege_name ON identifier
    | AUDIT privilege_name
    ;

// NOAUDIT statement
noaudit_statement
    : NOAUDIT privilege_name ON identifier
    | NOAUDIT privilege_name
    ;