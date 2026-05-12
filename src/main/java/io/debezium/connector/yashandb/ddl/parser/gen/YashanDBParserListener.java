// Generated from YashanDBParser.g4 by ANTLR 4.13.1
package io.debezium.connector.yashandb.ddl.parser.gen;

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link YashanDBParser}.
 */
public interface YashanDBParserListener extends ParseTreeListener {
    /**
     * Enter a parse tree produced by {@link YashanDBParser#sql_script}.
     * @param ctx the parse tree
     */
    void enterSql_script(YashanDBParser.Sql_scriptContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#sql_script}.
     * @param ctx the parse tree
     */
    void exitSql_script(YashanDBParser.Sql_scriptContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#unit_statement}.
     * @param ctx the parse tree
     */
    void enterUnit_statement(YashanDBParser.Unit_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#unit_statement}.
     * @param ctx the parse tree
     */
    void exitUnit_statement(YashanDBParser.Unit_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#data_manipulation_language_statements}.
     * @param ctx the parse tree
     */
    void enterData_manipulation_language_statements(YashanDBParser.Data_manipulation_language_statementsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#data_manipulation_language_statements}.
     * @param ctx the parse tree
     */
    void exitData_manipulation_language_statements(YashanDBParser.Data_manipulation_language_statementsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#data_definition_language_statements}.
     * @param ctx the parse tree
     */
    void enterData_definition_language_statements(YashanDBParser.Data_definition_language_statementsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#data_definition_language_statements}.
     * @param ctx the parse tree
     */
    void exitData_definition_language_statements(YashanDBParser.Data_definition_language_statementsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_table_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_table_statement(YashanDBParser.Create_table_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_table_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_table_statement(YashanDBParser.Create_table_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_index_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_index_statement(YashanDBParser.Create_index_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_index_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_index_statement(YashanDBParser.Create_index_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_sequence_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_sequence_statement(YashanDBParser.Create_sequence_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_sequence_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_sequence_statement(YashanDBParser.Create_sequence_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#sequence_name}.
     * @param ctx the parse tree
     */
    void enterSequence_name(YashanDBParser.Sequence_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#sequence_name}.
     * @param ctx the parse tree
     */
    void exitSequence_name(YashanDBParser.Sequence_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#sequence_option}.
     * @param ctx the parse tree
     */
    void enterSequence_option(YashanDBParser.Sequence_optionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#sequence_option}.
     * @param ctx the parse tree
     */
    void exitSequence_option(YashanDBParser.Sequence_optionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_view_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_view_statement(YashanDBParser.Create_view_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_view_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_view_statement(YashanDBParser.Create_view_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#with_check_option}.
     * @param ctx the parse tree
     */
    void enterWith_check_option(YashanDBParser.With_check_optionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#with_check_option}.
     * @param ctx the parse tree
     */
    void exitWith_check_option(YashanDBParser.With_check_optionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_table_as_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_table_as_statement(YashanDBParser.Create_table_as_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_table_as_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_table_as_statement(YashanDBParser.Create_table_as_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_database_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_database_statement(YashanDBParser.Create_database_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_database_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_database_statement(YashanDBParser.Create_database_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#database_name}.
     * @param ctx the parse tree
     */
    void enterDatabase_name(YashanDBParser.Database_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#database_name}.
     * @param ctx the parse tree
     */
    void exitDatabase_name(YashanDBParser.Database_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_table_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_table_statement(YashanDBParser.Drop_table_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_table_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_table_statement(YashanDBParser.Drop_table_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_index_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_index_statement(YashanDBParser.Drop_index_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_index_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_index_statement(YashanDBParser.Drop_index_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_view_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_view_statement(YashanDBParser.Drop_view_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_view_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_view_statement(YashanDBParser.Drop_view_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_sequence_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_sequence_statement(YashanDBParser.Drop_sequence_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_sequence_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_sequence_statement(YashanDBParser.Drop_sequence_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_database_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_database_statement(YashanDBParser.Drop_database_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_database_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_database_statement(YashanDBParser.Drop_database_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_sequence_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_sequence_statement(YashanDBParser.Alter_sequence_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_sequence_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_sequence_statement(YashanDBParser.Alter_sequence_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_database_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_database_statement(YashanDBParser.Alter_database_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_database_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_database_statement(YashanDBParser.Alter_database_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_materialized_view_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_materialized_view_statement(YashanDBParser.Create_materialized_view_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_materialized_view_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_materialized_view_statement(YashanDBParser.Create_materialized_view_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_mv_refresh_clause}.
     * @param ctx the parse tree
     */
    void enterCreate_mv_refresh_clause(YashanDBParser.Create_mv_refresh_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_mv_refresh_clause}.
     * @param ctx the parse tree
     */
    void exitCreate_mv_refresh_clause(YashanDBParser.Create_mv_refresh_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#query_rewrite_clause}.
     * @param ctx the parse tree
     */
    void enterQuery_rewrite_clause(YashanDBParser.Query_rewrite_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#query_rewrite_clause}.
     * @param ctx the parse tree
     */
    void exitQuery_rewrite_clause(YashanDBParser.Query_rewrite_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_materialized_view_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_materialized_view_statement(YashanDBParser.Drop_materialized_view_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_materialized_view_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_materialized_view_statement(YashanDBParser.Drop_materialized_view_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#materialized_view_name}.
     * @param ctx the parse tree
     */
    void enterMaterialized_view_name(YashanDBParser.Materialized_view_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#materialized_view_name}.
     * @param ctx the parse tree
     */
    void exitMaterialized_view_name(YashanDBParser.Materialized_view_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_materialized_view_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_materialized_view_statement(YashanDBParser.Alter_materialized_view_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_materialized_view_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_materialized_view_statement(YashanDBParser.Alter_materialized_view_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_mv_refresh_clause}.
     * @param ctx the parse tree
     */
    void enterAlter_mv_refresh_clause(YashanDBParser.Alter_mv_refresh_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_mv_refresh_clause}.
     * @param ctx the parse tree
     */
    void exitAlter_mv_refresh_clause(YashanDBParser.Alter_mv_refresh_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_mv_compose}.
     * @param ctx the parse tree
     */
    void enterAlter_mv_compose(YashanDBParser.Alter_mv_composeContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_mv_compose}.
     * @param ctx the parse tree
     */
    void exitAlter_mv_compose(YashanDBParser.Alter_mv_composeContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_mv_parallel}.
     * @param ctx the parse tree
     */
    void enterAlter_mv_parallel(YashanDBParser.Alter_mv_parallelContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_mv_parallel}.
     * @param ctx the parse tree
     */
    void exitAlter_mv_parallel(YashanDBParser.Alter_mv_parallelContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#backup_database_statement}.
     * @param ctx the parse tree
     */
    void enterBackup_database_statement(YashanDBParser.Backup_database_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#backup_database_statement}.
     * @param ctx the parse tree
     */
    void exitBackup_database_statement(YashanDBParser.Backup_database_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#restore_database_statement}.
     * @param ctx the parse tree
     */
    void enterRestore_database_statement(YashanDBParser.Restore_database_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#restore_database_statement}.
     * @param ctx the parse tree
     */
    void exitRestore_database_statement(YashanDBParser.Restore_database_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#recover_database_statement}.
     * @param ctx the parse tree
     */
    void enterRecover_database_statement(YashanDBParser.Recover_database_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#recover_database_statement}.
     * @param ctx the parse tree
     */
    void exitRecover_database_statement(YashanDBParser.Recover_database_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_pluggable_database_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_pluggable_database_statement(YashanDBParser.Create_pluggable_database_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_pluggable_database_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_pluggable_database_statement(YashanDBParser.Create_pluggable_database_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_pluggable_database_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_pluggable_database_statement(YashanDBParser.Drop_pluggable_database_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_pluggable_database_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_pluggable_database_statement(YashanDBParser.Drop_pluggable_database_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_pluggable_database_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_pluggable_database_statement(YashanDBParser.Alter_pluggable_database_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_pluggable_database_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_pluggable_database_statement(YashanDBParser.Alter_pluggable_database_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_tablespace_set_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_tablespace_set_statement(YashanDBParser.Create_tablespace_set_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_tablespace_set_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_tablespace_set_statement(YashanDBParser.Create_tablespace_set_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_tablespace_set_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_tablespace_set_statement(YashanDBParser.Drop_tablespace_set_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_tablespace_set_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_tablespace_set_statement(YashanDBParser.Drop_tablespace_set_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_tablespace_set_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_tablespace_set_statement(YashanDBParser.Alter_tablespace_set_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_tablespace_set_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_tablespace_set_statement(YashanDBParser.Alter_tablespace_set_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_database_link_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_database_link_statement(YashanDBParser.Create_database_link_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_database_link_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_database_link_statement(YashanDBParser.Create_database_link_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_database_link_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_database_link_statement(YashanDBParser.Drop_database_link_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_database_link_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_database_link_statement(YashanDBParser.Drop_database_link_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_database_link_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_database_link_statement(YashanDBParser.Alter_database_link_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_database_link_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_database_link_statement(YashanDBParser.Alter_database_link_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_context_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_context_statement(YashanDBParser.Create_context_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_context_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_context_statement(YashanDBParser.Create_context_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_context_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_context_statement(YashanDBParser.Drop_context_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_context_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_context_statement(YashanDBParser.Drop_context_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_outline_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_outline_statement(YashanDBParser.Create_outline_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_outline_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_outline_statement(YashanDBParser.Create_outline_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_outline_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_outline_statement(YashanDBParser.Drop_outline_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_outline_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_outline_statement(YashanDBParser.Drop_outline_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_outline_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_outline_statement(YashanDBParser.Alter_outline_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_outline_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_outline_statement(YashanDBParser.Alter_outline_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_procedure_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_procedure_statement(YashanDBParser.Drop_procedure_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_procedure_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_procedure_statement(YashanDBParser.Drop_procedure_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_function_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_function_statement(YashanDBParser.Drop_function_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_function_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_function_statement(YashanDBParser.Drop_function_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_package_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_package_statement(YashanDBParser.Drop_package_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_package_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_package_statement(YashanDBParser.Drop_package_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_trigger_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_trigger_statement(YashanDBParser.Drop_trigger_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_trigger_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_trigger_statement(YashanDBParser.Drop_trigger_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_type_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_type_statement(YashanDBParser.Drop_type_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_type_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_type_statement(YashanDBParser.Drop_type_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#analyze_statement}.
     * @param ctx the parse tree
     */
    void enterAnalyze_statement(YashanDBParser.Analyze_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#analyze_statement}.
     * @param ctx the parse tree
     */
    void exitAnalyze_statement(YashanDBParser.Analyze_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#commit_statement}.
     * @param ctx the parse tree
     */
    void enterCommit_statement(YashanDBParser.Commit_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#commit_statement}.
     * @param ctx the parse tree
     */
    void exitCommit_statement(YashanDBParser.Commit_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#rollback_statement}.
     * @param ctx the parse tree
     */
    void enterRollback_statement(YashanDBParser.Rollback_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#rollback_statement}.
     * @param ctx the parse tree
     */
    void exitRollback_statement(YashanDBParser.Rollback_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#savepoint_statement}.
     * @param ctx the parse tree
     */
    void enterSavepoint_statement(YashanDBParser.Savepoint_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#savepoint_statement}.
     * @param ctx the parse tree
     */
    void exitSavepoint_statement(YashanDBParser.Savepoint_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#explain_statement}.
     * @param ctx the parse tree
     */
    void enterExplain_statement(YashanDBParser.Explain_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#explain_statement}.
     * @param ctx the parse tree
     */
    void exitExplain_statement(YashanDBParser.Explain_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#set_transaction_statement}.
     * @param ctx the parse tree
     */
    void enterSet_transaction_statement(YashanDBParser.Set_transaction_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#set_transaction_statement}.
     * @param ctx the parse tree
     */
    void exitSet_transaction_statement(YashanDBParser.Set_transaction_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#lock_table_statement}.
     * @param ctx the parse tree
     */
    void enterLock_table_statement(YashanDBParser.Lock_table_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#lock_table_statement}.
     * @param ctx the parse tree
     */
    void exitLock_table_statement(YashanDBParser.Lock_table_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#purge_statement}.
     * @param ctx the parse tree
     */
    void enterPurge_statement(YashanDBParser.Purge_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#purge_statement}.
     * @param ctx the parse tree
     */
    void exitPurge_statement(YashanDBParser.Purge_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#use_statement}.
     * @param ctx the parse tree
     */
    void enterUse_statement(YashanDBParser.Use_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#use_statement}.
     * @param ctx the parse tree
     */
    void exitUse_statement(YashanDBParser.Use_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#flashback_statement}.
     * @param ctx the parse tree
     */
    void enterFlashback_statement(YashanDBParser.Flashback_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#flashback_statement}.
     * @param ctx the parse tree
     */
    void exitFlashback_statement(YashanDBParser.Flashback_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_session_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_session_statement(YashanDBParser.Alter_session_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_session_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_session_statement(YashanDBParser.Alter_session_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_system_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_system_statement(YashanDBParser.Alter_system_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_system_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_system_statement(YashanDBParser.Alter_system_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_sqlmap_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_sqlmap_statement(YashanDBParser.Create_sqlmap_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_sqlmap_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_sqlmap_statement(YashanDBParser.Create_sqlmap_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_sqlmap_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_sqlmap_statement(YashanDBParser.Drop_sqlmap_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_sqlmap_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_sqlmap_statement(YashanDBParser.Drop_sqlmap_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_audit_policy_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_audit_policy_statement(YashanDBParser.Create_audit_policy_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_audit_policy_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_audit_policy_statement(YashanDBParser.Create_audit_policy_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_audit_policy_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_audit_policy_statement(YashanDBParser.Drop_audit_policy_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_audit_policy_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_audit_policy_statement(YashanDBParser.Drop_audit_policy_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_audit_policy_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_audit_policy_statement(YashanDBParser.Alter_audit_policy_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_audit_policy_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_audit_policy_statement(YashanDBParser.Alter_audit_policy_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_procedure_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_procedure_statement(YashanDBParser.Alter_procedure_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_procedure_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_procedure_statement(YashanDBParser.Alter_procedure_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_function_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_function_statement(YashanDBParser.Alter_function_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_function_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_function_statement(YashanDBParser.Alter_function_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_package_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_package_statement(YashanDBParser.Alter_package_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_package_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_package_statement(YashanDBParser.Alter_package_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_trigger_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_trigger_statement(YashanDBParser.Alter_trigger_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_trigger_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_trigger_statement(YashanDBParser.Alter_trigger_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_type_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_type_statement(YashanDBParser.Alter_type_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_type_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_type_statement(YashanDBParser.Alter_type_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_type_body_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_type_body_statement(YashanDBParser.Drop_type_body_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_type_body_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_type_body_statement(YashanDBParser.Drop_type_body_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#backup_archivelog_statement}.
     * @param ctx the parse tree
     */
    void enterBackup_archivelog_statement(YashanDBParser.Backup_archivelog_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#backup_archivelog_statement}.
     * @param ctx the parse tree
     */
    void exitBackup_archivelog_statement(YashanDBParser.Backup_archivelog_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#restore_archivelog_statement}.
     * @param ctx the parse tree
     */
    void enterRestore_archivelog_statement(YashanDBParser.Restore_archivelog_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#restore_archivelog_statement}.
     * @param ctx the parse tree
     */
    void exitRestore_archivelog_statement(YashanDBParser.Restore_archivelog_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#release_savepoint_statement}.
     * @param ctx the parse tree
     */
    void enterRelease_savepoint_statement(YashanDBParser.Release_savepoint_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#release_savepoint_statement}.
     * @param ctx the parse tree
     */
    void exitRelease_savepoint_statement(YashanDBParser.Release_savepoint_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#shutdown_statement}.
     * @param ctx the parse tree
     */
    void enterShutdown_statement(YashanDBParser.Shutdown_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#shutdown_statement}.
     * @param ctx the parse tree
     */
    void exitShutdown_statement(YashanDBParser.Shutdown_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#administer_key_management_statement}.
     * @param ctx the parse tree
     */
    void enterAdminister_key_management_statement(YashanDBParser.Administer_key_management_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#administer_key_management_statement}.
     * @param ctx the parse tree
     */
    void exitAdminister_key_management_statement(YashanDBParser.Administer_key_management_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_restore_point_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_restore_point_statement(YashanDBParser.Create_restore_point_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_restore_point_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_restore_point_statement(YashanDBParser.Create_restore_point_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_restore_point_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_restore_point_statement(YashanDBParser.Drop_restore_point_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_restore_point_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_restore_point_statement(YashanDBParser.Drop_restore_point_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_table_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_table_statement(YashanDBParser.Alter_table_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_table_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_table_statement(YashanDBParser.Alter_table_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_table_action}.
     * @param ctx the parse tree
     */
    void enterAlter_table_action(YashanDBParser.Alter_table_actionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_table_action}.
     * @param ctx the parse tree
     */
    void exitAlter_table_action(YashanDBParser.Alter_table_actionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#rename_clause}.
     * @param ctx the parse tree
     */
    void enterRename_clause(YashanDBParser.Rename_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#rename_clause}.
     * @param ctx the parse tree
     */
    void exitRename_clause(YashanDBParser.Rename_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_column_clause}.
     * @param ctx the parse tree
     */
    void enterAlter_column_clause(YashanDBParser.Alter_column_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_column_clause}.
     * @param ctx the parse tree
     */
    void exitAlter_column_clause(YashanDBParser.Alter_column_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_index_clause}.
     * @param ctx the parse tree
     */
    void enterAlter_index_clause(YashanDBParser.Alter_index_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_index_clause}.
     * @param ctx the parse tree
     */
    void exitAlter_index_clause(YashanDBParser.Alter_index_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_constraint_clause}.
     * @param ctx the parse tree
     */
    void enterAlter_constraint_clause(YashanDBParser.Alter_constraint_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_constraint_clause}.
     * @param ctx the parse tree
     */
    void exitAlter_constraint_clause(YashanDBParser.Alter_constraint_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#references_clause}.
     * @param ctx the parse tree
     */
    void enterReferences_clause(YashanDBParser.References_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#references_clause}.
     * @param ctx the parse tree
     */
    void exitReferences_clause(YashanDBParser.References_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#constraint_name}.
     * @param ctx the parse tree
     */
    void enterConstraint_name(YashanDBParser.Constraint_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#constraint_name}.
     * @param ctx the parse tree
     */
    void exitConstraint_name(YashanDBParser.Constraint_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#add_table_partition}.
     * @param ctx the parse tree
     */
    void enterAdd_table_partition(YashanDBParser.Add_table_partitionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#add_table_partition}.
     * @param ctx the parse tree
     */
    void exitAdd_table_partition(YashanDBParser.Add_table_partitionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_table_partition}.
     * @param ctx the parse tree
     */
    void enterDrop_table_partition(YashanDBParser.Drop_table_partitionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_table_partition}.
     * @param ctx the parse tree
     */
    void exitDrop_table_partition(YashanDBParser.Drop_table_partitionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_index_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_index_statement(YashanDBParser.Alter_index_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_index_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_index_statement(YashanDBParser.Alter_index_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_index_action}.
     * @param ctx the parse tree
     */
    void enterAlter_index_action(YashanDBParser.Alter_index_actionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_index_action}.
     * @param ctx the parse tree
     */
    void exitAlter_index_action(YashanDBParser.Alter_index_actionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#modify_partition}.
     * @param ctx the parse tree
     */
    void enterModify_partition(YashanDBParser.Modify_partitionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#modify_partition}.
     * @param ctx the parse tree
     */
    void exitModify_partition(YashanDBParser.Modify_partitionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#modify_subpartition}.
     * @param ctx the parse tree
     */
    void enterModify_subpartition(YashanDBParser.Modify_subpartitionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#modify_subpartition}.
     * @param ctx the parse tree
     */
    void exitModify_subpartition(YashanDBParser.Modify_subpartitionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#rebuild_clause}.
     * @param ctx the parse tree
     */
    void enterRebuild_clause(YashanDBParser.Rebuild_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#rebuild_clause}.
     * @param ctx the parse tree
     */
    void exitRebuild_clause(YashanDBParser.Rebuild_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#reclaim_index_clause}.
     * @param ctx the parse tree
     */
    void enterReclaim_index_clause(YashanDBParser.Reclaim_index_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#reclaim_index_clause}.
     * @param ctx the parse tree
     */
    void exitReclaim_index_clause(YashanDBParser.Reclaim_index_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#subpartition_name}.
     * @param ctx the parse tree
     */
    void enterSubpartition_name(YashanDBParser.Subpartition_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#subpartition_name}.
     * @param ctx the parse tree
     */
    void exitSubpartition_name(YashanDBParser.Subpartition_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#if_exists}.
     * @param ctx the parse tree
     */
    void enterIf_exists(YashanDBParser.If_existsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#if_exists}.
     * @param ctx the parse tree
     */
    void exitIf_exists(YashanDBParser.If_existsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#index_expr}.
     * @param ctx the parse tree
     */
    void enterIndex_expr(YashanDBParser.Index_exprContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#index_expr}.
     * @param ctx the parse tree
     */
    void exitIndex_expr(YashanDBParser.Index_exprContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#if_not_exists}.
     * @param ctx the parse tree
     */
    void enterIf_not_exists(YashanDBParser.If_not_existsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#if_not_exists}.
     * @param ctx the parse tree
     */
    void exitIf_not_exists(YashanDBParser.If_not_existsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#table_name}.
     * @param ctx the parse tree
     */
    void enterTable_name(YashanDBParser.Table_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#table_name}.
     * @param ctx the parse tree
     */
    void exitTable_name(YashanDBParser.Table_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#schema}.
     * @param ctx the parse tree
     */
    void enterSchema(YashanDBParser.SchemaContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#schema}.
     * @param ctx the parse tree
     */
    void exitSchema(YashanDBParser.SchemaContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#relation_properties}.
     * @param ctx the parse tree
     */
    void enterRelation_properties(YashanDBParser.Relation_propertiesContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#relation_properties}.
     * @param ctx the parse tree
     */
    void exitRelation_properties(YashanDBParser.Relation_propertiesContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#column_definition}.
     * @param ctx the parse tree
     */
    void enterColumn_definition(YashanDBParser.Column_definitionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#column_definition}.
     * @param ctx the parse tree
     */
    void exitColumn_definition(YashanDBParser.Column_definitionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#column_constraint}.
     * @param ctx the parse tree
     */
    void enterColumn_constraint(YashanDBParser.Column_constraintContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#column_constraint}.
     * @param ctx the parse tree
     */
    void exitColumn_constraint(YashanDBParser.Column_constraintContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#inline_constraint}.
     * @param ctx the parse tree
     */
    void enterInline_constraint(YashanDBParser.Inline_constraintContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#inline_constraint}.
     * @param ctx the parse tree
     */
    void exitInline_constraint(YashanDBParser.Inline_constraintContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#index_definition}.
     * @param ctx the parse tree
     */
    void enterIndex_definition(YashanDBParser.Index_definitionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#index_definition}.
     * @param ctx the parse tree
     */
    void exitIndex_definition(YashanDBParser.Index_definitionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#index_name}.
     * @param ctx the parse tree
     */
    void enterIndex_name(YashanDBParser.Index_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#index_name}.
     * @param ctx the parse tree
     */
    void exitIndex_name(YashanDBParser.Index_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#index_type}.
     * @param ctx the parse tree
     */
    void enterIndex_type(YashanDBParser.Index_typeContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#index_type}.
     * @param ctx the parse tree
     */
    void exitIndex_type(YashanDBParser.Index_typeContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#index_option}.
     * @param ctx the parse tree
     */
    void enterIndex_option(YashanDBParser.Index_optionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#index_option}.
     * @param ctx the parse tree
     */
    void exitIndex_option(YashanDBParser.Index_optionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#parser_name}.
     * @param ctx the parse tree
     */
    void enterParser_name(YashanDBParser.Parser_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#parser_name}.
     * @param ctx the parse tree
     */
    void exitParser_name(YashanDBParser.Parser_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#charset_name}.
     * @param ctx the parse tree
     */
    void enterCharset_name(YashanDBParser.Charset_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#charset_name}.
     * @param ctx the parse tree
     */
    void exitCharset_name(YashanDBParser.Charset_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#collation_name}.
     * @param ctx the parse tree
     */
    void enterCollation_name(YashanDBParser.Collation_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#collation_name}.
     * @param ctx the parse tree
     */
    void exitCollation_name(YashanDBParser.Collation_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#default_expr}.
     * @param ctx the parse tree
     */
    void enterDefault_expr(YashanDBParser.Default_exprContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#default_expr}.
     * @param ctx the parse tree
     */
    void exitDefault_expr(YashanDBParser.Default_exprContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#table_options}.
     * @param ctx the parse tree
     */
    void enterTable_options(YashanDBParser.Table_optionsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#table_options}.
     * @param ctx the parse tree
     */
    void exitTable_options(YashanDBParser.Table_optionsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#table_option}.
     * @param ctx the parse tree
     */
    void enterTable_option(YashanDBParser.Table_optionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#table_option}.
     * @param ctx the parse tree
     */
    void exitTable_option(YashanDBParser.Table_optionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#partition_options}.
     * @param ctx the parse tree
     */
    void enterPartition_options(YashanDBParser.Partition_optionsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#partition_options}.
     * @param ctx the parse tree
     */
    void exitPartition_options(YashanDBParser.Partition_optionsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#range_partitions}.
     * @param ctx the parse tree
     */
    void enterRange_partitions(YashanDBParser.Range_partitionsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#range_partitions}.
     * @param ctx the parse tree
     */
    void exitRange_partitions(YashanDBParser.Range_partitionsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#list_partitions}.
     * @param ctx the parse tree
     */
    void enterList_partitions(YashanDBParser.List_partitionsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#list_partitions}.
     * @param ctx the parse tree
     */
    void exitList_partitions(YashanDBParser.List_partitionsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#hash_partitions}.
     * @param ctx the parse tree
     */
    void enterHash_partitions(YashanDBParser.Hash_partitionsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#hash_partitions}.
     * @param ctx the parse tree
     */
    void exitHash_partitions(YashanDBParser.Hash_partitionsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#range_columns_partitions}.
     * @param ctx the parse tree
     */
    void enterRange_columns_partitions(YashanDBParser.Range_columns_partitionsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#range_columns_partitions}.
     * @param ctx the parse tree
     */
    void exitRange_columns_partitions(YashanDBParser.Range_columns_partitionsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#list_columns_partitions}.
     * @param ctx the parse tree
     */
    void enterList_columns_partitions(YashanDBParser.List_columns_partitionsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#list_columns_partitions}.
     * @param ctx the parse tree
     */
    void exitList_columns_partitions(YashanDBParser.List_columns_partitionsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#linear_hash_partitions}.
     * @param ctx the parse tree
     */
    void enterLinear_hash_partitions(YashanDBParser.Linear_hash_partitionsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#linear_hash_partitions}.
     * @param ctx the parse tree
     */
    void exitLinear_hash_partitions(YashanDBParser.Linear_hash_partitionsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#linear_key_partitions}.
     * @param ctx the parse tree
     */
    void enterLinear_key_partitions(YashanDBParser.Linear_key_partitionsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#linear_key_partitions}.
     * @param ctx the parse tree
     */
    void exitLinear_key_partitions(YashanDBParser.Linear_key_partitionsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#composite_range_partitions}.
     * @param ctx the parse tree
     */
    void enterComposite_range_partitions(YashanDBParser.Composite_range_partitionsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#composite_range_partitions}.
     * @param ctx the parse tree
     */
    void exitComposite_range_partitions(YashanDBParser.Composite_range_partitionsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#composite_list_partitions}.
     * @param ctx the parse tree
     */
    void enterComposite_list_partitions(YashanDBParser.Composite_list_partitionsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#composite_list_partitions}.
     * @param ctx the parse tree
     */
    void exitComposite_list_partitions(YashanDBParser.Composite_list_partitionsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#partition}.
     * @param ctx the parse tree
     */
    void enterPartition(YashanDBParser.PartitionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#partition}.
     * @param ctx the parse tree
     */
    void exitPartition(YashanDBParser.PartitionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#partition_name}.
     * @param ctx the parse tree
     */
    void enterPartition_name(YashanDBParser.Partition_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#partition_name}.
     * @param ctx the parse tree
     */
    void exitPartition_name(YashanDBParser.Partition_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#range_values_clause}.
     * @param ctx the parse tree
     */
    void enterRange_values_clause(YashanDBParser.Range_values_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#range_values_clause}.
     * @param ctx the parse tree
     */
    void exitRange_values_clause(YashanDBParser.Range_values_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#list_values_clause}.
     * @param ctx the parse tree
     */
    void enterList_values_clause(YashanDBParser.List_values_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#list_values_clause}.
     * @param ctx the parse tree
     */
    void exitList_values_clause(YashanDBParser.List_values_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#list_values}.
     * @param ctx the parse tree
     */
    void enterList_values(YashanDBParser.List_valuesContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#list_values}.
     * @param ctx the parse tree
     */
    void exitList_values(YashanDBParser.List_valuesContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#partition_storage_clause}.
     * @param ctx the parse tree
     */
    void enterPartition_storage_clause(YashanDBParser.Partition_storage_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#partition_storage_clause}.
     * @param ctx the parse tree
     */
    void exitPartition_storage_clause(YashanDBParser.Partition_storage_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#individual_partition_clause}.
     * @param ctx the parse tree
     */
    void enterIndividual_partition_clause(YashanDBParser.Individual_partition_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#individual_partition_clause}.
     * @param ctx the parse tree
     */
    void exitIndividual_partition_clause(YashanDBParser.Individual_partition_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#hash_partitions_by_quantity}.
     * @param ctx the parse tree
     */
    void enterHash_partitions_by_quantity(YashanDBParser.Hash_partitions_by_quantityContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#hash_partitions_by_quantity}.
     * @param ctx the parse tree
     */
    void exitHash_partitions_by_quantity(YashanDBParser.Hash_partitions_by_quantityContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#subpartition_by_hash}.
     * @param ctx the parse tree
     */
    void enterSubpartition_by_hash(YashanDBParser.Subpartition_by_hashContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#subpartition_by_hash}.
     * @param ctx the parse tree
     */
    void exitSubpartition_by_hash(YashanDBParser.Subpartition_by_hashContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#subpartition_by_key}.
     * @param ctx the parse tree
     */
    void enterSubpartition_by_key(YashanDBParser.Subpartition_by_keyContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#subpartition_by_key}.
     * @param ctx the parse tree
     */
    void exitSubpartition_by_key(YashanDBParser.Subpartition_by_keyContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#hash_subparts_by_quantity}.
     * @param ctx the parse tree
     */
    void enterHash_subparts_by_quantity(YashanDBParser.Hash_subparts_by_quantityContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#hash_subparts_by_quantity}.
     * @param ctx the parse tree
     */
    void exitHash_subparts_by_quantity(YashanDBParser.Hash_subparts_by_quantityContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#subpartition_storage_clause}.
     * @param ctx the parse tree
     */
    void enterSubpartition_storage_clause(YashanDBParser.Subpartition_storage_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#subpartition_storage_clause}.
     * @param ctx the parse tree
     */
    void exitSubpartition_storage_clause(YashanDBParser.Subpartition_storage_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#algorithm}.
     * @param ctx the parse tree
     */
    void enterAlgorithm(YashanDBParser.AlgorithmContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#algorithm}.
     * @param ctx the parse tree
     */
    void exitAlgorithm(YashanDBParser.AlgorithmContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#tablespace_name}.
     * @param ctx the parse tree
     */
    void enterTablespace_name(YashanDBParser.Tablespace_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#tablespace_name}.
     * @param ctx the parse tree
     */
    void exitTablespace_name(YashanDBParser.Tablespace_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#select_statement}.
     * @param ctx the parse tree
     */
    void enterSelect_statement(YashanDBParser.Select_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#select_statement}.
     * @param ctx the parse tree
     */
    void exitSelect_statement(YashanDBParser.Select_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#insert_statement}.
     * @param ctx the parse tree
     */
    void enterInsert_statement(YashanDBParser.Insert_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#insert_statement}.
     * @param ctx the parse tree
     */
    void exitInsert_statement(YashanDBParser.Insert_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#update_statement}.
     * @param ctx the parse tree
     */
    void enterUpdate_statement(YashanDBParser.Update_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#update_statement}.
     * @param ctx the parse tree
     */
    void exitUpdate_statement(YashanDBParser.Update_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#update_set_clause}.
     * @param ctx the parse tree
     */
    void enterUpdate_set_clause(YashanDBParser.Update_set_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#update_set_clause}.
     * @param ctx the parse tree
     */
    void exitUpdate_set_clause(YashanDBParser.Update_set_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#delete_statement}.
     * @param ctx the parse tree
     */
    void enterDelete_statement(YashanDBParser.Delete_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#delete_statement}.
     * @param ctx the parse tree
     */
    void exitDelete_statement(YashanDBParser.Delete_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#merge_statement}.
     * @param ctx the parse tree
     */
    void enterMerge_statement(YashanDBParser.Merge_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#merge_statement}.
     * @param ctx the parse tree
     */
    void exitMerge_statement(YashanDBParser.Merge_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#target_table_clause}.
     * @param ctx the parse tree
     */
    void enterTarget_table_clause(YashanDBParser.Target_table_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#target_table_clause}.
     * @param ctx the parse tree
     */
    void exitTarget_table_clause(YashanDBParser.Target_table_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#source_table_clause}.
     * @param ctx the parse tree
     */
    void enterSource_table_clause(YashanDBParser.Source_table_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#source_table_clause}.
     * @param ctx the parse tree
     */
    void exitSource_table_clause(YashanDBParser.Source_table_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#merge_update_clause}.
     * @param ctx the parse tree
     */
    void enterMerge_update_clause(YashanDBParser.Merge_update_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#merge_update_clause}.
     * @param ctx the parse tree
     */
    void exitMerge_update_clause(YashanDBParser.Merge_update_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#merge_insert_clause}.
     * @param ctx the parse tree
     */
    void enterMerge_insert_clause(YashanDBParser.Merge_insert_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#merge_insert_clause}.
     * @param ctx the parse tree
     */
    void exitMerge_insert_clause(YashanDBParser.Merge_insert_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#merge_element}.
     * @param ctx the parse tree
     */
    void enterMerge_element(YashanDBParser.Merge_elementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#merge_element}.
     * @param ctx the parse tree
     */
    void exitMerge_element(YashanDBParser.Merge_elementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#merge_update_delete_part}.
     * @param ctx the parse tree
     */
    void enterMerge_update_delete_part(YashanDBParser.Merge_update_delete_partContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#merge_update_delete_part}.
     * @param ctx the parse tree
     */
    void exitMerge_update_delete_part(YashanDBParser.Merge_update_delete_partContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#column_based_update_set_clause}.
     * @param ctx the parse tree
     */
    void enterColumn_based_update_set_clause(YashanDBParser.Column_based_update_set_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#column_based_update_set_clause}.
     * @param ctx the parse tree
     */
    void exitColumn_based_update_set_clause(YashanDBParser.Column_based_update_set_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#single_table_insert}.
     * @param ctx the parse tree
     */
    void enterSingle_table_insert(YashanDBParser.Single_table_insertContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#single_table_insert}.
     * @param ctx the parse tree
     */
    void exitSingle_table_insert(YashanDBParser.Single_table_insertContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#multi_table_insert}.
     * @param ctx the parse tree
     */
    void enterMulti_table_insert(YashanDBParser.Multi_table_insertContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#multi_table_insert}.
     * @param ctx the parse tree
     */
    void exitMulti_table_insert(YashanDBParser.Multi_table_insertContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#multi_table_insert_element}.
     * @param ctx the parse tree
     */
    void enterMulti_table_insert_element(YashanDBParser.Multi_table_insert_elementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#multi_table_insert_element}.
     * @param ctx the parse tree
     */
    void exitMulti_table_insert_element(YashanDBParser.Multi_table_insert_elementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#insert_into_clause}.
     * @param ctx the parse tree
     */
    void enterInsert_into_clause(YashanDBParser.Insert_into_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#insert_into_clause}.
     * @param ctx the parse tree
     */
    void exitInsert_into_clause(YashanDBParser.Insert_into_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#static_returning_clause}.
     * @param ctx the parse tree
     */
    void enterStatic_returning_clause(YashanDBParser.Static_returning_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#static_returning_clause}.
     * @param ctx the parse tree
     */
    void exitStatic_returning_clause(YashanDBParser.Static_returning_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#into_clause}.
     * @param ctx the parse tree
     */
    void enterInto_clause(YashanDBParser.Into_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#into_clause}.
     * @param ctx the parse tree
     */
    void exitInto_clause(YashanDBParser.Into_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#on_duplicate_clause}.
     * @param ctx the parse tree
     */
    void enterOn_duplicate_clause(YashanDBParser.On_duplicate_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#on_duplicate_clause}.
     * @param ctx the parse tree
     */
    void exitOn_duplicate_clause(YashanDBParser.On_duplicate_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#set_clause}.
     * @param ctx the parse tree
     */
    void enterSet_clause(YashanDBParser.Set_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#set_clause}.
     * @param ctx the parse tree
     */
    void exitSet_clause(YashanDBParser.Set_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#general_element}.
     * @param ctx the parse tree
     */
    void enterGeneral_element(YashanDBParser.General_elementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#general_element}.
     * @param ctx the parse tree
     */
    void exitGeneral_element(YashanDBParser.General_elementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#general_element_part}.
     * @param ctx the parse tree
     */
    void enterGeneral_element_part(YashanDBParser.General_element_partContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#general_element_part}.
     * @param ctx the parse tree
     */
    void exitGeneral_element_part(YashanDBParser.General_element_partContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#function_argument}.
     * @param ctx the parse tree
     */
    void enterFunction_argument(YashanDBParser.Function_argumentContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#function_argument}.
     * @param ctx the parse tree
     */
    void exitFunction_argument(YashanDBParser.Function_argumentContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#argument}.
     * @param ctx the parse tree
     */
    void enterArgument(YashanDBParser.ArgumentContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#argument}.
     * @param ctx the parse tree
     */
    void exitArgument(YashanDBParser.ArgumentContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#general_table_ref}.
     * @param ctx the parse tree
     */
    void enterGeneral_table_ref(YashanDBParser.General_table_refContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#general_table_ref}.
     * @param ctx the parse tree
     */
    void exitGeneral_table_ref(YashanDBParser.General_table_refContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#paren_column_list}.
     * @param ctx the parse tree
     */
    void enterParen_column_list(YashanDBParser.Paren_column_listContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#paren_column_list}.
     * @param ctx the parse tree
     */
    void exitParen_column_list(YashanDBParser.Paren_column_listContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#values_clause}.
     * @param ctx the parse tree
     */
    void enterValues_clause(YashanDBParser.Values_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#values_clause}.
     * @param ctx the parse tree
     */
    void exitValues_clause(YashanDBParser.Values_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#paren_expressions}.
     * @param ctx the parse tree
     */
    void enterParen_expressions(YashanDBParser.Paren_expressionsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#paren_expressions}.
     * @param ctx the parse tree
     */
    void exitParen_expressions(YashanDBParser.Paren_expressionsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#for_update_clause}.
     * @param ctx the parse tree
     */
    void enterFor_update_clause(YashanDBParser.For_update_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#for_update_clause}.
     * @param ctx the parse tree
     */
    void exitFor_update_clause(YashanDBParser.For_update_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#for_update_of_part}.
     * @param ctx the parse tree
     */
    void enterFor_update_of_part(YashanDBParser.For_update_of_partContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#for_update_of_part}.
     * @param ctx the parse tree
     */
    void exitFor_update_of_part(YashanDBParser.For_update_of_partContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#column_list}.
     * @param ctx the parse tree
     */
    void enterColumn_list(YashanDBParser.Column_listContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#column_list}.
     * @param ctx the parse tree
     */
    void exitColumn_list(YashanDBParser.Column_listContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#column_name}.
     * @param ctx the parse tree
     */
    void enterColumn_name(YashanDBParser.Column_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#column_name}.
     * @param ctx the parse tree
     */
    void exitColumn_name(YashanDBParser.Column_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#column_field}.
     * @param ctx the parse tree
     */
    void enterColumn_field(YashanDBParser.Column_fieldContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#column_field}.
     * @param ctx the parse tree
     */
    void exitColumn_field(YashanDBParser.Column_fieldContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#for_update_options}.
     * @param ctx the parse tree
     */
    void enterFor_update_options(YashanDBParser.For_update_optionsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#for_update_options}.
     * @param ctx the parse tree
     */
    void exitFor_update_options(YashanDBParser.For_update_optionsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#pseudo_column}.
     * @param ctx the parse tree
     */
    void enterPseudo_column(YashanDBParser.Pseudo_columnContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#pseudo_column}.
     * @param ctx the parse tree
     */
    void exitPseudo_column(YashanDBParser.Pseudo_columnContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#subquery}.
     * @param ctx the parse tree
     */
    void enterSubquery(YashanDBParser.SubqueryContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#subquery}.
     * @param ctx the parse tree
     */
    void exitSubquery(YashanDBParser.SubqueryContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#pagination_clause}.
     * @param ctx the parse tree
     */
    void enterPagination_clause(YashanDBParser.Pagination_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#pagination_clause}.
     * @param ctx the parse tree
     */
    void exitPagination_clause(YashanDBParser.Pagination_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#row_limiting_clause}.
     * @param ctx the parse tree
     */
    void enterRow_limiting_clause(YashanDBParser.Row_limiting_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#row_limiting_clause}.
     * @param ctx the parse tree
     */
    void exitRow_limiting_clause(YashanDBParser.Row_limiting_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#offset_fetch_clause}.
     * @param ctx the parse tree
     */
    void enterOffset_fetch_clause(YashanDBParser.Offset_fetch_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#offset_fetch_clause}.
     * @param ctx the parse tree
     */
    void exitOffset_fetch_clause(YashanDBParser.Offset_fetch_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#offset_clause}.
     * @param ctx the parse tree
     */
    void enterOffset_clause(YashanDBParser.Offset_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#offset_clause}.
     * @param ctx the parse tree
     */
    void exitOffset_clause(YashanDBParser.Offset_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#fetch_clause}.
     * @param ctx the parse tree
     */
    void enterFetch_clause(YashanDBParser.Fetch_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#fetch_clause}.
     * @param ctx the parse tree
     */
    void exitFetch_clause(YashanDBParser.Fetch_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#subquery_body}.
     * @param ctx the parse tree
     */
    void enterSubquery_body(YashanDBParser.Subquery_bodyContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#subquery_body}.
     * @param ctx the parse tree
     */
    void exitSubquery_body(YashanDBParser.Subquery_bodyContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#query_block}.
     * @param ctx the parse tree
     */
    void enterQuery_block(YashanDBParser.Query_blockContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#query_block}.
     * @param ctx the parse tree
     */
    void exitQuery_block(YashanDBParser.Query_blockContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#hierarchical_group_clause}.
     * @param ctx the parse tree
     */
    void enterHierarchical_group_clause(YashanDBParser.Hierarchical_group_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#hierarchical_group_clause}.
     * @param ctx the parse tree
     */
    void exitHierarchical_group_clause(YashanDBParser.Hierarchical_group_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#group_by_clause}.
     * @param ctx the parse tree
     */
    void enterGroup_by_clause(YashanDBParser.Group_by_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#group_by_clause}.
     * @param ctx the parse tree
     */
    void exitGroup_by_clause(YashanDBParser.Group_by_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#group_by_right_part}.
     * @param ctx the parse tree
     */
    void enterGroup_by_right_part(YashanDBParser.Group_by_right_partContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#group_by_right_part}.
     * @param ctx the parse tree
     */
    void exitGroup_by_right_part(YashanDBParser.Group_by_right_partContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#rollup_cube_clause}.
     * @param ctx the parse tree
     */
    void enterRollup_cube_clause(YashanDBParser.Rollup_cube_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#rollup_cube_clause}.
     * @param ctx the parse tree
     */
    void exitRollup_cube_clause(YashanDBParser.Rollup_cube_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#grouping_sets_clause}.
     * @param ctx the parse tree
     */
    void enterGrouping_sets_clause(YashanDBParser.Grouping_sets_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#grouping_sets_clause}.
     * @param ctx the parse tree
     */
    void exitGrouping_sets_clause(YashanDBParser.Grouping_sets_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#having_clause}.
     * @param ctx the parse tree
     */
    void enterHaving_clause(YashanDBParser.Having_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#having_clause}.
     * @param ctx the parse tree
     */
    void exitHaving_clause(YashanDBParser.Having_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#from_clause}.
     * @param ctx the parse tree
     */
    void enterFrom_clause(YashanDBParser.From_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#from_clause}.
     * @param ctx the parse tree
     */
    void exitFrom_clause(YashanDBParser.From_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#table_clause}.
     * @param ctx the parse tree
     */
    void enterTable_clause(YashanDBParser.Table_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#table_clause}.
     * @param ctx the parse tree
     */
    void exitTable_clause(YashanDBParser.Table_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#table_ref}.
     * @param ctx the parse tree
     */
    void enterTable_ref(YashanDBParser.Table_refContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#table_ref}.
     * @param ctx the parse tree
     */
    void exitTable_ref(YashanDBParser.Table_refContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#flashback_query_clause}.
     * @param ctx the parse tree
     */
    void enterFlashback_query_clause(YashanDBParser.Flashback_query_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#flashback_query_clause}.
     * @param ctx the parse tree
     */
    void exitFlashback_query_clause(YashanDBParser.Flashback_query_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#pivot_clause}.
     * @param ctx the parse tree
     */
    void enterPivot_clause(YashanDBParser.Pivot_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#pivot_clause}.
     * @param ctx the parse tree
     */
    void exitPivot_clause(YashanDBParser.Pivot_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#pivot_element}.
     * @param ctx the parse tree
     */
    void enterPivot_element(YashanDBParser.Pivot_elementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#pivot_element}.
     * @param ctx the parse tree
     */
    void exitPivot_element(YashanDBParser.Pivot_elementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#pivot_for_clause}.
     * @param ctx the parse tree
     */
    void enterPivot_for_clause(YashanDBParser.Pivot_for_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#pivot_for_clause}.
     * @param ctx the parse tree
     */
    void exitPivot_for_clause(YashanDBParser.Pivot_for_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#pivot_in_clause}.
     * @param ctx the parse tree
     */
    void enterPivot_in_clause(YashanDBParser.Pivot_in_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#pivot_in_clause}.
     * @param ctx the parse tree
     */
    void exitPivot_in_clause(YashanDBParser.Pivot_in_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#pivot_in_clause_element}.
     * @param ctx the parse tree
     */
    void enterPivot_in_clause_element(YashanDBParser.Pivot_in_clause_elementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#pivot_in_clause_element}.
     * @param ctx the parse tree
     */
    void exitPivot_in_clause_element(YashanDBParser.Pivot_in_clause_elementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#tableview_name}.
     * @param ctx the parse tree
     */
    void enterTableview_name(YashanDBParser.Tableview_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#tableview_name}.
     * @param ctx the parse tree
     */
    void exitTableview_name(YashanDBParser.Tableview_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#table_collection_expression}.
     * @param ctx the parse tree
     */
    void enterTable_collection_expression(YashanDBParser.Table_collection_expressionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#table_collection_expression}.
     * @param ctx the parse tree
     */
    void exitTable_collection_expression(YashanDBParser.Table_collection_expressionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#collection_expression}.
     * @param ctx the parse tree
     */
    void enterCollection_expression(YashanDBParser.Collection_expressionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#collection_expression}.
     * @param ctx the parse tree
     */
    void exitCollection_expression(YashanDBParser.Collection_expressionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#t_alias}.
     * @param ctx the parse tree
     */
    void enterT_alias(YashanDBParser.T_aliasContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#t_alias}.
     * @param ctx the parse tree
     */
    void exitT_alias(YashanDBParser.T_aliasContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#sample_clause}.
     * @param ctx the parse tree
     */
    void enterSample_clause(YashanDBParser.Sample_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#sample_clause}.
     * @param ctx the parse tree
     */
    void exitSample_clause(YashanDBParser.Sample_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#seed_part}.
     * @param ctx the parse tree
     */
    void enterSeed_part(YashanDBParser.Seed_partContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#seed_part}.
     * @param ctx the parse tree
     */
    void exitSeed_part(YashanDBParser.Seed_partContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#partition_extension_clause}.
     * @param ctx the parse tree
     */
    void enterPartition_extension_clause(YashanDBParser.Partition_extension_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#partition_extension_clause}.
     * @param ctx the parse tree
     */
    void exitPartition_extension_clause(YashanDBParser.Partition_extension_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#partition_part}.
     * @param ctx the parse tree
     */
    void enterPartition_part(YashanDBParser.Partition_partContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#partition_part}.
     * @param ctx the parse tree
     */
    void exitPartition_part(YashanDBParser.Partition_partContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#dblink}.
     * @param ctx the parse tree
     */
    void enterDblink(YashanDBParser.DblinkContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#dblink}.
     * @param ctx the parse tree
     */
    void exitDblink(YashanDBParser.DblinkContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#dblink_name}.
     * @param ctx the parse tree
     */
    void enterDblink_name(YashanDBParser.Dblink_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#dblink_name}.
     * @param ctx the parse tree
     */
    void exitDblink_name(YashanDBParser.Dblink_nameContext ctx);

    /**
     * Enter a parse tree produced by the {@code cartesian_product}
     * labeled alternative in {@link YashanDBParser#join_clause}.
     * @param ctx the parse tree
     */
    void enterCartesian_product(YashanDBParser.Cartesian_productContext ctx);

    /**
     * Exit a parse tree produced by the {@code cartesian_product}
     * labeled alternative in {@link YashanDBParser#join_clause}.
     * @param ctx the parse tree
     */
    void exitCartesian_product(YashanDBParser.Cartesian_productContext ctx);

    /**
     * Enter a parse tree produced by the {@code table_join}
     * labeled alternative in {@link YashanDBParser#join_clause}.
     * @param ctx the parse tree
     */
    void enterTable_join(YashanDBParser.Table_joinContext ctx);

    /**
     * Exit a parse tree produced by the {@code table_join}
     * labeled alternative in {@link YashanDBParser#join_clause}.
     * @param ctx the parse tree
     */
    void exitTable_join(YashanDBParser.Table_joinContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#join}.
     * @param ctx the parse tree
     */
    void enterJoin(YashanDBParser.JoinContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#join}.
     * @param ctx the parse tree
     */
    void exitJoin(YashanDBParser.JoinContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#inner_cross_join_clause}.
     * @param ctx the parse tree
     */
    void enterInner_cross_join_clause(YashanDBParser.Inner_cross_join_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#inner_cross_join_clause}.
     * @param ctx the parse tree
     */
    void exitInner_cross_join_clause(YashanDBParser.Inner_cross_join_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#outer_join_clause}.
     * @param ctx the parse tree
     */
    void enterOuter_join_clause(YashanDBParser.Outer_join_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#outer_join_clause}.
     * @param ctx the parse tree
     */
    void exitOuter_join_clause(YashanDBParser.Outer_join_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#where_clause}.
     * @param ctx the parse tree
     */
    void enterWhere_clause(YashanDBParser.Where_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#where_clause}.
     * @param ctx the parse tree
     */
    void exitWhere_clause(YashanDBParser.Where_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#with_clause}.
     * @param ctx the parse tree
     */
    void enterWith_clause(YashanDBParser.With_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#with_clause}.
     * @param ctx the parse tree
     */
    void exitWith_clause(YashanDBParser.With_clauseContext ctx);

    /**
     * Enter a parse tree produced by the {@code connect_by_clause1}
     * labeled alternative in {@link YashanDBParser#hierarchical_query_clause}.
     * @param ctx the parse tree
     */
    void enterConnect_by_clause1(YashanDBParser.Connect_by_clause1Context ctx);

    /**
     * Exit a parse tree produced by the {@code connect_by_clause1}
     * labeled alternative in {@link YashanDBParser#hierarchical_query_clause}.
     * @param ctx the parse tree
     */
    void exitConnect_by_clause1(YashanDBParser.Connect_by_clause1Context ctx);

    /**
     * Enter a parse tree produced by the {@code connect_by_clause2}
     * labeled alternative in {@link YashanDBParser#hierarchical_query_clause}.
     * @param ctx the parse tree
     */
    void enterConnect_by_clause2(YashanDBParser.Connect_by_clause2Context ctx);

    /**
     * Exit a parse tree produced by the {@code connect_by_clause2}
     * labeled alternative in {@link YashanDBParser#hierarchical_query_clause}.
     * @param ctx the parse tree
     */
    void exitConnect_by_clause2(YashanDBParser.Connect_by_clause2Context ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#start_with_clause}.
     * @param ctx the parse tree
     */
    void enterStart_with_clause(YashanDBParser.Start_with_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#start_with_clause}.
     * @param ctx the parse tree
     */
    void exitStart_with_clause(YashanDBParser.Start_with_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#connect_by_clause}.
     * @param ctx the parse tree
     */
    void enterConnect_by_clause(YashanDBParser.Connect_by_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#connect_by_clause}.
     * @param ctx the parse tree
     */
    void exitConnect_by_clause(YashanDBParser.Connect_by_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#cte_clause_list}.
     * @param ctx the parse tree
     */
    void enterCte_clause_list(YashanDBParser.Cte_clause_listContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#cte_clause_list}.
     * @param ctx the parse tree
     */
    void exitCte_clause_list(YashanDBParser.Cte_clause_listContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#cte_clause}.
     * @param ctx the parse tree
     */
    void enterCte_clause(YashanDBParser.Cte_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#cte_clause}.
     * @param ctx the parse tree
     */
    void exitCte_clause(YashanDBParser.Cte_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#select_list}.
     * @param ctx the parse tree
     */
    void enterSelect_list(YashanDBParser.Select_listContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#select_list}.
     * @param ctx the parse tree
     */
    void exitSelect_list(YashanDBParser.Select_listContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#select_list_element}.
     * @param ctx the parse tree
     */
    void enterSelect_list_element(YashanDBParser.Select_list_elementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#select_list_element}.
     * @param ctx the parse tree
     */
    void exitSelect_list_element(YashanDBParser.Select_list_elementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#expression_clause}.
     * @param ctx the parse tree
     */
    void enterExpression_clause(YashanDBParser.Expression_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#expression_clause}.
     * @param ctx the parse tree
     */
    void exitExpression_clause(YashanDBParser.Expression_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#case_clause}.
     * @param ctx the parse tree
     */
    void enterCase_clause(YashanDBParser.Case_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#case_clause}.
     * @param ctx the parse tree
     */
    void exitCase_clause(YashanDBParser.Case_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#else_clause}.
     * @param ctx the parse tree
     */
    void enterElse_clause(YashanDBParser.Else_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#else_clause}.
     * @param ctx the parse tree
     */
    void exitElse_clause(YashanDBParser.Else_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#case_expression_list}.
     * @param ctx the parse tree
     */
    void enterCase_expression_list(YashanDBParser.Case_expression_listContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#case_expression_list}.
     * @param ctx the parse tree
     */
    void exitCase_expression_list(YashanDBParser.Case_expression_listContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#case_expression}.
     * @param ctx the parse tree
     */
    void enterCase_expression(YashanDBParser.Case_expressionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#case_expression}.
     * @param ctx the parse tree
     */
    void exitCase_expression(YashanDBParser.Case_expressionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#simple_case_expression}.
     * @param ctx the parse tree
     */
    void enterSimple_case_expression(YashanDBParser.Simple_case_expressionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#simple_case_expression}.
     * @param ctx the parse tree
     */
    void exitSimple_case_expression(YashanDBParser.Simple_case_expressionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#searched_case_expression}.
     * @param ctx the parse tree
     */
    void enterSearched_case_expression(YashanDBParser.Searched_case_expressionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#searched_case_expression}.
     * @param ctx the parse tree
     */
    void exitSearched_case_expression(YashanDBParser.Searched_case_expressionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#searched_case_when_list}.
     * @param ctx the parse tree
     */
    void enterSearched_case_when_list(YashanDBParser.Searched_case_when_listContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#searched_case_when_list}.
     * @param ctx the parse tree
     */
    void exitSearched_case_when_list(YashanDBParser.Searched_case_when_listContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#searched_case_when}.
     * @param ctx the parse tree
     */
    void enterSearched_case_when(YashanDBParser.Searched_case_whenContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#searched_case_when}.
     * @param ctx the parse tree
     */
    void exitSearched_case_when(YashanDBParser.Searched_case_whenContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#simple_case_when_list}.
     * @param ctx the parse tree
     */
    void enterSimple_case_when_list(YashanDBParser.Simple_case_when_listContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#simple_case_when_list}.
     * @param ctx the parse tree
     */
    void exitSimple_case_when_list(YashanDBParser.Simple_case_when_listContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#simple_case_when}.
     * @param ctx the parse tree
     */
    void enterSimple_case_when(YashanDBParser.Simple_case_whenContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#simple_case_when}.
     * @param ctx the parse tree
     */
    void exitSimple_case_when(YashanDBParser.Simple_case_whenContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#order_by_clause}.
     * @param ctx the parse tree
     */
    void enterOrder_by_clause(YashanDBParser.Order_by_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#order_by_clause}.
     * @param ctx the parse tree
     */
    void exitOrder_by_clause(YashanDBParser.Order_by_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#order_by_elements}.
     * @param ctx the parse tree
     */
    void enterOrder_by_elements(YashanDBParser.Order_by_elementsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#order_by_elements}.
     * @param ctx the parse tree
     */
    void exitOrder_by_elements(YashanDBParser.Order_by_elementsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#column_alias}.
     * @param ctx the parse tree
     */
    void enterColumn_alias(YashanDBParser.Column_aliasContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#column_alias}.
     * @param ctx the parse tree
     */
    void exitColumn_alias(YashanDBParser.Column_aliasContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#expression}.
     * @param ctx the parse tree
     */
    void enterExpression(YashanDBParser.ExpressionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#expression}.
     * @param ctx the parse tree
     */
    void exitExpression(YashanDBParser.ExpressionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#literal}.
     * @param ctx the parse tree
     */
    void enterLiteral(YashanDBParser.LiteralContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#literal}.
     * @param ctx the parse tree
     */
    void exitLiteral(YashanDBParser.LiteralContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#date_literal}.
     * @param ctx the parse tree
     */
    void enterDate_literal(YashanDBParser.Date_literalContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#date_literal}.
     * @param ctx the parse tree
     */
    void exitDate_literal(YashanDBParser.Date_literalContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#timestamp_literal}.
     * @param ctx the parse tree
     */
    void enterTimestamp_literal(YashanDBParser.Timestamp_literalContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#timestamp_literal}.
     * @param ctx the parse tree
     */
    void exitTimestamp_literal(YashanDBParser.Timestamp_literalContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#interval_year_to_month_literal}.
     * @param ctx the parse tree
     */
    void enterInterval_year_to_month_literal(YashanDBParser.Interval_year_to_month_literalContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#interval_year_to_month_literal}.
     * @param ctx the parse tree
     */
    void exitInterval_year_to_month_literal(YashanDBParser.Interval_year_to_month_literalContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#interval_day_to_second_literal}.
     * @param ctx the parse tree
     */
    void enterInterval_day_to_second_literal(YashanDBParser.Interval_day_to_second_literalContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#interval_day_to_second_literal}.
     * @param ctx the parse tree
     */
    void exitInterval_day_to_second_literal(YashanDBParser.Interval_day_to_second_literalContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#day}.
     * @param ctx the parse tree
     */
    void enterDay(YashanDBParser.DayContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#day}.
     * @param ctx the parse tree
     */
    void exitDay(YashanDBParser.DayContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#minute}.
     * @param ctx the parse tree
     */
    void enterMinute(YashanDBParser.MinuteContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#minute}.
     * @param ctx the parse tree
     */
    void exitMinute(YashanDBParser.MinuteContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#hour}.
     * @param ctx the parse tree
     */
    void enterHour(YashanDBParser.HourContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#hour}.
     * @param ctx the parse tree
     */
    void exitHour(YashanDBParser.HourContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#second}.
     * @param ctx the parse tree
     */
    void enterSecond(YashanDBParser.SecondContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#second}.
     * @param ctx the parse tree
     */
    void exitSecond(YashanDBParser.SecondContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#constant}.
     * @param ctx the parse tree
     */
    void enterConstant(YashanDBParser.ConstantContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#constant}.
     * @param ctx the parse tree
     */
    void exitConstant(YashanDBParser.ConstantContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#quoted_string}.
     * @param ctx the parse tree
     */
    void enterQuoted_string(YashanDBParser.Quoted_stringContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#quoted_string}.
     * @param ctx the parse tree
     */
    void exitQuoted_string(YashanDBParser.Quoted_stringContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#bind_variable}.
     * @param ctx the parse tree
     */
    void enterBind_variable(YashanDBParser.Bind_variableContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#bind_variable}.
     * @param ctx the parse tree
     */
    void exitBind_variable(YashanDBParser.Bind_variableContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#regular_id}.
     * @param ctx the parse tree
     */
    void enterRegular_id(YashanDBParser.Regular_idContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#regular_id}.
     * @param ctx the parse tree
     */
    void exitRegular_id(YashanDBParser.Regular_idContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#non_reserved_keywords}.
     * @param ctx the parse tree
     */
    void enterNon_reserved_keywords(YashanDBParser.Non_reserved_keywordsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#non_reserved_keywords}.
     * @param ctx the parse tree
     */
    void exitNon_reserved_keywords(YashanDBParser.Non_reserved_keywordsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#numeric}.
     * @param ctx the parse tree
     */
    void enterNumeric(YashanDBParser.NumericContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#numeric}.
     * @param ctx the parse tree
     */
    void exitNumeric(YashanDBParser.NumericContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#expression_list}.
     * @param ctx the parse tree
     */
    void enterExpression_list(YashanDBParser.Expression_listContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#expression_list}.
     * @param ctx the parse tree
     */
    void exitExpression_list(YashanDBParser.Expression_listContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#expressions}.
     * @param ctx the parse tree
     */
    void enterExpressions(YashanDBParser.ExpressionsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#expressions}.
     * @param ctx the parse tree
     */
    void exitExpressions(YashanDBParser.ExpressionsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#standard_function}.
     * @param ctx the parse tree
     */
    void enterStandard_function(YashanDBParser.Standard_functionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#standard_function}.
     * @param ctx the parse tree
     */
    void exitStandard_function(YashanDBParser.Standard_functionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#xml_function}.
     * @param ctx the parse tree
     */
    void enterXml_function(YashanDBParser.Xml_functionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#xml_function}.
     * @param ctx the parse tree
     */
    void exitXml_function(YashanDBParser.Xml_functionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#xml_namespaces_clause}.
     * @param ctx the parse tree
     */
    void enterXml_namespaces_clause(YashanDBParser.Xml_namespaces_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#xml_namespaces_clause}.
     * @param ctx the parse tree
     */
    void exitXml_namespaces_clause(YashanDBParser.Xml_namespaces_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#xml_general_default_part}.
     * @param ctx the parse tree
     */
    void enterXml_general_default_part(YashanDBParser.Xml_general_default_partContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#xml_general_default_part}.
     * @param ctx the parse tree
     */
    void exitXml_general_default_part(YashanDBParser.Xml_general_default_partContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#xmltable}.
     * @param ctx the parse tree
     */
    void enterXmltable(YashanDBParser.XmltableContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#xmltable}.
     * @param ctx the parse tree
     */
    void exitXmltable(YashanDBParser.XmltableContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#xml_passing_clause}.
     * @param ctx the parse tree
     */
    void enterXml_passing_clause(YashanDBParser.Xml_passing_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#xml_passing_clause}.
     * @param ctx the parse tree
     */
    void exitXml_passing_clause(YashanDBParser.Xml_passing_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#xml_table_column}.
     * @param ctx the parse tree
     */
    void enterXml_table_column(YashanDBParser.Xml_table_columnContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#xml_table_column}.
     * @param ctx the parse tree
     */
    void exitXml_table_column(YashanDBParser.Xml_table_columnContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#xml_column_name}.
     * @param ctx the parse tree
     */
    void enterXml_column_name(YashanDBParser.Xml_column_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#xml_column_name}.
     * @param ctx the parse tree
     */
    void exitXml_column_name(YashanDBParser.Xml_column_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#string_function}.
     * @param ctx the parse tree
     */
    void enterString_function(YashanDBParser.String_functionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#string_function}.
     * @param ctx the parse tree
     */
    void exitString_function(YashanDBParser.String_functionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#numeric_function}.
     * @param ctx the parse tree
     */
    void enterNumeric_function(YashanDBParser.Numeric_functionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#numeric_function}.
     * @param ctx the parse tree
     */
    void exitNumeric_function(YashanDBParser.Numeric_functionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#json_function}.
     * @param ctx the parse tree
     */
    void enterJson_function(YashanDBParser.Json_functionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#json_function}.
     * @param ctx the parse tree
     */
    void exitJson_function(YashanDBParser.Json_functionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#date_function}.
     * @param ctx the parse tree
     */
    void enterDate_function(YashanDBParser.Date_functionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#date_function}.
     * @param ctx the parse tree
     */
    void exitDate_function(YashanDBParser.Date_functionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#aggregate_function}.
     * @param ctx the parse tree
     */
    void enterAggregate_function(YashanDBParser.Aggregate_functionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#aggregate_function}.
     * @param ctx the parse tree
     */
    void exitAggregate_function(YashanDBParser.Aggregate_functionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#window_function}.
     * @param ctx the parse tree
     */
    void enterWindow_function(YashanDBParser.Window_functionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#window_function}.
     * @param ctx the parse tree
     */
    void exitWindow_function(YashanDBParser.Window_functionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#other_function}.
     * @param ctx the parse tree
     */
    void enterOther_function(YashanDBParser.Other_functionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#other_function}.
     * @param ctx the parse tree
     */
    void exitOther_function(YashanDBParser.Other_functionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#dbms_funcion}.
     * @param ctx the parse tree
     */
    void enterDbms_funcion(YashanDBParser.Dbms_funcionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#dbms_funcion}.
     * @param ctx the parse tree
     */
    void exitDbms_funcion(YashanDBParser.Dbms_funcionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#datatype_function}.
     * @param ctx the parse tree
     */
    void enterDatatype_function(YashanDBParser.Datatype_functionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#datatype_function}.
     * @param ctx the parse tree
     */
    void exitDatatype_function(YashanDBParser.Datatype_functionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#gis_function}.
     * @param ctx the parse tree
     */
    void enterGis_function(YashanDBParser.Gis_functionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#gis_function}.
     * @param ctx the parse tree
     */
    void exitGis_function(YashanDBParser.Gis_functionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#column_member_func}.
     * @param ctx the parse tree
     */
    void enterColumn_member_func(YashanDBParser.Column_member_funcContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#column_member_func}.
     * @param ctx the parse tree
     */
    void exitColumn_member_func(YashanDBParser.Column_member_funcContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#xml_member_func}.
     * @param ctx the parse tree
     */
    void enterXml_member_func(YashanDBParser.Xml_member_funcContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#xml_member_func}.
     * @param ctx the parse tree
     */
    void exitXml_member_func(YashanDBParser.Xml_member_funcContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#respect_or_ignore_nulls}.
     * @param ctx the parse tree
     */
    void enterRespect_or_ignore_nulls(YashanDBParser.Respect_or_ignore_nullsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#respect_or_ignore_nulls}.
     * @param ctx the parse tree
     */
    void exitRespect_or_ignore_nulls(YashanDBParser.Respect_or_ignore_nullsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#over_clause}.
     * @param ctx the parse tree
     */
    void enterOver_clause(YashanDBParser.Over_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#over_clause}.
     * @param ctx the parse tree
     */
    void exitOver_clause(YashanDBParser.Over_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#windowing_clause}.
     * @param ctx the parse tree
     */
    void enterWindowing_clause(YashanDBParser.Windowing_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#windowing_clause}.
     * @param ctx the parse tree
     */
    void exitWindowing_clause(YashanDBParser.Windowing_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#windowing_type}.
     * @param ctx the parse tree
     */
    void enterWindowing_type(YashanDBParser.Windowing_typeContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#windowing_type}.
     * @param ctx the parse tree
     */
    void exitWindowing_type(YashanDBParser.Windowing_typeContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#windowing_elements}.
     * @param ctx the parse tree
     */
    void enterWindowing_elements(YashanDBParser.Windowing_elementsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#windowing_elements}.
     * @param ctx the parse tree
     */
    void exitWindowing_elements(YashanDBParser.Windowing_elementsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#listagg_overflow_clause}.
     * @param ctx the parse tree
     */
    void enterListagg_overflow_clause(YashanDBParser.Listagg_overflow_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#listagg_overflow_clause}.
     * @param ctx the parse tree
     */
    void exitListagg_overflow_clause(YashanDBParser.Listagg_overflow_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#within_over_part}.
     * @param ctx the parse tree
     */
    void enterWithin_over_part(YashanDBParser.Within_over_partContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#within_over_part}.
     * @param ctx the parse tree
     */
    void exitWithin_over_part(YashanDBParser.Within_over_partContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#query_partition_clause}.
     * @param ctx the parse tree
     */
    void enterQuery_partition_clause(YashanDBParser.Query_partition_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#query_partition_clause}.
     * @param ctx the parse tree
     */
    void exitQuery_partition_clause(YashanDBParser.Query_partition_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#wrapper_clause}.
     * @param ctx the parse tree
     */
    void enterWrapper_clause(YashanDBParser.Wrapper_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#wrapper_clause}.
     * @param ctx the parse tree
     */
    void exitWrapper_clause(YashanDBParser.Wrapper_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#returning_clause}.
     * @param ctx the parse tree
     */
    void enterReturning_clause(YashanDBParser.Returning_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#returning_clause}.
     * @param ctx the parse tree
     */
    void exitReturning_clause(YashanDBParser.Returning_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#numeric_literal}.
     * @param ctx the parse tree
     */
    void enterNumeric_literal(YashanDBParser.Numeric_literalContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#numeric_literal}.
     * @param ctx the parse tree
     */
    void exitNumeric_literal(YashanDBParser.Numeric_literalContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#table_element}.
     * @param ctx the parse tree
     */
    void enterTable_element(YashanDBParser.Table_elementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#table_element}.
     * @param ctx the parse tree
     */
    void exitTable_element(YashanDBParser.Table_elementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#char_set_name}.
     * @param ctx the parse tree
     */
    void enterChar_set_name(YashanDBParser.Char_set_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#char_set_name}.
     * @param ctx the parse tree
     */
    void exitChar_set_name(YashanDBParser.Char_set_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#string_delimiter}.
     * @param ctx the parse tree
     */
    void enterString_delimiter(YashanDBParser.String_delimiterContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#string_delimiter}.
     * @param ctx the parse tree
     */
    void exitString_delimiter(YashanDBParser.String_delimiterContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#id_expression}.
     * @param ctx the parse tree
     */
    void enterId_expression(YashanDBParser.Id_expressionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#id_expression}.
     * @param ctx the parse tree
     */
    void exitId_expression(YashanDBParser.Id_expressionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#condition}.
     * @param ctx the parse tree
     */
    void enterCondition(YashanDBParser.ConditionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#condition}.
     * @param ctx the parse tree
     */
    void exitCondition(YashanDBParser.ConditionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#unary_expression}.
     * @param ctx the parse tree
     */
    void enterUnary_expression(YashanDBParser.Unary_expressionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#unary_expression}.
     * @param ctx the parse tree
     */
    void exitUnary_expression(YashanDBParser.Unary_expressionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#compound_expression}.
     * @param ctx the parse tree
     */
    void enterCompound_expression(YashanDBParser.Compound_expressionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#compound_expression}.
     * @param ctx the parse tree
     */
    void exitCompound_expression(YashanDBParser.Compound_expressionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#relational_operator}.
     * @param ctx the parse tree
     */
    void enterRelational_operator(YashanDBParser.Relational_operatorContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#relational_operator}.
     * @param ctx the parse tree
     */
    void exitRelational_operator(YashanDBParser.Relational_operatorContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#subquery_expression}.
     * @param ctx the parse tree
     */
    void enterSubquery_expression(YashanDBParser.Subquery_expressionContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#subquery_expression}.
     * @param ctx the parse tree
     */
    void exitSubquery_expression(YashanDBParser.Subquery_expressionContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#subquery_operator}.
     * @param ctx the parse tree
     */
    void enterSubquery_operator(YashanDBParser.Subquery_operatorContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#subquery_operator}.
     * @param ctx the parse tree
     */
    void exitSubquery_operator(YashanDBParser.Subquery_operatorContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#none_arguments}.
     * @param ctx the parse tree
     */
    void enterNone_arguments(YashanDBParser.None_argumentsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#none_arguments}.
     * @param ctx the parse tree
     */
    void exitNone_arguments(YashanDBParser.None_argumentsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#in_arguments}.
     * @param ctx the parse tree
     */
    void enterIn_arguments(YashanDBParser.In_argumentsContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#in_arguments}.
     * @param ctx the parse tree
     */
    void exitIn_arguments(YashanDBParser.In_argumentsContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#identifier}.
     * @param ctx the parse tree
     */
    void enterIdentifier(YashanDBParser.IdentifierContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#identifier}.
     * @param ctx the parse tree
     */
    void exitIdentifier(YashanDBParser.IdentifierContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#type_spec}.
     * @param ctx the parse tree
     */
    void enterType_spec(YashanDBParser.Type_specContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#type_spec}.
     * @param ctx the parse tree
     */
    void exitType_spec(YashanDBParser.Type_specContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#datatype}.
     * @param ctx the parse tree
     */
    void enterDatatype(YashanDBParser.DatatypeContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#datatype}.
     * @param ctx the parse tree
     */
    void exitDatatype(YashanDBParser.DatatypeContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#native_datatype_element}.
     * @param ctx the parse tree
     */
    void enterNative_datatype_element(YashanDBParser.Native_datatype_elementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#native_datatype_element}.
     * @param ctx the parse tree
     */
    void exitNative_datatype_element(YashanDBParser.Native_datatype_elementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#precision_part}.
     * @param ctx the parse tree
     */
    void enterPrecision_part(YashanDBParser.Precision_partContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#precision_part}.
     * @param ctx the parse tree
     */
    void exitPrecision_part(YashanDBParser.Precision_partContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#numeric_negative}.
     * @param ctx the parse tree
     */
    void enterNumeric_negative(YashanDBParser.Numeric_negativeContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#numeric_negative}.
     * @param ctx the parse tree
     */
    void exitNumeric_negative(YashanDBParser.Numeric_negativeContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_user_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_user_statement(YashanDBParser.Create_user_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_user_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_user_statement(YashanDBParser.Create_user_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#user_name}.
     * @param ctx the parse tree
     */
    void enterUser_name(YashanDBParser.User_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#user_name}.
     * @param ctx the parse tree
     */
    void exitUser_name(YashanDBParser.User_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_user_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_user_statement(YashanDBParser.Alter_user_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_user_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_user_statement(YashanDBParser.Alter_user_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_user_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_user_statement(YashanDBParser.Drop_user_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_user_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_user_statement(YashanDBParser.Drop_user_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_role_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_role_statement(YashanDBParser.Create_role_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_role_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_role_statement(YashanDBParser.Create_role_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#role_name}.
     * @param ctx the parse tree
     */
    void enterRole_name(YashanDBParser.Role_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#role_name}.
     * @param ctx the parse tree
     */
    void exitRole_name(YashanDBParser.Role_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_role_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_role_statement(YashanDBParser.Drop_role_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_role_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_role_statement(YashanDBParser.Drop_role_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_profile_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_profile_statement(YashanDBParser.Create_profile_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_profile_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_profile_statement(YashanDBParser.Create_profile_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#profile_parameters}.
     * @param ctx the parse tree
     */
    void enterProfile_parameters(YashanDBParser.Profile_parametersContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#profile_parameters}.
     * @param ctx the parse tree
     */
    void exitProfile_parameters(YashanDBParser.Profile_parametersContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#profile_parameter_name}.
     * @param ctx the parse tree
     */
    void enterProfile_parameter_name(YashanDBParser.Profile_parameter_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#profile_parameter_name}.
     * @param ctx the parse tree
     */
    void exitProfile_parameter_name(YashanDBParser.Profile_parameter_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#container_clause}.
     * @param ctx the parse tree
     */
    void enterContainer_clause(YashanDBParser.Container_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#container_clause}.
     * @param ctx the parse tree
     */
    void exitContainer_clause(YashanDBParser.Container_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_profile_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_profile_statement(YashanDBParser.Alter_profile_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_profile_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_profile_statement(YashanDBParser.Alter_profile_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_profile_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_profile_statement(YashanDBParser.Drop_profile_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_profile_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_profile_statement(YashanDBParser.Drop_profile_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#size_clause}.
     * @param ctx the parse tree
     */
    void enterSize_clause(YashanDBParser.Size_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#size_clause}.
     * @param ctx the parse tree
     */
    void exitSize_clause(YashanDBParser.Size_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_tablespace_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_tablespace_statement(YashanDBParser.Create_tablespace_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_tablespace_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_tablespace_statement(YashanDBParser.Create_tablespace_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#datafile_clause}.
     * @param ctx the parse tree
     */
    void enterDatafile_clause(YashanDBParser.Datafile_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#datafile_clause}.
     * @param ctx the parse tree
     */
    void exitDatafile_clause(YashanDBParser.Datafile_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#file_specification}.
     * @param ctx the parse tree
     */
    void enterFile_specification(YashanDBParser.File_specificationContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#file_specification}.
     * @param ctx the parse tree
     */
    void exitFile_specification(YashanDBParser.File_specificationContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#extent_management_clause}.
     * @param ctx the parse tree
     */
    void enterExtent_management_clause(YashanDBParser.Extent_management_clauseContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#extent_management_clause}.
     * @param ctx the parse tree
     */
    void exitExtent_management_clause(YashanDBParser.Extent_management_clauseContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_tablespace_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_tablespace_statement(YashanDBParser.Drop_tablespace_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_tablespace_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_tablespace_statement(YashanDBParser.Drop_tablespace_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#alter_tablespace_statement}.
     * @param ctx the parse tree
     */
    void enterAlter_tablespace_statement(YashanDBParser.Alter_tablespace_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#alter_tablespace_statement}.
     * @param ctx the parse tree
     */
    void exitAlter_tablespace_statement(YashanDBParser.Alter_tablespace_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_synonym_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_synonym_statement(YashanDBParser.Create_synonym_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_synonym_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_synonym_statement(YashanDBParser.Create_synonym_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_synonym_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_synonym_statement(YashanDBParser.Drop_synonym_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_synonym_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_synonym_statement(YashanDBParser.Drop_synonym_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#create_directory_statement}.
     * @param ctx the parse tree
     */
    void enterCreate_directory_statement(YashanDBParser.Create_directory_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#create_directory_statement}.
     * @param ctx the parse tree
     */
    void exitCreate_directory_statement(YashanDBParser.Create_directory_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#drop_directory_statement}.
     * @param ctx the parse tree
     */
    void enterDrop_directory_statement(YashanDBParser.Drop_directory_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#drop_directory_statement}.
     * @param ctx the parse tree
     */
    void exitDrop_directory_statement(YashanDBParser.Drop_directory_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#rename_statement}.
     * @param ctx the parse tree
     */
    void enterRename_statement(YashanDBParser.Rename_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#rename_statement}.
     * @param ctx the parse tree
     */
    void exitRename_statement(YashanDBParser.Rename_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#comment_statement}.
     * @param ctx the parse tree
     */
    void enterComment_statement(YashanDBParser.Comment_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#comment_statement}.
     * @param ctx the parse tree
     */
    void exitComment_statement(YashanDBParser.Comment_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#truncate_table_statement}.
     * @param ctx the parse tree
     */
    void enterTruncate_table_statement(YashanDBParser.Truncate_table_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#truncate_table_statement}.
     * @param ctx the parse tree
     */
    void exitTruncate_table_statement(YashanDBParser.Truncate_table_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#grant_statement}.
     * @param ctx the parse tree
     */
    void enterGrant_statement(YashanDBParser.Grant_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#grant_statement}.
     * @param ctx the parse tree
     */
    void exitGrant_statement(YashanDBParser.Grant_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#privilege_name}.
     * @param ctx the parse tree
     */
    void enterPrivilege_name(YashanDBParser.Privilege_nameContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#privilege_name}.
     * @param ctx the parse tree
     */
    void exitPrivilege_name(YashanDBParser.Privilege_nameContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#revoke_statement}.
     * @param ctx the parse tree
     */
    void enterRevoke_statement(YashanDBParser.Revoke_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#revoke_statement}.
     * @param ctx the parse tree
     */
    void exitRevoke_statement(YashanDBParser.Revoke_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#audit_statement}.
     * @param ctx the parse tree
     */
    void enterAudit_statement(YashanDBParser.Audit_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#audit_statement}.
     * @param ctx the parse tree
     */
    void exitAudit_statement(YashanDBParser.Audit_statementContext ctx);

    /**
     * Enter a parse tree produced by {@link YashanDBParser#noaudit_statement}.
     * @param ctx the parse tree
     */
    void enterNoaudit_statement(YashanDBParser.Noaudit_statementContext ctx);

    /**
     * Exit a parse tree produced by {@link YashanDBParser#noaudit_statement}.
     * @param ctx the parse tree
     */
    void exitNoaudit_statement(YashanDBParser.Noaudit_statementContext ctx);
}