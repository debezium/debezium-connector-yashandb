// $antlr-format alignTrailingComments true, columnLimit 150, maxEmptyLinesToKeep 1, reflowComments false, useTab false
// $antlr-format allowShortRulesOnASingleLine true, allowShortBlocksOnASingleLine true, minEmptyLines 0, alignSemicolons ownLine
// $antlr-format alignColons trailing, singleLineOverrulesHangingColon true, alignLexerCommands true, alignLabels true, alignTrailers true

lexer grammar YashanDBLexer;

options {
    // superClass = PlSqlLexerBase;
    caseInsensitive = true;
}

ABORT                             : 'ABORT';
ABS                               : 'ABS';
ACCESS                            : 'ACCESS';
ACCOUNT                           : 'ACCOUNT';
ACOS                              : 'ACOS';
ACTIONS                           : 'ACTIONS';
ADD                               : 'ADD';
ADD_MONTHS                        : 'ADD_MONTHS';
ADMINISTER                        : 'ADMINISTER';
AGE                               : 'AGE';
ALL                               : 'ALL';
ALL_ROWS                          : 'ALL_ROWS';
ALGORITHM                        : 'ALGORITHM';
ALTER                             : 'ALTER';
ANALYZE                           : 'ANALYZE';
AND                               : 'AND';
ANY                               : 'ANY';
APPEND                            : 'APPEND';
ARCHIVE                           : 'ARCHIVE';
ARCHIVELOG                        : 'ARCHIVELOG';
ARRAY_APPEND                      : 'ARRAY_APPEND';
ARRAY_LENGTH                      : 'ARRAY_LENGTH';
ARRAY_NDIMS                       : 'ARRAY_NDIMS';
ARRAY_POSITION                    : 'ARRAY_POSITION';
ARRAY_REMOVE                      : 'ARRAY_REMOVE';
ARRAY_REPLACE                     : 'ARRAY_REPLACE';
ARRAY_TO_STRING                   : 'ARRAY_TO_STRING';
ARRAY_UPPER                       : 'ARRAY_UPPER';
AS                                : 'AS';
ASC                               : 'ASC';
ASCII                             : 'ASCII';
ASIN                              : 'ASIN';
AT                                : 'AT';
ATAN2                             : 'ATAN2';
ATAN                              : 'ATAN';
AUDIT                             : 'AUDIT';
AUTO_INCREMENT                   : 'AUTO_INCREMENT';
AVG                               : 'AVG';
AUTOALLOCATE                      : 'AUTOALLOCATE';
BACKUP                            : 'BACKUP';
BACKUPSET                        : 'BACKUPSET';
BASE                              : 'BASE';
BEFORE                            : 'BEFORE';
BEGIN                             : 'BEGIN';
BETWEEN                           : 'BETWEEN';
BIGFILE                           : 'BIGFILE';
BIGINT                            : 'BIGINT';
BIN                               : 'BIN';
BINARY                            : 'BINARY';
BINARY_BIGINT                     : 'BINARY_BIGINT';
BINARY_DOUBLE                     : 'BINARY_DOUBLE';
BINARY_FLOAT                      : 'BINARY_FLOAT';
BINARY_INTEGER                    : 'BINARY_INTEGER';
BINARY_SMALLINT                   : 'BINARY_SMALLINT';
BINARY_TINYINT                    : 'BINARY_TINYINT';
BIN_TO_NUM                        : 'BIN_TO_NUM';
BIT                               : 'BIT';
BITAND                            : 'BITAND';
BITOR                             : 'BITOR';
BITXOR                            : 'BITXOR';
BIT_LENGTH                        : 'BIT_LENGTH';
BLOB                              : 'BLOB';
BOOLEAN                           : 'BOOLEAN';
BODY                              : 'BODY';
BOUND                             : 'BOUND';
BUFFER_POOL                       : 'BUFFER_POOL';
BUILD                             : 'BUILD';
BULK                              : 'BULK';
BULKLOAD                          : 'BULKLOAD';
CACHE                             : 'CACHE';
CALL                              : 'CALL';
CANCEL                            : 'CANCEL';
CASCADE                           : 'CASCADE';
CASCADED                         : 'CASCADED';
CASE                              : 'CASE';
CAST                              : 'CAST';
CATEGORY                          : 'CATEGORY';
CEIL                              : 'CEIL';
CELL_FLASH_CACHE                  : 'CELL_FLASH_CACHE';
CHANGE                            : 'CHANGE';
CHAR                              : 'CHAR';
CHARACTER                         : 'CHARACTER';
CHARSET                           : 'CHARSET';
CHARACTER_LENGTH                  : 'CHARACTER_LENGTH';
CHAR_CS                           : 'CHAR_CS';
CHAR_LENGTH                       : 'CHAR_LENGTH';
CHAR_TO_LABEL                     : 'CHAR_TO_LABEL';
CHECK                             : 'CHECK';
CHECKPOINT                        : 'CHECKPOINT';
CHECK_SYS_PRIVILEGE               : 'CHECK_SYS_PRIVILEGE';
CHOOSE                            : 'CHOOSE';
CHR                               : 'CHR';
CHUNK                             : 'CHUNK';
CLEAN                             : 'CLEAN';
CLEANUP                           : 'CLEANUP';
CLOB                              : 'CLOB';
CLOSE                             : 'CLOSE';
COPY                              : 'COPY';
CLUSTER                           : 'CLUSTER';
COALESCE                          : 'COALESCE';
COLLATE                           : 'COLLATE';
COLUMN                            : 'COLUMN';
COLUMNAR                          : 'COLUMNAR';
COMMENT                           : 'COMMENT';
COMMIT                            : 'COMMIT';
COMPAT                           : 'COMPAT';
COMPAT_MODE                      : 'COMPAT_MODE';
COMPLETE                          : 'COMPLETE';
COMPRESS                          : 'COMPRESS';
COMPRESSION                       : 'COMPRESSION';
CONCAT                            : 'CONCAT';
CONCAT_WS                         : 'CONCAT_WS';
CONTAINER                         : 'CONTAINER';
CONDITION                         : 'CONDITION';
CONNECT                           : 'CONNECT';
CONSTRAINT                        : 'CONSTRAINT';
CONSTRAINTS                       : 'CONSTRAINTS';
CONTEXT                           : 'CONTEXT';
CONTINUE                          : 'CONTINUE';
CONNECT_BY_ISCYCLE                : 'CONNECT_BY_ISCYCLE';
CONNECT_BY_ISLEAF                 : 'CONNECT_BY_ISLEAF';
CONNECT_BY_ROOT                   : 'CONNECT_BY_ROOT';
CONTROLFILES                      : 'CONTROLFILES';
CONVERT                           : 'CONVERT';
COS                               : 'COS';
COT                               : 'COT';
COUNT                             : 'COUNT';
CREATE                            : 'CREATE';
CROSS                             : 'CROSS';
CRYPT_ASYM_DECRYPT                : 'CRYPT_ASYM_DECRYPT';
CRYPT_ASYM_ENCRYPT                : 'CRYPT_ASYM_ENCRYPT';
CRYPT_DECRYPT                     : 'CRYPT_DECRYPT';
CRYPT_ENCRYPT                     : 'CRYPT_ENCRYPT';
CRYPT_HASH                        : 'CRYPT_HASH';
CRYPT_HMAC                        : 'CRYPT_HMAC';
CRYPT_KEY                         : 'CRYPT_KEY';
CRYPT_RANDOM                      : 'CRYPT_RANDOM';
CRYPT_SELFTEST                    : 'CRYPT_SELFTEST';
CRYPT_SIGN                        : 'CRYPT_SIGN';
CRYPT_VERIFY                      : 'CRYPT_VERIFY';
CUBE                              : 'CUBE';
CURRENT                           : 'CURRENT';
CURRENT_DATE                      : 'CURRENT_DATE';
CURRENT_TIMESTAMP                 : 'CURRENT_TIMESTAMP';
CYCLE                             : 'CYCLE';
DATABASE                          : 'DATABASE';
DATAFILE                          : 'DATAFILE';
DATAFILES                         : 'DATAFILES';
DATABUCKET                        : 'DATABUCKET';
DATAOID_REUSE                     : 'DATAOID_REUSE';
DATASPACE                         : 'DATASPACE';
DATE                              : 'DATE';
DATE_ADD                          : 'DATE_ADD';
DATE_FORMAT                       : 'DATE_FORMAT';
DATE_SUB                          : 'DATE_SUB';
DAY                               : 'DAY';
DAYOFWEEK                         : 'DAYOFWEEK';
DECIMAL                           : 'DECIMAL';
DECLARE                           : 'DECLARE';
DECODE                            : 'DECODE';
DECRYPTION                        : 'DECRYPTION';
DECRYPT_AES128                    : 'DECRYPT_AES128';
DEDUPLICATE                       : 'DEDUPLICATE';
DEFAULT                           : 'DEFAULT';
DEFINER                          : 'DEFINER';
DEFINITION                        : 'DEFINITION';
DELETE                            : 'DELETE';
DEMAND                            : 'DEMAND';
DENSE_RANK                        : 'DENSE_RANK';
DESC                              : 'DESC';
DEFERRED                          : 'DEFERRED';
DIRECTORY                         : 'DIRECTORY';
DISABLE                           : 'DISABLE';
DISTINCT                          : 'DISTINCT';
DISTRIBUTE                        : 'DISTRIBUTE';
DIV                               : 'DIV';
DOUBLE                            : 'DOUBLE';
DOUBLE_PRECISION                  : 'DOUBLE_PRECISION';
DOUBLE_WRITE                      : 'DOUBLE_WRITE';
DROP                              : 'DROP';
DUMP                              : 'DUMP';
DUPLICATE                         : 'DUPLICATE';
DUPLICATED                        : 'DUPLICATED';
EDITIONABLE                       : 'EDITIONABLE';
ELSE                              : 'ELSE';
ELSIF                             : 'ELSIF';
EMPTY_BLOB                        : 'EMPTY_BLOB';
EMPTY_CLOB                        : 'EMPTY_CLOB';
ENABLE                            : 'ENABLE';
ENCODING                          : 'ENCODING';
ENCRYPT                           : 'ENCRYPT';
ENCRYPTION                        : 'ENCRYPTION';
ENCRYPT_AES128                    : 'ENCRYPT_AES128';
END                               : 'END';
ENGINE                            : 'ENGINE';
ENGINE_COL                        : 'ENGINE_COL';
ENGINE_ROW                        : 'ENGINE_ROW';
ESCAPE                            : 'ESCAPE';
ESTIMATE                          : 'ESTIMATE';
EXCEPT                            : 'EXCEPT';
EXCEPTION                         : 'EXCEPTION';
EXEC                              : 'EXEC';
EXECUTE                           : 'EXECUTE';
EXCLUSIVE                         : 'EXCLUSIVE';
EXPIRE                            : 'EXPIRE';
SHARE                            : 'SHARE';
MODE                             : 'MODE';
EXISTS                            : 'EXISTS';
EXIT                              : 'EXIT';
EXP                               : 'EXP';
EXPLAIN                           : 'EXPLAIN';
EXTEND                            : 'EXTEND';
EXTENT                            : 'EXTENT';
EXTERNAL                          : 'EXTERNAL';
EXTRACT                           : 'EXTRACT';
FAILOVER                          : 'FAILOVER';
FALSE                             : 'FALSE';
FAST                              : 'FAST';
FETCH                             : 'FETCH';
FIND_IN_SET                       : 'FIND_IN_SET';
FIRST                             : 'FIRST';
FIRST_ROWS                        : 'FIRST_ROWS';
FIRST_VALUE                       : 'FIRST_VALUE';
FLASHBACK                         : 'FLASHBACK';
FLASH_CACHE                       : 'FLASH_CACHE';
FLOAT                             : 'FLOAT';
FLOOR                             : 'FLOOR';
FLUSH                             : 'FLUSH';
FOLLOWING                         : 'FOLLOWING';
FOR                               : 'FOR';
FORALL                            : 'FORALL';
FORCE                             : 'FORCE';
FOREIGN                           : 'FOREIGN';
FORMAT                            : 'FORMAT';
FREELIST                          : 'FREELIST';
FREELISTS                         : 'FREELISTS';
FROM                              : 'FROM';
FULL                              : 'FULL';
FUNCTION                          : 'FUNCTION';
GEOMETRYTYPE                      : 'GEOMETRYTYPE';
GET_TYPE_NAME                     : 'GET_TYPE_NAME';
GLOBAL                            : 'GLOBAL';
GOTO                              : 'GOTO';
GRANT                             : 'GRANT';
GREATEST                          : 'GREATEST';
GROUP                             : 'GROUP';
GROUPING                          : 'GROUPING';
GROUPING_ID                       : 'GROUPING_ID';
GROUP_CONCAT                      : 'GROUP_CONCAT';
GROUP_ID                          : 'GROUP_ID';
HASH_AJ                           : 'HASH_AJ';
HASH_SJ                           : 'HASH_SJ';
HASH                              : 'HASH';
HAVING                            : 'HAVING';
HEAP                              : 'HEAP';
HEXTORAW                          : 'HEXTORAW';
HORDER                            : 'HORDER';
HOUR                              : 'HOUR';
IDENTIFIED                        : 'IDENTIFIED';
IF                                : 'IF';
IFNULL                            : 'IFNULL';
IGNORE                            : 'IGNORE';
IMMEDIATE                         : 'IMMEDIATE';
IN                                : 'IN';
INCLUDE                           : 'INCLUDE';
INCREMENT                         : 'INCREMENT';
INCREMENTAL                       : 'INCREMENTAL';
INDEPEND                          : 'INDEPEND';
INDEX                             : 'INDEX';
INDEXES                           : 'INDEXES';
INDEX_ASC                         : 'INDEX_ASC';
INDEX_DESC                        : 'INDEX_DESC';
INDEX_FFS                         : 'INDEX_FFS';
INDEX_JOIN                        : 'INDEX_JOIN';
INDEX_SS                          : 'INDEX_SS';
INDEX_SS_ASC                      : 'INDEX_SS_ASC';
INDEX_SS_DESC                     : 'INDEX_SS_DESC';
INITCAP                           : 'INITCAP';
INITIAL                           : 'INITIAL';
INITRANS                          : 'INITRANS';
INMEMORY                          : 'INMEMORY';
INNER                             : 'INNER';
INSERT                            : 'INSERT';
INPLACE                           : 'INPLACE';
INSTANCE                          : 'INSTANCE';
INSTANCES                         : 'INSTANCES';
INSTR                             : 'INSTR';
INSTRB                            : 'INSTRB';
INT                               : 'INT';
INTEGER                           : 'INTEGER';
INTERSECT                         : 'INTERSECT';
INTERVAL                          : 'INTERVAL';
INTO                              : 'INTO';
INVALIDATE                        : 'INVALIDATE';
INCLUDING                         : 'INCLUDING';
AUTOEXTEND                        : 'AUTOEXTEND';
CONTENTS                          : 'CONTENTS';
UNLIMITED                         : 'UNLIMITED';
INVISIBLE                         : 'INVISIBLE';
IS                                : 'IS';
ISNULL                            : 'ISNULL';
JOIN                              : 'JOIN';
JSON                              : 'JSON';
ISOLATION                        : 'ISOLATION';
JSON_ARRAY_GET                    : 'JSON_ARRAY_GET';
JSON_ARRAY_LENGTH                 : 'JSON_ARRAY_LENGTH';
JSON_EXISTS                       : 'JSON_EXISTS';
JSON_FORMAT                       : 'JSON_FORMAT';
JSON_PARSE                        : 'JSON_PARSE';
JSON_QUERY                        : 'JSON_QUERY';
JSON_SERIALIZE                    : 'JSON_SERIALIZE';
JSON_VALUE                        : 'JSON_VALUE';
KEEP_DUPLICATES                   : 'KEEP_DUPLICATES';
KEEP                              : 'KEEP';
KILL                              : 'KILL';
LABEL_TO_CHAR                     : 'LABEL_TO_CHAR';
LAG                               : 'LAG';
LAST_DAY                          : 'LAST_DAY';
LAST_INSERT_ID                    : 'LAST_INSERT_ID';
LAST_VALUE                        : 'LAST_VALUE';
LEAD                              : 'LEAD';
LEADING                           : 'LEADING';
LEAST                             : 'LEAST';
LEFT                              : 'LEFT';
LENGTH2                           : 'LENGTH2';
LENGTH                            : 'LENGTH';
LENGTHB                           : 'LENGTHB';
LESS                             : 'LESS';
LEVEL                             : 'LEVEL';
LIBRARY                           : 'LIBRARY';
LIKE                              : 'LIKE';
LIMIT                             : 'LIMIT';
// PROFILE 参数关键字
FAILED_LOGIN_ATTEMPTS             : 'FAILED_LOGIN_ATTEMPTS';
PASSWORD_LIFE_TIME                : 'PASSWORD_LIFE_TIME';
PASSWORD_REUSE_TIME               : 'PASSWORD_REUSE_TIME';
PASSWORD_REUSE_MAX                : 'PASSWORD_REUSE_MAX';
PASSWORD_LOCK_TIME                : 'PASSWORD_LOCK_TIME';
IDLE_TIME                         : 'IDLE_TIME';
CONNECT_TIME                      : 'CONNECT_TIME';
CPU_PER_CALL                      : 'CPU_PER_CALL';
LOGICAL_READS_PER_CALL           : 'LOGICAL_READS_PER_CALL';
CPU_PER_SESSION                   : 'CPU_PER_SESSION';
LOGICAL_READS_PER_SESSION        : 'LOGICAL_READS_PER_SESSION';
PRIVATE_SGA                       : 'PRIVATE_SGA';
COMPOSITE_LIMIT                   : 'COMPOSITE_LIMIT';
LINK                              : 'LINK';
LISTAGG                           : 'LISTAGG';
LIST                              : 'LIST';
LINEAR                            : 'LINEAR';
LN                                : 'LN';
LNNVL                             : 'LNNVL';
LOAD                              : 'LOAD';
LOB                               : 'LOB';
LOCAL                             : 'LOCAL';
LOCALTIME                         : 'LOCALTIME';
LOCALTIMESTAMP                    : 'LOCALTIMESTAMP';
LOCK                              : 'LOCK';
UNLOCK                            : 'UNLOCK';
LOG                               : 'LOG';
LOGFILE                           : 'LOGFILE';
LOGGING                           : 'LOGGING';
LOGOFF                            : 'LOGOFF';
LOGON                             : 'LOGON';
LOOP                              : 'LOOP';
LOWER                             : 'LOWER';
LPAD                              : 'LPAD';
LSFA_LISTAGG                      : 'LSFA_LISTAGG';
LSC                               : 'LSC';
LTRIM                             : 'LTRIM';
MATCHED                           : 'MATCHED';
MATERIALIZED                      : 'MATERIALIZED';
MANAGEMENT                        : 'MANAGEMENT';
MAX                               : 'MAX';
MAXDATABUCKETS                    : 'MAXDATABUCKETS';
MAXDATAFILES                      : 'MAXDATAFILES';
MAXEXTENTS                        : 'MAXEXTENTS';
MAXINSTANCES                      : 'MAXINSTANCES';
MAXLOGFILES                       : 'MAXLOGFILES';
MAXLOGHISTORY                     : 'MAXLOGHISTORY';
MAXSIZE                           : 'MAXSIZE';
MAXTRANS                          : 'MAXTRANS';
MAXVALUE                          : 'MAXVALUE';
MAX_WORKERS_PER_EXEC              : 'MAX_WORKERS_PER_EXEC';
MCOL                              : 'MCOL';
MD5                               : 'MD5';
MEDIAN                            : 'MEDIAN';
METADATA                          : 'METADATA';
MERGE                             : 'MERGE';
MERGE_AJ                          : 'MERGE_AJ';
MERGE_SJ                          : 'MERGE_SJ';
MIN                               : 'MIN';
MINEXTENTS                        : 'MINEXTENTS';
MINUS                             : 'MINUS';
MINUTE                            : 'MINUTE';
MINVALUE                          : 'MINVALUE';
MOD                               : 'MOD';
MODIFY                            : 'MODIFY';
MONTH                             : 'MONTH';
MONTHS_BETWEEN                    : 'MONTHS_BETWEEN';
MOUNT                             : 'MOUNT';
NATIONAL                          : 'NATIONAL';
NATURAL                           : 'NATURAL';
NEVER                             : 'NEVER';
NCHAR                             : 'NCHAR';
NCHAR_CS                          : 'NCHAR_CS';
NCLOB                             : 'NCLOB';
NESTED                            : 'NESTED';
NEW                               : 'NEW';
NEXT                              : 'NEXT';
NEXTVAL                           : 'NEXTVAL';
NEXT_DAY                          : 'NEXT_DAY';
NLSSORT                           : 'NLSSORT';
NL_AJ                             : 'NL_AJ';
NL_SJ                             : 'NL_SJ';
NO                                : 'NO';
NOAPPEND                          : 'NOAPPEND';
NOARCHIVELOG                      : 'NOARCHIVELOG';
NOAUDIT                           : 'NOAUDIT';
NOCACHE                           : 'NOCACHE';
NOCOMPRESS                        : 'NOCOMPRESS';
NOCYCLE                           : 'NOCYCLE';
NOLOGGING                         : 'NOLOGGING';
NOMAXVALUE                        : 'NOMAXVALUE';
NOMINVALUE                        : 'NOMINVALUE';
NOMOUNT                           : 'NOMOUNT';
NONEDITIONABLE                    : 'NONEDITIONABLE';
NOORDER                           : 'NOORDER';
NOPARALLEL                        : 'NOPARALLEL';
NONE                              : 'NONE';
NOREVERSE                         : 'NOREVERSE';
NORMAL                            : 'NORMAL';
NOT                               : 'NOT';
NOVALIDATE                        : 'NOVALIDATE';
NOW                               : 'NOW';
NOWAIT                            : 'NOWAIT';
NO_INDEX                          : 'NO_INDEX';
NO_INDEX_FFS                      : 'NO_INDEX_FFS';
NO_INDEX_SS                       : 'NO_INDEX_SS';
NO_USE_HASH                       : 'NO_USE_HASH';
NO_USE_MERGE                      : 'NO_USE_MERGE';
NO_USE_NL                         : 'NO_USE_NL';
NULL                              : 'NULL';
NULLIF                            : 'NULLIF';
NULLS                             : 'NULLS';
NUMBER                            : 'NUMBER';
NUMERIC                           : 'NUMERIC';
NUMTODSINTERVAL                   : 'NUMTODSINTERVAL';
NUMTOYMINTERVAL                   : 'NUMTOYMINTERVAL';
NVARCHAR2                         : 'NVARCHAR2';
NVARCHAR                          : 'NVARCHAR';
NVL2                              : 'NVL2';
NVL                               : 'NVL';
OBJNO_REUSE                       : 'OBJNO_REUSE';
OCTET_LENGTH                      : 'OCTET_LENGTH';
OF                                : 'OF';
OFFLINE                           : 'OFFLINE';
OFFSET                            : 'OFFSET';
OFF                               : 'OFF';
ON                                : 'ON';
ONLINE                            : 'ONLINE';
ONLY                              : 'ONLY';
OPTION                            : 'OPTION';
OPEN                              : 'OPEN';
OR                                : 'OR';
ORDER                             : 'ORDER';
ORDERED                           : 'ORDERED';
ORGANIZATION                      : 'ORGANIZATION';
OUTER                             : 'OUTER';
OUTLINE                           : 'OUTLINE';
OVER                              : 'OVER';
OVERLAPS                          : 'OVERLAPS';
PACKAGE                           : 'PACKAGE';
PARALLEL                          : 'PARALLEL';
PARALLELISM                       : 'PARALLELISM';
PARTITION                         : 'PARTITION';
PARSER                           : 'PARSER';
MAX_ROWS                         : 'MAX_ROWS';
MIN_ROWS                         : 'MIN_ROWS';
PASSWORD                          : 'PASSWORD';
PLUGGABLE                        : 'PLUGGABLE';
PCTFREE                           : 'PCTFREE';
PCTINCREASE                       : 'PCTINCREASE';
PCTUSED                           : 'PCTUSED';
PERCENTILE_CONT                   : 'PERCENTILE_CONT';
PI                                : 'PI';
PIPE                              : 'PIPE';
PIVOT                             : 'PIVOT';
PLAN                              : 'PLAN';
POINT                             : 'POINT';
PLS_INTEGER                       : 'PLS_INTEGER';
POSITION                          : 'POSITION';
POW                               : 'POW';
POWER                             : 'POWER';
PRAGMA                            : 'PRAGMA';
PRECEDING                         : 'PRECEDING';
PREPARE                           : 'PREPARE';
PRESERVE                          : 'PRESERVE';
PRIMARY                           : 'PRIMARY';
PRIOR                             : 'PRIOR';
PRIVATE                           : 'PRIVATE';
PRIVILEGE                         : 'PRIVILEGE';
PRIVILEGES                        : 'PRIVILEGES';
POLICY                           : 'POLICY';
PROCEDURE                         : 'PROCEDURE';
PROFILE                           : 'PROFILE';
PUBLIC                            : 'PUBLIC';
PURGE                             : 'PURGE';
QUOTA                             : 'QUOTA';
QUERY                             : 'QUERY';
RAISE                             : 'RAISE';
RANDOM                            : 'RANDOM';
RANGE                             : 'RANGE';
RANK                              : 'RANK';
RAW                               : 'RAW';
READ                              : 'READ';
READONLY                          : 'READONLY';
READWRITE                         : 'READWRITE';
REAL                              : 'REAL';
REBUILD                           : 'REBUILD';
RECLAIM                           : 'RECLAIM';
RECOVER                           : 'RECOVER';
RECYCLEBIN                        : 'RECYCLEBIN';
REFERENCES                        : 'REFERENCES';
REFRESH                           : 'REFRESH';
REGEXP_COUNT                      : 'REGEXP_COUNT';
REGEXP_INSTR                      : 'REGEXP_INSTR';
REGEXP_LIKE                       : 'REGEXP_LIKE';
REGEXP_REPLACE                    : 'REGEXP_REPLACE';
REGEXP_SUBSTR                     : 'REGEXP_SUBSTR';
REGISTER                          : 'REGISTER';
RELEASE                           : 'RELEASE';
RENAME                            : 'RENAME';
REPLACE                           : 'REPLACE';
RESETLOGS                         : 'RESETLOGS';
RESPECT                           : 'RESPECT';
RESTART                           : 'RESTART';
RESTORE                           : 'RESTORE';
RESTRICT                          : 'RESTRICT';
RETURN                            : 'RETURN';
RETURNING                         : 'RETURNING';
REUSE                             : 'REUSE';
REVERSE                           : 'REVERSE';
REWRITE                           : 'REWRITE';
ROOT                              : 'ROOT';
REVOKE                            : 'REVOKE';
RIGHT                             : 'RIGHT';
RLIKE                             : 'RLIKE';
RLIKE_FILTER                      : 'RLIKE_FILTER';
ROLE                              : 'ROLE';
ROLES                             : 'ROLES';
ROLLBACK                          : 'ROLLBACK';
ROLLUP                            : 'ROLLUP';
ROUND                             : 'ROUND';
ROW_FORMAT                       : 'ROW_FORMAT';
ROW                               : 'ROW';
ROWID                             : 'ROWID';
ROWIDTOCHAR                       : 'ROWIDTOCHAR';
ROWNUM                            : 'ROWNUM';
ROWS                              : 'ROWS';
ROWSCN                            : 'ROWSCN';
ROW_NUMBER                        : 'ROW_NUMBER';
RPAD                              : 'RPAD';
RTREE                             : 'RTREE';
RTRIM                             : 'RTRIM';
RULE                              : 'RULE';
SAMPLE                            : 'SAMPLE';
SAVEPOINT                         : 'SAVEPOINT';
SCHEMA                            : 'SCHEMA';
SCN                               : 'SCN';
SCN_TO_TIMESTAMP                  : 'SCN_TO_TIMESTAMP';
SECOND                            : 'SECOND';
SECURITY                          : 'SECURITY';
SECTION                           : 'SECTION';
SECURITY_CLEAR_CSP                : 'SECURITY_CLEAR_CSP';
SECURITY_MOD_STATUS               : 'SECURITY_MOD_STATUS';
SECURITY_MOD_VERSION              : 'SECURITY_MOD_VERSION';
SESSIONTIMEZONE                   : 'SESSIONTIMEZONE';
SEGMENT                           : 'SEGMENT';
SELECT                            : 'SELECT';
SELECTIVITY                       : 'SELECTIVITY';
SEPARATOR                         : 'SEPARATOR';
SEQUENCE                          : 'SEQUENCE';
SIZE                              : 'SIZE';
SESSION                           : 'SESSION';
SET                               : 'SET';
SETS                              : 'SETS';
SHARDED                           : 'SHARDED';
SHARED                            : 'SHARED';
SHRINK                            : 'SHRINK';
SHUTDOWN                          : 'SHUTDOWN';
SIBLINGS                          : 'SIBLINGS';
STARTUP                          : 'STARTUP';
SIGN                              : 'SIGN';
SIN                               : 'SIN';
SINH                              : 'SINH';
SKIP_                             : 'SKIP';
SLICE                             : 'SLICE';
S3                                : 'S3';
SLOT                              : 'SLOT';
SMALLFILE                         : 'SMALLFILE';
SMALLINT                          : 'SMALLINT';
SOME                              : 'SOME';
SOUNDEX                           : 'SOUNDEX';
SPLIT                             : 'SPLIT';
SQL                               : 'SQL';
SQLCODE                           : 'SQLCODE';
SQLERRM                           : 'SQLERRM';
SQLMAP                            : 'SQLMAP';
SQRT                              : 'SQRT';
STABLE                            : 'STABLE';
STANDBY                           : 'STANDBY';
START                             : 'START';
STDDEV                            : 'STDDEV';
STDDEV_POP                        : 'STDDEV_POP';
STDDEV_SAMP                       : 'STDDEV_SAMP';
STORAGE                           : 'STORAGE';
STRING_AGG                        : 'STRING_AGG';
STRING_TO_ARRAY                   : 'STRING_TO_ARRAY';
STRPOS                            : 'STRPOS';
ST_AREA                           : 'ST_AREA';
ST_ASBINARY                       : 'ST_ASBINARY';
ST_ASEWKB                         : 'ST_ASEWKB';
ST_ASGEOJSON                      : 'ST_ASGEOJSON';
ST_ASHEXEWKB                      : 'ST_ASHEXEWKB';
ST_ASLATLONTEXT                   : 'ST_ASLATLONTEXT';
ST_ASTEXT                         : 'ST_ASTEXT';
ST_BOUNDARY                       : 'ST_BOUNDARY';
ST_BUFFER                         : 'ST_BUFFER';
ST_BUILDAREA                      : 'ST_BUILDAREA';
ST_CENTROID                       : 'ST_CENTROID';
ST_CLIPBYBOX2D                    : 'ST_CLIPBYBOX2D';
ST_CLOSESTPOINT                   : 'ST_CLOSESTPOINT';
ST_COLLECT                        : 'ST_COLLECT';
ST_COLLECTIONEXTRACT              : 'ST_COLLECTIONEXTRACT';
ST_CONCAVEHULL                    : 'ST_CONCAVEHULL';
ST_CONTAINS                       : 'ST_CONTAINS';
ST_CONTAINSPROPERLY               : 'ST_CONTAINSPROPERLY';
ST_COVEREDBY                      : 'ST_COVEREDBY';
ST_COVERS                         : 'ST_COVERS';
ST_CROSSES                        : 'ST_CROSSES';
ST_DIFFERENCE                     : 'ST_DIFFERENCE';
ST_DISJOINT                       : 'ST_DISJOINT';
ST_DISTANCE                       : 'ST_DISTANCE';
ST_DISTANCE_SPHERE                : 'ST_DISTANCE_SPHERE';
ST_DUMP                           : 'ST_DUMP';
ST_DWITHIN                        : 'ST_DWITHIN';
ST_ENVELOPE                       : 'ST_ENVELOPE';
ST_EQUALS                         : 'ST_EQUALS';
ST_EXTENT                         : 'ST_EXTENT';
ST_GEOMCOLLFROMTEXT               : 'ST_GEOMCOLLFROMTEXT';
ST_GEOMETRICMEDIAN                : 'ST_GEOMETRICMEDIAN';
ST_GEOMETRYFROMTEXT               : 'ST_GEOMETRYFROMTEXT';
ST_GEOMETRYTYPE                   : 'ST_GEOMETRYTYPE';
ST_GEOMFROMEWKB                   : 'ST_GEOMFROMEWKB';
ST_GEOMFROMGEOJSON                : 'ST_GEOMFROMGEOJSON';
ST_GEOMFROMTEXT                   : 'ST_GEOMFROMTEXT';
ST_GEOMFROMWKB                    : 'ST_GEOMFROMWKB';
ST_INTERSECTION                   : 'ST_INTERSECTION';
ST_INTERSECTS                     : 'ST_INTERSECTS';
ST_ISCLOSED                       : 'ST_ISCLOSED';
ST_ISEMPTY                        : 'ST_ISEMPTY';
ST_ISSIMPLE                       : 'ST_ISSIMPLE';
ST_ISVALID                        : 'ST_ISVALID';
ST_LENGTH                         : 'ST_LENGTH';
ST_LINEFROMTEXT                   : 'ST_LINEFROMTEXT';
ST_LINEMERGE                      : 'ST_LINEMERGE';
ST_LONGESTLINE                    : 'ST_LONGESTLINE';
ST_MAKEENVELOPE                   : 'ST_MAKEENVELOPE';
ST_MAKELINE                       : 'ST_MAKELINE';
ST_MAKEPOINT                      : 'ST_MAKEPOINT';
ST_MAKEVALID                      : 'ST_MAKEVALID';
ST_MAXDISTANCE                    : 'ST_MAXDISTANCE';
ST_MULTI                          : 'ST_MULTI';
ST_OVERLAPS                       : 'ST_OVERLAPS';
ST_PERIMETER                      : 'ST_PERIMETER';
ST_POINT                          : 'ST_POINT';
ST_POINTONSURFACE                 : 'ST_POINTONSURFACE';
ST_POINTZ                         : 'ST_POINTZ';
ST_POLYGON                        : 'ST_POLYGON';
ST_RELATE                         : 'ST_RELATE';
ST_SETSRID                        : 'ST_SETSRID';
ST_SHORTESTLINE                   : 'ST_SHORTESTLINE';
ST_SIMPLIFY                       : 'ST_SIMPLIFY';
ST_SPLIT                          : 'ST_SPLIT';
ST_SRID                           : 'ST_SRID';
ST_TOUCHES                        : 'ST_TOUCHES';
ST_TRANSFORM                      : 'ST_TRANSFORM';
ST_UNION                          : 'ST_UNION';
ST_WITHIN                         : 'ST_WITHIN';
ST_X                              : 'ST_X';
ST_Y                              : 'ST_Y';
ST_Z                              : 'ST_Z';
SUBPARTITION                      : 'SUBPARTITION';
SUBSTR                            : 'SUBSTR';
SUBSTRB                           : 'SUBSTRB';
SUBSTRING                         : 'SUBSTRING';
SUBSTRING_INDEX                   : 'SUBSTRING_INDEX';
SUCCESSFUL                        : 'SUCCESSFUL';
SUM                               : 'SUM';
SUPPLEMENTAL                      : 'SUPPLEMENTAL';
SWAP                              : 'SWAP';
SWITCH                            : 'SWITCH';
SWITCHOVER                        : 'SWITCHOVER';
SYNONYM                           : 'SYNONYM';
SYSAUX                            : 'SYSAUX';
SYSDATE                           : 'SYSDATE';
SYSTEM                            : 'SYSTEM';
SYSTIMESTAMP                      : 'SYSTIMESTAMP';
SYS_CONNECT_BY_PATH               : 'SYS_CONNECT_BY_PATH';
SYS_CONTEXT                       : 'SYS_CONTEXT';
SYS_EXTRACT_UTC                   : 'SYS_EXTRACT_UTC';
SYS_GUID                          : 'SYS_GUID';
SYS_REFCURSOR                     : 'SYS_REFCURSOR';
TABLE                             : 'TABLE';
TABLESPACE                        : 'TABLESPACE';
TAC                               : 'TAC';
TAG                               : 'TAG';
TAN                               : 'TAN';
TANH                              : 'TANH';
TDE                               : 'TDE';
TEMPFILE                          : 'TEMPFILE';
TEMPORARY                         : 'TEMPORARY';
TEMPTABLE                        : 'TEMPTABLE';
THEN                              : 'THEN';
TIME                              : 'TIME';
TIMEDIFF                          : 'TIMEDIFF';
TIMESTAMP                         : 'TIMESTAMP';
TIMESTAMPDIFF                     : 'TIMESTAMPDIFF';
TIMESTAMP_TO_SCN                  : 'TIMESTAMP_TO_SCN';
TINYINT                           : 'TINYINT';
TO                                : 'TO';
TO_BASE64                         : 'TO_BASE64';
TO_CHAR                           : 'TO_CHAR';
TO_DATE                           : 'TO_DATE';
TO_DSINTERVAL                     : 'TO_DSINTERVAL';
TO_NUMBER                         : 'TO_NUMBER';
TO_TIMESTAMP                      : 'TO_TIMESTAMP';
TO_YMINTERVAL                     : 'TO_YMINTERVAL';
TRANSACTION                       : 'TRANSACTION';
TRANSACTIONAL                     : 'TRANSACTIONAL';
TRANSLATE                         : 'TRANSLATE';
TRANSPORT                         : 'TRANSPORT';
TREAT                             : 'TREAT';
TRIGGER                           : 'TRIGGER';
TRIM                              : 'TRIM';
TRUE                              : 'TRUE';
TRUNC                             : 'TRUNC';
TRUNCATE                          : 'TRUNCATE';
TTL                               : 'TTL';
TYPE                              : 'TYPE';
TYPEOF                            : 'TYPEOF';
UNBOUNDED                         : 'UNBOUNDED';
UNDO                              : 'UNDO';
UNDO_SEGMENTS                     : 'UNDO_SEGMENTS';
UNION                             : 'UNION';
UNDEFINED                         : 'UNDEFINED';
UNIQUE                            : 'UNIQUE';
UNIFORM                           : 'UNIFORM';
UNTIL                             : 'UNTIL';
UNISTR                            : 'UNISTR';
UNSUPPORT_ERROR                   : 'UNSUPPORT_ERROR';
UNUSABLE                          : 'UNUSABLE';
UPDATE                            : 'UPDATE';
UPGRADE                           : 'UPGRADE';
UPPER                             : 'UPPER';
UROWID                            : 'UROWID';
USABLE                            : 'USABLE';
USER                              : 'USER';
USERENV                           : 'USERENV';
USE_HASH                          : 'USE_HASH';
USE_MERGE                         : 'USE_MERGE';
USE_NL                            : 'USE_NL';
USING                             : 'USING';
UTC_TIMESTAMP                     : 'UTC_TIMESTAMP';
VALIDATE                          : 'VALIDATE';
VALUES                            : 'VALUES';
VARCHAR2                          : 'VARCHAR2';
VARCHAR                           : 'VARCHAR';
VARIANCE                          : 'VARIANCE';
VAR_POP                           : 'VAR_POP';
VAR_SAMP                          : 'VAR_SAMP';
VIEW                              : 'VIEW';
VISIBLE                           : 'VISIBLE';
WAIT                              : 'WAIT';
WHEN                              : 'WHEN';
WHENEVER                          : 'WHENEVER';
WHERE                             : 'WHERE';
WHILE                             : 'WHILE';
WITH                              : 'WITH';
WORK                              : 'WORK';
WITHOUT                           : 'WITHOUT';
WRITE                             : 'WRITE';
WM_CONCAT                         : 'WM_CONCAT';
YASDECODE                         : 'YASDECODE';
WRAPPED                           : 'WRAPPED';
XMLTYPE                           : 'XMLTYPE';
YEAR                              : 'YEAR';
YASHAN                           : 'YASHAN';
MYSQL                            : 'MYSQL';
ZORDER                            : 'ZORDER';
INNER_FUNC__GEOM_CHECK_MODIFIER__ : '__GEOM_CHECK_MODIFIER__';
INNER_FUNC__MAKE_RTREE_KEY3__     : '__MAKE_RTREE_KEY3__';
INNER_FUNC__MAKE_RTREE_KEY__      : '__MAKE_RTREE_KEY__';
INNER_FUNC__MY_COLLATE_SORT       : '__MY_COLLATE_SORT';


EXISTSNODE                        : 'EXISTSNODE';
GETCLOBVAL                        : 'GETCLOBVAL';
GETSTRINGVAL                      : 'GETSTRINGVAL';
TRANSFORM                         : 'TRANSFORM';
EXTRACTVALUE                      : 'EXTRACTVALUE';
XMLEXTRACT                        : 'XMLEXTRACT';
XMLPARSE                          : 'XMLPARSE';
XMLAGG                            : 'XMLAGG';
XMLTABLE                          : 'XMLTABLE';
DOCUMENT                          : 'DOCUMENT';
CONTENT                           : 'CONTENT';
WELLFORMED                        : 'WELLFORMED';
PASSING                           : 'PASSING';
COLUMNS                           : 'COLUMNS';
XMLNAMESPACES                     : 'XMLNAMESPACES';
ORDINALITY                        : 'ORDINALITY';
PATH                              : 'PATH';
VALUE                             : 'VALUE';


// 关键字缺失
BTREE     : 'BTREE';
KEY        : 'KEY';
LOCKED     : 'LOCKED';
BY         : 'BY';
SEED       : 'SEED';
LAST       : 'LAST';
OVERFLOW   : 'OVERFLOW';
KEY_BLOCK_SIZE : 'KEY_BLOCK_SIZE';
DYNAMIC    : 'DYNAMIC';
FIXED      : 'FIXED';
COMPRESSED : 'COMPRESSED';
REDUNDANT  : 'REDUNDANT';
COMPACT    : 'COMPACT';
COMPILE    : 'COMPILE';
COMPOSE    : 'COMPOSE';
COMPUTE    : 'COMPUTE';
STATISTICS : 'STATISTICS';
THAN       : 'THAN';
PARTITIONS : 'PARTITIONS';
STORE      : 'STORE';
SUBPARTITIONS : 'SUBPARTITIONS';
ARRAY      : 'ARRAY';
WRAPPER    : 'WRAPPER';
WITHIN     : 'WITHIN';
CONVERSION : 'CONVERSION';
ERROR      : 'ERROR';
TRAILING   : 'TRAILING';
BOTH       : 'BOTH';
EXTENDED   : 'EXTENDED';
PRETTY     : 'PRETTY';

// 函数缺失
PX_CHANNEL : 'PX_CHANNEL';
PX_OBJ     : 'PX_OBJ';

// Rule #358 <NATIONAL_CHAR_STRING_LIT> - subtoken typecast in <REGULAR_ID>, it also incorporates <character_representation>
//  Lowercase 'n' is a usual addition to the standard

NATIONAL_CHAR_STRING_LIT: 'N' '\'' (~('\'' | '\r' | '\n') | '\'' '\'' | NEWLINE)* '\'';

//  Rule #040 <BIT_STRING_LIT> - subtoken typecast in <REGULAR_ID>
//  Lowercase 'b' is a usual addition to the standard

BIT_STRING_LIT: 'B' ('\'' [01]* '\'')+;

//  Rule #284 <HEX_STRING_LIT> - subtoken typecast in <REGULAR_ID>
//  Lowercase 'x' is a usual addition to the standard

HEX_STRING_LIT : 'X' ('\'' [A-F0-9]* '\'')+;
DOUBLE_PERIOD  : '..';
PERIOD         : '.';

OUTER_JOIN_OPERATOR: '(+)';

//{ Rule #238 <EXACT_NUM_LIT>
//  This rule is a bit tricky - it resolves the ambiguity with <PERIOD>
//  It also incorporates <mantisa> and <exponent> for the <APPROXIMATE_NUM_LIT>
//  Rule #501 <signed_integer> was incorporated directly in the token <APPROXIMATE_NUM_LIT>
//  See also the rule #617 <unsigned_num_lit>
/*
    : (
            UNSIGNED_INTEGER
            ( '.' UNSIGNED_INTEGER
            | {$type = UNSIGNED_INTEGER;}
            ) ( E ('+' | '-')? UNSIGNED_INTEGER {$type = APPROXIMATE_NUM_LIT;} )?
    | '.' UNSIGNED_INTEGER ( E ('+' | '-')? UNSIGNED_INTEGER {$type = APPROXIMATE_NUM_LIT;} )?
    )
    (D | F)?
    ;*/

UNSIGNED_INTEGER    : [0-9]+;
APPROXIMATE_NUM_LIT : FLOAT_FRAGMENT ('E' ('+' | '-')? (FLOAT_FRAGMENT | [0-9]+))? ('D' | 'F')?;

// Rule #--- <CHAR_STRING> is a base for Rule #065 <char_string_lit> , it incorporates <character_representation>
// and a superfluous subtoken typecasting of the "QUOTE"
CHAR_STRING     : '\'' (~('\'' | '\r' | '\n') | '\'' '\'' | NEWLINE)* '\'';
SECONED_LITERAL : SECOND ('(' UNSIGNED_INTEGER ')')?;
//MINUTE_LITERAL:MINUTE (TO SECOND)?;
//HOUR_LITERAL:HOUR (TO (MINUTE | SECONED_LITERAL))?;
//DAY_LITERAL: DAY ('(' UNSIGNED_INTEGER ')')? (TO (HOUR | MINUTE | SECONED_LITERAL))?;
//
//TIMESTAMP_LITERAL: TIMESTAMP (SPACES|NEWLINE)* CHAR_STRING;
//DATE_LITERAL: DATE (SPACES|NEWLINE)* CHAR_STRING;
//INTERVAL_YEAR_TO_MONTH_LITERAL: INTERVAL CHAR_STRING ((YEAR ('(' UNSIGNED_INTEGER ')')? (TO MONTH)?) | MONTH);
//INTERVAL_DAY_TO_SECOND_LITERAL: INTERVAL CHAR_STRING (DAY_LITERAL | HOUR_LITERAL | MINUTE_LITERAL | SECONED_LITERAL);

//

// See https://livesql.oracle.com/apex/livesql/file/content_CIREYU9EA54EOKQ7LAMZKRF6P.html
// TODO: context sensitive string quotes (any characted after quote)
CHAR_STRING_PERL:
    'Q' '\'' (
        QS_ANGLE
        | QS_BRACE
        | QS_BRACK
        | QS_PAREN
        | QS_EXCLAM
        | QS_SHARP
        | QS_QUOTE
        | QS_DQUOTE
        | QS_TILDA
        | QS_SOLIDUS
        | QS_RSOLIDUS
    ) '\'' -> type(CHAR_STRING)
;
fragment QS_ANGLE    : '<' .*? '>';
fragment QS_BRACE    : '{' .*? '}';
fragment QS_BRACK    : '[' .*? ']';
fragment QS_PAREN    : '(' .*? ')';
fragment QS_EXCLAM   : '!' .*? '!';
fragment QS_SHARP    : '#' .*? '#';
fragment QS_QUOTE    : '\'' .*? '\'';
fragment QS_DQUOTE   : '"' .*? '"';
fragment QS_TILDA    : '~' .*? '~';
fragment QS_SOLIDUS  : '/' .*? '/';
fragment QS_RSOLIDUS : '\\' .*? '\\';

DELIMITED_ID: '"' (~ [\u0000"] | '"' '"')+ '"';

PERCENT         : '%';
AMPERSAND       : '&';
LEFT_PAREN      : '(';
RIGHT_PAREN     : ')';
DOUBLE_ASTERISK : '**';
ASTERISK        : '*';
PLUS_SIGN       : '+';
MINUS_SIGN      : '-';
COMMA           : ',';
SOLIDUS         : '/';
AT_SIGN         : '@';
ASSIGN_OP       : ':=';
HASH_OP         : '#';
BACKSLASH       : '\\';
DOUBLE_QS       : '"';

LEFT_CURLY_BRACKETS  : '{';
RIGHT_CURLY_BRACKETS : '}';

SQ: '\'';

BINDVAR:
    ':' SIMPLE_LETTER (SIMPLE_LETTER | [0-9] | '_')*
    | ':' DELIMITED_ID // not used in SQL but spotted in v$sqltext when using cursor_sharing
    | ':' UNSIGNED_INTEGER
    | QUESTION_MARK // not in SQL, not in Oracle, not in OCI, use this for JDBC
;

NOT_EQUAL_OP              : '!=' | '<>' | '^=' | '~=';
CARRET_OPERATOR_PART      : '^';
TILDE_OPERATOR_PART       : '~';
EXCLAMATION_OPERATOR_PART : '!';
GREATER_THAN_OP           : '>';
LESS_THAN_OP              : '<';
COLON                     : ':';
SEMICOLON                 : ';';

BAR       : '|';
EQUALS_OP : '=';

LEFT_BRACKET  : '[';
RIGHT_BRACKET : ']';

INTRODUCER: '_';

// Comments https://docs.oracle.com/cd/E11882_01/server.112/e41084/sql_elements006.htm

SINGLE_LINE_COMMENT : '--' ~('\r' | '\n')* NEWLINE_EOF -> channel(HIDDEN);
MULTI_LINE_COMMENT  : '/*' .*? '*/'                    -> channel(HIDDEN);
// https://docs.oracle.com/cd/E11882_01/server.112/e16604/ch_twelve034.htm#SQPUG054
REMARK_COMMENT: 'REM' 'ARK'? (' ' ~('\r' | '\n')*)? NEWLINE_EOF -> channel(HIDDEN);

// https://docs.oracle.com/cd/E11882_01/server.112/e16604/ch_twelve032.htm#SQPUG052
PROMPT_MESSAGE: 'PRO' 'MPT'? (' ' ~('\r' | '\n')*)? NEWLINE_EOF;

// TODO: should starts with newline
START_CMD: // https://docs.oracle.com/cd/B19306_01/server.102/b14357/ch12002.htm
    '@' '@'?
; // https://docs.oracle.com/cd/B19306_01/server.102/b14357/ch12003.htm

REGULAR_ID: NORMAL_CHARACTER (NORMAL_CHARACTER)*;

NORMAL_CHARACTER:
    ~(
        ','
        | ' '
        | '+'
        | '-'
        | '*'
        | '.'
        | '/'
        | '|'
        | '('
        | ')'
        | ':'
        | '?'
        | '\t'
        | '\r'
        | '\n'
        | '='
        | '\\'
        | '!'
        | '<'
        | '>'
        | ';'
        | '&'
        | '^'
        | '"'
        | '~'
        | '\''
        | '{'
        | '}'
        | '['
        | ']'
        | '@'
    )
;

INQUIRY_DIRECTIVE: '$$' (SIMPLE_LETTER | '_')+;

SPACES  : [ \t\r]+ -> channel(HIDDEN);
LINEEND : [\n]+    -> channel(HIDDEN);

// Fragment rules

fragment NEWLINE_EOF   : NEWLINE | EOF;
fragment QUESTION_MARK : '?';
fragment SIMPLE_LETTER : [\p{Letter}\p{Emoji}];

fragment SPECIAL_CHARACTERS:
    ','
    | ' '
    | '+'
    | '-'
    | '*'
    | '/'
    | '|'
    | '('
    | ')'
    | ':'
    | '?'
    | '\t'
    | '\r'
    | '\n'
    | '='
    | '\\'
    | '!'
    | '<'
    | '>'
    | ';'
    | '&'
    | '^'
    | '"'
    | '~'
    | '\''
    | '{'
    | '}'
    | '['
    | ']'
;
fragment FLOAT_FRAGMENT : UNSIGNED_INTEGER* '.'? UNSIGNED_INTEGER+;
fragment NEWLINE        : '\r'? '\n';
fragment SPACE          : [ \t];