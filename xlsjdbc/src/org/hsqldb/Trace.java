/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP, 
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals 
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2004, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG, 
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb;

import java.io.PrintWriter;

import org.hsqldb.lib.HsqlByteArrayOutputStream;

/**
 * handles creation and reporting of error messages and throwing HsqlException
 *
 * @version 1.7.0
 */

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP) - error reporting
// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP) - setting trace
// the system property hsqldb.tracesystemout == true is now used for printing
// trace message to System.out
// fredt@users 20020305 - patch 1.7.0 - various new messages added
// tony_lai@users 20020820 - patch 595073 - Duplicated exception msg
// fredt@users 20021230 - patch 488118 by xclay@users - allow multithreading
// wsonic@users 20031005 - moved string literal messages to Trace with new methods
// nitin chauhan 20031005 - moved concatenated string in asserts and checks to Trace with new methods
// fredt@users 20040322 - removed unused code - class is a collection of static methods now
//

/** @todo  fredt - 20021022 management of nested throws inside the program in
 * such a way that it is possible to return exactly the text of the error
 *  thrown at a given level withou higher level messages being added and to
 * preserve the original error code
 */
public class Trace {

    public static boolean       TRACE          = false;
    public static boolean       TRACESYSTEMOUT = false;
    public static final boolean STOP           = false;
    public static final boolean DOASSERT       = false;

    //
    public static final int DATABASE_ALREADY_IN_USE                   = 1,
                            CONNECTION_IS_CLOSED                      = 2,
                            CONNECTION_IS_BROKEN                      = 3,
                            DATABASE_IS_SHUTDOWN                      = 4,
                            COLUMN_COUNT_DOES_NOT_MATCH               = 5,
                            DIVISION_BY_ZERO                          = 6,
                            INVALID_ESCAPE                            = 7,
                            INTEGRITY_CONSTRAINT_VIOLATION            = 8,
                            VIOLATION_OF_UNIQUE_INDEX                 = 9,
                            TRY_TO_INSERT_NULL                        = 10,
                            UNEXPECTED_TOKEN                          = 11,
                            UNEXPECTED_END_OF_COMMAND                 = 12,
                            UNKNOWN_FUNCTION                          = 13,
                            NEED_AGGREGATE                            = 14,
                            SUM_OF_NON_NUMERIC                        = 15,
                            WRONG_DATA_TYPE                           = 16,
                            SINGLE_VALUE_EXPECTED                     = 17,
                            SERIALIZATION_FAILURE                     = 18,
                            TRANSFER_CORRUPTED                        = 19,
                            FUNCTION_NOT_SUPPORTED                    = 20,
                            TABLE_ALREADY_EXISTS                      = 21,
                            TABLE_NOT_FOUND                           = 22,
                            INDEX_ALREADY_EXISTS                      = 23,
                            SECOND_PRIMARY_KEY                        = 24,
                            DROP_PRIMARY_KEY                          = 25,
                            INDEX_NOT_FOUND                           = 26,
                            COLUMN_ALREADY_EXISTS                     = 27,
                            COLUMN_NOT_FOUND                          = 28,
                            FILE_IO_ERROR                             = 29,
                            WRONG_DATABASE_FILE_VERSION               = 30,
                            DATABASE_IS_READONLY                      = 31,
                            DATA_IS_READONLY                          = 32,
                            ACCESS_IS_DENIED                          = 33,
                            INPUTSTREAM_ERROR                         = 34,
                            NO_DATA_IS_AVAILABLE                      = 35,
                            USER_ALREADY_EXISTS                       = 36,
                            USER_NOT_FOUND                            = 37,
                            ASSERT_FAILED                             = 38,
                            EXTERNAL_STOP                             = 39,
                            GENERAL_ERROR                             = 40,
                            WRONG_OUT_PARAMETER                       = 41,
                            FUNCTION_NOT_FOUND                        = 42,
                            TRIGGER_NOT_FOUND                         = 43,
                            SAVEPOINT_NOT_FOUND                       = 44,
                            LABEL_REQUIRED                            = 45,
                            WRONG_DEFAULT_CLAUSE                      = 46,
                            FOREIGN_KEY_NOT_ALLOWED                   = 47,
                            UNKNOWN_DATA_SOURCE                       = 48,
                            BAD_INDEX_CONSTRAINT_NAME                 = 49,
                            DROP_FK_INDEX                             = 50,
                            RESULTSET_FORWARD_ONLY                    = 51,
                            VIEW_ALREADY_EXISTS                       = 52,
                            VIEW_NOT_FOUND                            = 53,
                            NOT_A_VIEW                                = 54,
                            NOT_A_TABLE                               = 55,
                            SYSTEM_INDEX                              = 56,
                            COLUMN_TYPE_MISMATCH                      = 57,
                            BAD_ADD_COLUMN_DEFINITION                 = 58,
                            DROP_SYSTEM_CONSTRAINT                    = 59,
                            CONSTRAINT_ALREADY_EXISTS                 = 60,
                            CONSTRAINT_NOT_FOUND                      = 61,
                            INVALID_JDBC_ARGUMENT                     = 62,
                            DATABASE_IS_MEMORY_ONLY                   = 63,
                            OUTER_JOIN_CONDITION                      = 64,
                            NUMERIC_VALUE_OUT_OF_RANGE                = 65,
                            MISSING_SOFTWARE_MODULE                   = 66,
                            NOT_IN_AGGREGATE_OR_GROUP_BY              = 67,
                            INVALID_GROUP_BY                          = 68,
                            INVALID_HAVING                            = 69,
                            INVALID_ORDER_BY                          = 70,
                            INVALID_ORDER_BY_IN_DISTINCT_SELECT       = 71,
                            OUT_OF_MEMORY                             = 72,
                            OPERATION_NOT_SUPPORTED                   = 73,
                            INVALID_IDENTIFIER                        = 74,
                            TEXT_TABLE_SOURCE                         = 75,
                            TEXT_FILE                                 = 76,
                            BAD_IDENTITY_VALUE                        = 77,
                            ERROR_IN_SCRIPT_FILE                      = 78,
                            NULL_IN_VALUE_LIST                        = 79,
                            SOCKET_ERROR                              = 80,
                            INVALID_CHARACTER_ENCODING                = 81,
                            NO_CLASSLOADER_FOR_TLS                    = 82,
                            NO_JSSE                                   = 83,
                            NO_SSLSOCKETFACTORY_METHOD                = 84,
                            UNEXPECTED_EXCEPTION                      = 85,
                            TLS_ERROR                                 = 86,
                            MISSING_TLS_METHOD                        = 87,
                            TLS_SECURITY_ERROR                        = 88,
                            NO_TLS_DATA                               = 89,
                            NO_PRINCIPAL                              = 90,
                            INCOMPLETE_CERTIFICATE                    = 91,
                            TLS_HOSTNAME_MISMATCH                     = 92,
                            KEYSTORE_PROBLEM                          = 93,
                            DATABASE_NOT_EXISTS                       = 94,
                            INVALID_CONVERSION                        = 95,
                            ERROR_IN_BINARY_SCRIPT_1                  = 96,
                            ERROR_IN_BINARY_SCRIPT_2                  = 97,
                            Cache_cleanUp                             = 98,
                            Cache_saveAll                             = 99,
                            Constraint_violation                      = 100,
                            Database_dropTable                        = 101,
                            ERROR_IN_CONSTRAINT_COLUMN_LIST           = 102,
                            TABLE_HAS_NO_PRIMARY_KEY                  = 103,
                            VIOLATION_OF_UNIQUE_CONSTRAINT            = 104,
                            NO_DEFAULT_VALUE_FOR_COLUMN               = 105,
                            NULL_VALUE_AS_BOOLEAN                     = 106,
                            DatabaseManager_getDatabase               = 107,
                            DatabaseManager_getDatabaseObject         = 108,
                            DatabaseManager_releaseSession            = 109,
                            DatabaseManager_releaseDatabase           = 110,
                            DatabaseRowInput_newDatabaseRowInput      = 111,
                            DatabaseRowOutput_newDatabaseRowOutput    = 112,
                            DatabaseScriptReader_readDDL              = 113,
                            DatabaseScriptReader_readExistingData     = 114,
                            Message_Pair                              = 115,
                            HsqlDatabaseProperties_load               = 116,
                            HsqlDatabaseProperties_save               = 117,
                            JDBC_INVALID_BRI_SCOPE                    = 118,
                            JDBC_NO_RESULT_SET_METADATA               = 119,
                            JDBC_NO_RESULT_SET                        = 120,
                            MISSING_CLOSEBRACKET                      = 121,
                            TableFilter_findFirst                     = 122,
                            Table_moveDefinition                      = 123,
                            STRING_DATA_TRUNCATION                    = 124,
                            QUOTED_IDENTIFIER_REQUIRED                = 125,
                            STATEMENT_IS_CLOSED                       = 126,
                            DatabaseRowInput_skipBytes                = 127,
                            DatabaseRowInput_readLine                 = 128,
                            DataFileDefrag_writeTableToDataFile       = 129,
                            DiskNode_writeTranslatePointer            = 130,
                            HsqlDateTime_null_string                  = 131,
                            HsqlDateTime_invalid_timestamp            = 132,
                            HsqlDateTime_null_date                    = 133,
                            HsqlDateTime_invalid_date                 = 134,
                            HsqlProperties_load                       = 135,
                            HsqlSocketFactorySecure_verify            = 136,
                            HsqlSocketFactorySecure_verify2           = 137,
                            jdbcConnection_nativeSQL                  = 138,
                            HsqlSocketFactorySecure_verify3           = 139,
                            jdbcPreparedStatement_setCharacterStream  = 140,
                            jdbcPreparedStatement_setClob             = 141,
                            jdbcStatement_executeUpdate               = 142,
                            LockFile_checkHeartbeat                   = 143,
                            LockFile_checkHeartbeat2                  = 144,
                            TEXT_STRING_HAS_NEWLINE                   = 145,
                            Result_Result                             = 146,
                            SERVER_NO_DATABASE                        = 147,
                            Server_openServerSocket                   = 148,
                            Server_openServerSocket2                  = 149,
                            TextDatabaseRowOutput_checkConvertString  = 150,
                            TextDatabaseRowOutput_checkConvertString2 = 151,
                            TextDatabaseRowOutput_writeIntData        = 152,
                            ORDER_BY_POSITION                         = 153,
                            JDBC_STATEMENT_NOT_ROW_COUNT              = 154,
                            JDBC_STATEMENT_NOT_RESULTSET              = 155,
                            AMBIGUOUS_COLUMN_REFERENCE                = 156,
                            CHECK_CONSTRAINT_VIOLATION                = 157,
                            JDBC_RESULTSET_IS_CLOSED                  = 158,
                            SINGLE_COLUMN_EXPECTED                    = 159,
                            TOKEN_REQUIRED                            = 160,
                            Logger_checkFilesInJar                    = 161,
                            Logger_checkFilesInJar1                   = 162,
                            Logger_checkFilesInJar2                   = 163,
                            TRIGGER_ALREADY_EXISTS                    = 164,
                            ASSERT_DIRECT_EXEC_WITH_PARAM             = 165,
                            DataFileCache_backup                      = 166,
                            Expression_compareValues                  = 167,
                            Parser_parseLimit1                        = 168,
                            Parser_parseLimit2                        = 169,
                            SQL_CONSTRAINT_REQUIRED                   = 170,
                            TableWorks_dropConstraint                 = 171,
                            TEXT_TABLE_SOURCE_FILENAME                = 172,
                            TEXT_TABLE_SOURCE_VALUE_MISSING           = 173,
                            TEXT_TABLE_SOURCE_SEPARATOR               = 174,
                            UNSUPPORTED_PARAM_CLASS                   = 175,
                            JDBC_NULL_STREAM                          = 176,
                            INTEGRITY_CONSTRAINT_VIOLATION_NOPARENT   = 177,
                            DatabaseRowInput_getPos                   = 178,
                            DatabaseRowInput_getNextPos               = 179,
                            QuotedTextDatabaseRowInput_getField       = 180,
                            QuotedTextDatabaseRowInput_getField2      = 181,
                            TextDatabaseRowInput_getField             = 182,
                            TextDatabaseRowInput_getField2            = 183,
                            TextDatabaseRowInput_getField3            = 184,
                            Parser_ambiguous_between1                 = 185,
                            SEQUENCE_REFERENCED_BY_VIEW               = 186,
                            Generic_reading_file_error                = 187,
                            TextCache_openning_file_error             = 188,
                            TextCache_closing_file_error              = 189,
                            TextCache_purging_file_error              = 190,
                            SEQUENCE_NOT_FOUND                        = 191,
                            SEQUENCE_ALREADY_EXISTS                   = 192,
                            TABLE_REFERENCED_CONSTRAINT               = 193,
                            TABLE_REFERENCED_VIEW                     = 194,
                            PARAMETRIC_TABLE_NAME                     = 195,
                            TEXT_SOURCE_EXISTS                        = 196,
                            COLUMN_IS_REFERENCED                      = 197,
                            FUNCTION_CALL_ERROR                       = 198,
                            TRIGGERED_DATA_CHANGE                     = 199,
                            INVALID_FUNCTION_ARGUMENT                 = 200,
                            INTERNAL_unknown_internal_statement_type  = 201,
                            INTERNAL_session_operation_not_supported  = 202,
                            INVALID_PREPARED_STATEMENT                = 203,
                            CREATE_TRIGGER_COMMAND_1                  = 204,
                            CREATE_TRIGGER_COMMAND_2                  = 205,
                            BAD_SAVEPOINT_NAME                        = 206,
                            DataFileCache_defrag                      = 207,
                            DataFileCache_closeFile                   = 208,
                            DataFileCache_makeRow                     = 209,
                            DataFileCache_open                        = 210,
                            DataFileCache_close                       = 211,
                            Expression_resolveTypes1                  = 212,
                            Expression_resolveTypes2                  = 213,
                            Expression_resolveTypes3                  = 214,
                            Expression_resolveTypes4                  = 215,
                            UNRESOLVED_PARAMETER_TYPE                 = 216,
                            Expression_resolveTypes6                  = 217,
                            Expression_resolveTypes7                  = 218,
                            Expression_resolveTypeForLike             = 219,
                            Expression_resolveTypeForIn1              = 220,
                            Expression_resolveTypeForIn2              = 221,
                            Session_execute                           = 222,
                            Session_sqlExecuteDirect                  = 223,
                            Session_sqlExecuteCompiled                = 224,
                            LAST_ERROR_HANDLE                         = 225;

    //
    static String MESSAGE_TAG = "$$";

    //
    private static final String[] sDescription = {
        "NOT USED",                                                     //
        "08001 The database is already in use by another process",      // 1
        "08003 Connection is closed",                                   //
        "08003 Connection is broken",                                   //
        "08003 The database is shutdown",                               //
        "21000 Column count does not match",                            //
        "22012 Division by zero",                                       //
        "22019 Invalid escape character",                               //
        "23000 Integrity constraint violation",                         //
        "23000 Violation of unique index",                              //
        "23000 Try to insert null into a non-nullable column",          //
        "37000 Unexpected token",                                       // 11
        "37000 Unexpected end of command",                              //
        "37000 Unknown function",                                       //
        "37000 Need aggregate function or group by",                    //
        "37000 Sum on non-numeric data not allowed",                    //
        "37000 Wrong data type",                                        //
        "37000 Single value expected",                                  //
        "40001 Serialization failure",                                  //
        "40001 Transfer corrupted",                                     //
        "IM001 This function is not supported",                         //
        "S0001 Table already exists",                                   // 21
        "S0002 Table not found",                                        //
        "S0011 Index already exists",                                   //
        "S0011 Attempt to define a second primary key",                 //
        "S0011 Attempt to drop the primary key",                        //
        "S0012 Index not found",                                        //
        "S0021 Column already exists",                                  //
        "S0022 Column not found",                                       //
        "S1000 File input/output error",                                //
        "S1000 Wrong database file version",                            //
        "S1000 The database is in read only mode",                      // 31
        "S1000 The table data is read only",                            //
        "S1000 Access is denied",                                       //
        "S1000 InputStream error",                                      //
        "S1000 No data is available",                                   //
        "S1000 User already exists",                                    //
        "S1000 User not found",                                         //
        "S1000 Assert failed",                                          //
        "S1000 External stop request",                                  //
        "S1000 General error",                                          //
        "S1009 Wrong OUT parameter",                                    // 41
        "S1010 Function not found",                                     //
        "S0002 Trigger not found",                                      //
        "S1011 Savepoint not found",                                    //
        "37000 Label required for value list",                          //
        "37000 Wrong data type or data too long in DEFAULT clause",     //
        "S0011 Both tables must be permanent or temporary",             //
        "S1000 The table's data source for has not been defined",       //
        "S0000 Index or constraint name cannot begin with SYS_",        //
        "S0011 Attempt to drop a foreign key index",                    //
        "S1000 ResultSet was set to forward only",                      // 51
        "S0003 View already exists",                                    //
        "S0004 View not found",                                         //
        "S0005 Not a View",                                             //
        "S0005 Not a Table",                                            //
        "S0011 Attempt to drop or rename a system index",               //
        "S0021 Column types do not match",                              //
        "s0021 Column constraints are not acceptable",                  //
        "S0011 Attempt to drop a system constraint",                    //
        "S0011 Constraint already exists",                              //
        "S0011 Constraint not found",                                   // 61
        "SOO10 Invalid argument in JDBC call",                          //
        "S1000 Database is memory only",                                //
        "37000 not allowed in OUTER JOIN condition",                    //
        "22003 Numeric value out of range",                             //
        "37000 Software module not installed",                          //
        "37000 Not in aggregate function or group by clause",           //
        "37000 Cannot be in GROUP BY clause",                           //
        "37000 Cannot be in HAVING clause",                             //
        "37000 Cannot be in ORDER BY clause",                           //
        "37000 ORDER BY item should be in the SELECT DISTINCT list",    // 71
        "S1000 Out of Memory",                                          //
        "S1000 This operation is not supported",                        //
        "22019 Invalid identifier",                                     //
        "22019 Invalid TEXT table source string",                       //
        "S1000 bad TEXT table source file - line number: $$ $$",        //
        "23000 negative value not allowed for identity column",         //
        "S1000 error in script file",                                   //
        "37000 NULL in value list",                                     //
        "08000 socket creation error",                                  //
        "37000 invalid character encoding",                             // 81
        "08000 Failed to obtain a Java Classloader for TLS",            //
        "08000 Trying to use security, but JSSE not available",         //
        "08000 Failed to obtain an SSL Socket Factory",                 //
        "08000 Unexpected exceptin when setting up TLS",                //
        "08000 TLS Error",                                              //
        "08000 A required method for TLS capability is missing",        //
        "08000 TLS Security error",                                     //
        "08000 TLS failed to obtain security data",                     //
        "08000 No authentication principal",                            //
        "08000 Incomplete security certificate",                        // 91
        "08000 Authentication refusal due to hostname mismatch",        //
        "08000 Certificate or private key keystore problem",            //
        "08003 Database does not exists",                               //
        "22003 Type Conversion not supported",                          //
        " table $$ row count error : $$ read, needed $$",               // BinaryDatabaseScriptReader_readExistingData
        " wrong data for insert operation",                             // BinaryDatabaseScriptReader_readTableInit
        " SaveRow $$",                                                  // Cache_cleanUp
        " SaveRow $$",                                                  // Cache_saveAll
        " $$ table: $$",                                                // Constraint_checkInsert
        " $$ table: $$",                                                // 101 Database_dropTable
        "duplicate column in list",                                     // DatabaseCommandInterpreter_processColumnList
        "table has no primary key",                                     // DatabaseCommandInterpreter_processCreateConstraints
        "23000 Unique constraint violation",                            //
        "missing DEFAULT value on column '$$'",                         // DatabaseCommandInterpreter_checkFKColumnDefaults
        "S1000 NULL value as BOOLEAN",                                  //
        "attempt to connect while db opening /closing",                 // DatabaseManager_getDatabase
        "problem in db access count",                                   // DatabaseManager_getDatabaseObject
        "problem in db access count",                                   // DatabaseManager_releaseSession
        "problem in db access count",                                   // DatabaseManager_releaseDatabase
        "legacy db support",                                            // 111 DatabaseRowInput_newDatabaseRowInput
        "legacy db support",                                            // DatabaseRowOutput_newDatabaseRowOutput
        " line: $$ $$",                                                 // DatabaseScriptReader_readDDL
        " line: $$ $$",                                                 // DatabaseScriptReader_readExistingData
        " $$ $$",                                                       // Function_Function
        "$$.properties $$",                                             // HsqlDatabaseProperties_load
        "$$.properties $$",                                             // HsqlDatabaseProperties_save
        "invalid scope value",                                          // jdbcDatabaseMetaData_getBestRowIdentifier
        "result set is null",                                           // jdbcResultSetMetaData_jdbcResultSetMetaData
        "result set is closed",                                         // jdbcResultSetMetaData_jdbcResultSetMetaData_2
        "37000 missing )",                                              // MISSING_CLOSEBRACKET
        "37000 an index is required on table $$, column $$",            // TableFilter_findFirst
        "37000 there is an index on the column to be removed",          // Table_moveDefinition
        "22001 string too long",                                        //
        "00000 quoted identifier required",                             // SET PROPERTY "name" "value"
        "00000 statement is closed",                                    // SET PROPERTY "name" "value"
        "Method skipBytes() not yet implemented.",                      // DatabaseRowInput_skipBytes
        "Method readLine() not yet implemented.",                       // DatabaseRowInput_readLine
        "S1000 ",                                                       // DataFileDefrag_writeTableToDataFile
        "S1000 ",                                                       // DiskNode_writeTranslatePointer
        "null string",                                                  // 131 HsqlDateTime_null_string
        "invalid timestamp",                                            // HsqlDateTime_invalid_timestamp
        "null date",                                                    // HsqlDateTime_null_date
        "invalid date",                                                 // HsqlDateTime_invalid_date
        "properties name is null or empty",                             // HsqlProperties_load
        "Server certificate has no Common Name",                        // HsqlSocketFactorySecure_verify
        "Server certificate has empty Common Name",                     // HsqlSocketFactorySecure_verify2
        "Unknown JDBC escape sequence: {",                              // jdbcConnection_nativeSQL
        "Certificate Common Name[$$] does not match host name[$$]",     // HsqlSocketFactorySecure_verify3
        "End of stream with no data read",                              // jdbcPreparedStatement_setCharacterStream
        "End of stream with no data read",                              // 141 jdbcPreparedStatement_setClob
        "executeUpdate() cannot be used with this statement",           // jdbcStatement_executeUpdate
        "$$ : $$",                                                      // LockFile_checkHeartbeat
        "$$$$ is presumably locked by another process.",                // LockFile_checkHeartbeat2
        "end of line characters not allowed",                           // QuotedTextDatabaseRowOutput_checkConvertString
        "trying to use unsupported result mode: $$",                    // Result_Result
        "no valid database paths",                                      // SERVER_NO_DATABASE
        "Invalid address : $$\nTry one of: $$",                         // Server_openServerSocket
        "Invalid address : $$",                                         // Server_openServerSocket2
        "end of line characters not allowed",                           // TextDatabaseRowOutput_checkConvertString
        "separator not allowed in unquoted string",                     // 151 TextDatabaseRowOutput_checkConvertString2
        "not implemented",                                              // TextDatabaseRowOutput_writeIntData
        "00000 ORDER BY must be at the end of the statement",           //
        "00000 Statement does not generate a row count",                //
        "00000 Statement does not generate a result set",               //
        "S0022 ambiguous Column reference",                             //
        "23000 Check constraint violation",                             //
        "S1000 ResultSet is closed",                                    //
        "37000 Single column select required in IN predicate",          //
        " $$, requires $$",                                             // Tokenizer.getThis()
        "path is null",                                                 // 161
        "file does not exist: ",                                        //
        "wrong resource protocol: ",                                    //
        "S0002 Trigger already exists",                                 //
        "S0000 direct execute with param count > 0",                    //
        "while creating ",                                              // DataFileCache_backup
        "Expression.compareValues",                                     // Expression_compareValues
        "LIMIT n m",                                                    // Parser_parseLimit1
        "TOP n",                                                        // Parser_parseLimit2
        "S0011 primary or unique constraint required on main table",    //
        "$$ in table: $$",                                              // 171
        "no file name specified for source",                            //
        "no value for: ",                                               //
        "zero length separator",                                        //
        "Unsupported parameter/return value class: ",                   //
        "input stream is null",                                         //
        "23000 Integrity constraint violation - no parent",             //
        "No position specified",                                        // DatabaseRowInput_getPos
        "No next position specified",                                   // DatabaseRowInput_getNextPos
        "No sep.",                                                      // QuotedTextDatabaseRowInput_getField
        "field $$ ($$)",                                                // 181 QuotedTextDatabaseRowInput_getField2
        "No end sep.",                                                  // TextDatabaseRowInput_getField
        "No end sep.",                                                  // TextDatabaseRowInput_getField2
        "field $$ ($$)",                                                // TextDatabaseRowInput_getField3
        "as operands of a BETWEEN predicate",                           //
        "23000 Sequence is referenced by view",                         //
        "error reading script file",                                    //
        "openning file: $$ error: $$",                                  // TextCache - or generic file error
        "closing file: $$ error: $$",                                   // TextCache - or generic file error
        "purging file: $$ error: $$",                                   // TextCache - or generic file error
        "S0002 Sequence not found",                                     // 191
        "S1000 Sequence already exists",                                //
        "23000 Table is referenced by a constraint in table",           //
        "23000 Table is referenced by view",                            //
        "parametric table identifier",                                  // Parser
        "S1000 text source file already exists",                        // SELECT INTO TEXT <name>
        "23000 column is referenced in",                                //
        "S1000 Error calling function",                                 //
        "27000 Triggered data change violation",                        //
        "37000 Invalid argument",                                       //
        "S1000 Internal Error : Unknown SQL Statement Type:",           // 201
        "S1000 Internal Error : Unknown Session Operation Type:",       //
        "S1000 prepared statement is no longer valid",                  //
        "parsing trigger command ",                                     // DatabaseCommandInterpreter_processCreateTrigger1
        "loading trigger class ",                                       // DatabaseCommandInterpreter_processCreateTrigger2
        "missing or zero-length savepoint name",                        // DatabaseCommandInterpreter_processSavepoint
        "error $$ during defrag - file $$",                             // DataFileCache_defrag
        "error $$ during shutdown - file $$",                           // DataFileCache_closeFile
        "error $$ reading row - file $$",                               // DataFileCache_makeRow
        "error $$ opening file - file $$",                              // DataFileCache_makeRow
        "error $$ closing file - file $$",                              // 211 DataFileCache_makeRow
        "in unary negation operation",                                  // Expression_resolveTypes1
        "as both operands of aritmetic operator",                       // Expression_resolveTypes2
        "as both comparison expression",                                // Expression_resolveTypes3
        "parameter not allowed as the argument of a set function",      // Expression_resolveTypes4
        "unresolved parameter type ",                                   // Expression_resolveTypes5
        "as both operands of a CASE operation",                         // Expression_resolveTypes6
        "as output of CASE when operand types are $$ and $$",           // Expression_resolveTypes7
        "as both expressions of LIKE",                                  // Expression_resolveTypeForLike
        "when the value list is empty",                                 // Expression_resolveTypeForIn1
        "as both expression and first entry of an IN operation",        // 221 Expression_resolveTypeForIn2
        "Session is closed",                                            // Session_execute
        "Session is closed",                                            // Session_sqlExecuteDirect
        "Session is closed",                                            // Session_sqlExecuteCompiled
        "LAST"                                                          // Control variable
    };

    /** Used during tests. */
    public static final int NUMBER_OF_ERROR_MESSAGES = sDescription.length;

    static {
        try {
            TRACE = TRACE || Boolean.getBoolean("hsqldb.trace");
            TRACESYSTEMOUT = TRACESYSTEMOUT
                             || Boolean.getBoolean("hsqldb.tracesystemout");
        } catch (Exception e) {}

        if (!sDescription[LAST_ERROR_HANDLE].equals("LAST")) {
            throw new RuntimeException(sDescription[Trace.GENERAL_ERROR]);
        }
    }

    /**
     * Compose error message by inserting the strings in the add parameters
     * in placeholders within the error message. The message string contains
     * $$ markers for each context variable. Context variables are supplied in
     * the add parameters.
     *
     * @param code      main error code
     * @param subCode   sub error code (if 0 => no subMessage!)
     * @param   add     optional parameters
     *
     * @return an <code>HsqlException</code>
     */
    public static HsqlException error(int code, int subCode,
                                      final Object[] add) {

        // in case of negative code
        code = Math.abs(code);

        String mainErrorMessage = getMessage(code);
        String state            = "S1000";

        if (mainErrorMessage.length() >= 5) {
            state            = mainErrorMessage.substring(0, 5);
            mainErrorMessage = mainErrorMessage.substring(6);
        }

        if (subCode != 0) {
            mainErrorMessage += getMessage(Math.abs(subCode));
        }

        StringBuffer sb = new StringBuffer(mainErrorMessage.length() + 32);
        int          lastIndex = 0;
        int          escIndex  = mainErrorMessage.length();

        if (add != null) {

            // removed test: i < add.length
            // because if mainErrorMessage is equal to "blabla $$"
            // then the statement escIndex = mainErrorMessage.length();
            // is never reached!  ???
            for (int i = 0; i < add.length; i++) {
                escIndex = mainErrorMessage.indexOf(MESSAGE_TAG, lastIndex);

                if (escIndex == -1) {
                    break;
                }

                sb.append(mainErrorMessage.substring(lastIndex, escIndex));
                sb.append(add[i] == null ? "null exception message"
                                         : add[i].toString());

                lastIndex = escIndex + MESSAGE_TAG.length();
            }
        }

        escIndex = mainErrorMessage.length();

        sb.append(mainErrorMessage.substring(lastIndex, escIndex));

        return new HsqlException(sb.toString(), state, -code);
    }

    /**
     * Compose error message by inserting the strings in the add parameters
     * in placeholders within the error message. The message string contains
     * $$ markers for each context variable. Context variables are supplied in
     * the add parameters.
     *
     * @param code      main error code
     * @param   add     optional parameters
     *
     * @return an <code>HsqlException</code>
     */
    public static HsqlException error(int code, final Object[] add) {
        return error(code, 0, add);
    }

    public static HsqlException error(int code, int code2, String add) {
        return error(code, getMessage(code2) + add);
    }

    public static HsqlException error(int code, int code2) {
        return error(code, getMessage(code2));
    }

    /**
     * Method declaration
     *
     *
     * @param code
     * @param add
     *
     * @return
     */
    public static HsqlException error(int code, Object add) {

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
        code = Math.abs(code);

        String s = getMessage(code);

        if (add != null) {
            s += ": " + add.toString();
        }

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
        return new HsqlException(s.substring(6), s.substring(0, 5), -code);

        //return getError(s);
    }

    /**
     *     Return a new <code>HsqlException</code> according to the result parameter.
     *
     * @param result    the <code>Result</code> associated with the exception
     *     @return a new <code>HsqlException</code> according to the result parameter
     */
    static HsqlException error(final Result result) {
        return new HsqlException(result);
    }

    /**
     * Return a new <code>Result</code> of type error.
     *
     * @param result    the <code>Result</code> associated with the exception
     *     @return a new <code>HsqlException</code> according to the result parameter
     */

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)

    /**
     *  Constructor for errors
     *
     * @param  e exception
     */
    static Result toResult(HsqlException e) {
        return new Result(e.getMessage(), e.getSQLState(), e.getErrorCode());
    }

    /**
     * Returns the error message given the error code.<br/>
     * Note: this method must be used when throwing exception other
     * than <code>HsqlException</code>.
     *
     * @param errorCode    the error code associated to the error message
     * @return  the error message associated with the error code
     * @see #sDescription
     */
    public static String getMessage(final int errorCode) {
        return getMessage(errorCode, false, null);
    }

    /**
     * Returns the error message given the error code.<br/>
     * Note: this method must be used when throwing exception other
     * than <code>HsqlException</code>.
     *
     * @param errorCode    the error code associated to the error message
     * @param substitute    substitute the $$ tokens using data in the values
     * @param values       value(s) to use to replace the token(s)
     * @return the error message associated with the error code
     * @see #sDescription
     */
    public static String getMessage(final int errorCode,
                                    final boolean substitute,
                                    final Object[] values) {

        if (errorCode < 0 || errorCode >= sDescription.length) {
            return "";
        } else {
            if (!substitute) {
                return sDescription[errorCode];
            } else {
                final String mainErrorMessage = sDescription[errorCode];
                final StringBuffer sb =
                    new StringBuffer(mainErrorMessage.length() + 32);
                int lastIndex = 0;
                int escIndex  = mainErrorMessage.length();

                if (values != null) {

                    // removed test: i < add.length
                    // because if mainErrorMessage is equal to "blabla $$"
                    // then the statement escIndex = mainErrorMessage.length();
                    // is never reached!  ???
                    for (int i = 0; i < values.length; i++) {
                        escIndex = mainErrorMessage.indexOf(MESSAGE_TAG,
                                                            lastIndex);

                        if (escIndex == -1) {
                            break;
                        }

                        sb.append(mainErrorMessage.substring(lastIndex,
                                                             escIndex));
                        sb.append(values[i].toString());

                        lastIndex = escIndex + MESSAGE_TAG.length();
                    }
                }

                escIndex = mainErrorMessage.length();

                sb.append(mainErrorMessage.substring(lastIndex, escIndex));

                return sb.toString();
            }
        }
    }

    /**
     * Method declaration
     *
     *
     * @param code
     *
     * @return
     */
    public static HsqlException error(int code) {
        return error(code, null);
    }

    /**
     *     Throws exception if condition is false
     *
     *     @param condition
     *     @param code
     *
     * @throws HsqlException
     */
    static void check(boolean condition, int code) throws HsqlException {
        check(condition, code, null, null, null, null);
    }

    /**
     *     Throws exception if condition is false
     *
     *     @param condition
     *     @param code
     *     @param add
     *
     * @throws HsqlException
     */
    public static void check(boolean condition, int code,
                             Object add) throws HsqlException {

        if (!condition) {
            throw error(code, add);
        }
    }

    /**
     * Method declaration
     *
     *
     * @param code
     * @param add
     *
     * @throws HsqlException
     */
    static void throwerror(int code, Object add) throws HsqlException {
        throw error(code, add);
    }

    /**
     * Used to print messages to System.out
     *
     *
     * @param message message to print
     */
    public static void printSystemOut(String message) {

        if (TRACESYSTEMOUT) {
            System.out.println(message);
        }
    }

    /**
     * Used to print messages to System.out
     *
     *
     * @param message1 message to print
     * @param message2 message to print
     */
    public static void printSystemOut(String message1, long message2) {

        if (TRACESYSTEMOUT) {
            System.out.print(message1);
            System.out.println(message2);
        }
    }

    /**
     * Returns the stack trace for doAssert()
     */
    private static String getStackTrace() {

        try {
            Exception e = new Exception();

            throw e;
        } catch (Exception e1) {
            HsqlByteArrayOutputStream os = new HsqlByteArrayOutputStream();
            PrintWriter               pw = new PrintWriter(os, true);

            e1.printStackTrace(pw);

            return os.toString();
        }
    }

    /**
     * Throws exception if condition is false
     *
     * @param condition
     * @param code
     * @param add1
     * @param add2
     *
     * @throws HsqlException
     */
    static void check(boolean condition, int code, String add1,
                      String add2) throws HsqlException {
        check(condition, code, add1, add2, null, null);
    }

    /**
     * Throws exception if condition is false
     *
     * @param condition
     * @param code
     * @param add1
     * @param add2
     * @param add3
     *
     * @throws HsqlException
     */
    static void check(boolean condition, int code, String add1, String add2,
                      String add3) throws HsqlException {
        check(condition, code, add1, add2, add3, null);
    }

    /**
     * Throws exception if condition is false
     *
     * @param condition
     * @param code
     * @param add1
     * @param add2
     * @param add3
     * @param add4
     *
     * @throws HsqlException
     */
    static void check(boolean condition, int code, String add1, String add2,
                      String add3, String add4) throws HsqlException {

        if (!condition) {
            String add = "";

            if (add1 != null) {
                add += add1;
            }

            if (add2 != null) {
                add += add2;
            }

            if (add3 != null) {
                add += add3;
            }

            if (add4 != null) {
                add += add4;
            }

            throw error(code, add.length() > 0 ? add
                                               : null);
        }
    }

    /**
     * Throws exception if assertion fails
     *
     * @param condition
     * @throws HsqlException
     */
    static void doAssert(boolean condition) throws HsqlException {
        doAssert(condition, null);
    }

    /**
     * Throws exception if assertion fails
     *
     * @param condition
     * @param error
     * @throws HsqlException
     */
    static void doAssert(boolean condition,
                         String error) throws HsqlException {

        if (!condition) {
            if (error == null) {
                error = "";
            }

            error += getStackTrace();

            throw error(ASSERT_FAILED, error);
        }
    }
}
