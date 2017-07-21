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

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;

import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.scriptio.ScriptWriterBase;
import org.hsqldb.scriptio.ScriptWriterText;
import org.hsqldb.HsqlNameManager.HsqlName;

/**
 * Provides SQL Interpreter services relative to a Session and
 * its Database.
 *
 * @version 1.7.2
 * @since HSQLDB 1.7.2
 */

// fredt@users 20020430 - patch 549741 by velichko - ALTER TABLE RENAME
// fredt@users 20020405 - patch 1.7.0 - other ALTER TABLE statements
// tony_lai@users 20020820 - patch 595099 - use user-defined PK name
// tony_lai@users 20020820 - patch 595156 - violation of constraint name
// fredt@users 20020912 - patch 1.7.1 by fredt - log alter statements
// kloska@users 20021030 - patch 1.7.2 - ON UPDATE CASCADE | SET NULL | SET DEFAULT
// kloska@users 20021112 - patch 1.7.2 - ON DELETE SET NULL | SET DEFAULT
// boucherb@users 20020310 - disable ALTER TABLE DDL on VIEWs (avoid NPE)
// fredt@users 20030314 - patch 1.7.2 by gilead@users - drop table if exists syntax
// boucherb@users 20030425 - DDL methods are moved to DatabaseCommandInterpreter.java
// boucherb@users 20030425 - refactoring DDL methods into smaller units
// fredt@users 20030609 - support for ALTER COLUMN SET/DROP DEFAULT / RENAME TO
// wondersonic@users 20031205 - IF EXISTS support for DROP INDEX
// fredt@users 20031224 - support for CREATE SEQUENCE ...
class DatabaseCommandInterpreter {

    Tokenizer          tokenizer = new Tokenizer();
    protected Database database;
    protected Session  session;

    /**
     * Constructs a new DatabaseCommandInterpreter for the given Session
     *
     * @param s session
     */
    DatabaseCommandInterpreter(Session s) {
        session  = s;
        database = s.getDatabase();
    }

    /**
     * Executes the SQL String. This method is always called from a block
     * synchronized on the database object.
     *
     * @param sql query
     * @return the result of executing the given SQL String
     */
    Result execute(String sql) {

        Parser parser;
        Result result;
        String token;
        int    cmd;
        Logger logger;

        DatabaseManager.gc();

        result = null;
        cmd    = Token.UNKNOWNTOKEN;
        logger = database.logger;

        try {
            tokenizer.reset(sql);

            parser = new Parser(session, database, tokenizer);

            while (true) {
                tokenizer.setPartMarker();
                session.setScripting(false);

                token = tokenizer.getString();

                if (token.length() == 0) {
                    break;
                }

                cmd = Token.get(token);

                if (cmd == Token.SEMICOLON) {
                    continue;
                }

                // TODO:  build up list of Results in session context
                // RE:    ability to gen/retrieve multiple result sets
                // EX:    session.addResult(executePart(cmd, token, parser));
                result = executePart(cmd, token, parser);

                // PATCH -- needs executePart to return not null result
                if (result.mode == ResultConstants.ERROR) {
                    break;
                }

                if (session.getScripting()) {
                    logger.writeToLog(session, tokenizer.getLastPart());
                }
            }
        } catch (Throwable t) {

/** @todo fredt - when out of memory error is thrown it does not get classified
 *   making scriptRunner vulnerable */
            result = new Result(t, tokenizer.getLastPart());
        }

        return result == null ? Session.emptyUpdateCount
                              : result;
    }

    private Result executePart(int cmd, String token,
                               Parser parser) throws Throwable {

        Result result   = Session.emptyUpdateCount;
        int    brackets = 0;

        switch (cmd) {

            case Token.OPENBRACKET : {
                brackets = Parser.parseOpenBrackets(tokenizer) + 1;

                tokenizer.getThis(Token.T_SELECT);
            }
            case Token.SELECT : {
                CompiledStatement cStatement =
                    parser.compileSelectStatement(brackets);

                if (cStatement.parameters.length != 0) {
                    Trace.doAssert(
                        false,
                        Trace.getMessage(
                            Trace.ASSERT_DIRECT_EXEC_WITH_PARAM));
                }

                result = session.sqlExecuteCompiledNoPreChecks(cStatement);

                break;
            }
            case Token.INSERT : {
                CompiledStatement cStatement =
                    parser.compileInsertStatement();

                if (cStatement.parameters.length != 0) {
                    Trace.doAssert(
                        false,
                        Trace.getMessage(
                            Trace.ASSERT_DIRECT_EXEC_WITH_PARAM));
                }

                result = session.sqlExecuteCompiledNoPreChecks(cStatement);

                break;
            }
            case Token.UPDATE : {
                CompiledStatement cStatement =
                    parser.compileUpdateStatement();

                if (cStatement.parameters.length != 0) {
                    Trace.doAssert(
                        false,
                        Trace.getMessage(
                            Trace.ASSERT_DIRECT_EXEC_WITH_PARAM));
                }

                result = session.sqlExecuteCompiledNoPreChecks(cStatement);

                break;
            }
            case Token.DELETE : {
                CompiledStatement cStatement =
                    parser.compileDeleteStatement();

                if (cStatement.parameters.length != 0) {
                    Trace.doAssert(
                        false,
                        Trace.getMessage(
                            Trace.ASSERT_DIRECT_EXEC_WITH_PARAM));
                }

                result = session.sqlExecuteCompiledNoPreChecks(cStatement);

                break;
            }
            case Token.CALL : {
                CompiledStatement cStatement = parser.compileCallStatement();

                if (cStatement.parameters.length != 0) {
                    Trace.doAssert(
                        false,
                        Trace.getMessage(
                            Trace.ASSERT_DIRECT_EXEC_WITH_PARAM));
                }

                result = session.sqlExecuteCompiledNoPreChecks(cStatement);

                break;
            }
            case Token.SET :
                processSet();
                break;

            case Token.COMMIT :
                processCommit();
                break;

            case Token.ROLLBACK :
                processRollback();
                break;

            case Token.SAVEPOINT :
                processSavepoint();
                break;

            case Token.CREATE :
                processCreate();
                database.setMetaDirty(true);
                break;

            case Token.ALTER :
                processAlter();
                database.setMetaDirty(true);
                break;

            case Token.DROP :
                processDrop();
                database.setMetaDirty(true);
                break;

            case Token.GRANT :
                processGrantOrRevoke(true);
                database.setMetaDirty(false);
                break;

            case Token.REVOKE :
                processGrantOrRevoke(false);
                database.setMetaDirty(true);
                break;

            case Token.CONNECT :
                processConnect();
                database.setMetaDirty(false);
                session.setScripting(false);
                break;

            case Token.DISCONNECT :
                processDisconnect();
                session.setScripting(true);
                break;

            case Token.SCRIPT :
                result = processScript();
                break;

            case Token.SHUTDOWN :
                processShutdown();
                break;

            case Token.CHECKPOINT :
                processCheckpoint();
                break;

            case Token.EXPLAIN :
                result = processExplainPlan();
                break;

            case Token.RELEASE :
                processReleaseSavepoint();
                break;

            default :
                throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
        }

        return result;
    }

    /**
     * Responsible for parsing and executing the SCRIPT SQL statement
     *
     * @return either an empty result or one in which each row is a DDL or DML
     * @throws IOException
     * @throws HsqlException
     */
    private Result processScript() throws IOException, HsqlException {

        String           token = tokenizer.getString();
        ScriptWriterText dsw   = null;

        try {
            if (tokenizer.wasValue()) {
                token = (String) tokenizer.getAsValue();
                dsw   = new ScriptWriterText(database, token, true, true);

                dsw.writeAll();

                return new Result(ResultConstants.UPDATECOUNT);
            } else {
                tokenizer.back();
                session.checkAdmin();

                return DatabaseScript.getScript(database, false);
            }
        } finally {
            if (dsw != null) {
                dsw.close();
            }
        }
    }

    /**
     *  Responsible for handling CREATE ...
     *
     *  All CREATE command require an ADMIN user except: <p>
     *
     * <pre>
     * CREATE TEMP [MEMORY] TABLE
     * </pre>
     *
     * @throws  HsqlException
     */
    private void processCreate() throws HsqlException {

        String  token;
        boolean isTemp;
        boolean unique;
        int     tableType;

        session.checkReadWrite();

        token  = tokenizer.getString();
        isTemp = false;

        if (token.equals(Token.T_TEMP)) {
            isTemp = true;
            token  = tokenizer.getString();

            switch (Token.get(token)) {

                case Token.TEXT :
                    session.checkAdmin();

                // fall thru
                case Token.TABLE :
                case Token.MEMORY :
                    session.setScripting(false);
                    break;

                default :
                    throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
            }
        } else {
            session.checkAdmin();
            session.checkDDLWrite();
            session.setScripting(true);
        }

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
        unique    = false;
        tableType = 0;

        switch (Token.get(token)) {

            // table
            case Token.TABLE :
                tableType = isTemp ? Table.TEMP_TABLE
                                   : Table.MEMORY_TABLE;

                processCreateTable(tableType);
                break;

            case Token.MEMORY :
                tokenizer.getThis(Token.T_TABLE);

                tableType = isTemp ? Table.TEMP_TABLE
                                   : Table.MEMORY_TABLE;

                processCreateTable(tableType);
                break;

            case Token.CACHED :
                tokenizer.getThis(Token.T_TABLE);
                processCreateTable(Table.CACHED_TABLE);
                break;

            case Token.TEXT :
                tokenizer.getThis(Token.T_TABLE);

                tableType = isTemp ? Table.TEMP_TEXT_TABLE
                                   : Table.TEXT_TABLE;

                processCreateTable(tableType);
                break;

            // other objects
            case Token.ALIAS :
                processCreateAlias();
                break;

            case Token.SEQUENCE :
                processCreateSequence();
                break;

            case Token.TRIGGER :
                processCreateTrigger();
                break;

            case Token.USER :
                processCreateUser();
                break;

            case Token.VIEW :
                processCreateView();
                break;

            // index
            case Token.UNIQUE :
                unique = true;

                tokenizer.getThis(Token.T_INDEX);

            //fall thru
            case Token.INDEX :
                processCreateIndex(unique);
                break;

            default : {
                throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
            }
        }
    }

    /**
     *  Process a bracketed column list as used in the declaration of SQL
     *  CONSTRAINTS and return an array containing the indexes of the columns
     *  within the table.
     *
     * @param  t table that contains the columns
     * @return  column index map
     * @throws  HsqlException if a column is not found or is duplicate
     */
    private int[] processColumnList(Table t) throws HsqlException {

        HsqlArrayList list;
        HashSet       set;
        String        token;
        int           col[];
        int           size;

        list = new HsqlArrayList();
        set  = new HashSet();

        tokenizer.getThis(Token.T_OPENBRACKET);

        while (true) {
            token = tokenizer.getName();

            list.add(token);
            set.add(token);

            if (list.size() != set.size()) {
                throw Trace.error(Trace.COLUMN_ALREADY_EXISTS,
                                  Trace.ERROR_IN_CONSTRAINT_COLUMN_LIST);
            }

            token = tokenizer.getString();

            if (token.equals(Token.T_DESC) || token.equals(Token.T_ASC)) {
                token = tokenizer.getString();    // OJ: eat it up
            }

            if (token.equals(Token.T_COMMA)) {
                continue;
            }

            if (token.equals(Token.T_CLOSEBRACKET)) {
                break;
            }

            throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
        }

        size = list.size();
        col  = new int[size];

        for (int i = 0; i < size; i++) {
            col[i] = t.getColumnNr((String) list.get(i));
        }

        return col;
    }

    /**
     *  Responsible for tail end of CREATE INDEX DDL. <p>
     *
     *  Indexes defined in DDL scripts are handled by this method. If the
     *  name of an existing index begins with "SYS_", the name is changed to
     *  begin with "USER_". The name should be unique within the database.
     *  For compatibility with old database, non-unique names are modified
     *  and assigned a new name. <p>
     *
     *  In 1.7.2 no new index is created if an equivalent already exists. <p>
     *
     *  (fredt@users) <p>
     *
     * @param  t table
     * @param  indexName index
     *
     * @param  indexNameQuoted is quoted
     * @param  unique is unique
     * @throws  HsqlException
     */
    private void addIndexOn(Table t, String indexName,
                            boolean indexNameQuoted,
                            boolean unique) throws HsqlException {

        HsqlName indexHsqlName;
        int[]    indexColumns;

        if (database.indexNameList.containsName(indexName)) {
            throw Trace.error(Trace.INDEX_ALREADY_EXISTS);
        }

        indexHsqlName = newIndexHsqlName(indexName, indexNameQuoted);
        indexColumns  = processColumnList(t);

        session.commit();
        session.setScripting(!t.isTemp());

        TableWorks tableWorks = new TableWorks(session, t);

        tableWorks.createIndex(indexColumns, indexHsqlName, unique, false,
                               false);
    }

    /**
     *  Responsible for handling the execution of CREATE TRIGGER SQL
     *  statements. <p>
     *
     *  typical sql is: CREATE TRIGGER tr1 AFTER INSERT ON tab1 CALL "pkg.cls"
     *
     * @throws HsqlException
     */
    private void processCreateTrigger() throws HsqlException {

        Table      t;
        boolean    isForEach;
        boolean    isNowait;
        int        queueSize;
        String     triggerName;
        boolean    isQuoted;
        String     sWhen;
        String     sOper;
        String     tableName;
        String     token;
        String     className;
        TriggerDef td;
        Trigger    o;
        Class      cl;

        triggerName = tokenizer.getName();

        checkTriggerExists(triggerName, false);

        isQuoted  = tokenizer.wasQuotedIdentifier();
        isForEach = false;
        isNowait  = false;
        queueSize = TriggerDef.getDefaultQueueSize();
        sWhen     = tokenizer.getString();
        sOper     = tokenizer.getString();

        tokenizer.getThis(Token.T_ON);

        tableName = tokenizer.getString();
        t         = database.getTable(session, tableName);

        checkIsReallyTable(t);

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
        session.setScripting(!t.isTemp());

        // "FOR EACH ROW" or "CALL"
        token = tokenizer.getString();

        if (token.equals(Token.T_FOR)) {
            token = tokenizer.getString();

            if (token.equals(Token.T_EACH)) {
                token = tokenizer.getString();

                if (token.equals(Token.T_ROW)) {
                    isForEach = true;

                    // should be 'NOWAIT' or 'QUEUE' or 'CALL'
                    token = tokenizer.getString();
                } else {
                    throw Trace.error(Trace.UNEXPECTED_END_OF_COMMAND, token);
                }
            } else {
                throw Trace.error(Trace.UNEXPECTED_END_OF_COMMAND, token);
            }
        }

        if (token.equals(Token.T_NOWAIT)) {
            isNowait = true;

            // should be 'CALL' or 'QUEUE'
            token = tokenizer.getString();
        }

        if (token.equals(Token.T_QUEUE)) {
            queueSize = tokenizer.getInt();    //queueSize = Integer.parseInt(tokenizer.getString());

            // should be 'CALL'
            token = tokenizer.getString();
        }

        if (!token.equals(Token.T_CALL)) {
            throw Trace.error(Trace.UNEXPECTED_END_OF_COMMAND, token);
        }

        // PRE: double quotes have been stripped
        className = tokenizer.getString();

        try {

            // dynamically load class
            cl = classForName(className);

            // dynamically instantiate it
            o = (Trigger) cl.newInstance();

            HsqlName name = database.nameManager.newHsqlName(triggerName,
                isQuoted);

            td = new TriggerDef(name, sWhen, sOper, isForEach, t, o,
                                "\"" + className + "\"", isNowait, queueSize);

            if (td.isValid()) {
                t.addTrigger(td);

                // start the trigger thread
                td.start();
            } else {
                throw Trace.error(Trace.UNEXPECTED_TOKEN,
                                  Trace.CREATE_TRIGGER_COMMAND_1);
            }
        } catch (Exception e) {
            throw Trace.error(Trace.UNKNOWN_FUNCTION,
                              Trace.CREATE_TRIGGER_COMMAND_2, e.getMessage());
        }

// boucherb@users 20021128 - enforce unique trigger names
        database.triggerNameList.addName(triggerName, t.getName(),
                                         Trace.TRIGGER_ALREADY_EXISTS);

// --
    }

    /**
     *  Responsible for handling the creation of table columns during the
     *  process of executing CREATE TABLE DDL statements.
     *
     *  @param  t target table
     *  @return a Column object with indicated attributes
     *  @throws  HsqlException
     */
    private Column processCreateColumn(Table t) throws HsqlException {

        boolean    isIdentity        = false;
        long       identityStart     = database.firstIdentity;
        long       identityIncrement = 1;
        boolean    isPrimaryKey      = false;
        String     columnName;
        boolean    isQuoted;
        String     typeName;
        int        type;
        String     sLen;
        int        length = 0;
        String     sScale;
        int        scale       = 0;
        boolean    isNullable  = true;
        Expression defaultExpr = null;
        String     token       = tokenizer.getString();

        columnName = token;

        Trace.check(!columnName.equals(Table.DEFAULT_PK),
                    Trace.COLUMN_ALREADY_EXISTS, columnName);

        isQuoted = tokenizer.wasQuotedIdentifier();
        typeName = tokenizer.getString();
        type     = Types.getTypeNr(typeName);

        if (typeName.equals(Token.T_IDENTITY)) {
            isIdentity   = true;
            isPrimaryKey = true;
        }

        // fredt - when SET IGNORECASE is in effect, all new VARCHAR columns are defined as VARCHAR_IGNORECASE

        /**
         * @todo fredt - drop support for SET IGNORECASE and replace the
         * type name with a qualifier specifying the case sensitivity of VARCHAR
         */
        if (type == Types.VARCHAR && database.isIgnoreCase()) {
            type = Types.VARCHAR_IGNORECASE;
        }

        token = tokenizer.getString();

        if (type == Types.DOUBLE && token.equals(Token.T_PRECISION)) {
            token = tokenizer.getString();
        }

        // fredt@users 20020130 - patch 491987 by jimbag@users
        sLen = "";

        if (token.equals(Token.T_OPENBRACKET)) {
            while (true) {
                token = tokenizer.getString();

                if (token.equals(Token.T_CLOSEBRACKET)) {
                    break;
                }

                sLen += token;
            }

            token = tokenizer.getString();
        }

        // see if we have a scale specified
        int index;

        if ((index = sLen.indexOf(Token.T_COMMA)) != -1) {
            sScale = sLen.substring(index + 1, sLen.length());
            sLen   = sLen.substring(0, index);

            Trace.check(Types.acceptsScaleCreateParam(type),
                        Trace.UNEXPECTED_TOKEN);

            try {
                scale = Integer.parseInt(sScale.trim());
            } catch (NumberFormatException ne) {
                throw Trace.error(Trace.UNEXPECTED_TOKEN, sLen);
            }
        }

        // convert the length
        if (!org.hsqldb.lib.StringUtil.isEmpty(sLen)) {
            Trace.check(Types.acceptsPrecisionCreateParam(type),
                        Trace.UNEXPECTED_TOKEN);

            try {
                length = Integer.parseInt(sLen.trim());
            } catch (NumberFormatException ne) {
                throw Trace.error(Trace.UNEXPECTED_TOKEN, sLen);
            }
        }

        if (token.equals(Token.T_DEFAULT)) {
            defaultExpr = processCreateDefaultExpression(type, length);
            token       = tokenizer.getString();
        } else if (token.equals(Token.T_GENERATED)) {
            tokenizer.getThis(Token.T_BY);
            tokenizer.getThis(Token.T_DEFAULT);
            tokenizer.getThis(Token.T_AS);
            tokenizer.getThis(Token.T_IDENTITY);

            if (tokenizer.isGetThis(Token.T_OPENBRACKET)) {
                tokenizer.getThis(Token.T_START);
                tokenizer.getThis(Token.T_WITH);

                try {
                    identityStart = tokenizer.getBigint();
                } catch (NumberFormatException ne) {
                    throw Trace.error(Trace.UNEXPECTED_TOKEN, sLen);
                }

                if (tokenizer.isGetThis(Token.T_COMMA)) {
                    tokenizer.getThis(Token.T_INCREMENT);
                    tokenizer.getThis(Token.T_BY);

                    identityIncrement = tokenizer.getBigint();
                }

                tokenizer.getThis(Token.T_CLOSEBRACKET);
            }

            isIdentity   = true;
            isPrimaryKey = true;
            token        = tokenizer.getString();
        }

        // fredt@users - accept IDENTITY before or after NOT NULL
        if (token.equals(Token.T_IDENTITY)) {
            isIdentity   = true;
            isPrimaryKey = true;
            token        = tokenizer.getString();
        }

        if (token.equals(Token.T_NULL)) {
            token = tokenizer.getString();
        } else if (token.equals(Token.T_NOT)) {
            tokenizer.getThis(Token.T_NULL);

            isNullable = false;
            token      = tokenizer.getString();
        }

        if (token.equals(Token.T_IDENTITY)) {
            if (isIdentity) {
                throw Trace.error(Trace.SECOND_PRIMARY_KEY, Token.T_IDENTITY);
            }

            isIdentity   = true;
            isPrimaryKey = true;
            token        = tokenizer.getString();
        }

        if (token.equals(Token.T_PRIMARY)) {
            tokenizer.getThis(Token.T_KEY);

            isPrimaryKey = true;
        } else {
            tokenizer.back();
        }

        // make sure IDENTITY and DEFAULT are not used together
        if (isIdentity && defaultExpr != null) {
            throw Trace.error(Trace.UNEXPECTED_TOKEN, Token.T_DEFAULT);
        }

        return new Column(
            database.nameManager.newHsqlName(columnName, isQuoted),
            isNullable, type, length, scale, isIdentity, identityStart,
            identityIncrement, isPrimaryKey, defaultExpr);
    }

    /**
     * @param type data type of column
     * @param length maximum length of column
     * @throws HsqlException
     * @return new Expression
     */
    private Expression processCreateDefaultExpression(int type,
            int length) throws HsqlException {

        if (type == Types.OTHER) {
            throw Trace.error(Trace.WRONG_DEFAULT_CLAUSE);
        }

        Parser     parser = new Parser(session, database, tokenizer);
        Expression expr   = parser.readDefaultClause(type);

        expr.resolveTypes();

        if (expr.getType() == Expression.VALUE
                || (expr.getType() == Expression.FUNCTION
                    && expr.function.isSimple)) {
            Object defValTemp;

            try {
                defValTemp = expr.getValue(session, type);
            } catch (HsqlException e) {
                throw Trace.error(Trace.WRONG_DEFAULT_CLAUSE);
            }

            if (defValTemp != null
                    && (database.sqlEnforceSize
                        || database.sqlEnforceStrictSize)) {
                Object defValTest = Table.enforceSize(defValTemp, type,
                                                      length, false, false);

                if (!defValTemp.equals(defValTest)) {

                    // default value is too long for fixed size column
                    throw Trace.error(Trace.WRONG_DEFAULT_CLAUSE);
                }
            }

            return expr;
        }

        throw Trace.error(Trace.WRONG_DEFAULT_CLAUSE);
    }

    public static void checkBooleanDefault(String s,
                                           int type) throws HsqlException {

        if (type != Types.BOOLEAN || s == null) {
            return;
        }

        s = s.toUpperCase();

        if (s.equals(Token.T_TRUE) || s.equals(Token.T_FALSE)) {
            return;
        }

        if (s.equals("0") || s.equals("1")) {
            return;
        }

        throw Trace.error(Trace.WRONG_DEFAULT_CLAUSE, s);
    }

    /**
     * Responsible for handling constraints section of CREATE TABLE ...
     *
     * @param t table
     * @param constraint CONSTRAINT keyword used
     * @param primarykeycolumn primary columns
     * @throws HsqlException
     * @return list of constraints
     */
    private HsqlArrayList processCreateConstraints(Table t,
            boolean constraint, int[] primarykeycolumn) throws HsqlException {

        String        token;
        HsqlArrayList tcList;
        Constraint    tempConst;
        HsqlName      pkHsqlName;

// fredt@users 20020225 - comment
// HSQLDB relies on primary index to be the first one defined
// and needs original or system added primary key before any
// non-unique index is created
        tcList = new HsqlArrayList();
        tempConst = new Constraint(null, primarykeycolumn, null, null,
                                   Constraint.MAIN, Constraint.NO_ACTION,
                                   Constraint.NO_ACTION);

// tony_lai@users 20020820 - patch 595099
        pkHsqlName = null;

        tcList.add(tempConst);

        if (!constraint) {
            return tcList;
        }

        while (true) {
            HsqlName cname = null;

            if (tokenizer.isGetThis(Token.T_CONSTRAINT)) {
                token = tokenizer.getName();
                cname = database.nameManager.newHsqlName(token,
                        tokenizer.wasQuotedIdentifier());
            }

            token = tokenizer.getString();

            switch (Token.get(token)) {

                case Token.PRIMARY : {
                    tokenizer.getThis(Token.T_KEY);

                    // tony_lai@users 20020820 - patch 595099
                    pkHsqlName = cname;

                    int        col[] = processColumnList(t);
                    Constraint mainConst;

                    mainConst = (Constraint) tcList.get(0);

                    if (mainConst.core.mainColArray != null) {
                        if (!ArrayUtil.areEqual(mainConst.core.mainColArray,
                                                col, col.length, true)) {
                            throw Trace.error(Trace.SECOND_PRIMARY_KEY);
                        }
                    }

                    mainConst.core.mainColArray = col;
                    mainConst.constName         = pkHsqlName;

                    break;
                }
                case Token.UNIQUE : {
                    int col[] = processColumnList(t);

                    if (cname == null) {
                        cname = database.nameManager.newAutoName("CT");
                    }

                    tempConst = new Constraint(cname, col, null, null,
                                               Constraint.UNIQUE,
                                               Constraint.NO_ACTION,
                                               Constraint.NO_ACTION);

                    tcList.add(tempConst);

                    break;
                }
                case Token.FOREIGN : {
                    tokenizer.getThis(Token.T_KEY);

                    tempConst = processCreateFK(t, cname);

                    if (tempConst.core.refColArray == null) {
                        Constraint mainConst = (Constraint) tcList.get(0);

                        tempConst.core.refColArray =
                            mainConst.core.mainColArray;

                        if (tempConst.core.refColArray == null) {
                            throw Trace.error(Trace.INDEX_NOT_FOUND,
                                              Trace.TABLE_HAS_NO_PRIMARY_KEY);
                        }
                    }

                    checkFKColumnDefaults(t, tempConst);
                    t.checkColumnsMatch(tempConst.core.mainColArray,
                                        tempConst.core.refTable,
                                        tempConst.core.refColArray);
                    tcList.add(tempConst);

                    break;
                }
                case Token.CHECK : {
                    if (cname == null) {
                        cname = database.nameManager.newAutoName("CT");
                    }

                    tempConst = new Constraint(cname, null, null, null,
                                               Constraint.CHECK, 0, 0);

                    processCreateCheckConstraintCondition(tempConst);
                    tcList.add(tempConst);

                    break;
                }
            }

            token = tokenizer.getString();

            if (token.equals(Token.T_COMMA)) {
                continue;
            }

            if (token.equals(Token.T_CLOSEBRACKET)) {
                break;
            }

            throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
        }

        return tcList;
    }

    /**
     * Responsible for handling check constraints section of CREATE TABLE ...
     *
     * @param c check constraint
     * @throws HsqlException
     */
    private void processCreateCheckConstraintCondition(Constraint c)
    throws HsqlException {

        tokenizer.getThis(Token.T_OPENBRACKET);

        Parser     parser    = new Parser(session, database, tokenizer);
        Expression condition = parser.parseExpression();

        tokenizer.getThis(Token.T_CLOSEBRACKET);

        c.core.check = condition;
    }

    /**
     * Responsible for handling the execution CREATE TABLE SQL statements.
     *
     * @param type Description of the Parameter
     * @throws HsqlException
     */
    private void processCreateTable(int type) throws HsqlException {

        Table         t;
        String        token;
        boolean       isnamequoted;
        int[]         pkCols;
        int           colIndex;
        boolean       constraint;
        HsqlArrayList tempConstraints;

        token = tokenizer.getName();

        checkTableExists(token, false);

        isnamequoted = tokenizer.wasQuotedIdentifier();
        t            = newTable(type, token, isnamequoted);

        tokenizer.getThis(Token.T_OPENBRACKET);

        pkCols     = null;
        colIndex   = 0;
        constraint = false;

        while (true) {
            token        = tokenizer.getString();
            isnamequoted = tokenizer.wasQuotedIdentifier();

            // fredt@users 20020225 - comment
            // we can check here for reserved words used with
            // quotes as column names
            switch (Token.get(token)) {

                case Token.CONSTRAINT :
                case Token.PRIMARY :
                case Token.FOREIGN :
                case Token.UNIQUE :
                case Token.CHECK :
                    constraint = true;
            }

            tokenizer.back();

            if (constraint) {
                break;
            }

            Column newcolumn = processCreateColumn(t);

            t.addColumn(newcolumn);

            if (newcolumn.isPrimaryKey()) {
                Trace.check(pkCols == null, Trace.SECOND_PRIMARY_KEY,
                            newcolumn.columnName.name);

                pkCols = new int[]{ colIndex };
            }

            token = tokenizer.getString();

            if (token.equals(Token.T_COMMA)) {
                colIndex++;

                continue;
            }

            if (token.equals(Token.T_CLOSEBRACKET)) {
                break;
            }

            throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
        }

        tempConstraints = processCreateConstraints(t, constraint, pkCols);

        try {
            session.commit();

            Constraint primaryConst = (Constraint) tempConstraints.get(0);

            if (primaryConst.constName != null) {
                database.indexNameList.addName(primaryConst.constName.name,
                                               t.getName(),
                                               Trace.INDEX_ALREADY_EXISTS);
                database.constraintNameList.addName(
                    primaryConst.constName.name, t.getName(),
                    Trace.CONSTRAINT_ALREADY_EXISTS);
            }

            t.createPrimaryKey(primaryConst.constName,
                               primaryConst.core.mainColArray, true);

            for (int i = 1; i < tempConstraints.size(); i++) {
                Constraint tempConst = (Constraint) tempConstraints.get(i);

                if (tempConst.constType == Constraint.UNIQUE) {
                    TableWorks tableWorks = new TableWorks(session, t);

                    tableWorks.createUniqueConstraint(
                        tempConst.core.mainColArray, tempConst.constName);

                    t = tableWorks.getTable();
                }

                if (tempConst.constType == Constraint.FOREIGN_KEY) {
                    TableWorks tableWorks = new TableWorks(session, t);

                    tableWorks.createForeignKey(tempConst.core.mainColArray,
                                                tempConst.core.refColArray,
                                                tempConst.constName,
                                                tempConst.core.refTable,
                                                tempConst.core.deleteAction,
                                                tempConst.core.updateAction);

                    t = tableWorks.getTable();
                }

                if (tempConst.constType == Constraint.CHECK) {
                    TableWorks tableWorks = new TableWorks(session, t);

                    tableWorks.createCheckConstraint(tempConst,
                                                     tempConst.constName);

                    t = tableWorks.getTable();
                }
            }

            database.linkTable(t);
        } catch (HsqlException e) {

// fredt@users 20020225 - comment
// if a HsqlException is thrown while creating table, any foreign key that has
// been created leaves it modification to the expTable in place
// need to undo those modifications. This should not happen in practice.
            database.removeExportedKeys(t);
            database.indexNameList.removeOwner(t.tableName);
            database.constraintNameList.removeOwner(t.tableName);

            throw e;
        }
    }

    /**
     * @param t table
     * @param cname foreign key name
     * @throws HsqlException
     * @return constraint
     */
    private Constraint processCreateFK(Table t,
                                       HsqlName cname) throws HsqlException {

        int[]  localcol;
        int[]  expcol;
        String expTableName;
        Table  expTable;
        String token;

        localcol = processColumnList(t);

        tokenizer.getThis(Token.T_REFERENCES);

        expTableName = tokenizer.getString();

// fredt@users 20020221 - patch 520213 by boucherb@users - self reference FK
// allows foreign keys that reference a column in the same table
        if (t.equals(expTableName)) {
            expTable = t;
        } else {
            expTable = database.getTable(session, expTableName);
        }

        expcol = null;
        token  = tokenizer.getString();

        tokenizer.back();

// fredt@users 20020503 - patch 1.7.0 by fredt -  FOREIGN KEY on table
        if (token.equals(Token.T_OPENBRACKET)) {
            expcol = processColumnList(expTable);
        } else {

            // the exp table must have a user defined primary key
            if (!expTable.hasPrimaryKey()) {
                throw Trace.error(Trace.CONSTRAINT_NOT_FOUND,
                                  Trace.TABLE_HAS_NO_PRIMARY_KEY,
                                  new Object[]{ expTableName });
            }

            expcol = expTable.getPrimaryKey();

            // with CREATE TABLE, (expIndex == null) when self referencing FK
            // is declared in CREATE TABLE
            // null will be returned for expCol and will be checked
            // in caller method
            // with ALTER TABLE, (expIndex == null) when table has no PK
        }

        token = tokenizer.getString();

        // -- In a while loop we parse a maximium of two
        // -- "ON" statements following the foreign key
        // -- definition this can be
        // -- ON [UPDATE|DELETE] [NO ACTION|RESTRICT|CASCADE|SET [NULL|DEFAULT]]
        int deleteAction = Constraint.NO_ACTION;
        int updateAction = Constraint.NO_ACTION;

        while (token.equals(Token.T_ON)) {
            token = tokenizer.getString();

            if (deleteAction == Constraint.NO_ACTION
                    && token.equals(Token.T_DELETE)) {
                token = tokenizer.getString();

                if (token.equals(Token.T_SET)) {
                    token = tokenizer.getString();

                    if (token.equals(Token.T_DEFAULT)) {
                        deleteAction = Constraint.SET_DEFAULT;
                    } else if (token.equals(Token.T_NULL)) {
                        deleteAction = Constraint.SET_NULL;
                    } else {
                        throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
                    }
                } else if (token.equals(Token.T_CASCADE)) {
                    deleteAction = Constraint.CASCADE;
                } else if (token.equals(Token.T_RESTRICT)) {

                    // LEGACY compatibility/usability
                    // - same as NO ACTION or nothing at all
                } else {
                    tokenizer.getCurrentThis(Token.T_NO);
                    tokenizer.getThis(Token.T_ACTION);
                }
            } else if (updateAction == Constraint.NO_ACTION
                       && token.equals(Token.T_UPDATE)) {
                token = tokenizer.getString();

                if (token.equals(Token.T_SET)) {
                    token = tokenizer.getString();

                    if (token.equals(Token.T_DEFAULT)) {
                        updateAction = Constraint.SET_DEFAULT;
                    } else if (token.equals(Token.T_NULL)) {
                        updateAction = Constraint.SET_NULL;
                    } else {
                        throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
                    }
                } else if (token.equals(Token.T_CASCADE)) {
                    updateAction = Constraint.CASCADE;
                } else if (token.equals(Token.T_RESTRICT)) {

                    // LEGACY compatibility/usability
                    // - same as NO ACTION or nothing at all
                } else {
                    tokenizer.getCurrentThis(Token.T_NO);
                    tokenizer.getThis(Token.T_ACTION);
                }
            } else {
                throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
            }

            token = tokenizer.getString();
        }

        tokenizer.back();

        if (cname == null) {
            cname = database.nameManager.newAutoName("FK");
        }

        return new Constraint(cname, localcol, expTable, expcol,
                              Constraint.FOREIGN_KEY, deleteAction,
                              updateAction);
    }

    /**
     * Responsible for handling the execution CREATE VIEW SQL statements.
     *
     * @throws HsqlException
     */
    private void processCreateView() throws HsqlException {

        String token       = tokenizer.getName();
        int    logposition = tokenizer.getPartMarker();

        checkViewExists(token, false);

        HsqlName viewHsqlName = database.nameManager.newHsqlName(token,
            tokenizer.wasQuotedIdentifier());
        HsqlName[] colList = null;

        if (tokenizer.isGetThis(Token.T_OPENBRACKET)) {
            HsqlArrayList list = Parser.getColumnNames(database, tokenizer,
                true);

            colList = new HsqlName[list.size()];
            colList = (HsqlName[]) list.toArray(colList);
        }

        tokenizer.getThis(Token.T_AS);
        tokenizer.setPartMarker();

        Parser parser   = new Parser(session, database, tokenizer);
        int    brackets = 0;

        if (tokenizer.isGetThis(Token.T_OPENBRACKET)) {
            brackets += Parser.parseOpenBrackets(tokenizer) + 1;
        }

        tokenizer.getThis(Token.T_SELECT);

        Select select;

        // do not accept LIMIT and ORDER BY - accept unions
        select = parser.parseSelect(brackets, false, true);

        if (select.sIntoTable != null) {
            throw (Trace.error(Trace.TABLE_NOT_FOUND));
        }

        select.prepareResult();

        View view = new View(database, viewHsqlName, tokenizer.getLastPart(),
                             colList);

        session.commit();
        database.linkTable(view);
        tokenizer.setPartMarker(logposition);
    }

    /**
     * Responsible for handling tail of ALTER TABLE ... RENAME ...
     * @param t table
     * @throws HsqlException
     */
    private void processAlterTableRename(Table t) throws HsqlException {

        String  tableName;
        String  newName;
        boolean isquoted;

        tableName = t.getName().name;

        // ensures that if temp table, it also belongs to this session
        if (!t.equals(session, tableName)) {
            throw Trace.error(Trace.TABLE_NOT_FOUND);
        }

        tokenizer.getThis(Token.T_TO);

        newName  = tokenizer.getName();
        isquoted = tokenizer.wasQuotedIdentifier();

        checkTableExists(newName, false);
        session.commit();
        session.setScripting(!t.isTemp());
        t.renameTable(newName, isquoted);
    }

    /**
     * Handles ALTER TABLE statements. <p>
     *
     * ALTER TABLE <name> RENAME TO <newname>
     * ALTER INDEX <name> RENAME TO <newname>
     *
     * ALTER TABLE <name> ADD CONSTRAINT <constname> FOREIGN KEY (<col>, ...)
     * REFERENCE <other table> (<col>, ...) [ON DELETE CASCADE]
     *
     * ALTER TABLE <name> ADD CONSTRAINT <constname> UNIQUE (<col>, ...)
     *
     * @throws HsqlException
     */
    private void processAlter() throws HsqlException {

        String token;

        session.checkDDLWrite();
        session.checkAdmin();
        session.setScripting(true);

        token = tokenizer.getString();

        switch (Token.get(token)) {

            case Token.INDEX : {
                processAlterIndex();

                break;
            }
            case Token.SEQUENCE : {
                processAlterSequence();

                break;
            }
            case Token.TABLE : {
                processAlterTable();

                break;
            }
            case Token.USER : {
                processAlterUser();

                break;
            }
            default : {
                throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
            }
        }
    }

    /**
     * Handles ALTER TABLE DDL.
     *
     * @throws HsqlException
     */
    private void processAlterTable() throws HsqlException {

        String tableName = tokenizer.getString();
        Table  t         = database.getUserTable(session, tableName);
        String token;

        checkIsReallyTable(t);
        session.setScripting(!t.isTemp());

        token = tokenizer.getString();

        switch (Token.get(token)) {

            case Token.RENAME : {
                processAlterTableRename(t);

                return;
            }
            case Token.ADD : {
                HsqlName cname = null;

                if (tokenizer.isGetThis(Token.T_CONSTRAINT)) {
                    token = tokenizer.getName();
                    cname = database.nameManager.newHsqlName(token,
                            tokenizer.wasQuotedIdentifier());
                }

                token = tokenizer.getString();

                switch (Token.get(token)) {

                    case Token.FOREIGN :
                        tokenizer.getThis(Token.T_KEY);
                        processAlterTableAddForeignKeyConstraint(t, cname);

                        return;

                    case Token.UNIQUE :
                        processAlterTableAddUniqueConstraint(t, cname);

                        return;

                    case Token.CHECK :
                        processAlterTableAddCheckConstraint(t, cname);

                        return;

                    default :
                        if (cname != null) {
                            throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
                        }

                        tokenizer.back();
                    case Token.COLUMN :
                        processAlterTableAddColumn(t);

                        return;
                }
            }
            case Token.DROP : {
                token = tokenizer.getString();

                switch (Token.get(token)) {

                    case Token.CONSTRAINT :
                        processAlterTableDropConstraint(t);

                        return;

                    default :
                        tokenizer.back();
                    case Token.COLUMN :
                        processAlterTableDropColumn(t);

                        return;
                }
            }
            case Token.ALTER : {
                tokenizer.getThis(Token.T_COLUMN);
                processAlterColumn(t);

                return;
            }
            default : {
                throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
            }
        }
    }

    /**
     * Handles ALTER COLUMN
     *
     * @param t table
     * @throws HsqlException
     */
    private void processAlterColumn(Table t) throws HsqlException {

        String columnName  = tokenizer.getString();
        int    columnIndex = t.getColumnNr(columnName);
        Column column      = t.getColumn(columnIndex);
        String token       = tokenizer.getString();

        switch (Token.get(token)) {

            case Token.RENAME : {
                tokenizer.getThis(Token.T_TO);
                processAlterColumnRename(t, column);

                return;
            }
            case Token.DROP : {
                tokenizer.getThis(Token.T_DEFAULT);
                t.setDefaultExpression(columnIndex, null);

                return;
            }
            case Token.SET : {
                tokenizer.getThis(Token.T_DEFAULT);

                int iType = column.getType();
                int iLen  = column.getSize();

                t.setDefaultExpression(columnIndex,
                                       processCreateDefaultExpression(iType,
                                           iLen));

                return;
            }
            default : {
                throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
            }
        }
    }

    /**
     * Responsible for handling tail of ALTER COLUMN ... RENAME ...
     * @param t table
     * @param column column
     * @throws HsqlException
     */
    private void processAlterColumnRename(Table t,
                                          Column column)
                                          throws HsqlException {

        String  newName  = tokenizer.getName();
        boolean isquoted = tokenizer.wasQuotedIdentifier();

        if (t.searchColumn(newName) > -1) {
            throw Trace.error(Trace.COLUMN_ALREADY_EXISTS, newName);
        }

        session.commit();
        session.setScripting(!t.isTemp());
        t.renameColumn(column, newName, isquoted);
    }

    /**
     * Handles ALTER INDEX.
     *
     * @throws HsqlException
     */
    private void processAlterIndex() throws HsqlException {

        // only the one supported operation, so far
        processAlterIndexRename();
    }

    /**
     * Responsible for handling parse and execute of SQL DROP DDL
     *
     * @throws  HsqlException
     */
    private void processDrop() throws HsqlException {

        String  token;
        boolean isview;

        session.checkReadWrite();
        session.checkAdmin();
        session.setScripting(true);

        token  = tokenizer.getString();
        isview = false;

        switch (Token.get(token)) {

            case Token.INDEX : {
                processDropIndex();

                break;
            }
            case Token.SEQUENCE : {
                processDropSequence();

                break;
            }
            case Token.TRIGGER : {
                processDropTrigger();

                break;
            }
            case Token.USER : {
                processDropUser();

                break;
            }
            case Token.VIEW : {
                isview = true;
            }    //fall thru
            case Token.TABLE : {
                processDropTable(isview);

                break;
            }
            default : {
                throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
            }
        }
    }

    /**
     *  Responsible for handling the execution of GRANT and REVOKE SQL
     *  statements.
     *
     * @param grant true if grant, false if revoke
     * @throws HsqlException
     */
    private void processGrantOrRevoke(boolean grant) throws HsqlException {

        int    right;
        Object accessKey;
        String token;

        session.checkDDLWrite();
        session.checkAdmin();

        // fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
        session.setScripting(true);

        right = 0;

        do {
            token = tokenizer.getString();
            right |= UserManager.getRight(token);
            token = tokenizer.getString();
        } while (token.equals(Token.T_COMMA));

        if (!token.equals(Token.T_ON)) {
            throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
        }

        accessKey = null;
        token     = tokenizer.getString();

        if (token.equals(Token.T_CLASS)) {
            accessKey = tokenizer.getString();
        } else {

            // fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
            // to make sure the table exists
            Table t = database.getTable(session, token);

            accessKey = t.getName();

            session.setScripting(!t.isTemp());
        }

        tokenizer.getThis(Token.T_TO);

        token = tokenizer.getUserOrPassword();

        UserManager um = database.getUserManager();

        if (grant) {
            um.grant(token, accessKey, right);
        } else {
            um.revoke(token, accessKey, right);
        }
    }

    /**
     * Responsible for handling CONNECT
     *
     * @throws HsqlException
     */
    private void processConnect() throws HsqlException {

        String userName;
        String password;
        User   user;

        tokenizer.getThis(Token.T_USER);

        userName = tokenizer.getUserOrPassword();

        if (tokenizer.isGetThis(Token.T_PASSWORD)) {

            // legacy log statement or connect statement issued by user
            password = tokenizer.getUserOrPassword();
            user     = database.getUserManager().getUser(userName, password);

            session.commit();
            session.setUser(user);
            database.logger.logConnectUser(session);
        } else if (session.getUser().isSys()) {

            // processing log statement
            user = database.getUserManager().get(userName);

            session.commit();
            session.setUser(user);
            database.logger.logConnectUser(session);
        } else {

            // force throw if not log statement
            tokenizer.getThis(Token.T_PASSWORD);
        }
    }

    /**
     * Responsible for handling the execution of SET SQL statements
     *
     * @throws  HsqlException
     */
    private void processSet() throws HsqlException {

        String token;

        // fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
        session.setScripting(true);

        token = tokenizer.getString();

        switch (Token.get(token)) {

            case Token.PROPERTY : {
                HsqlDatabaseProperties p;

                session.checkAdmin();

                token = tokenizer.getString().toLowerCase();

                if (!tokenizer.wasQuotedIdentifier()) {
                    throw Trace.error(Trace.QUOTED_IDENTIFIER_REQUIRED);
                }

                p = database.getProperties();

                Trace.check(p.isSetPropertyAllowed(token),
                            Trace.ACCESS_IS_DENIED, token);

                boolean isboolean  = p.isBoolean(token);
                boolean isintegral = p.isIntegral(token);

                Trace.check(isboolean || isintegral, Trace.ACCESS_IS_DENIED,
                            token);

                Object value = tokenizer.getInType(isboolean ? Types.BOOLEAN
                                                             : Types.INTEGER);

                p.setProperty(token, value.toString().toLowerCase());

                token = tokenizer.getString();

                break;
            }
            case Token.PASSWORD : {
                session.checkDDLWrite();
                session.setPassword(tokenizer.getUserOrPassword());

                break;
            }
            case Token.READONLY : {
                session.commit();
                session.setReadOnly(processTrueOrFalse());

                break;
            }
            case Token.LOGSIZE : {
                session.checkAdmin();
                session.checkDDLWrite();

                int i = tokenizer.getInt();        // Integer.parseInt(tokenizer.getString());

                database.logger.setLogSize(i);

                break;
            }
            case Token.SCRIPTFORMAT : {
                session.checkAdmin();
                session.checkDDLWrite();
                session.setScripting(false);

                token = tokenizer.getString();

                int i = ArrayUtil.find(ScriptWriterBase.LIST_SCRIPT_FORMATS,
                                       token);

                if (i == 0 || i == 1 || i == 3) {
                    database.logger.setScriptType(i);
                } else {
                    throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
                }

                break;
            }
            case Token.IGNORECASE : {
                session.checkAdmin();
                session.checkDDLWrite();
                database.setIgnoreCase(processTrueOrFalse());

                break;
            }
            case Token.MAXROWS : {
                session.setScripting(false);

                int i = tokenizer.getInt();        // Integer.parseInt(tokenizer.getString());

                session.setSQLMaxRows(i);

                break;
            }
            case Token.AUTOCOMMIT : {
                session.setAutoCommit(processTrueOrFalse());

                break;
            }
            case Token.TABLE : {

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
// support for SET TABLE <table> READONLY [TRUE|FALSE]
// sqlbob@users 20020427 support for SET TABLE <table> SOURCE "spec" [DESC]
                session.checkDDLWrite();

                Table t = database.getTable(session, tokenizer.getString());

                token = tokenizer.getString();

                session.setScripting(!t.isTemp());

                switch (Token.get(token)) {

                    default : {
                        throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
                    }
                    case Token.SOURCE : {
                        if (!t.isTemp()) {
                            session.checkAdmin();
                        }

                        token = tokenizer.getString();

                        if (!tokenizer.wasQuotedIdentifier()) {
                            throw Trace.error(Trace.TEXT_TABLE_SOURCE);
                        }

                        boolean isDesc = false;

                        if (tokenizer.getString().equals(Token.T_DESC)) {
                            isDesc = true;
                        } else {
                            tokenizer.back();
                        }

                        t.setDataSource(session, token, isDesc, false);

                        break;
                    }
                    case Token.READONLY : {
                        session.checkAdmin();
                        t.setDataReadOnly(processTrueOrFalse());

                        break;
                    }
                    case Token.INDEX : {
                        session.checkAdmin();
                        tokenizer.getString();
                        t.setIndexRoots((String) tokenizer.getAsValue());

                        break;
                    }
                }

                break;
            }
            case Token.REFERENTIAL_INTEGRITY : {
                session.checkAdmin();
                session.checkDDLWrite();
                session.setScripting(false);
                database.setReferentialIntegrity(processTrueOrFalse());

                break;
            }
            case Token.WRITE_DELAY : {
                session.checkAdmin();
                session.checkDDLWrite();

                int    delay = 0;
                String s     = tokenizer.getString();

                if (s.equals(Token.T_TRUE)) {
                    delay = 60;
                } else if (s.equals(Token.T_FALSE)) {
                    delay = 0;
                } else {
                    tokenizer.back();

                    delay = tokenizer.getInt();    // Integer.parseInt(s);
                }

                database.logger.setWriteDelay(delay);

                break;
            }
            default : {
                throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
            }
        }
    }

    /**
     * Retrieves boolean value corresponding to the next token.
     *
     * @return   true if next token is "TRUE"; false if next token is "FALSE"
     * @throws  HsqlException if the next token is neither "TRUE" or "FALSE"
     */
    private boolean processTrueOrFalse() throws HsqlException {

        String sToken = tokenizer.getString();

        if (sToken.equals(Token.T_TRUE)) {
            return true;
        } else if (sToken.equals(Token.T_FALSE)) {
            return false;
        } else {
            throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
        }
    }

    /**
     * Responsible for  handling the execution of COMMIT [WORK]
     *
     * @throws  HsqlException
     */
    private void processCommit() throws HsqlException {

        if (!tokenizer.getString().equals(Token.T_WORK)) {
            tokenizer.back();
        }

        session.commit();
    }

    /**
     * Responsible for handling the execution of ROLLBACK SQL statements.
     *
     * @throws  HsqlException
     */
    private void processRollback() throws HsqlException {

        String  token;
        boolean toSavepoint;

        token       = tokenizer.getString();
        toSavepoint = false;

        if (token.equals(Token.T_WORK)) {

            // do nothing
        } else if (token.equals(Token.T_TO)) {
            tokenizer.getThis(Token.T_SAVEPOINT);

            token       = tokenizer.getString();
            toSavepoint = true;
        } else {
            tokenizer.back();
        }

        if (toSavepoint) {
            if (token.length() == 0) {
                throw Trace.error(Trace.UNEXPECTED_TOKEN,
                                  Trace.BAD_SAVEPOINT_NAME);
            }

            session.rollbackToSavepoint(token);
        } else {
            session.rollback();
        }
    }

    /**
     * Responsible for handling the execution of SAVEPOINT SQL statements.
     *
     * @throws  HsqlException
     */
    private void processSavepoint() throws HsqlException {

        String token;

        token = tokenizer.getString();

        if (token.length() == 0) {
            throw Trace.error(Trace.UNEXPECTED_TOKEN,
                              Trace.BAD_SAVEPOINT_NAME);
        }

        session.savepoint(token);
    }

    /**
     * Responsible for handling the execution of SHUTDOWN SQL statements
     *
     * @throws  HsqlException
     */
    private void processShutdown() throws HsqlException {

        int    closemode;
        String token;

        // HUH?  We should *NEVER* be able to get here if session is closed
        if (!session.isClosed()) {
            session.checkAdmin();
        }

        closemode = Database.CLOSEMODE_NORMAL;
        token     = tokenizer.getString();

        // fredt - todo - catch misspelt qualifiers here and elsewhere
        if (token.equals(Token.T_IMMEDIATELY)) {
            closemode = Database.CLOSEMODE_IMMEDIATELY;
        } else if (token.equals(Token.T_COMPACT)) {
            closemode = Database.CLOSEMODE_COMPACT;
        } else if (token.equals(Token.T_SCRIPT)) {
            closemode = Database.CLOSEMODE_SCRIPT;
        } else {
            tokenizer.back();
        }

        database.close(closemode);
    }

    /**
     * Responsible for handling CHECKPOINT [DEFRAG].
     *
     * @throws  HsqlException
     */
    private void processCheckpoint() throws HsqlException {

        boolean defrag;
        String  token;

        session.checkAdmin();
        session.checkDDLWrite();

        defrag = false;
        token  = tokenizer.getString();

        // fredt - todo - catch misspelt qualifiers here and elsewhere
        if (token.equals(Token.T_DEFRAG)) {
            defrag = true;
        }

        database.logger.checkpoint(defrag);
    }

// --------------------- new methods / simplifications ------------------------
    private HsqlName newIndexHsqlName(String name,
                                      boolean isQuoted) throws HsqlException {

        return HsqlName.isReservedIndexName(name)
               ? database.nameManager.newAutoName("USER", name)
               : database.nameManager.newHsqlName(name, isQuoted);
    }

    private Table newTable(int type, String name,
                           boolean quoted) throws HsqlException {

        HsqlName tableHsqlName;
        int      sid;

        tableHsqlName = database.nameManager.newHsqlName(name, quoted);
        sid           = session.getId();

        switch (type) {

            case Table.TEMP_TEXT_TABLE :
            case Table.TEXT_TABLE : {
                return new TextTable(database, tableHsqlName, type, sid);
            }
            default : {
                return new Table(database, tableHsqlName, type, sid);
            }
        }
    }

    /**
     * Checks if an Index object with given name either exists or does not,
     * based on the value of the argument, yes.
     *
     * @param indexName to check
     * @param yes if true, check if exists, else check not exists
     * @throws HsqlException if existence of Index does not match value of
     *      the argument, yes.
     */
    private void checkIndexExists(String indexName,
                                  boolean yes) throws HsqlException {

        boolean exists;
        int     code;

        exists = database.findUserTableForIndex(session, indexName) != null;

        if (exists != yes) {
            code = yes ? Trace.INDEX_NOT_FOUND
                       : Trace.INDEX_ALREADY_EXISTS;

            throw Trace.error(code, indexName);
        }
    }

    /**
     * Checks if a Table object with given name either exists or does not,
     * based on the value of the argument, yes.
     *
     * @param tableName to check
     * @param yes if true, check if exists, else check if not exists
     * @throws HsqlException if existence of table does not match value of
     *      the argument, yes.
     */
    private void checkTableExists(String tableName,
                                  boolean yes) throws HsqlException {

        boolean exists;
        int     code;

        exists = database.dInfo.isSystemTable(tableName);

        if (!exists) {
            exists = database.findUserTable(session, tableName) != null;
        }

        if (exists != yes) {
            code = yes ? Trace.TABLE_NOT_FOUND
                       : Trace.TABLE_ALREADY_EXISTS;

            throw Trace.error(code, tableName);
        }
    }

    /**
     * Checks if a View object with given name either exists or does not,
     * based on the value of the argument, yes.
     *
     * @param viewName to check
     * @param yes if true, check if exists, else check not exists
     * @throws HsqlException if existence of View does not match value of
     *      the argument, yes.
     */
    private void checkViewExists(String viewName,
                                 boolean yes) throws HsqlException {

        Table   t;
        boolean exists;
        boolean isView;
        int     code;

        t      = database.findUserTable(session, viewName);
        exists = (t != null);
        isView = exists && t.isView();

        if (!exists) {
            exists = database.dInfo.isSystemTable(viewName);
        }

        if (exists != yes) {
            if (exists) {
                code = isView ? Trace.VIEW_ALREADY_EXISTS
                              : Trace.TABLE_ALREADY_EXISTS;
            } else {
                code = Trace.VIEW_NOT_FOUND;
            }

            throw Trace.error(code, viewName);
        }
    }

    private void checkIsReallyTable(Table t) throws HsqlException {

        if (t.isView() || t.getTableType() == Table.SYSTEM_TABLE) {
            throw Trace.error(Trace.NOT_A_TABLE);
        }
    }

    /**
     * Checks if a Trigger with given name either exists or does not, based on
     * the value of the argument, yes.
     *
     * @param triggerName to check
     * @param yes if true, check if exists, else check not exists
     * @throws HsqlException if existence of trigger does not match value of
     *      the argument, yes.
     */
    private void checkTriggerExists(String triggerName,
                                    boolean yes) throws HsqlException {

        boolean exists = database.triggerNameList.containsName(triggerName);

        if (exists != yes) {
            int code = yes ? Trace.TRIGGER_NOT_FOUND
                           : Trace.TRIGGER_ALREADY_EXISTS;

            throw Trace.error(code, triggerName);
        }
    }

    /**
     * Checks if the attributes of the Column argument, c, are compatible with
     * the operation of adding such a Column to the Table argument, t.
     *
     * @param t to which to add the Column, c
     * @param c the Column to add to the Table, t
     * @throws HsqlException if the operation of adding the Column, c, to
     *      the table t is not valid
     */
    private void checkAddColumn(Table t, Column c) throws HsqlException {

        boolean canAdd = true;

        if (c.isIdentity()) {
            canAdd = false;
        } else if (c.isPrimaryKey()) {
            canAdd = false;
        } else if (!t.isEmpty()) {
            canAdd = c.isNullable() || c.getDefaultExpression() != null;
        }

        if (!canAdd) {
            throw Trace.error(Trace.BAD_ADD_COLUMN_DEFINITION);
        }
    }

    private void checkFKColumnDefaults(Table t,
                                       Constraint tc) throws HsqlException {

        boolean check = tc.core.updateAction == Constraint.SET_DEFAULT;

        check = check || tc.core.deleteAction == Constraint.SET_DEFAULT;

        if (check) {
            int[] localCol = tc.core.mainColArray;

            for (int j = 0; j < localCol.length; j++) {
                Column     column  = t.getColumn(localCol[j]);
                Expression defExpr = column.getDefaultExpression();

                if (defExpr == null) {
                    String columnName = column.columnName.name;

                    throw Trace.error(Trace.COLUMN_TYPE_MISMATCH,
                                      Trace.NO_DEFAULT_VALUE_FOR_COLUMN,
                                      new Object[]{ columnName });
                }
            }
        }
    }

    private void processAlterSequence() throws HsqlException {

        long   start;
        String name = tokenizer.getIdentifier();

        tokenizer.getThis(Token.T_RESTART);
        tokenizer.getThis(Token.T_WITH);

        start = tokenizer.getBigint();

        NumberSequence seq = database.sequenceManager.getSequence(name);

        Trace.check(seq != null, Trace.SEQUENCE_NOT_FOUND);
        seq.reset(start);
    }

    /**
     * Handles ALTER INDEX &lt;index-name&gt; RENAME.
     *
     * @throws HsqlException
     */
    private void processAlterIndexRename() throws HsqlException {

        String  indexName;
        String  newName;
        boolean isQuoted;
        Table   t;

        indexName = tokenizer.getName();

        tokenizer.getThis(Token.T_RENAME);
        tokenizer.getThis(Token.T_TO);

        newName  = tokenizer.getName();
        isQuoted = tokenizer.wasQuotedIdentifier();
        t        = database.findUserTableForIndex(session, indexName);

        if (t == null) {
            throw Trace.error(Trace.INDEX_NOT_FOUND, indexName);
        }

        checkIndexExists(newName, false);

        if (HsqlName.isReservedIndexName(indexName)) {
            throw Trace.error(Trace.SYSTEM_INDEX, indexName);
        }

        if (HsqlName.isReservedIndexName(newName)) {
            throw Trace.error(Trace.BAD_INDEX_CONSTRAINT_NAME, newName);
        }

        session.setScripting(!t.isTemp());
        session.commit();
        t.getIndex(indexName).setName(newName, isQuoted);
        database.indexNameList.rename(indexName, newName,
                                      Trace.INDEX_ALREADY_EXISTS);
    }

    /**
     *
     * @param t table
     * @throws HsqlException
     */
    private void processAlterTableAddColumn(Table t) throws HsqlException {

        String token;
        int    colindex = t.getColumnCount();
        Column column   = processCreateColumn(t);

        checkAddColumn(t, column);

        token = tokenizer.getString();

        if (token.equals(Token.T_BEFORE)) {
            token    = tokenizer.getName();
            colindex = t.getColumnNr(token);
        } else {
            tokenizer.back();
        }

        session.commit();

        TableWorks tableWorks = new TableWorks(session, t);

        tableWorks.addOrDropColumn(column, colindex, 1);

        return;
    }

    /**
     * Responsible for handling tail of ALTER TABLE ... DROP COLUMN ...
     *
     * @param t table
     * @throws HsqlException
     */
    private void processAlterTableDropColumn(Table t) throws HsqlException {

        String token;
        int    colindex;

        token    = tokenizer.getName();
        colindex = t.getColumnNr(token);

        // CHECKME:
        // shouldn't the commit come only *after* the DDL is
        // actually successful?
        // fredt - no, uncommitted data may include the column
        session.commit();

        TableWorks tableWorks = new TableWorks(session, t);

        tableWorks.addOrDropColumn(null, colindex, -1);
    }

    /**
     * Responsible for handling tail of ALTER TABLE ... DROP CONSTRAINT ...
     *
     * @param t table
     * @throws HsqlException
     */
    private void processAlterTableDropConstraint(Table t)
    throws HsqlException {

        String cname;

        cname = tokenizer.getName();

        session.commit();

        TableWorks tableWorks = new TableWorks(session, t);

        tableWorks.dropConstraint(cname);

        return;
    }

    private void processCreateAlias() throws HsqlException {

        String alias;
        String methodFQN;

        alias = tokenizer.getString();

        tokenizer.getThis(Token.T_FOR);

        methodFQN = upgradeMethodFQN(tokenizer.getString());

        database.getAliasMap().put(alias, methodFQN);
    }

    private void processCreateIndex(boolean unique) throws HsqlException {

        String  name;
        boolean isQuoted;
        Table   t;

        name     = tokenizer.getName();
        isQuoted = tokenizer.wasQuotedIdentifier();

        tokenizer.getThis(Token.T_ON);

        t = database.getTable(session, tokenizer.getName());

        addIndexOn(t, name, isQuoted, unique);

        String extra = tokenizer.getString();

        if (!Token.T_DESC.equals(extra) &&!Token.T_ASC.equals(extra)) {
            tokenizer.back();
        }
    }

    /**
     * limitations in Tokenizer dictate that initial value or increment must
     * be positive
     * @throws HsqlException
     */
    private void processCreateSequence() throws HsqlException {

/*
        CREATE SEQUENCE <name>
        [AS {INTEGER | BIGINT}]
        [START WITH <value>]
        [INCREMENT BY <value>]
*/
        int     type      = Types.INTEGER;
        long    increment = 1;
        long    start     = 0;
        String  name      = tokenizer.getIdentifier();
        boolean isquoted  = tokenizer.wasQuotedIdentifier();

        if (tokenizer.isGetThis(Token.T_AS)) {
            String typestring = tokenizer.getString();

            type = Types.getTypeNr(typestring);

            Trace.check(type == Types.INTEGER || type == Types.BIGINT,
                        Trace.WRONG_DATA_TYPE);
        }

        if (tokenizer.isGetThis(Token.T_START)) {
            tokenizer.getThis(Token.T_WITH);

            start = tokenizer.getBigint();
        }

        if (tokenizer.isGetThis(Token.T_INCREMENT)) {
            tokenizer.getThis(Token.T_BY);

            increment = tokenizer.getBigint();
        }

        HsqlName hsqlname = database.nameManager.newHsqlName(name, isquoted);

        database.sequenceManager.createSequence(hsqlname, start, increment,
                type);
    }

    private void processCreateUser() throws HsqlException {

        String  name;
        String  password;
        boolean admin;

        name = tokenizer.getUserOrPassword();

        tokenizer.getThis(Token.T_PASSWORD);

        password = tokenizer.getUserOrPassword();
        admin    = tokenizer.getString().equals(Token.T_ADMIN);

        if (!admin) {
            tokenizer.back();
        }

        database.getUserManager().createUser(name, password, admin);
    }

    private void processDisconnect() throws HsqlException {
        session.close();
    }

    private void processDropTable(boolean isView) throws HsqlException {

        String  tableName;
        String  token;
        boolean ifExists;

        tableName = token = tokenizer.getString();
        ifExists  = false;

        if (token.equals(Token.T_IF)) {
            token = tokenizer.getString();

            if (token.equals(Token.T_EXISTS)) {
                ifExists  = true;
                tableName = tokenizer.getString();
            } else if (token.equals(Token.T_IF)) {
                tokenizer.getThis(Token.T_EXISTS);

                ifExists = true;
            } else {
                tokenizer.back();
            }
        } else {
            token = tokenizer.getString();

            if (token.equals(Token.T_IF)) {
                tokenizer.getThis(Token.T_EXISTS);

                ifExists = true;
            } else {
                tokenizer.back();
            }
        }

        database.dropTable(session, tableName, ifExists, isView);
    }

    private void processDropUser() throws HsqlException {
        session.checkDDLWrite();
        database.getUserManager().dropUser(tokenizer.getUserOrPassword());
    }

    private void processDropSequence() throws HsqlException {

        session.checkDDLWrite();

        String         name     = tokenizer.getString();
        NumberSequence sequence = database.sequenceManager.getSequence(name);

        if (sequence != null) {
            database.checkSequenceIsInView(sequence);
        }

        database.sequenceManager.dropSequence(name);
    }

    private void processDropTrigger() throws HsqlException {
        session.checkDDLWrite();
        database.dropTrigger(session, tokenizer.getString());
    }

    private void processDropIndex() throws HsqlException {

        String  indexName = tokenizer.getString();
        String  token     = tokenizer.getString();
        boolean ifExists  = false;
        String  tableName = null;

        // accept a table name - no check performed if it is the right table
        if (token.equals(Token.T_ON)) {
            tableName = tokenizer.getString();
            token     = tokenizer.getString();
        }

        if (token.equals(Token.T_IF)) {
            tokenizer.getThis(Token.T_EXISTS);

            ifExists = true;
        } else {
            tokenizer.back();
        }

        session.checkDDLWrite();
        database.dropIndex(session, indexName, null, ifExists);
    }

    private Result processExplainPlan() throws IOException, HsqlException {

        // PRE:  we assume only one DML or DQL has been submitted
        //       and simply ignore anything following the first
        //       sucessfully compliled statement
        String            token;
        Parser            parser;
        int               cmd;
        CompiledStatement cs;
        Result            result;
        String            line;
        LineNumberReader  lnr;

        tokenizer.getThis(Token.T_PLAN);
        tokenizer.getThis(Token.T_FOR);

        parser = new Parser(session, database, tokenizer);
        token  = tokenizer.getString();
        cmd    = Token.get(token);
        result = Result.newSingleColumnResult("OPERATION", Types.VARCHAR);

        int brackets = 0;

        switch (cmd) {

            case Token.OPENBRACKET : {
                brackets = Parser.parseOpenBrackets(tokenizer) + 1;

                tokenizer.getThis(Token.T_SELECT);
            }
            case Token.SELECT :
                cs = parser.compileSelectStatement(brackets);
                break;

            case Token.INSERT :
                cs = parser.compileInsertStatement();
                break;

            case Token.UPDATE :
                cs = parser.compileUpdateStatement();
                break;

            case Token.DELETE :
                cs = parser.compileDeleteStatement();
                break;

            case Token.CALL :
                cs = parser.compileCallStatement();
                break;

            default :

                // - No real need to throw, so why bother?
                // - Just return result with no rows for now
                // - Later, maybe there will be plan desciptions
                //   for other operations
                return result;
        }

        lnr = new LineNumberReader(new StringReader(cs.toString()));

        while (null != (line = lnr.readLine())) {
            result.add(new Object[]{ line });
        }

        return result;
    }

// fredt@users 20010701 - patch 1.6.1 by fredt - open <1.60 db files
// convert org.hsql.Library aliases from versions < 1.60 to org.hsqldb
// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP) - ABS function
    static final String oldLib    = "org.hsql.Library.";
    static final int    oldLibLen = oldLib.length();
    static final String newLib    = "org.hsqldb.Library.";

    private static String upgradeMethodFQN(String fqn) {

        if (fqn.startsWith(oldLib)) {
            fqn = newLib + fqn.substring(oldLibLen);
        } else if (fqn.equals("java.lang.Math.abs")) {
            fqn = "org.hsqldb.Library.abs";
        }

        return fqn;
    }

    private Class classForName(String fqn) throws ClassNotFoundException {

        ClassLoader classLoader = database.classLoader;

        return classLoader == null ? Class.forName(fqn)
                                   : classLoader.loadClass(fqn);
    }

    Result processSelectInto(Result result, HsqlName intoHsqlName,
                             int intoType) throws HsqlException {

        int sid;

        // fredt@users 20020215 - patch 497872 by Nitin Chauhan
        // to require column labels in SELECT INTO TABLE
        int colCount = result.getColumnCount();

        for (int i = 0; i < colCount; i++) {
            if (result.metaData.colLabels[i].length() == 0) {
                throw Trace.error(Trace.LABEL_REQUIRED);
            }
        }

        sid = session.getId();

        // fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
        Table t = (intoType == Table.TEXT_TABLE)
                  ? new TextTable(database, intoHsqlName, intoType, sid)
                  : new Table(database, intoHsqlName, intoType, sid);

        t.addColumns(result.metaData, result.getColumnCount());
        t.createPrimaryKey();
        database.linkTable(t);

        // fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
        if (intoType == Table.TEXT_TABLE) {
            try {

                // Use default lowercase name "<table>.csv" (with invalid
                // char's converted to underscores):
                String txtSrc =
                    StringUtil.toLowerSubset(intoHsqlName.name, '_') + ".csv";

                t.setDataSource(session, txtSrc, false, true);
                logTableDDL(t);
                t.insertIntoTable(session, result);
            } catch (HsqlException e) {
                database.dropTable(session, intoHsqlName.name, false, false);

                throw (e);
            }
        } else {
            logTableDDL(t);

            // SELECT .. INTO can't fail because of constraint violation
            t.insertIntoTable(session, result);
        }

        Result uc = new Result(ResultConstants.UPDATECOUNT);

        uc.updateCount = result.getSize();

        return uc;
    }

    /**
     *  Logs the DDL for a table created with INTO.
     *  Uses three dummy arguments for getTableDDL() as the new table has no
     *  FK constraints.
     *
     *
     * @param t table
     * @throws  HsqlException
     */
    private void logTableDDL(Table t) throws HsqlException {

        StringBuffer tableDDL;
        String       sourceDDL;

        if (t.isTemp()) {
            return;
        }

        tableDDL = new StringBuffer();

        DatabaseScript.getTableDDL(database, t, 0, null, null, tableDDL);

        sourceDDL = DatabaseScript.getDataSource(t);

        database.logger.writeToLog(session, tableDDL.toString());

        if (sourceDDL != null) {
            database.logger.writeToLog(session, sourceDDL);
        }
    }

    private void processAlterTableAddUniqueConstraint(Table t,
            HsqlName n) throws HsqlException {

        int col[];

        col = processColumnList(t);

        if (n == null) {
            n = database.nameManager.newAutoName("CT");
        }

        session.commit();

        TableWorks tableWorks = new TableWorks(session, t);

        tableWorks.createUniqueConstraint(col, n);
    }

    private void processAlterTableAddForeignKeyConstraint(Table t,
            HsqlName n) throws HsqlException {

        Constraint tc;

        if (n == null) {
            n = database.nameManager.newAutoName("FK");
        }

        tc = processCreateFK(t, n);

        t.checkColumnsMatch(tc.core.mainColArray, tc.core.refTable,
                            tc.core.refColArray);
        session.commit();

        TableWorks tableWorks = new TableWorks(session, t);

        tableWorks.createForeignKey(tc.core.mainColArray,
                                    tc.core.refColArray, tc.constName,
                                    tc.core.refTable, tc.core.deleteAction,
                                    tc.core.updateAction);
    }

    private void processAlterTableAddCheckConstraint(Table table,
            HsqlName name) throws HsqlException {

        Constraint check;

        if (name == null) {
            name = database.nameManager.newAutoName("CT");
        }

        check = new Constraint(name, null, null, null, Constraint.CHECK, 0,
                               0);

        processCreateCheckConstraintCondition(check);
        session.commit();

        TableWorks tableWorks = new TableWorks(session, table);

        tableWorks.createCheckConstraint(check, name);
    }

    private void processReleaseSavepoint() throws HsqlException {

        String token;

        tokenizer.getThis(Token.T_SAVEPOINT);

        token = tokenizer.getString();

        if (token.length() == 0) {
            throw Trace.error(Trace.UNEXPECTED_TOKEN,
                              Trace.BAD_SAVEPOINT_NAME);
        }

        session.releaseSavepoint(token);
    }

    private void processAlterUser() throws HsqlException {

        String userName;
        String password;
        User   userObject;

        userName = tokenizer.getUserOrPassword();
        userObject =
            (User) database.getUserManager().getUsers().get(userName);

        Trace.check(userObject != null, Trace.USER_NOT_FOUND, userName);
        tokenizer.getThis(Token.T_SET);
        tokenizer.getThis(Token.T_PASSWORD);

        password = tokenizer.getUserOrPassword();

        userObject.setPassword(password);
        database.logger.writeToLog(session, userObject.getAlterUserDDL());
        session.setScripting(false);
    }
}
