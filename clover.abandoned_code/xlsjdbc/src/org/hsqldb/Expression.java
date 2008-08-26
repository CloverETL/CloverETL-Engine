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

import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.store.ValuePool;

// fredt@users 20020215 - patch 1.7.0 by fredt
// to preserve column size etc. when SELECT INTO TABLE is used
// tony_lai@users 20021020 - patch 1.7.2 - improved aggregates and HAVING
// fredt@users 20021112 - patch 1.7.2 by Nitin Chauhan - use of switch
// rewrite of the majority of multiple if(){}else{} chains with switch(){}
// vorburger@users 20021229 - patch 1.7.2 - null handling
// boucherb@users 200307?? - patch 1.7.2 - resolve param nodes
// boucherb@users 200307?? - patch 1.7.2 - compress constant expr during resolve
// boucherb@users 200307?? - patch 1.7.2 - eager pmd and rsmd
// boucherb@users 20031005 - patch 1.7.2 - optimised LIKE
// boucherb@users 20031005 - patch 1.7.2 - improved IN value lists
// fredt@users 20031012 - patch 1.7.2 - better OUTER JOIN implementation

/**@todo fredt - move error string literals to Trace*/

/**
 * Expression class declaration
 *
 * @version    1.7.2
 */
public class Expression {

    // leaf types
    static final int VALUE     = 1,
                     COLUMN    = 2,
                     QUERY     = 3,
                     TRUE      = 4,
                     FALSE     = -4,    // arbitrary
                     VALUELIST = 5,
                     ASTERIX   = 6,
                     FUNCTION  = 7,
                     LIMIT     = 8;

// boucherb@users 20020410 - parametric compiled statements
    // new leaf type
    static final int PARAM = 9;

// --
    // operations
    static final int NEGATE   = 10,
                     ADD      = 11,
                     SUBTRACT = 12,
                     MULTIPLY = 13,
                     DIVIDE   = 14,
                     CONCAT   = 15;

    // logical operations
    static final int NOT           = 20,
                     EQUAL         = 21,
                     BIGGER_EQUAL  = 22,
                     BIGGER        = 23,
                     SMALLER       = 24,
                     SMALLER_EQUAL = 25,
                     NOT_EQUAL     = 26,
                     LIKE          = 27,
                     AND           = 28,
                     OR            = 29,
                     IN            = 30,
                     EXISTS        = 31,
                     IS_NULL       = 32;

    // aggregate functions
    static final int COUNT       = 40,
                     SUM         = 41,
                     MIN         = 42,
                     MAX         = 43,
                     AVG         = 44,
                     EVERY       = 45,
                     SOME        = 46,
                     STDDEV_POP  = 47,
                     STDDEV_SAMP = 48,
                     VAR_POP     = 49,
                     VAR_SAMP    = 50;

    // system functions
    static final int IFNULL      = 60,
                     CONVERT     = 61,
                     CASEWHEN    = 62,
                     EXTRACT     = 63,
                     POSITION    = 64,
                     TRIM        = 65,
                     SUBSTRING   = 66,
                     NULLIF      = 67,
                     CASE        = 68,
                     COALESCE    = 69,
                     ALTERNATIVE = 70,
                     SEQUENCE    = 71;

    // temporary used during parsing
    static final int PLUS         = 100,
                     OPEN         = 101,
                     CLOSE        = 102,
                     SELECT       = 103,
                     COMMA        = 104,
                     STRINGCONCAT = 105,
                     BETWEEN      = 106,
                     CAST         = 107,
                     END          = 108,
                     IS           = 109,
                     WHEN         = 110,
                     THEN         = 111,
                     ELSE         = 112,
                     ENDWHEN      = 113;

    // used inside brackets for system functions
    static final int     AS                      = 122,
                         FOR                     = 123,
                         FROM                    = 124,
                         BOTH                    = 125,
                         LEADING                 = 126,
                         TRAILING                = 127,
                         YEAR                    = 128,
                         MONTH                   = 129,
                         DAY                     = 130,
                         HOUR                    = 131,
                         MINUTE                  = 132,
                         SECOND                  = 133,
                         TIMEZONE_HOUR           = 134,
                         T_TIMEZONE_MINUTE       = 135;
    static final HashSet SQL_EXTRACT_FIELD_NAMES = new HashSet();
    static final HashSet SQL_TRIM_SPECIFICATION  = new HashSet();

    static {
        SQL_EXTRACT_FIELD_NAMES.addAll(new Object[] {
            Token.T_YEAR, Token.T_MONTH, Token.T_DAY, Token.T_HOUR,
            Token.T_MINUTE, Token.T_SECOND, Token.T_TIMEZONE_HOUR,
            Token.T_TIMEZONE_MINUTE
        });
        SQL_TRIM_SPECIFICATION.addAll(new Object[] {
            Token.T_LEADING, Token.T_TRAILING, Token.T_BOTH
        });
    }

    private static final int AGGREGATE_SELF     = -1;
    private static final int AGGREGATE_NONE     = 0;
    private static final int AGGREGATE_LEFT     = 1;
    private static final int AGGREGATE_RIGHT    = 2;
    private static final int AGGREGATE_BOTH     = 3;
    private static final int AGGREGATE_FUNCTION = 4;

    // type
    int         exprType;
    private int aggregateSpec = AGGREGATE_NONE;

    // nodes
    private Expression eArg, eArg2;

    // VALUE, VALUELIST
    Object          valueData;
    private HashSet hList;
    private int     dataType;

    // VALUE LIST NEW
    Expression[]    valueList;
    private boolean isFixedConstantValueList;

    // QUERY - in single value selects, IN or EXISTS predicates
    Select  subSelect;
    boolean isCorrelated;                     // correlated subquery
    Table   subTable;                         // if not correlated

    // FUNCTION
    Function function;

    // LIKE
    private Like likeObject;

    // COLUMN
    private String      catalog;
    private String      schema;
    private String      tableName;
    private String      columnName;
    private TableFilter tableFilter;          // null if not yet resolved
    TableFilter         outerFilter;          // defined if this is part of an OUTER JOIN condition tree

    //
    private int     columnIndex;
    private boolean columnQuoted;
    private int     columnSize;
    private int     columnScale;
    private String  columnAlias;              // if it is a column of a select column list
    private boolean aliasQuoted;

    //
    private boolean isDescending;             // if it is a column in a order by
    int             orderColumnIndex = -1;    // >= 0 when it is used for order by

// rougier@users 20020522 - patch 552830 - COUNT(DISTINCT)
    // {COUNT|SUM|MIN|MAX|AVG}(distinct ...)
    boolean isDistinctAggregate;

    // PARAM
    private boolean isParam;

    // does Expression stem from a JOIN <table> ON <expression>
    boolean isInJoin;

    //
    static final Integer INTEGER_0 = ValuePool.getInt(0);
    static final Integer INTEGER_1 = ValuePool.getInt(1);

    /**
     * Creates a new boolean expression
     * @param b boolean constant
     */
    Expression(boolean b) {
        exprType = b ? TRUE
                     : FALSE;
    }

    /**
     * Creates a new FUNCTION expression
     * @param f function
     */
    Expression(Function f) {

        exprType = FUNCTION;
        function = f;

        if (f.hasAggregate) {
            aggregateSpec = AGGREGATE_FUNCTION;
        }
    }

    /**
     * Creates a new SEQUENCE expression
     * @param sequence number sequence
     */
    Expression(NumberSequence sequence) {

        exprType  = SEQUENCE;
        valueData = sequence;
        dataType  = sequence.getType();
    }

    /**
     * Copy Constructor. Used by TableFilter to move a condition to a filter.
     * @param e source expression
     */
    Expression(Expression e) {

        exprType = e.exprType;
        dataType = e.dataType;
        eArg     = e.eArg;
        eArg2    = e.eArg2;
        isInJoin = e.isInJoin;

        //
        likeObject = e.likeObject;
        subSelect  = e.subSelect;
        function   = e.function;

        checkAggregate();
    }

    /**
     * Creates a new QUERY expression
     * @param s select
     * @param t table
     * @param correlated boolean
     */
    Expression(Select s, Table t, boolean correlated) {

        exprType     = QUERY;
        subSelect    = s;
        subTable     = t;
        isCorrelated = correlated;
    }

    /**
     * Creates a new VALUELIST expression
     * @param valueList array of Expression
     */
    Expression(Expression[] valueList) {
        exprType       = VALUELIST;
        this.valueList = valueList;
    }

    /**
     * Creates a new binary (or unary) operation expression
     *
     * @param type operator type
     * @param e operand 1
     * @param e2 operand 2
     */
    Expression(int type, Expression e, Expression e2) {

        exprType = type;
        eArg     = e;
        eArg2    = e2;

        checkAggregate();
    }

    /**
     * Creates a new LIKE expression
     *
     * @param e operand 1
     * @param e2 operand 2
     * @param escape escape character
     */
    Expression(Expression e, Expression e2, Character escape) {

        exprType   = LIKE;
        eArg       = e;
        eArg2      = e2;
        likeObject = new Like(escape);

        checkAggregate();
    }

    /**
     * Creates a new ASTERIX or COLUMN expression
     * @param table table
     * @param column column
     */
    Expression(String table, String column) {

        tableName = table;

        if (column == null) {
            exprType = ASTERIX;
        } else {
            exprType   = COLUMN;
            columnName = column;
        }
    }

    /**
     * Creates a new ASTERIX or possibly quoted COLUMN expression
     * @param table table
     * @param column column name
     * @param isquoted boolean
     */
    Expression(String table, String column, boolean isquoted) {

        tableName = table;

        if (column == null) {
            exprType = ASTERIX;
        } else {
            exprType     = COLUMN;
            columnName   = column;
            columnQuoted = isquoted;
        }
    }

    Expression(String table, Column column) {

        tableName = table;

        if (column == null) {
            exprType = ASTERIX;
        } else {
            exprType     = COLUMN;
            columnName   = column.columnName.name;
            columnQuoted = column.columnName.isNameQuoted;
            dataType     = column.getType();
        }
    }

    /**
     * Creates a new VALUE expression
     *
     * @param datatype data type
     * @param o data
     */
    Expression(int datatype, Object o) {

        exprType  = VALUE;
        dataType  = datatype;
        valueData = o;
    }

    /**
     * Creates a new (possibly PARAM) VALUE expression
     *
     * @param datatype initial datatype
     * @param o initial value
     * @param isParam true if this is to be a PARAM VALUE expression
     */
    Expression(int datatype, Object o, boolean isParam) {

        this(datatype, o);

        this.isParam = isParam;

        if (isParam) {
            paramMode = PARAM_IN;
        }
    }

    private void checkAggregate() {

        if (isAggregate(exprType)) {
            aggregateSpec = AGGREGATE_SELF;
        } else {
            aggregateSpec = AGGREGATE_NONE;

            if ((eArg != null) && eArg.isAggregate()) {
                aggregateSpec += AGGREGATE_LEFT;
            }

            if ((eArg2 != null) && eArg2.isAggregate()) {
                aggregateSpec += AGGREGATE_RIGHT;
            }
        }
    }

    public String toString() {
        return toString(0);
    }

    static String getContextDDL(Expression expression) throws HsqlException {

        String ddl = expression.getDDL();

        if (expression.exprType != VALUE && expression.exprType != COLUMN
                && expression.exprType != FUNCTION
                && expression.exprType != ALTERNATIVE
                && expression.exprType != CASEWHEN
                && expression.exprType != CONVERT) {
            StringBuffer temp = new StringBuffer();

            ddl = temp.append('(').append(ddl).append(')').toString();
        }

        return ddl;
    }

    /**
     * For use with CHECK constraints. Under development.
     *
     * Currently supports a subset of expressions and is suitable for CHECK
     * search conditions that refer only to the inserted/updated row.
     *
     * For full DDL reporting of VIEW select queries and CHECK search
     * conditions, future improvements here are dependent upon improvements to
     * SELECT query parsing, so that it is performed in a number of passes.
     * An early pass should result in the query turned into an Expression tree
     * that contains the information in the original SQL without any
     * alterations, and with tables and columns all resolved. This Expression
     * can then be preserved for future use. Table and column names that
     * are not user-defined aliases should be kept as the HsqlName structures
     * so that table or column renaming is reflected in the precompiled
     * query.
     *
     * @return DDL
     * @throws HsqlException
     */
    String getDDL() throws HsqlException {

        StringBuffer buf   = new StringBuffer(64);
        String       left  = null;
        String       right = null;

        if (eArg != null) {
            left = Expression.getContextDDL(eArg);
        }

        if (eArg2 != null) {
            right = Expression.getContextDDL(eArg2);
        }

        switch (exprType) {

            case FUNCTION :
                return function.getDLL();

            case VALUE :
                try {
                    return Column.createSQLString(valueData, dataType);
                } catch (HsqlException e) {}

                return buf.toString();

            case COLUMN :

                // this is a limited solution
                Table table = tableFilter.getTable();

                if (tableName != null) {
                    buf.append(table.tableName.statementName);
                    buf.append('.');
                }

                buf.append(
                    table.getColumn(columnIndex).columnName.statementName);

                return buf.toString();

            case TRUE :
                return Token.T_TRUE;

            case FALSE :
                return Token.T_FALSE;

            case VALUELIST :
                for (int i = 0; i < valueList.length; i++) {
                    buf.append(valueList[i].getDDL());

                    if (i < valueList.length - 1) {
                        buf.append(',');
                    }
                }

                return buf.toString();

            case ASTERIX :
                buf.append('*');

                return buf.toString();

            case NEGATE :
                buf.append('-').append(left);

                return buf.toString();

            case ADD :
                buf.append(left).append('+').append(right);

                return buf.toString();

            case SUBTRACT :
                buf.append(left).append('-').append(right);

                return buf.toString();

            case MULTIPLY :
                buf.append(left).append('*').append(right);

                return buf.toString();

            case DIVIDE :
                buf.append(left).append('/').append(right);

                return buf.toString();

            case CONCAT :
                buf.append(left).append("||").append(right);

                return buf.toString();

            case NOT :
                if (eArg.exprType == IS_NULL) {
                    buf.append(getContextDDL(eArg.eArg)).append(' ').append(
                        Token.T_IS).append(' ').append(Token.T_NOT).append(
                        ' ').append(Token.T_NULL);

                    return buf.toString();
                }

                buf.append(Token.T_NOT).append(' ').append(left);

                return buf.toString();

            case EQUAL :
                buf.append(left).append('=').append(right);

                return buf.toString();

            case BIGGER_EQUAL :
                buf.append(left).append(">=").append(right);

                return buf.toString();

            case BIGGER :
                buf.append(left).append('>').append(right);

                return buf.toString();

            case SMALLER :
                buf.append(left).append('<').append(right);

                return buf.toString();

            case SMALLER_EQUAL :
                buf.append(left).append("<=").append(right);

                return buf.toString();

            case NOT_EQUAL :
                if (Token.T_NULL.equals(right)) {
                    buf.append(left).append(" IS NOT ").append(right);
                } else {
                    buf.append(left).append("!=").append(right);
                }

                return buf.toString();

            case LIKE :
                buf.append(left).append(' ').append(Token.T_LIKE).append(' ');
                buf.append(right);

                /** @todo fredt - scripting of non-ascii escapes needs changes to general script logging */
                if (likeObject.escapeChar != null) {
                    buf.append(' ').append(Token.T_ESCAPE).append(' ').append(
                        '\'');
                    buf.append(likeObject.escapeChar.toString()).append('\'');
                    buf.append(' ');
                }

                return buf.toString();

            case AND :
                buf.append(left).append(' ').append(Token.T_AND).append(
                    ' ').append(right);

                return buf.toString();

            case OR :
                buf.append(left).append(' ').append(Token.T_OR).append(
                    ' ').append(right);

                return buf.toString();

            case IN :
                buf.append(left).append(' ').append(Token.T_IN).append(
                    ' ').append(right);

                return buf.toString();

            case CONVERT :
                buf.append(' ').append(Token.T_CONVERT).append('(');
                buf.append(left).append(',');
                buf.append(Types.getTypeString(dataType));
                buf.append(')');

                return buf.toString();

            case CASEWHEN :
                buf.append(' ').append(Token.T_CASEWHEN).append('(');
                buf.append(left).append(',').append(right).append(')');

                return buf.toString();

            case IS_NULL :
                buf.append(left).append(' ').append(Token.T_IS).append(
                    ' ').append(Token.T_NULL);

                return buf.toString();

            case ALTERNATIVE :
                buf.append(left).append(',').append(right);

                return buf.toString();

            case QUERY :
/*
                buf.append('(');
                buf.append(subSelect.getDDL());
                buf.append(')');
*/
                break;

            case EXISTS :
                buf.append(' ').append(Token.T_EXISTS).append(' ');
                break;

            case COUNT :
                buf.append(' ').append(Token.T_COUNT).append('(');
                break;

            case SUM :
                buf.append(' ').append(Token.T_SUM).append('(');
                buf.append(left).append(')');
                break;

            case MIN :
                buf.append(' ').append(Token.T_MIN).append('(');
                buf.append(left).append(')');
                break;

            case MAX :
                buf.append(' ').append(Token.T_MAX).append('(');
                buf.append(left).append(')');
                break;

            case AVG :
                buf.append(' ').append(Token.T_AVG).append('(');
                buf.append(left).append(')');
                break;

            case EVERY :
                buf.append(' ').append(Token.T_EVERY).append('(');
                buf.append(left).append(')');
                break;

            case SOME :
                buf.append(' ').append(Token.T_SOME).append('(');
                buf.append(left).append(')');
                break;

            case STDDEV_POP :
                buf.append(' ').append(Token.T_STDDEV_POP).append('(');
                buf.append(left).append(')');
                break;

            case STDDEV_SAMP :
                buf.append(' ').append(Token.T_STDDEV_SAMP).append('(');
                buf.append(left).append(')');
                break;

            case VAR_POP :
                buf.append(' ').append(Token.T_VAR_POP).append('(');
                buf.append(left).append(')');
                break;

            case VAR_SAMP :
                buf.append(' ').append(Token.T_VAR_SAMP).append('(');
                buf.append(left).append(')');
                break;
        }

        throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED,
                          "check constraint expression");
    }

    private String toString(int blanks) {

        int          lIType;
        StringBuffer buf = new StringBuffer(64);

        buf.append('\n');

        for (int i = 0; i < blanks; i++) {
            buf.append(' ');
        }

        if (oldIType != -1) {
            buf.append("SET TRUE, WAS: ");
        }

        lIType = oldIType == -1 ? exprType
                                : oldIType;

        switch (lIType) {

            case FUNCTION :
                buf.append("FUNCTION ");
                buf.append(function);

                return buf.toString();

            case VALUE :
                if (isParam) {
                    buf.append("PARAM ");
                }

                buf.append("VALUE = ").append(valueData);
                buf.append(", TYPE = ").append(Types.getTypeString(dataType));

                return buf.toString();

            case COLUMN :
                buf.append("COLUMN ");

                if (tableName != null) {
                    buf.append(tableName);
                    buf.append('.');
                }

                buf.append(columnName);

                return buf.toString();

            case QUERY :
                buf.append("QUERY ");
                buf.append(subSelect);

                return buf.toString();

            case TRUE :
                buf.append("TRUE ");
                break;

            case FALSE :
                buf.append("FALSE ");
                break;

            case VALUELIST :
                buf.append("VALUELIST ");
                buf.append(" TYPE = ").append(Types.getTypeString(dataType));

                if (valueList != null) {
                    for (int i = 0; i < valueList.length; i++) {
                        buf.append(valueList[i].toString(blanks + blanks));
                        buf.append(' ');
                    }
                }
                break;

            case ASTERIX :
                buf.append("* ");
                break;

            case NEGATE :
                buf.append("NEGATE ");
                break;

            case ADD :
                buf.append("ADD ");
                break;

            case SUBTRACT :
                buf.append("SUBTRACT ");
                break;

            case MULTIPLY :
                buf.append("MULTIPLY ");
                break;

            case DIVIDE :
                buf.append("DIVIDE ");
                break;

            case CONCAT :
                buf.append("CONCAT ");
                break;

            case NOT :
                buf.append("NOT ");
                break;

            case EQUAL :
                buf.append("EQUAL ");
                break;

            case BIGGER_EQUAL :
                buf.append("BIGGER_EQUAL ");
                break;

            case BIGGER :
                buf.append("BIGGER ");
                break;

            case SMALLER :
                buf.append("SMALLER ");
                break;

            case SMALLER_EQUAL :
                buf.append("SMALLER_EQUAL ");
                break;

            case NOT_EQUAL :
                buf.append("NOT_EQUAL ");
                break;

            case LIKE :
                buf.append("LIKE ");
                break;

            case AND :
                buf.append("AND ");
                break;

            case OR :
                buf.append("OR ");
                break;

            case IN :
                buf.append("IN ");
                break;

            case IS_NULL :
                buf.append("IS_NULL ");
                break;

            case EXISTS :
                buf.append("EXISTS ");
                break;

            case COUNT :
                buf.append("COUNT ");
                break;

            case SUM :
                buf.append("SUM ");
                break;

            case MIN :
                buf.append("MIN ");
                break;

            case MAX :
                buf.append("MAX ");
                break;

            case AVG :
                buf.append("AVG ");
                break;

            case EVERY :
                buf.append(Token.T_EVERY).append(' ');
                break;

            case SOME :
                buf.append(Token.T_SOME).append(' ');
                break;

            case STDDEV_POP :
                buf.append(Token.T_STDDEV_POP).append(' ');
                break;

            case STDDEV_SAMP :
                buf.append(Token.T_STDDEV_SAMP).append(' ');
                break;

            case VAR_POP :
                buf.append(Token.T_VAR_POP).append(' ');
                break;

            case VAR_SAMP :
                buf.append(Token.T_VAR_SAMP).append(' ');
                break;

            case CONVERT :
                buf.append("CONVERT ");
                buf.append(Types.getTypeString(dataType));
                buf.append(' ');
                break;

            case CASEWHEN :
                buf.append("CASEWHEN ");
                break;
        }

        if (isInJoin) {
            buf.append(" join");
        }

        if (eArg != null) {
            buf.append(" arg1=[");
            buf.append(eArg.toString(blanks + 1));
            buf.append(']');
        }

        if (eArg2 != null) {
            buf.append(" arg2=[");
            buf.append(eArg2.toString(blanks + 1));
            buf.append(']');
        }

        return buf.toString();
    }

    /**
     * Set the data type
     *
     *
     * @param type data type
     */
    void setDataType(int type) {
        dataType = type;
    }

    int oldIType = -1;

    /**
     * When an Expression is assigned to a TableFilter, a copy is made for use
     * there and the original is set to Expression.TRUE
     *
     */
    void setTrue() {

        if (oldIType == -1) {
            oldIType = exprType;
        }

        exprType = TRUE;
    }

    void setNull() {

        exprType  = VALUE;
        dataType  = Types.NULL;
        valueData = null;
        eArg      = null;
        eArg2     = null;
    }

    /**
     * Check if the given expression defines similar operation as this
     * expression. This method is used for ensuring an expression in
     * the ORDER BY clause has a matching column in the SELECT list. This check
     * is necessary with a SELECT DISTINCT query.<br>
     *
     * In the future we may perform the test when evaluating the search
     * condition to get a more accurate match.
     *
     * @param exp expression
     * @return boolean
     */
    public boolean similarTo(Expression exp) {

        if (exp == null) {
            return false;
        }

        if (exp == this) {
            return true;
        }

        /** @todo fredt - equals() method for valueList, subSelect and function are needed */
        return exprType == exp.exprType && dataType == exp.dataType
               && equals(valueData, exp.valueData)
               && equals(valueList, exp.valueList)
               && equals(subSelect, exp.subSelect)
               && equals(function, exp.function)
               && equals(tableName, exp.tableName)
               && equals(columnName, exp.columnName)
               && similarTo(eArg, exp.eArg) && similarTo(eArg2, exp.eArg2);
    }

    static boolean equals(Object o1, Object o2) {
        return (o1 == null) ? o2 == null
                            : o1.equals(o2);
    }

    static boolean equals(Expression[] ae1, Expression[] ae2) {

        if (ae1 == ae2) {
            return true;
        }

        if (ae1.length != ae2.length) {
            return false;
        }

        int     len    = ae1.length;
        boolean equals = true;

        for (int i = 0; i < len; i++) {
            Expression e1 = ae1[i];
            Expression e2 = ae2[i];

            equals = (e1 == null) ? e2 == null
                                  : e1.equals(e2);
        }

        return equals;
    }

    static boolean similarTo(Expression e1, Expression e2) {
        return (e1 == null) ? e2 == null
                            : e1.similarTo(e2);
    }

/** @todo fredt - workaround for functions in ORDER BY and GROUP BY needs
 *  checking the argument of the function to ensure they are valid. */

    /**
     * Check if this expression can be included in a group by clause.
     * <p>
     * It can, if itself is a column expression, and it is not an aggregate
     * expression.
     *
     * @return boolean
     */
    boolean canBeInGroupBy() {

        if (exprType == FUNCTION) {
            return true;
        }

        return isColumn() && (!(isAggregate()));
    }

    /**
     * Check if this expression can be included in an order by clause.
     * <p>
     * It can, if itself is a column expression.
     *
     * @return boolean
     */
    boolean canBeInOrderBy() {
        return exprType == FUNCTION || orderColumnIndex != -1 || isColumn()
               || isAggregate();
    }

    /**
     * Check if this expression defines at least one column.
     * <p>
     * It is, if itself is a column expression, or any the argument
     * expressions is a column expression.
     *
     * @return boolean
     */
    private boolean isColumn() {

        switch (exprType) {

            case COLUMN :
                return true;

            case NEGATE :
                return eArg.isColumn();

            case ADD :
            case SUBTRACT :
            case MULTIPLY :
            case DIVIDE :
            case CONCAT :
                return eArg.isColumn() || eArg2.isColumn();
        }

        return false;
    }

    /**
     * Collect column name used in this expression.
     *
     * @param columnNames set to be filled
     * @return true if a column name is used in this expression
     */
    boolean collectColumnName(HashSet columnNames) {

        boolean result = exprType == COLUMN;

        if (result) {
            columnNames.add(columnName);
        }

        return result;
    }

    /**
     * Collect all column names used in this expression or any of nested
     * expression.
     *
     * @param columnNames set to be filled
     */
    void collectAllColumnNames(HashSet columnNames) {

        if (!collectColumnName(columnNames)) {
            if (eArg != null) {
                eArg.collectAllColumnNames(columnNames);
            }

            if (eArg2 != null) {
                eArg2.collectAllColumnNames(columnNames);
            }
        }
    }

    /**
     * Check if this expression defines a constant value.
     * <p>
     * It does, if it is a constant value expression, or all the argument
     * expressions define constant values.
     *
     * @return boolean
     */
    boolean isConstant() {

        switch (exprType) {

            case VALUE :
                return true;

            case NEGATE :
                return eArg.isConstant();

            case ADD :
            case SUBTRACT :
            case MULTIPLY :
            case DIVIDE :
            case CONCAT :
                return eArg.isConstant() && eArg2.isConstant();
        }

        return false;
    }

    /**
     * Check if this expression can be included as a result column in an
     * aggregated select statement.
     * <p>
     * It can, if itself is an aggregate expression, or it results a constant
     * value.
     *
     * @return boolean
     */
    boolean canBeInAggregate() {
        return isAggregate() || isConstant();
    }

    /**
     *  Is this (indirectly) an aggregate expression
     *
     *  @return boolean
     */
    boolean isAggregate() {
        return aggregateSpec != AGGREGATE_NONE;
    }

    /**
     *  Is this directly an aggregate expression
     *
     *
     *  @return boolean
     */
    boolean isSelfAggregate() {
        return aggregateSpec == AGGREGATE_SELF;
    }

    static boolean isAggregate(int type) {

        switch (type) {

            case COUNT :
            case MAX :
            case MIN :
            case SUM :
            case AVG :
            case EVERY :
            case SOME :
            case STDDEV_POP :
            case STDDEV_SAMP :
            case VAR_POP :
            case VAR_SAMP :
                return true;
        }

        return false;
    }

// tony_lai@users having

    /**
     *  Checks for conditional expression.
     *
     *
     *  @return boolean
     */
    boolean isConditional() {

        switch (exprType) {

            case TRUE :
            case FALSE :
            case EQUAL :
            case BIGGER_EQUAL :
            case BIGGER :
            case SMALLER :
            case SMALLER_EQUAL :
            case NOT_EQUAL :
            case LIKE :
            case IN :
            case EXISTS :
            case IS_NULL :
                return true;

            case NOT :
                return eArg.isConditional();

            case AND :
            case OR :
                return eArg.isConditional() && eArg2.isConditional();

            default :
                return false;
        }
    }

    /**
     * Collects all expressions that must be in the GROUP BY clause, for a
     * grouped select statement.
     *
     * @param colExps expression list
     */
    void collectInGroupByExpressions(HsqlArrayList colExps) {

        if (!(isConstant() || isSelfAggregate())) {
            if (isColumn()) {
                colExps.add(this);
            } else {
                if (eArg != null) {
                    eArg.collectInGroupByExpressions(colExps);
                }

                if (eArg2 != null) {
                    eArg2.collectInGroupByExpressions(colExps);
                }
            }
        }
    }

    /**
     * Set an ORDER BY column expression DESC
     *
     */
    void setDescending() {
        isDescending = true;
    }

    /**
     * Is an ORDER BY column expression DESC
     *
     *
     * @return boolean
     */
    boolean isDescending() {
        return isDescending;
    }

    /**
     * Set the column alias and whether the name is quoted
     *
     * @param s alias
     * @param isquoted boolean
     */
    void setAlias(String s, boolean isquoted) {
        columnAlias = s;
        aliasQuoted = isquoted;
    }

    /**
     * Change the column name
     *
     * @param newname name
     * @param isquoted quoted
     */
    void setColumnName(String newname, boolean isquoted) {
        columnName   = newname;
        columnQuoted = isquoted;
    }

    /**
     * Change the table name
     *
     * @param newname table name for column expression
     */
    void setTableName(String newname) {
        tableName = newname;
    }

    /**
     * Return the user defined alias or null if none
     *
     * @return alias
     */
    String getDefinedAlias() {
        return columnAlias;
    }

    /**
     * Get the column alias
     *
     *
     * @return alias
     */
    String getAlias() {

        if (columnAlias != null) {
            return columnAlias;
        }

        if (exprType == VALUE) {
            return "";
        }

        if (exprType == COLUMN) {
            return columnName;
        }

// fredt@users 20020130 - patch 497872 by Nitin Chauhan - modified
// return column name for aggregates without alias
        if (eArg != null) {
            String name = eArg.getColumnName();

            if (name.length() > 0) {
                return name;
            }
        }

        return eArg2 == null ? ""
                             : eArg2.getAlias();
    }

    /**
     * Is an column alias quoted
     *
     * @return boolean
     */
    boolean isAliasQuoted() {

        if (columnAlias != null) {
            return aliasQuoted;
        }

        if (exprType == COLUMN) {
            return columnQuoted;
        }

        if (eArg != null) {
            String name = eArg.getColumnName();

            if (name.length() > 0) {
                return eArg.columnQuoted;
            }
        }

        return eArg2 == null ? false
                             : eArg2.columnQuoted;
    }

    /**
     * Returns the type of expression
     *
     *
     * @return type
     */
    int getType() {
        return exprType;
    }

    /**
     * Returns the left node
     *
     *
     * @return argument
     */
    Expression getArg() {
        return eArg;
    }

    /**
     * Returns the right node
     *
     *
     * @return argument
     */
    Expression getArg2() {
        return eArg2;
    }

    /**
     * Returns the table filter for a COLUMN expression
     *
     * @return table filter
     */
    TableFilter getFilter() {
        return tableFilter;
    }

    /**
     * Final check for all expressions.
     *
     * @param check boolean
     * @return boolean
     * @throws HsqlException
     */
    boolean checkResolved(boolean check) throws HsqlException {

        boolean result = true;

        if (eArg != null) {
            result = result && eArg.checkResolved(check);
        }

        if (eArg2 != null) {
            result = result && eArg2.checkResolved(check);
        }

        if (subSelect != null) {
            result = result && subSelect.checkResolved(check);
        }

        if (function != null) {
            result = result && function.checkResolved(check);
        }

        if (valueList != null) {
            for (int i = 0; i < valueList.length; i++) {
                result = result && valueList[i].checkResolved(check);
            }
        }

        if (exprType == COLUMN && tableFilter == null) {

            // if an order by column alias
            result = orderColumnIndex != -1;

            if (!result && check) {
                String err = tableName == null ? columnName
                                               : tableName + "." + columnName;

                throw Trace.error(Trace.COLUMN_NOT_FOUND, err);
            }
        }

        return result;
    }

    /**
     * Resolve the table names for columns and throws if a column remains
     * unresolved.
     *
     * @param filters list of filters
     *
     * @throws HsqlException
     */
    void checkTables(HsqlArrayList filters) throws HsqlException {

        if (filters == null || exprType == Expression.VALUE) {
            return;
        }

        if (eArg != null) {
            eArg.checkTables(filters);
        }

        if (eArg2 != null) {
            eArg2.checkTables(filters);
        }

        switch (exprType) {

            case COLUMN :
                boolean found = false;
                int     len   = filters.size();

                for (int j = 0; j < len; j++) {
                    TableFilter filter     = (TableFilter) filters.get(j);
                    String      filterName = filter.getName();

                    if (tableName == null || filterName.equals(tableName)) {
                        Table table = filter.getTable();
                        int   i     = table.searchColumn(columnName);

                        if (i != -1) {
                            if (tableName == null) {
                                if (found) {
                                    throw Trace.error(
                                        Trace.AMBIGUOUS_COLUMN_REFERENCE,
                                        columnName);
                                }

                                //
                                found = true;
                            } else {
                                return;
                            }
                        }
                    }
                }

                if (found) {
                    return;
                }

                throw Trace.error(Trace.COLUMN_NOT_FOUND, columnName);
            case QUERY :

                // fredt - subquery in join condition !
                break;

            case FUNCTION :
                if (function != null) {
                    function.checkTables(filters);
                }
                break;

            case IN :
                if (eArg2.exprType != QUERY) {
                    Expression[] vl = eArg2.valueList;

                    for (int i = 0; i < vl.length; i++) {
                        vl[i].checkTables(filters);
                    }
                }
                break;

            default :
        }
    }

    /**
     * Workaround for CHECK constraints. We don't want optimisation so we
     * flag all LIKE expressions as already optimised.
     *
     * @throws HsqlException
     */
    void setLikeOptimised() throws HsqlException {

        if (eArg != null) {
            eArg.setLikeOptimised();
        }

        if (eArg2 != null) {
            eArg2.setLikeOptimised();
        }

        if (exprType == LIKE) {
            likeObject.optimised = true;
        }
    }

    /**
     * Removes table filter resolution from an Expression tree.
     */
/*
    void removeFilters() throws HsqlException {

        if (eArg != null) {
            eArg.removeFilters();
        }

        if (eArg2 != null) {
            eArg2.removeFilters();
        }

        switch (exprType) {

            case COLUMN :
                tableFilter = null;

                return;

            case QUERY :
                if (subSelect != null) {
                    subSelect.removeFilters();
                }
                break;

            case FUNCTION :
                if (function != null) {
                    function.removeFilters();
                }
                break;

            case IN :
                if (eArg2.exprType != QUERY) {
                    Expression[] vl = eArg2.valueList;

                    for (int i = 0; i < vl.length; i++) {
                        vl[i].removeFilters();
                    }
                }
                break;

            default :
        }
    }
*/

    /**
     * set boolean flags and expressions for columns in a join
     *
     * @param filter target table filter
     * @param columns boolean array
     * @param elist expression list
     */
    void getEquiJoinColumns(TableFilter filter, boolean[] columns,
                            Expression[] elist) {

        if (eArg != null) {
            eArg.getEquiJoinColumns(filter, columns, elist);
        }

        if (eArg2 != null) {
            eArg2.getEquiJoinColumns(filter, columns, elist);
        }

        if (exprType == EQUAL) {
            if (eArg.tableFilter == eArg2.tableFilter) {
                return;
            }

            // an elist element may be set more than once - OK
            if (eArg.tableFilter == filter) {
                if (eArg2.exprType == COLUMN || eArg2.exprType == VALUE) {
                    columns[eArg.columnIndex] = true;
                    elist[eArg.columnIndex]   = eArg2;
                }

                return;
            }

            if (eArg2.tableFilter == filter) {
                if (eArg.exprType == COLUMN || eArg.exprType == VALUE) {
                    columns[eArg2.columnIndex] = true;
                    elist[eArg2.columnIndex]   = eArg;
                }
            }
        }
    }

    /**
     * Resolve the table names for columns
     *
     * @param f table filter
     *
     * @throws HsqlException
     */
    void resolveTables(TableFilter f) throws HsqlException {

        if (isParam || f == null || exprType == Expression.VALUE) {
            return;
        }

        if (eArg != null) {
            eArg.resolveTables(f);
        }

        if (eArg2 != null) {
            eArg2.resolveTables(f);
        }

        switch (exprType) {

            case COLUMN :
                if (tableFilter != null) {
                    break;
                }

                String filterName = f.getName();

                if (tableName == null || tableName.equals(filterName)) {
                    Table table = f.getTable();
                    int   i     = table.searchColumn(columnName);

                    if (i != -1) {
/*
// fredt@users 20011110 - fix for 471711 - subselects
                        boolean repeat = tableFilter != null && !tableFilter.getName().equals(filterName);
                        if ( repeat){
                             throw Trace.error(Trace.AMBIGUOUS_COLUMN_REFERENCE, columnName);
                        }
*/
                        tableFilter = f;
                        columnIndex = i;
                        tableName   = filterName;

                        setTableColumnAttributes(table, i);

                        // COLUMN is leaf; we are done
                        return;
                    }
                }
                break;

            case QUERY :

                // we now (1_7_2_ALPHA_R) resolve independently first, then
                // resolve in the enclosing context
                if (subSelect != null) {
                    subSelect.resolveTables(f);
                }
                break;

            case FUNCTION :
                if (function != null) {
                    function.resolveTables(f);
                }
                break;

            case IN :
                if (eArg2.exprType != QUERY) {
                    Expression[] vl = eArg2.valueList;

                    for (int i = 0; i < vl.length; i++) {
                        vl[i].resolveTables(f);
                    }
                }
                break;

            default :
        }
    }

    void resolveTypes() throws HsqlException {

        if (isParam || exprType == Expression.VALUE) {
            return;
        }

        if (eArg != null) {
            eArg.resolveTypes();
        }

        if (eArg2 != null) {
            eArg2.resolveTypes();
        }

        switch (exprType) {

            case FUNCTION :
                function.resolveType();

                dataType = function.getReturnType();
                break;

            case QUERY : {
                subSelect.resolveTypes();

                dataType = subSelect.exprColumns[0].dataType;

                break;
            }
            case NEGATE :
                if (eArg.isParam) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                                      Trace.Expression_resolveTypes1);
                }

                dataType = eArg.dataType;

                if (isFixedConstant()) {
                    valueData = getValue(null, dataType);
                    eArg      = null;
                    exprType  = VALUE;
                }
                break;

            case ADD :

                // concat using + operator
                // non-standard concat operator to be deprecated
                if (Types.isCharacterType(eArg.dataType)
                        || Types.isCharacterType(eArg2.dataType)) {
                    exprType = Expression.CONCAT;
                    dataType = Types.VARCHAR;

                    if (isFixedConstant()) {
                        valueData = getValue(null, dataType);
                        eArg      = null;
                        eArg2     = null;
                        exprType  = VALUE;
                    } else {
                        if (eArg.isParam) {
                            eArg.dataType = Types.VARCHAR;
                        }

                        if (eArg2.isParam) {
                            eArg2.dataType = Types.VARCHAR;
                        }
                    }

                    break;
                }
            case SUBTRACT :
            case MULTIPLY :
            case DIVIDE :
                if (eArg.isParam && eArg2.isParam) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                                      Trace.Expression_resolveTypes2);
                }

                if (isFixedConstant()) {
                    dataType = Column.getCombinedNumberType(eArg.dataType,
                            eArg2.dataType, exprType);
                    valueData = getValue(null, dataType);
                    eArg      = null;
                    eArg2     = null;
                    exprType  = VALUE;
                } else {
                    if (eArg.isParam) {
                        eArg.dataType = eArg2.dataType;
                    } else if (eArg2.isParam) {
                        eArg2.dataType = eArg.dataType;
                    }

                    // fredt@users 20011010 - patch 442993 by fredt
                    dataType = Column.getCombinedNumberType(eArg.dataType,
                            eArg2.dataType, exprType);
                }
                break;

            case CONCAT :
                dataType = Types.VARCHAR;

                if (isFixedConstant()) {
                    valueData = getValue(null, dataType);
                    eArg      = null;
                    eArg2     = null;
                    exprType  = VALUE;
                } else {
                    if (eArg.isParam) {
                        eArg.dataType = Types.VARCHAR;
                    }

                    if (eArg2.isParam) {
                        eArg2.dataType = Types.VARCHAR;
                    }
                }
                break;

            case EQUAL :
            case BIGGER_EQUAL :
            case BIGGER :
            case SMALLER :
            case SMALLER_EQUAL :
            case NOT_EQUAL :
                if (eArg.isParam && eArg2.isParam) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                                      Trace.Expression_resolveTypes3);
                }

                if (isFixedConditional()) {
                    Boolean result = test(null);

                    if (result == null) {
                        setNull();
                    } else if (result.booleanValue()) {
                        exprType = TRUE;
                    } else {
                        exprType = FALSE;
                    }

                    eArg  = null;
                    eArg2 = null;
                } else if (eArg.isParam) {
                    eArg.dataType = eArg2.dataType == Types.NULL
                                    ? Types.VARCHAR
                                    : eArg2.dataType;

                    if (eArg2.exprType == COLUMN) {
                        eArg.setTableColumnAttributes(eArg2);
                    }
                } else if (eArg2.isParam) {
                    eArg2.dataType = eArg.dataType == Types.NULL
                                     ? Types.VARCHAR
                                     : eArg.dataType;

                    if (eArg.exprType == COLUMN) {
                        eArg2.setTableColumnAttributes(eArg);
                    }
                }

                dataType = Types.BOOLEAN;
                break;

            case LIKE :
                resolveTypeForLike();

                dataType = Types.BOOLEAN;
                break;

            case AND : {
                boolean argFixed  = eArg.isFixedConditional();
                boolean arg2Fixed = eArg2.isFixedConditional();
                Boolean arg       = argFixed ? (eArg.test(null))
                                             : null;
                Boolean arg2      = arg2Fixed ? eArg2.test(null)
                                              : null;

                if (argFixed && arg2Fixed) {
                    if (arg == null || arg2 == null) {
                        setNull();
                    } else {
                        exprType = arg.booleanValue() && arg2.booleanValue()
                                   ? TRUE
                                   : FALSE;
                        eArg  = null;
                        eArg2 = null;
                    }
                } else if ((argFixed &&!Boolean.TRUE.equals(arg))
                           || (arg2Fixed &&!Boolean.TRUE.equals(arg2))) {
                    exprType = FALSE;
                    eArg     = null;
                    eArg2    = null;
                } else {
                    if (eArg.isParam) {
                        eArg.dataType = Types.BOOLEAN;
                    }

                    if (eArg2.isParam) {
                        eArg2.dataType = Types.BOOLEAN;
                    }
                }

                dataType = Types.BOOLEAN;

                break;
            }
            case OR : {
                boolean argFixed  = eArg.isFixedConditional();
                boolean arg2Fixed = eArg2.isFixedConditional();
                Boolean arg       = argFixed ? (eArg.test(null))
                                             : null;
                Boolean arg2      = arg2Fixed ? eArg2.test(null)
                                              : null;

                if (argFixed && arg2Fixed) {
                    if (arg == null || arg2 == null) {
                        setNull();
                    } else {
                        exprType = arg.booleanValue() || arg2.booleanValue()
                                   ? TRUE
                                   : FALSE;
                        eArg  = null;
                        eArg2 = null;
                    }
                } else if ((argFixed && Boolean.TRUE.equals(arg))
                           || (arg2Fixed && Boolean.TRUE.equals(arg2))) {
                    exprType = TRUE;
                    eArg     = null;
                    eArg2    = null;
                } else {
                    if (eArg.isParam) {
                        eArg.dataType = Types.BOOLEAN;
                    }

                    if (eArg2.isParam) {
                        eArg2.dataType = Types.BOOLEAN;
                    }
                }

                dataType = Types.BOOLEAN;

                break;
            }
            case IS_NULL :
                if (isFixedConditional()) {
                    exprType = Boolean.TRUE.equals(test(null)) ? TRUE
                                                               : FALSE;
                    eArg     = null;
                }

                dataType = Types.BOOLEAN;
                break;

            case NOT :
                if (isFixedConditional()) {
                    Boolean arg = test(null);

                    if (arg == null) {
                        setNull();
                    } else {
                        exprType = arg.booleanValue() ? TRUE
                                                      : FALSE;
                        eArg     = null;
                    }
                } else if (eArg.isParam) {
                    eArg.dataType = Types.BOOLEAN;
                }

                dataType = Types.BOOLEAN;
                break;

            case IN :
                resolveTypeForIn();

                dataType = Types.BOOLEAN;
                break;

            case EXISTS :

                // NOTE: no such thing as a param arg if expression is EXISTS
                // Also, cannot detect if result is fixed value
                dataType = Types.BOOLEAN;
                break;

            /** @todo fredt - set the correct return type */
            case COUNT :
                if (eArg.isParam) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                                      Trace.Expression_resolveTypes4);
                }

                dataType = Types.INTEGER;
                break;

            case MAX :
            case MIN :
            case SUM :
            case AVG :
            case EVERY :
            case SOME :
            case STDDEV_POP :
            case STDDEV_SAMP :
            case VAR_POP :
            case VAR_SAMP :
                if (eArg.isParam) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                                      Trace.Expression_resolveTypes4);
                }

                dataType = SetFunction.getType(exprType, eArg.dataType);
                break;

            case CONVERT :

                // NOTE: both iDataType for this expr and for eArg (if isParm)
                // are already set in Parser during read
                if (eArg.isFixedConstant() || eArg.isFixedConditional()) {
                    valueData = getValue(null);
                    exprType  = VALUE;
                    eArg      = null;
                }
                break;

            case CASEWHEN :

                // We use CASEWHEN as parent type.
                // In the parent, eArg is the condition, and eArg2 is
                // the leaf, tagged as type ALTERNATIVE; its eArg is
                // case 1 (how to get the value when the condition in
                // the parent evaluates to true), while its eArg2 is case 2
                // (how to get the value when the condition in
                // the parent evaluates to false).
                if (eArg.isParam) {

                    // condition is a paramter marker,
                    // as in casewhen(?, v1, v2)
                    eArg.dataType = Types.BOOLEAN;
                }

                dataType = eArg2.dataType;
                break;

            case ALTERNATIVE : {
                Expression case1 = eArg;
                Expression case2 = eArg2;

                if (case1.isParam && case2.isParam) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                                      Trace.Expression_resolveTypes6);
                }

                if (case1.isParam || case1.dataType == Types.NULL) {
                    case1.dataType = case2.dataType;
                } else if (case2.isParam || case2.dataType == Types.NULL) {
                    case2.dataType = case1.dataType;
                }

                if (case1.dataType == Types.NULL
                        && case2.dataType == Types.NULL) {
                    dataType = Types.NULL;
                }

                if (Types.isNumberType(case1.dataType)
                        && Types.isNumberType(case2.dataType)) {
                    dataType = Column.getCombinedNumberType(case1.dataType,
                            case2.dataType, ALTERNATIVE);
                } else if (Types.isCharacterType(case1.dataType)
                           && Types.isCharacterType(case2.dataType)) {
                    dataType = Types.LONGVARCHAR;
                } else if (case1.dataType != case2.dataType) {
                    if (case2.exprType == Expression.VALUE) {
                        dataType = case2.dataType = case1.dataType;
                        case2.valueData =
                            Column.convertObject(case2.valueData, dataType);
                    } else if (case1.exprType == Expression.VALUE) {
                        dataType = case1.dataType = case2.dataType;
                        case1.valueData =
                            Column.convertObject(case1.valueData, dataType);
                    } else {
                        throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                                          Trace.Expression_resolveTypes7,
                                          new String[] {
                            Types.getTypeString(case1.dataType),
                            Types.getTypeString(case2.dataType)
                        });
                    }
                } else {
                    dataType = case1.dataType;
                }

                break;
            }
        }
    }

    void resolveTypeForLike() throws HsqlException {

        if (eArg.isParam && eArg2.isParam) {
            throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                              Trace.Expression_resolveTypeForLike);
        }

        if (isFixedConditional()) {
            Boolean arg = test(null);

            if (arg == null) {
                setNull();
            } else {
                exprType = arg.booleanValue() ? TRUE
                                              : FALSE;
                eArg     = null;
                eArg2    = null;
            }
        } else if (eArg.isParam) {
            eArg.dataType = Types.VARCHAR;
        } else if (eArg2.isParam) {
            eArg2.dataType = Types.VARCHAR;
        }

// boucherb@users 2003-09-25 - patch 1.7.2 Alpha P
//
// Some optimizations for LIKE
//
// TODO:
//
// See if the same optimizations can be done dynamically at execute time when
// eArg2 is PARAM.  Unfortunately, this currently requires re-resolving from
// the root any expression containing at least one parameterized LIKE in the
// compiled statement and reseting conditions on any involved table filters,
// so the answer is: probably not, at least not under the current code.
//
// CHECKME:
//
// Test for correct results under all XXXCHAR types (padding, etc.?)
//
// NOTE:
//
// For the old behaviour, simply comment out the block below
        if (likeObject.optimised) {
            return;
        }

        boolean isRightArgFixedConstant = eArg2.isFixedConstant();
        String likeStr = isRightArgFixedConstant
                         ? (String) eArg2.getValue(null, Types.VARCHAR)
                         : null;
        boolean ignoreCase = eArg.dataType == Types.VARCHAR_IGNORECASE
                             || eArg2.dataType == Types.VARCHAR_IGNORECASE;

        likeObject.setParams(likeStr, ignoreCase);

        if (!isRightArgFixedConstant) {

            // Then we are done here, since it's impossible
            // to determine at this point if the right expression
            // will have a fixed prefix that can be used to optimize
            // any involved table filters
            return;
        }

        if (likeObject.isEquivalentToFalsePredicate()) {
            exprType   = FALSE;
            eArg       = null;
            eArg2      = null;
            likeObject = null;
        } else if (likeObject.isEquivalentToEqualsPredicate()) {
            exprType   = EQUAL;
            eArg2 = new Expression(Types.VARCHAR, likeObject.getRangeLow());
            likeObject = null;
        } else if (likeObject.isEquivalentToNotNullPredicate()) {

            // X LIKE '%' <=>  X IS NOT NULL
            exprType   = NOT;
            eArg       = new Expression(IS_NULL, eArg, null);
            eArg2      = null;
            likeObject = null;
        } else {
            if (eArg.exprType != Expression.COLUMN) {

                // Then we are done here, since range predicates are
                // not picked up for use to optimize table filters
                // unless the predicate is on the first column of
                // an index.
                // TODO:
                // We might go one step further here and check if the
                // column is elligible (is the first column of some
                // index on its table).  If it is not, it may be that
                // substituting/inserting range predicate below
                // can actually lower performance.
                // Indeed, we might better consider delaying the
                // optimizations below till the TableFilter.setConditions()
                // phase.
                return;
            }

            if (!Types.isCharacterType(eArg.dataType)) {

                // TODO:
                // correct range low / range high generation for
                // types other than XXXCHAR
                return;
            }

            boolean between = false;
            boolean like    = false;
            boolean larger  = false;

            if (likeObject.isEquivalentToBetweenPredicate()) {

                // X LIKE 'abc%' <=> X >= 'abc' AND X <= 'abc' || max_collation_char
                larger  = Column.sql_compare_in_locale;
                between = !larger;
                like    = larger;
            } else if (likeObject
                    .isEquivalentToBetweenPredicateAugmentedWithLike()) {

                // X LIKE 'abc%...' <=> X >= 'abc' AND X <= 'abc' || max_collation_char AND X LIKE 'abc%...'
                larger  = Column.sql_compare_in_locale;
                between = !larger;
                like    = true;
            }

            if (between == false && larger == false) {
                return;
            }

            Expression eFirst = new Expression(Types.VARCHAR,
                                               likeObject.getRangeLow());
            Expression eLast = new Expression(Types.VARCHAR,
                                              likeObject.getRangeHigh());

            if (between &&!like) {
                Expression eArgOld = eArg;

                eArg       = new Expression(BIGGER_EQUAL, eArgOld, eFirst);
                eArg2      = new Expression(SMALLER_EQUAL, eArgOld, eLast);
                exprType   = AND;
                likeObject = null;
            } else if (between && like) {
                Expression gte = new Expression(BIGGER_EQUAL, eArg, eFirst);
                Expression lte = new Expression(SMALLER_EQUAL, eArg, eLast);

                eArg2 = new Expression(eArg, eArg2, likeObject.escapeChar);
                eArg2.likeObject = likeObject;
                eArg             = new Expression(AND, gte, lte);
                exprType         = AND;
                likeObject       = null;
            } else if (larger) {
                Expression gte = new Expression(BIGGER_EQUAL, eArg, eFirst);

                eArg2 = new Expression(eArg, eArg2, likeObject.escapeChar);
                eArg2.likeObject = likeObject;
                eArg             = gte;
                exprType         = AND;
                likeObject       = null;
            }
        }
    }

// PARAM Resolution rules:
//
// Expression used with IN:    Same as the first value or the result column
//                             of the subquery
//
// A value used with IN:       Same as the expression or the first value if
//                             there is a parameter marker in the expression
// Implies ambiguity if:       A parameter marker is both the expression and
//                             the first value of an IN operation, from which
//                             follows that it is ambiguous for the expression
//                             to be a parameter marker if the list is empty.
// CHECKME:
// Is an empty IN list legal?  Why would anyone ever use it?
    void resolveTypeForIn() throws HsqlException {

        if (eArg2.exprType == QUERY) {
            if (eArg.isParam) {
                eArg.dataType = eArg2.dataType;
            }
        } else {    // eArg2.exprType == VALUELIST
            Expression[] vl  = eArg2.valueList;
            int          len = vl.length;

            if (eArg.isParam) {
                if (len == 0) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                                      Trace.Expression_resolveTypeForIn1);
                }

                if (vl[0].isParam) {
                    throw Trace.error(Trace.UNRESOLVED_PARAMETER_TYPE,
                                      Trace.Expression_resolveTypeForIn2);
                }

                Expression e  = vl[0];
                int        dt = e.dataType;

                // PARAM datatype same as first value list expression
                // should never be Types.NULL when all is said and done
                if (dt == Types.NULL) {

                    // do nothing...
                } else {
                    if (eArg.dataType == Types.NULL) {
                        eArg.dataType = dt;
                    }

                    if (eArg2.dataType == Types.NULL) {
                        eArg2.dataType = dt;
                    }
                }

                for (int i = 1; i < len; i++) {
                    e = vl[i];

                    if (e.isParam) {
                        if (e.dataType == Types.NULL && dt != Types.NULL) {
                            e.dataType = dt;
                        }
                    } else {
                        e.resolveTypes();
                    }
                }
            } else {
                int dt = eArg.dataType;

                if (eArg2.dataType == Types.NULL && dt != Types.NULL) {
                    eArg2.dataType = dt;
                }

                for (int i = 0; i < len; i++) {
                    Expression e = vl[i];

                    if (e.isParam) {
                        if (e.dataType == Types.NULL && dt != Types.NULL) {
                            e.dataType = dt;
                        }
                    } else {
                        e.resolveTypes();
                    }
                }
            }

            eArg2.isFixedConstantValueList = true;

            for (int i = 0; i < len; i++) {
                if (!vl[i].isFixedConstant()) {
                    eArg2.isFixedConstantValueList = false;

                    break;
                }
            }

            if (eArg2.isFixedConstantValueList) {
                eArg2.hList = new HashSet();

                for (int i = 0; i < len; i++) {
                    try {
                        Object value = eArg2.valueList[i].getValue(null);

                        value = Column.convertObject(value, eArg2.dataType);

                        eArg2.hList.add(value);
                    } catch (HsqlException e) {}
                }
            }
        }
    }

    /**
     * Has this expression been resolved
     *
     *
     * @return boolean
     */
    boolean isResolved() {

        switch (exprType) {

            case VALUE :
            case NEGATE :
                return true;

            case COLUMN :
                return tableFilter != null && tableFilter.isAssigned;

            case QUERY :
                return true;
        }

        // todo: could recurse here, but never miss a 'false'!
        return false;
    }

    /**
     * Is the argument expression type a comparison expression
     *
     * @param i expresion type
     *
     * @return boolean
     */
    static boolean isCompare(int i) {

        switch (i) {

            case EQUAL :
            case BIGGER_EQUAL :
            case BIGGER :
            case SMALLER :
            case SMALLER_EQUAL :
            case NOT_EQUAL :
                return true;
        }

        return false;
    }

    /**
     * Returns the table name for a column expression as a string
     *
     * @return table name
     */
    String getTableName() {

        if (exprType == ASTERIX) {
            return tableName;
        }

        if (exprType == COLUMN) {
            if (tableFilter == null) {
                return tableName;
            } else {
                return tableFilter.getTable().getName().name;
            }
        }

        // todo
        return "";
    }

    /**
     * Returns the name of a column as string
     *
     * @return column name
     */
    String getColumnName() {

        if (exprType == COLUMN) {
            if (tableFilter == null) {
                return columnName;
            } else {
                return tableFilter.getTable().getColumn(
                    columnIndex).columnName.name;
            }
        }

        return getAlias();
    }

    /**
     * Returns the column index in the table
     *
     * @return column index
     */
    int getColumnNr() {
        return columnIndex;
    }

    /**
     * Returns the column size
     *
     * @return size
     */
    int getColumnSize() {
        return columnSize;
    }

    /**
     * Returns the column scale
     *
     *
     * @return scale
     */
    int getColumnScale() {
        return columnScale;
    }

    /**
     * Set this as a set function with / without DISTINCT
     *
     * @param distinct is distinct
     */
    void setDistinctAggregate(boolean distinct) {

        isDistinctAggregate = distinct && (eArg.exprType != ASTERIX);

        if (exprType == COUNT) {
            dataType = distinct ? dataType
                                : Types.INTEGER;
        }
    }

    /**
     * Swap the condition with its complement
     *
     * @throws HsqlException
     */
    void swapCondition() throws HsqlException {

        int i = EQUAL;

        switch (exprType) {

            case BIGGER_EQUAL :
                i = SMALLER_EQUAL;
                break;

            case SMALLER_EQUAL :
                i = BIGGER_EQUAL;
                break;

            case SMALLER :
                i = BIGGER;
                break;

            case BIGGER :
                i = SMALLER;
                break;

            case EQUAL :
                break;

            default :
                Trace.doAssert(false, "Expression.swapCondition");
        }

        exprType = i;

        Expression e = eArg;

        eArg  = eArg2;
        eArg2 = e;
    }

    /**
     * Returns the data type
     *
     *
     * @return type
     */
    int getDataType() {
        return dataType;
    }

    /**
     * Get the value in the given type in the given session context
     *
     *
     * @param type returned type
     * @param session context
     * @return value
     *
     * @throws HsqlException
     */
    Object getValue(Session session, int type) throws HsqlException {

        Object o = getValue(session);

        if ((o == null) || (dataType == type)) {
            return o;
        }

        return Column.convertObject(o, type);
    }

/** @todo fredt - should be rewritten to handle only set function operation,
     *  with other operations handled in the normal way */
    Object getAggregatedValue(Session session, Object currValue,
                              int type) throws HsqlException {
        return Column.convertObject(getAggregatedValue(session, currValue),
                                    type);
    }

    /**
     * Get the result of a SetFunction or an ordinary value
     *
     * @param currValue instance of set function or value
     * @param session context
     * @return object
     *
     * @throws HsqlException
     */
    Object getAggregatedValue(Session session,
                              Object currValue) throws HsqlException {

        if (!isAggregate()) {
            return currValue;
        }

        // handles results of aggregates plus NEGATE and CONVERT
        switch (exprType) {

            case COUNT :
                if (currValue == null) {
                    return INTEGER_0;
                }

                return ((SetFunction) currValue).getValue();

            case MAX :
            case MIN :
            case SUM :
            case AVG :
            case EVERY :
            case SOME :
            case STDDEV_POP :
            case STDDEV_SAMP :
            case VAR_POP :
            case VAR_SAMP :
                if (currValue == null) {
                    return null;
                }

                return ((SetFunction) currValue).getValue();

            case NEGATE :
                return Column.negate(
                    eArg.getAggregatedValue(session, currValue), dataType);

            case CONVERT :
                return Column.convertObject(
                    eArg.getAggregatedValue(session, currValue), dataType);
        }

        // handle expressions
        Object leftValue  = null,
               rightValue = null;

        switch (aggregateSpec) {

            case AGGREGATE_LEFT :
                leftValue  = eArg.getAggregatedValue(session, currValue);
                rightValue = eArg2 == null ? null
                                           : eArg2.getValue(session);
                break;

            case AGGREGATE_RIGHT :
                leftValue  = eArg == null ? null
                                          : eArg.getValue(session);
                rightValue = eArg2.getAggregatedValue(session, currValue);
                break;

            case AGGREGATE_BOTH :
                if (currValue == null) {
                    currValue = new Object[2];
                }

                leftValue =
                    eArg.getAggregatedValue(session,
                                            ((Object[]) currValue)[0]);
                rightValue =
                    eArg2.getAggregatedValue(session,
                                             ((Object[]) currValue)[1]);
                break;
        }

        // handle other operations
        switch (exprType) {

// tony_lai@users having >>>
            case TRUE :
                return Boolean.TRUE;

            case FALSE :
                return Boolean.FALSE;

            case NOT :
                Trace.doAssert(eArg2 == null,
                               "Expression.getAggregatedValue.NOT");

                return ((Boolean) leftValue).booleanValue() ? Boolean.FALSE
                                                            : Boolean.TRUE;

            case AND :
                return ((Boolean) leftValue).booleanValue()
                       && ((Boolean) rightValue).booleanValue() ? Boolean.TRUE
                                                                : Boolean
                                                                .FALSE;

            case OR :
                return ((Boolean) leftValue).booleanValue()
                       || ((Boolean) rightValue).booleanValue() ? Boolean.TRUE
                                                                : Boolean
                                                                .FALSE;

            case IS_NULL :
                return leftValue == null ? Boolean.TRUE
                                         : Boolean.FALSE;

            case LIKE :
                String s = (String) Column.convertObject(rightValue,
                    Types.VARCHAR);

                if (eArg2.isParam || eArg2.exprType != VALUE) {
                    likeObject.resetPattern(s);
                }

                String c = (String) Column.convertObject(leftValue,
                    Types.VARCHAR);

                return likeObject.compare(c) ? Boolean.TRUE
                                             : Boolean.FALSE;

            case IN :
                return eArg2.testValueList(session, leftValue);

            case EXISTS :
                if (eArg.isCorrelated) {
                    Result r = eArg.subSelect.getResult(session, 1);    // 1 is already enough

                    return r.rRoot == null ? Boolean.FALSE
                                           : Boolean.TRUE;
                } else {
                    return subTable.isEmpty() ? Boolean.FALSE
                                              : Boolean.TRUE;
                }
            case CASEWHEN :
                if (leftValue instanceof SetFunction) {
                    leftValue = Column.convertObject(
                        ((SetFunction) leftValue).getValue(), Types.BOOLEAN);
                } else {
                    leftValue = Column.convertObject(leftValue,
                                                     Types.BOOLEAN);
                }

                boolean test   = ((Boolean) leftValue).booleanValue();
                Object  result = test ? ((Object[]) rightValue)[0]
                                      : ((Object[]) rightValue)[1];

                if (result instanceof SetFunction) {
                    return Column.convertObject(
                        ((SetFunction) result).getValue(), dataType);
                } else {
                    return Column.convertObject(result, dataType);
                }
            case ALTERNATIVE :
                if (leftValue instanceof SetFunction) {
                    leftValue = Column.convertObject(
                        ((SetFunction) leftValue).getValue(), dataType);
                } else {
                    leftValue = Column.convertObject(leftValue, dataType);
                }

                if (rightValue instanceof SetFunction) {
                    rightValue = Column.convertObject(
                        ((SetFunction) rightValue).getValue(), dataType);
                } else {
                    rightValue = Column.convertObject(rightValue, dataType);
                }

                Object[] objectPair = new Object[2];

                objectPair[0] = leftValue;
                objectPair[1] = rightValue;

                return objectPair;

            case FUNCTION :
                return function.getAggregatedValue(session, currValue);
        }

        // handle comparisons
        // convert vals
        if (isCompare(exprType)) {
            int valueType = eArg.isColumn() ? eArg.dataType
                                            : eArg2.dataType;

            return compareValues(leftValue, rightValue, valueType, exprType)
                   ? Boolean.TRUE
                   : Boolean.FALSE;
        }

        // handle arithmetic and concat operations
        if (leftValue != null) {
            leftValue = Column.convertObject(leftValue, dataType);
        }

        if (rightValue != null) {
            rightValue = Column.convertObject(rightValue, dataType);
        }

        switch (exprType) {

            case ADD :
                return Column.add(leftValue, rightValue, dataType);

            case SUBTRACT :
                return Column.subtract(leftValue, rightValue, dataType);

            case MULTIPLY :
                return Column.multiply(leftValue, rightValue, dataType);

            case DIVIDE :
                return Column.divide(leftValue, rightValue, dataType);

            case CONCAT :
                return Column.concat(leftValue, rightValue);

            default :
                throw Trace.error(Trace.NEED_AGGREGATE, this.toString());
        }
    }

    /**
     * Instantiate the SetFunction or recurse, returning the result
     *
     * @param currValue setFunction
     * @param session context
     * @return a normal value or SetFunction instance
     *
     * @throws HsqlException
     */
    Object updateAggregatingValue(Session session,
                                  Object currValue) throws HsqlException {

        if (!isAggregate()) {
            return getValue(session);
        }

        switch (aggregateSpec) {

            case AGGREGATE_SELF : {
                if (currValue == null) {
                    currValue = new SetFunction(exprType, eArg.dataType,
                                                isDistinctAggregate);
                }

                Object newValue = eArg.exprType == ASTERIX ? INTEGER_1
                                                           : eArg.getValue(
                                                               session);

                ((SetFunction) currValue).add(newValue);

                return currValue;
            }
            case AGGREGATE_BOTH : {
                Object[] valuePair = (Object[]) currValue;

                if (valuePair == null) {
                    valuePair = new Object[2];
                }

                valuePair[0] = eArg.updateAggregatingValue(session,
                        valuePair[0]);
                valuePair[1] = eArg2.updateAggregatingValue(session,
                        valuePair[1]);

                return valuePair;
            }
            case AGGREGATE_LEFT :
                return eArg.updateAggregatingValue(session, currValue);

            case AGGREGATE_RIGHT :
                return eArg2.updateAggregatingValue(session, currValue);

            case AGGREGATE_FUNCTION :
                return function.updateAggregatingValue(session, currValue);

            default :

                // never gets here
                return currValue;
        }
    }

    Object getValue(Session session) throws HsqlException {

        switch (exprType) {

            case VALUE :
                return valueData;

            case COLUMN :
                try {
                    return tableFilter.currentData[columnIndex];
                } catch (NullPointerException e) {
                    throw Trace.error(Trace.COLUMN_NOT_FOUND, columnName);
                }
            case FUNCTION :
                return function.getValue(session);

            case QUERY :
                return subSelect.getValue(session, dataType);

            case NEGATE :
                return Column.negate(eArg.getValue(session, dataType),
                                     dataType);

            case AND :
            case OR :
            case LIKE :
            case EXISTS :
            case IN :
                return test(session);

            case CONVERT :
                return eArg.getValue(session, dataType);

            case CASEWHEN :
                Boolean result = eArg.test(session);

                if (Boolean.TRUE.equals(result)) {
                    return eArg2.eArg.getValue(session, dataType);
                } else {
                    return eArg2.eArg2.getValue(session, dataType);
                }

            // gets here from getAggregatedValue()
            case ALTERNATIVE :
                return new Object[] {
                    eArg.getValue(session, dataType),
                    eArg2.getValue(session, dataType)
                };
        }

        // todo: simplify this
        Object a = null,
               b = null;

        if (eArg != null) {
            a = eArg.getValue(session, dataType);
        }

        if (eArg2 != null) {
            b = eArg2.getValue(session, dataType);
        }

        switch (exprType) {

            case ADD :
                return Column.add(a, b, dataType);

            case SUBTRACT :
                return Column.subtract(a, b, dataType);

            case MULTIPLY :
                return Column.multiply(a, b, dataType);

            case DIVIDE :
                return Column.divide(a, b, dataType);

            case CONCAT :
                return Column.concat(a, b);

            case SEQUENCE :
                return ((NumberSequence) valueData).getValueObject();

            default :

                /** @todo fredt - make sure the expression type is always comparison here */
                return test(session);
        }
    }

    boolean testCondition(Session session) throws HsqlException {
        return Boolean.TRUE.equals(test(session));
    }

    /**
     * Returns the test result of a conditional expression
     *
     * @param session session
     * @return boolean
     * @throws HsqlException
     */
    Boolean test(Session session) throws HsqlException {

        switch (exprType) {

            case TRUE :
                return Boolean.TRUE;

            case FALSE :
                return Boolean.FALSE;

            case NOT :
                Trace.doAssert(eArg2 == null, "Expression.test");

                Boolean result = eArg.test(session);

                return result == null ? null
                                      : result.booleanValue() ? Boolean.FALSE
                                                              : Boolean.TRUE;

            case AND : {

                // can't use C style optimization because of NULL
                Boolean r1 = eArg.test(session);

                if (r1 == null) {
                    return null;
                }

                Boolean r2 = eArg2.test(session);

                if (r2 == null) {
                    return null;
                }

                return r1.booleanValue() && r2.booleanValue() ? Boolean.TRUE
                                                              : Boolean.FALSE;
            }
            case OR : {

                // TODO: maybe use C style optimization?
                boolean r1 = Boolean.TRUE.equals(eArg.test(session));

                if (r1) {
                    return Boolean.TRUE;
                }

                return Boolean.TRUE.equals(eArg2.test(session)) ? Boolean.TRUE
                                                                : Boolean
                                                                .FALSE;
            }
            case IS_NULL :
                return eArg.getValue(session) == null ? Boolean.TRUE
                                                      : Boolean.FALSE;

            case LIKE :
                String s = (String) eArg2.getValue(session, Types.VARCHAR);

                if (eArg2.isParam || eArg2.exprType != VALUE) {
                    likeObject.resetPattern(s);
                }

                String c = (String) eArg.getValue(session, Types.VARCHAR);

                return likeObject.compare(c) ? Boolean.TRUE
                                             : Boolean.FALSE;

            case IN :
                return eArg2.testValueList(session, eArg.getValue(session));

            case EXISTS :
                Result r = eArg.subSelect.getResult(session, 1);    // 1 is already enough

                return r.rRoot == null ? Boolean.FALSE
                                       : Boolean.TRUE;

            case FUNCTION :
                Object value =
                    Column.convertObject(function.getValue(session),
                                         Types.BOOLEAN);

                return (Boolean) value;
        }

        if (eArg == null || eArg2 == null) {
            if (exprType == COLUMN) {
                if (dataType == Types.BOOLEAN
                        || Types.isNumberType(dataType)) {
                    Object value = Column.convertObject(getValue(session),
                                                        Types.BOOLEAN);

                    return (Boolean) value;
                }
            }

            throw Trace.error(Trace.NULL_VALUE_AS_BOOLEAN);
        }

        int    type = eArg.dataType;
        Object o    = eArg.getValue(session, type);
        Object o2   = eArg2.getValue(session, type);

        if (o == null || o2 == null) {
            /*
             TableFilter.swapCondition() ensures that with LEFT OUTER, eArg is the
             column expression for the table on the right hand side.
             We do not join tables on nulls apart from outer joins
             Any comparison operator can exist in WHERE or JOIN conditions
             */
            if (eArg.tableFilter != null && eArg.tableFilter.isOuterJoin) {
                if (isInJoin) {
                    if (eArg.tableFilter.isCurrentOuter && o == null) {
                        return Boolean.TRUE;
                    }
                } else {

                    // this is used in WHERE <OUTER JOIN COL> IS [NOT] NULL
                    eArg.tableFilter.nonJoinIsNull = o2 == null;
                }
            }

            return null;
        }

        return compareValues(o, o2, type, exprType) ? Boolean.TRUE
                                                    : Boolean.FALSE;
    }

    private static boolean compareValues(Object o, Object o2, int valueType,
                                         int exprType) throws HsqlException {

        int result = Column.compare(o, o2, valueType);

        switch (exprType) {

            case EQUAL :
                return result == 0;

            case BIGGER :
                return result > 0;

            case BIGGER_EQUAL :
                return result >= 0;

            case SMALLER_EQUAL :
                return result <= 0;

            case SMALLER :
                return result < 0;

            case NOT_EQUAL :
                return result != 0;

            default :
                throw Trace.error(Trace.GENERAL_ERROR,
                                  Trace.Expression_compareValues);
        }
    }

    /**
     * Returns the result of testing a VALUE_LIST expression
     *
     * @param o value to check against
     * @param session context
     * @return boolean
     * @throws HsqlException
     */

// fredt - in the future testValueList can be turned into a join query
// boucherb@users - 2003-09-25 - patch 1.7.2 Alpha Q - parametric IN lists
//                  and correlated IN list expressions
// fredt - catch type conversion exception due to narrowing
    private Boolean testValueList(Session session,
                                  Object o) throws HsqlException {

        if (o == null) {
            return null;
        }

        if (exprType == VALUELIST) {
            try {
                o = Column.convertObject(o, this.dataType);
            } catch (HsqlException e) {
                return Boolean.FALSE;
            }

            if (isFixedConstantValueList) {
                return hList.contains(o) ? Boolean.TRUE
                                         : Boolean.FALSE;
            }

            final int len = valueList.length;

            for (int i = 0; i < len; i++) {
                Object o2 = valueList[i].getValue(session, dataType);

                if (Column.compare(o, o2, dataType) == 0) {
                    return Boolean.TRUE;
                }
            }

            return Boolean.FALSE;
        } else if (exprType == QUERY) {

            /** @todo fredt - convert to join */
            if (subTable != null) {
                try {
                    o = Column.convertObject(o, subTable.getColumnTypes()[0]);
                } catch (HsqlException e) {
                    return Boolean.FALSE;
                }

                return subTable.getPrimaryIndex().findFirst(o, Expression.EQUAL)
                       == null ? Boolean.FALSE
                               : Boolean.TRUE;
            }

            Result r = subSelect.getResult(session, 0);

            // fredt - reduce the size if possible
            r.removeDuplicates();

            Record n    = r.rRoot;
            int    type = r.metaData.colTypes[0];

            try {
                o = Column.convertObject(o, type);
            } catch (HsqlException e) {
                return Boolean.FALSE;
            }

            while (n != null) {
                Object o2 = n.data[0];

                if (o2 != null && Column.compare(o2, o, type) == 0) {
                    return Boolean.TRUE;
                }

                n = n.next;
            }

            return Boolean.FALSE;
        }

        throw Trace.error(Trace.WRONG_DATA_TYPE);
    }

    /**
     * Marks all the expressions in the tree for a condition that is part
     * of a JOIN .. ON ....<br>
     *
     * For LEFT OUTER joins, also tests the expression tree for the join
     * condition to ensure only permitted expression types are there.
     *
     * If we want to exapand the expressions to include arithmetic operations
     * or functions ...
     *
     * (fredt@users)
     *
     * @param tf table filter
     * @param outer boolean
     * @return boolean
     */
    boolean setForJoin(TableFilter tf, boolean outer) {

        isInJoin = outer;

        if (outer) {
            outerFilter = tf;
        }

        if (eArg != null) {
            if (eArg.setForJoin(tf, outer) == false) {
                return false;
            }
        }

        if (eArg2 != null) {
            if (eArg2.setForJoin(tf, outer) == false) {
                return false;
            }
        }

        return !outer
               || (exprType == Expression.AND || exprType == Expression.OR
                   || exprType == Expression.COLUMN
                   || exprType == Expression.VALUE
                   || exprType == Expression.EQUAL
                   || exprType == Expression.NOT_EQUAL
                   || exprType == Expression.BIGGER
                   || exprType == Expression.BIGGER_EQUAL
                   || exprType == Expression.SMALLER
                   || exprType == Expression.SMALLER_EQUAL
                   || exprType == Expression.IS_NULL);
    }

    /**
     * Returns a Select object that can be used for checking the contents
     * of an existing table against the given CHECK search condition.
     *
     * @param t table
     * @param e expression
     * @return select object
     * @throws HsqlException
     */
    static Select getCheckSelect(Table t, Expression e) throws HsqlException {

        Select s = new Select();

        s.exprColumns    = new Expression[1];
        s.exprColumns[0] = new Expression(VALUE, Boolean.TRUE);
        s.tFilter        = new TableFilter[1];
        s.tFilter[0]     = new TableFilter(t, null, false);

        Expression condition = new Expression(NOT, e, null);

        s.queryCondition = condition;

        s.resolveAll(true);

        return s;
    }

    /**
     * Sets the left leaf.
     *
     * @param e expression
     */
    void setLeftExpression(Expression e) {
        eArg = e;
    }

    void setRightExpression(Expression e) {
        eArg2 = e;
    }

    /**
     * Gets the right leaf.
     *
     * @return expression
     */
    Expression getRightExpression() {
        return eArg2;
    }

// boucherb@users 20030417 - patch 1.7.2 - compiled statement support
    void bind(Object o) throws HsqlException {
        valueData = o;
    }

    boolean isParam() {
        return isParam;
    }

    boolean isFixedConstant() {

        switch (exprType) {

            case VALUE :
                return !isParam;

            case NEGATE :
                return eArg.isFixedConstant();

            case ADD :
            case SUBTRACT :
            case MULTIPLY :
            case DIVIDE :
            case CONCAT :
                return eArg.isFixedConstant() && eArg2.isFixedConstant();
        }

        return false;
    }

    boolean isFixedConditional() {

        switch (exprType) {

            case TRUE :
            case FALSE :
                return true;

            case EQUAL :
            case BIGGER_EQUAL :
            case BIGGER :
            case SMALLER :
            case SMALLER_EQUAL :
            case NOT_EQUAL :
            case LIKE :

                //case IN : TODO
                return eArg.isFixedConstant() && eArg2.isFixedConstant();

            case IS_NULL :
                return eArg.isFixedConstant();

            case NOT :
                return eArg.isFixedConditional();

            case AND :
            case OR :
                return eArg.isFixedConditional()
                       && eArg2.isFixedConditional();

            default :
                return false;
        }
    }

    void setTableColumnAttributes(Expression e) {

        columnSize  = e.columnSize;
        columnScale = e.columnScale;
        isIdentity  = e.isIdentity;
        nullability = e.nullability;
        isWritable  = e.isWritable;
        catalog     = e.catalog;
        schema      = e.schema;
    }

    void setTableColumnAttributes(Table t, int i) {

        Column c = t.getColumn(i);

        dataType    = c.getType();
        columnSize  = c.getSize();
        columnScale = c.getScale();
        isIdentity  = c.isIdentity();

        // IDENTITY columns are not nullable but NULLs are accepted
        // and converted into the next identity value for the table
        nullability = c.isNullable() &&!isIdentity ? NULLABLE
                                                   : NO_NULLS;
        isWritable  = t.isWritable();
        catalog     = t.getCatalogName();
        schema      = t.getSchemaName();
    }

    String getValueClassName() {

        int        ditype;
        int        ditypesub;
        DITypeInfo ti;

        if (valueClassName != null) {
            return valueClassName;
        }

        if (function != null) {
            valueClassName = function.getReturnClass().getName();

            return valueClassName;
        }

        if (dataType == Types.VARCHAR_IGNORECASE) {
            ditype    = Types.VARCHAR;
            ditypesub = Types.TYPE_SUB_IGNORECASE;
        } else {
            ditype    = dataType;
            ditypesub = Types.TYPE_SUB_DEFAULT;
        }

        ti = new DITypeInfo();

        ti.setTypeCode(ditype);
        ti.setTypeSub(ditypesub);

        valueClassName = ti.getColStClsName();

        return valueClassName;
    }

    // parameter modes
    static final int        PARAM_UNKNOWN = 0;
    public static final int PARAM_IN      = 1;
    public static final int PARAM_IN_OUT  = 2;
    public static final int PARAM_OUT     = 4;

    // result set (output column value) or parameter expression nullability
    static final int NO_NULLS         = 0;
    static final int NULLABLE         = 1;
    static final int NULLABLE_UNKNOWN = 2;

    // output column and parameter expression metadata values
    boolean isIdentity;        // = false
    int     nullability = NULLABLE_UNKNOWN;
    boolean isWritable;        // = false; true if column of writable table
    int     paramMode = PARAM_UNKNOWN;
    String  valueClassName;    // = null

// boucherb@users 20040111 - patch 1.7.2 RC1 - metadata xxxusage support
//-------------------------------------------------------------------
    // TODO:  Maybe provide an interface or abstract class + a default
    // implementation instead?  This would allow a more powerful system
    // of collectors to be created, for example to assist in the optimization
    // of condition expression trees:
    //
    // HashSet joins = new JoinConditionCollector();
    // joins.addAll(select.whereCondition);
    // for(Iterator it = joins.iterator(); it.hasNext();) {
    //      process((it.next());
    // }

    /**
     * Provides a generic way to collect a set of distinct expressions
     * of some type from a tree rooted at a specified Expression.
     */
    static class Collector extends HashSet {

        Collector() {
            super();
        }

        void addAll(Expression e, int type) {

            Function     function;
            Expression[] list;

            if (e == null) {
                return;
            }

            addAll(e.getArg(), type);
            addAll(e.getArg2(), type);

            // CHECKME: What about setTrue() Expressions?
            if (e.exprType == type) {
                add(e);
            }

            addAll(e.subSelect, type);

            function = e.function;

            if (function != null) {
                list = function.eArg;

                if (list != null) {
                    for (int i = 0; i < list.length; i++) {
                        addAll(list[i], type);
                    }
                }
            }

            list = e.valueList;

            if (list != null) {
                for (int i = 0; i < list.length; i++) {
                    addAll(list[i], type);
                }
            }
        }

        void addAll(Select select, int type) {

            for (; select != null; select = select.unionSelect) {
                Expression[] list = select.exprColumns;

                for (int i = 0; i < list.length; i++) {
                    addAll(list[i], type);
                }

                addAll(select.queryCondition, type);
                addAll(select.havingCondition, type);

                // todo order by columns
            }
        }
    }
}
