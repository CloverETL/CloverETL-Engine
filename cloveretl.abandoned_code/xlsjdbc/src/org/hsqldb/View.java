/* Copyright (c) 2001-2004, The HSQL Development Group
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

import org.hsqldb.lib.Iterator;
import org.hsqldb.HsqlNameManager.HsqlName;

// fredt@users 20020420 - patch523880 by leptipre@users - VIEW support - modified
// fredt@users 20031227 - remimplementated as compiled query

/**
 * Represents an SQL VIEW based on a SELECT statement.
 *
 * @author leptipre@users
 * @version 1.7.2
 * @since 1.7.0
 */
class View extends Table {

    Table              workingTable;
    Select             viewSelect;
    SubQuery           viewSubQuery;
    private String     statement;
    private HsqlName[] colList;

    /**
     * List of subqueries in this view in order of materialization. Last
     * element is the view itself.
     */
    SubQuery[] viewSubqueries;

    /**
     * Constructor.
     *
     * @param db database
     * @param name HsqlName of the view
     * @param definition SELECT statement of the view
     * @param columns array of HsqlName column names
     * @throws HsqlException
     */
    View(Database db, HsqlName name, String definition,
            HsqlName[] columns) throws HsqlException {

        super(db, name, VIEW, 0);

        isReadOnly = true;
        colList    = columns;
        statement  = trimStatement(definition);

        compile();
    }

    /**
     * Returns the SELECT statement trimmed of any terminating SQL
     * whitespace, separators or SQL comments.
     */
    static String trimStatement(String s) throws HsqlException {

        int       position;
        String    str;
        Tokenizer tokenizer = new Tokenizer(s);

        // fredt@users - this establishes the end of the actual statement
        // to get rid of any end semicolon or comment line after the end
        // of statement
        do {
            position = tokenizer.getPosition();
            str      = tokenizer.getString();
        } while (str.length() != 0 || tokenizer.wasValue());

        return s.substring(0, position).trim();
    }

    /**
     * Compiles the SELECT statement and sets up the columns.
     */
    void compile() throws HsqlException {

        // create the working table
        Tokenizer tokenizer = new Tokenizer(statement);
        int       brackets  = 0;

        if (tokenizer.isGetThis(Token.T_OPENBRACKET)) {
            brackets += Parser.parseOpenBrackets(tokenizer) + 1;
        }

        tokenizer.getThis(Token.T_SELECT);

        Parser p = new Parser(database.sessionManager.getSysSession(),
                              this.database, tokenizer);

        viewSubQuery = p.parseSubquery(brackets, colList, true,
                                       Expression.QUERY);

        p.setAsView(this);

        viewSubqueries = p.getSortedSubqueries();
        workingTable   = viewSubQuery.table;
        viewSelect     = viewSubQuery.select;

        viewSelect.prepareResult();

        Result.ResultMetaData metadata = viewSelect.resultMetaData;
        int                   columns  = viewSelect.iResultLen;

        if (super.columnCount == 0) {

            // do not add columns at recompile time
            super.addColumns(metadata, columns);
        }
    }

    /**
     * Returns the SELECT statement for the view.
     */
    String getStatement() {
        return statement;
    }

    /**
     * Overridden to disable SET TABLE READONLY DDL for View objects.
     */
    void setDataReadOnly(boolean value) throws HsqlException {
        throw Trace.error(Trace.NOT_A_TABLE);
    }

    boolean hasView(View view) {

        if (view == this) {
            return false;
        }

        for (int i = 0; i < viewSubqueries.length; i++) {
            if (viewSubqueries[i].view == view) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns true if the view references any column of the named table.
     */
    boolean hasTable(String table) {

        for (int i = 0; i < viewSubqueries.length; i++) {
            Select select = viewSubqueries[i].select;

            for (; select != null; select = select.unionSelect) {
                TableFilter tfilter[] = select.tFilter;

                for (int j = 0; j < tfilter.length; j++) {
                    if (table.equals(tfilter[j].filterTable.tableName.name)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Returns true if the view references the named column of the named table,
     * otherwise false.
     */
    boolean hasColumn(String table, String colname) {

        if (hasTable(table)) {
            Expression.Collector coll = new Expression.Collector();

            coll.addAll(viewSubqueries[viewSubqueries.length - 1].select,
                        Expression.COLUMN);

            Iterator it = coll.iterator();

            for (; it.hasNext(); ) {
                Expression e = (Expression) it.next();

                if (e.getColumnName().equals(colname)
                        && table.equals(e.getTableName())) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Returns true if the view references the named SEQUENCE,
     * otherwise false.
     */
    boolean hasSequence(NumberSequence sequence) {

        Expression.Collector coll = new Expression.Collector();

        coll.addAll(viewSubqueries[viewSubqueries.length - 1].select,
                    Expression.SEQUENCE);

        Iterator it = coll.iterator();

        for (; it.hasNext(); ) {
            Expression e = (Expression) it.next();

            if (e.valueData == sequence) {
                return true;
            }
        }

        return false;
    }
}
