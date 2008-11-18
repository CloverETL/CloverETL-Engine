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

import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.HsqlNameManager.HsqlName;

/**
 * Script generation.
 *
 * @version 1.7.2
 */
public class DatabaseScript {

    /**
     * Returns the DDL and all other statements for the database excluding
     * INSERT and SET <tablename> READONLY statements.
     * bCached == true indicates that SET <tablenmae> INDEX statements should
     * also be included.
     *
     * This class should not have any dependencies on metadata reporting.
     */
    public static Result getScript(Database dDatabase, boolean bCached) {

        HsqlArrayList tTable          = dDatabase.getTables();
        HsqlArrayList forwardFK       = new HsqlArrayList();
        HsqlArrayList forwardFKSource = new HsqlArrayList();
        Result r = Result.newSingleColumnResult("COMMAND", Types.VARCHAR);

        r.metaData.tableNames[0] = "SYSTEM_SCRIPT";

        // sequences
        /*
        CREATE SEQUENCE <name>
        [AS {INTEGER | BIGINT}]
        [START WITH <value>]
        [INCREMENT BY <value>]
        */
        HashMappedList seqmap = dDatabase.sequenceManager.sequenceMap;

        for (int i = 0, sSize = seqmap.size(); i < sSize; i++) {
            NumberSequence seq = (NumberSequence) seqmap.get(i);
            StringBuffer   a   = new StringBuffer(128);

            a.append(Token.T_CREATE).append(' ');
            a.append(Token.T_SEQUENCE).append(' ');
            a.append(seq.getName().statementName).append(' ');
            a.append(Token.T_AS).append(' ');
            a.append(Types.getTypeString(seq.getType())).append(' ');
            a.append(Token.T_START).append(' ');
            a.append(Token.T_WITH).append(' ');
            a.append(seq.peek()).append(' ');

            if (seq.getIncrement() != 1) {
                a.append(Token.T_INCREMENT).append(' ');
                a.append(Token.T_BY).append(' ');
                a.append(seq.getIncrement()).append(' ');
            }

            addRow(r, a.toString());
        }

        // tables
        for (int i = 0, tSize = tTable.size(); i < tSize; i++) {
            Table t = (Table) tTable.get(i);

            if (t.isTemp() || t.isView()) {
                continue;
            }

            StringBuffer a = new StringBuffer(128);

            getTableDDL(dDatabase, t, i, forwardFK, forwardFKSource, a);
            addRow(r, a.toString());

            // indexes for table
            for (int j = 1; j < t.getIndexCount(); j++) {
                Index index = t.getIndex(j);

                if (HsqlName.isReservedIndexName(index.getName().name)) {

                    // the following are autocreated with the table
                    // indexes for primary keys
                    // indexes for unique constraints
                    // own table indexes for foreign keys
                    continue;
                }

                a = new StringBuffer(64);

                a.append(Token.T_CREATE).append(' ');

                if (index.isUnique()) {
                    a.append(Token.T_UNIQUE).append(' ');
                }

                a.append(Token.T_INDEX).append(' ');
                a.append(index.getName().statementName);
                a.append(' ').append(Token.T_ON).append(' ');
                a.append(t.getName().statementName);

                int col[] = index.getColumns();
                int len   = index.getVisibleColumns();

                getColumnList(t, col, len, a);
                addRow(r, a.toString());
            }

            // readonly for TEXT tables only
            if (t.isText() && t.isDataReadOnly()) {
                a = new StringBuffer(64);

                a.append(Token.T_SET).append(' ').append(
                    Token.T_TABLE).append(' ');
                a.append(t.getName().statementName);
                a.append(' ').append(Token.T_READONLY).append(' ').append(
                    Token.T_TRUE);
                addRow(r, a.toString());
            }

            // data source
            String dataSource = getDataSource(t);

            if (dataSource != null) {
                addRow(r, dataSource);
            }

            // triggers
            int numTrigs = TriggerDef.NUM_TRIGS;

            for (int tv = 0; tv < numTrigs; tv++) {
                HsqlArrayList trigVec = t.triggerLists[tv];

                if (trigVec == null) {
                    continue;
                }

                int trCount = trigVec.size();

                for (int k = 0; k < trCount; k++) {
                    a = ((TriggerDef) trigVec.get(k)).getDDL();

                    addRow(r, a.toString());
                }
            }
        }

        // forward referencing foreign keys
        for (int i = 0, tSize = forwardFK.size(); i < tSize; i++) {
            Constraint   c = (Constraint) forwardFK.get(i);
            StringBuffer a = new StringBuffer(128);

            a.append(Token.T_ALTER).append(' ').append(Token.T_TABLE).append(
                ' ');
            a.append(c.getRef().getName().statementName);
            a.append(' ').append(Token.T_ADD).append(' ');
            getFKStatement(c, a);
            addRow(r, a.toString());
        }

        // SET <tablename> INDEX statements
        for (int i = 0, tSize = tTable.size(); i < tSize; i++) {
            Table t = (Table) tTable.get(i);

            if (bCached && t.isIndexCached() &&!t.isEmpty()) {
                addRow(r, getIndexRootsDDL((Table) tTable.get(i)));
            }
        }

        // ignorecase for future CREATE TABLE statements
        if (dDatabase.isIgnoreCase()) {
            addRow(r, "SET IGNORECASE TRUE");
        }

        // aliases
        HashMap  h       = dDatabase.getAliasMap();
        HashMap  builtin = Library.getAliasMap();
        Iterator it      = h.keySet().iterator();

        while (it.hasNext()) {
            String alias  = (String) it.next();
            String java   = (String) h.get(alias);
            String biJava = (String) builtin.get(alias);

            if (biJava != null && biJava.equals(java)) {
                continue;
            }

            StringBuffer buffer = new StringBuffer(64);

            buffer.append(Token.T_CREATE).append(' ').append(
                Token.T_ALIAS).append(' ');
            buffer.append(alias);
            buffer.append(" FOR \"");
            buffer.append(java);
            buffer.append('"');
            addRow(r, buffer.toString());
        }

        // views
        for (int i = 0, tSize = tTable.size(); i < tSize; i++) {
            Table t = (Table) tTable.get(i);

            if (t.isView()) {
                View         v = (View) tTable.get(i);
                StringBuffer a = new StringBuffer(128);

                a.append(Token.T_CREATE).append(' ').append(
                    Token.T_VIEW).append(' ');
                a.append(v.getName().statementName).append(' ').append('(');

                int count = v.getColumnCount();

                for (int j = 0; j < count; j++) {
                    a.append(v.getColumn(j).columnName.statementName);

                    if (j < count - 1) {
                        a.append(',');
                    }
                }

                a.append(')').append(' ').append(Token.T_AS).append(' ');
                a.append(v.getStatement());
                addRow(r, a.toString());
            }
        }

        // rights for classes, tables and views
        addRightsStatements(dDatabase, r);

        if (dDatabase.logger.hasLog()) {
            int    delay     = dDatabase.logger.lLog.writeDelay;
            String statement = "SET WRITE_DELAY " + delay;

            addRow(r, statement);
        }

        return r;
    }

    static String getIndexRootsDDL(Table t) {

        StringBuffer a = new StringBuffer(128);

        a.append(Token.T_SET).append(' ').append(Token.T_TABLE).append(' ');
        a.append(t.getName().statementName);
        a.append(' ').append(Token.T_INDEX).append('\'');
        a.append(t.getIndexRoots());
        a.append('\'');

        return a.toString();
    }

    static void getTableDDL(Database dDatabase, Table t, int i,
                            HsqlArrayList forwardFK,
                            HsqlArrayList forwardFKSource, StringBuffer a) {

        a.append(Token.T_CREATE).append(' ');

        if (t.isText()) {
            a.append(Token.T_TEXT).append(' ');
        } else if (t.isCached()) {
            a.append(Token.T_CACHED).append(' ');
        }

        a.append(Token.T_TABLE).append(' ');
        a.append(t.getName().statementName);
        a.append('(');

        int   columns = t.getColumnCount();
        Index pki     = t.getIndex(0);
        int   pk[]    = pki.getColumns();

        for (int j = 0; j < columns; j++) {
            Column column  = t.getColumn(j);
            String colname = column.columnName.statementName;

            a.append(colname);
            a.append(' ');

            String sType = Types.getTypeString(column.getType());

            a.append(sType);

            // append the size and scale if > 0
            if (column.getSize() > 0) {
                a.append('(');
                a.append(column.getSize());

                if (column.getScale() > 0) {
                    a.append(',');
                    a.append(column.getScale());
                }

                a.append(')');
            }

            String defaultString = column.getDefaultDDL();

            if (defaultString != null) {
                a.append(' ').append(Token.T_DEFAULT).append(' ');
                a.append(defaultString);
            }

            if (j == t.getIdentityColumn()) {
                a.append(" GENERATED BY DEFAULT AS IDENTITY(START WITH ");
                a.append(column.identityStart);

                if (column.identityIncrement != 1) {
                    a.append(Token.T_COMMA).append(Token.T_INCREMENT).append(
                        ' ').append(Token.T_BY).append(' ');
                    a.append(column.identityIncrement);
                }

                a.append(") ");
            }

            if (!column.isNullable()) {
                a.append(' ').append(Token.T_NOT).append(' ').append(
                    Token.T_NULL);
            }

            if ((pk.length == 1) && (j == pk[0])
                    && pki.getName().isReservedIndexName()) {
                a.append(' ').append(Token.T_PRIMARY).append(' ').append(
                    Token.T_KEY);
            }

            if (j < columns - 1) {
                a.append(',');
            }
        }

        if (pk.length > 1 ||!pki.getName().isReservedIndexName()) {
            a.append(',').append(Token.T_CONSTRAINT).append(' ');
            a.append(pki.getName().statementName);
            a.append(' ').append(Token.T_PRIMARY).append(' ').append(
                Token.T_KEY);
            getColumnList(t, pk, pk.length, a);
        }

        Constraint[] v = t.getConstraints();

        for (int j = 0, vSize = v.length; j < vSize; j++) {
            Constraint c = v[j];

            switch (c.getType()) {

                case Constraint.UNIQUE :
                    a.append(',').append(Token.T_CONSTRAINT).append(' ');
                    a.append(c.getName().statementName);
                    a.append(' ').append(Token.T_UNIQUE);

                    int col[] = c.getMainColumns();

                    getColumnList(c.getMain(), col, col.length, a);
                    break;

                case Constraint.FOREIGN_KEY :

                    // forward referencing FK
                    Table maintable      = c.getMain();
                    int   maintableindex = dDatabase.getTableIndex(maintable);

                    if (maintableindex > i) {
                        if (i >= forwardFKSource.size()) {
                            forwardFKSource.setSize(i + 1);
                        }

                        forwardFKSource.set(i, c);
                        forwardFK.add(c);
                    } else {
                        a.append(',');
                        getFKStatement(c, a);
                    }
                    break;

                case Constraint.CHECK :
                    try {
                        a.append(',').append(Token.T_CONSTRAINT).append(' ');
                        a.append(c.getName().statementName);
                        a.append(' ').append(Token.T_CHECK).append('(');
                        a.append(c.core.check.getDDL());
                        a.append(')');
                    } catch (HsqlException e) {

                        // should not throw as it is already tested OK
                    }
                    break;
            }
        }

        a.append(')');
    }

    /**
     * Generates the SET TABLE <tablename> SOURCE <string> statement for a
     * text table;
     */
    static String getDataSource(Table t) {

        String dataSource = t.getDataSource();

        if (dataSource == null) {
            return null;
        }

        boolean      isDesc = t.isDescDataSource();
        StringBuffer a      = new StringBuffer(128);

        a.append(Token.T_SET).append(' ').append(Token.T_TABLE).append(' ');
        a.append(t.getName().statementName);
        a.append(' ').append(Token.T_SOURCE).append('"');
        a.append(dataSource);
        a.append('"');

        if (isDesc) {
            a.append(' ').append(Token.T_DESC);
        }

        return a.toString();
    }

    /**
     * Generates the column definitions for a table.
     */
    private static void getColumnList(Table t, int col[], int len,
                                      StringBuffer a) {

        a.append('(');

        for (int i = 0; i < len; i++) {
            a.append(t.getColumn(col[i]).columnName.statementName);

            if (i < len - 1) {
                a.append(',');
            }
        }

        a.append(')');
    }

    /**
     * Generates the foreign key declaration for a given Constraint object.
     */
    private static void getFKStatement(Constraint c, StringBuffer a) {

        a.append(Token.T_CONSTRAINT).append(' ');
        a.append(c.getName().statementName);
        a.append(' ').append(Token.T_FOREIGN).append(' ').append(Token.T_KEY);

        int col[] = c.getRefColumns();

        getColumnList(c.getRef(), col, col.length, a);
        a.append(' ').append(Token.T_REFERENCES).append(' ');
        a.append(c.getMain().getName().statementName);

        col = c.getMainColumns();

        getColumnList(c.getMain(), col, col.length, a);

        if (c.getDeleteAction() != Constraint.NO_ACTION) {
            a.append(' ').append(Token.T_ON).append(' ').append(
                Token.T_DELETE).append(' ');
            a.append(getFKAction(c.getDeleteAction()));
        }

        if (c.getUpdateAction() != Constraint.NO_ACTION) {
            a.append(' ').append(Token.T_ON).append(' ').append(
                Token.T_UPDATE).append(' ');
            a.append(getFKAction(c.getUpdateAction()));
        }
    }

    /**
     * Returns the foreign key action rule.
     */
    private static String getFKAction(int action) {

        switch (action) {

            case Constraint.CASCADE :
                return Token.T_CASCADE;

            case Constraint.SET_DEFAULT :
                return Token.T_SET + ' ' + Token.T_DEFAULT;

            case Constraint.SET_NULL :
                return Token.T_SET + ' ' + Token.T_NULL;

            default :
                return Token.T_NO + ' ' + Token.T_ACTION;
        }
    }

    /**
     * Adds a script line to the result.
     */
    private static void addRow(Result r, String sql) {

        String s[] = new String[1];

        s[0] = sql;

        r.add(s);
    }

    /**
     * Generates the GRANT statements for users.
     *
     * When views is true, generates rights for views only. Otherwise generates
     * rights for tables and classes.
     *
     * Does not generate script for:
     *
     * grant on builtin classes to public
     * grant select on system tables
     *
     */
    private static void addRightsStatements(Database dDatabase, Result r) {

        StringBuffer   a;
        HashMappedList uv = dDatabase.getUserManager().getUsers();
        Iterator       it = uv.values().iterator();

        for (; it.hasNext(); ) {
            User   u    = (User) it.next();
            String name = u.getName();

            if (!name.equals(Token.T_PUBLIC)) {
                addRow(r, u.getCreateUserDDL());
            }

            IntValueHashMap rights = u.getRights();

            if (rights == null) {
                continue;
            }

            Iterator e = rights.keySet().iterator();

            while (e.hasNext()) {
                Object object = e.next();
                int    right  = rights.get(object, 0);

                a = new StringBuffer(64);

                a.append(Token.T_GRANT).append(' ');
                a.append(UserManager.getRight(right));
                a.append(' ').append(Token.T_ON).append(' ');

                if (object instanceof String) {
                    if (object.equals("java.lang.Math")
                            || object.equals("org.hsqldb.Library")) {
                        continue;
                    }

                    a.append("CLASS \"");
                    a.append((String) object);
                    a.append('\"');
                } else {

                    // either table != null or is system table
                    Table table =
                        dDatabase.findUserTable(((HsqlName) object).name);

                    // assumes all non String objects are table names
                    if (table != null) {
                        a.append(((HsqlName) object).statementName);
                    } else {
                        continue;
                    }
                }

                a.append(' ').append(Token.T_TO).append(' ');
                a.append(u.getName());
                addRow(r, a.toString());
            }
        }
    }
}
