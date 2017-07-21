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

import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.IntKeyIntValueHashMap;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.lib.Iterator;

/**
 * This class manages the reuse of CompiledStatement objects for prepared
 * statements for a Database instance.<p>
 *
 * A compiled statement is registered by a session to be managed. Once
 * registered, it is linked with one or more sessions.<p>
 *
 * The sql statement text distinguishes different compiled statements and acts
 * as lookup key when a session initially looks for an existing instance of
 * the compiled sql statement.<p>
 *
 * Once a session is linked with a statement, it uses the uniqe compiled
 * statement id for the sql statement to access the statement.<p>
 *
 * Changes to database structure via DDL statements, will result in all
 * registered CompiledStatement objects to become invalidated. This is done by
 * setting to null all the managed CompiledStatement instances, while keeping
 * their id and sql string. When a session subsequently attempts to use an
 * invalidated (null) CompiledStatement via its id, it will reinstantiate the
 * CompiledStatement using its sql statement still held by this class.<p>
 *
 * This class keeps count of the number of different sessions that are linked
 * to each registered compiled statement, and the number of times each session
 * is linked.  It unregisters a compiled statement when no session remains
 * linked to it.<p>
 *
 * Modified by fredt@users from the original by boucherb@users to simplify,
 * support multiple identical prepared statements per session, and avoid
 * keeping references to CompiledStatement objects after DDL changes which
 * could result in memory leaks. <p>
 *
 *
 * @author boucherb@users
 * @author fredt@users
 *
 * @since 1.7.2
 * @version 1.7.2
 */
final class CompiledStatementManager {

    /**
     * The Database for which this object is managing
     * CompiledStatement objects.
     */
    Database database;

    /** Map:  SQL String => Compiled Statement id (int) */
    IntValueHashMap sqlMap;

    /** Map: Compiled Statement id (int) => SQL String */
    IntKeyHashMap sqlLookup;

    /** Map: Compiled statment id (int) => CompiledStatement object. */
    IntKeyHashMap csidMap;

    /** Map: Session id (int) => Map: compiled statement id (int) => use count in session; */
    IntKeyHashMap sessionMap;

    /** Map: Compiled statment id (int) => total use count (all sessions) */
    IntKeyIntValueHashMap useMap;

    /**
     * Monotonically increasing counter used to assign unique ids to compiled
     * statements.
     */
    private int next_cs_id;

    /**
     * Constructs a new instance of <code>CompiledStatementManager</code>.
     *
     * @param database the Database instance for which this object is to
     *      manage compiled statement objects.
     */
    CompiledStatementManager(Database database) {

        this.database = database;
        sqlMap        = new IntValueHashMap();
        sqlLookup     = new IntKeyHashMap();
        csidMap       = new IntKeyHashMap();
        sessionMap    = new IntKeyHashMap();
        useMap        = new IntKeyIntValueHashMap();
        next_cs_id    = 0;
    }

    /**
     * Clears all internal data structures, removing any references to compiled statements.
     */
    synchronized void reset() {

        sqlMap.clear();
        sqlLookup.clear();
        csidMap.clear();
        sessionMap.clear();
        useMap.clear();

        next_cs_id = 0;
    }

    /**
     * Used after a DDL change that could impact the compiled statements.
     * Clears references to CompiledStatement objects while keeping the counts
     * and references to the sql strings.
     */
    synchronized void resetStatements() {

        Iterator it = csidMap.keySet().iterator();

        while (it.hasNext()) {
            int key = it.nextInt();

            csidMap.put(key, null);
        }
    }

    /**
     * Retrieves the next compiled statement identifier in the sequence.
     *
     * @return the next compiled statement identifier in the sequence.
     */
    private int nextID() {

        next_cs_id++;

        return next_cs_id;
    }

    /**
     * Retrieves the registered compiled statement identifier associated with
     * the specified SQL String, or a value less than zero, if no such
     * statement has been registered.
     *
     * @param sql the SQL String
     * @return the compiled statement identifier associated with the
     *      specified SQL String
     */
    synchronized int getStatementID(String sql) {
        return sqlMap.get(sql, -1);
    }

    /**
     * Retrieves the CompiledStatement object having the specified compiled
     * statement identifier, or null if the CompiledStatement object
     * has been invalidated.
     *
     * @param csid the identifier of the requested CompiledStatement object
     * @return the requested CompiledStatement object
     */
    synchronized CompiledStatement getStatement(int csid) {
        return (CompiledStatement) csidMap.get(csid);
    }

    /**
     * Retrieves the sql statement for a registered compiled statement.
     *
     * @param csid the compiled statement identifier
     * @return sql string
     */
    synchronized String getSql(int csid) {
        return (String) sqlLookup.get(csid);
    }

    /**
     * Links a session with a registered compiled statement.
     *
     * If this session has not already been linked with the given
     * statement, then the statement use count is incremented.
     *
     * @param csid the compiled statement identifier
     * @param sid the session identifier
     */
    synchronized void linkSession(int csid, int sid) {

        IntKeyIntValueHashMap scsMap;

        scsMap = (IntKeyIntValueHashMap) sessionMap.get(sid);

        if (scsMap == null) {
            scsMap = new IntKeyIntValueHashMap();

            sessionMap.put(sid, scsMap);
        }

        int count = scsMap.get(csid, 0);

        scsMap.put(csid, count + 1);

        if (count == 0) {
            useMap.put(csid, useMap.get(csid, 0) + 1);
        }
    }

    /**
     * Registers a compiled statement to be managed.
     *
     * The only caller should be a Session that is attempting to prepare
     * a statement for the first time or process a statement that has been
     * invalidated due to DDL changes.
     *
     * @param csid existing id or negative if the statement is not yet managed
     * @param cs The CompiledStatement to add
     * @return The compiled statement id assigned to the CompiledStatement
     *  object
     */
    synchronized int registerStatement(int csid, CompiledStatement cs) {

        if (csid < 0) {
            csid = nextID();

            sqlMap.put(cs.sql, csid);
            sqlLookup.put(csid, cs.sql);
        }

        csidMap.put(csid, cs);

        return csid;
    }

    /**
     * Removes the link between a session and a compiled statement.
     *
     * If the statement is not linked with any other session, it is removed
     * from management.
     *
     * @param csid the compiled statment identifier
     * @param sid the session identifier
     */
    synchronized void freeStatement(int csid, int sid) {

        IntKeyIntValueHashMap scsMap =
            (IntKeyIntValueHashMap) sessionMap.get(sid);
        int count = scsMap.get(csid) - 1;

        if (count != 0) {
            scsMap.put(csid, count);
        } else {
            scsMap.remove(csid);

            int usecount = useMap.get(csid, 1) - 1;

            if (usecount == 0) {
                String sql = (String) sqlLookup.remove(csid);

                sqlMap.remove(sql);
                csidMap.remove(csid);
                useMap.remove(csid);
            } else {
                useMap.put(csid, usecount);
            }
        }
    }

    /**
     * Releases the link betwen the session and all compiled statement objects
     * it is linked to.
     *
     * If any such statement is not linked with any other session, it is
     * removed from management.
     *
     * @param sid the session identifier
     */
    synchronized void removeSession(int sid) {

        IntKeyIntValueHashMap scsMap;
        int                   csid;
        Iterator              i;

        scsMap = (IntKeyIntValueHashMap) sessionMap.remove(sid);

        if (scsMap == null) {
            return;
        }

        i = scsMap.keySet().iterator();

        while (i.hasNext()) {
            csid = i.nextInt();

            int usecount = useMap.get(csid, 1) - 1;

            if (usecount == 0) {
                String sql = (String) sqlLookup.remove(csid);

                sqlMap.remove(sql);
                csidMap.remove(csid);
                useMap.remove(csid);
            } else {
                useMap.put(csid, usecount);
            }
        }
    }
}
