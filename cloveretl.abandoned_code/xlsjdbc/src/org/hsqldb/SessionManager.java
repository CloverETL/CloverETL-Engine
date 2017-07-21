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
import org.hsqldb.lib.Iterator;

/**
 * Container that maintains a map of session id's to Session objects.
 * Responsible for managing opening and closing of sessions.
 *
 * @author fredt@usrs
 * @since 1.7.2
 * @version 1.7.2
 */
public class SessionManager {

    int                   sessionIdCount;
    private IntKeyHashMap sessionMap = new IntKeyHashMap();
    Session               sysSession;

// TODO:
//
// Eliminate the Database-centric nature of SessionManager.
// e.g. Sessions should be able to migrate from one Database instance
// to another using session control language moderated by
// SessionManager

    /**
     * Constructs an new SessionManager handling the specified Database using
     * the specified SYS User
     */
    public SessionManager(Database db, User sysUser) {
        sysSession = newSession(db, sysUser, false);
    }

// TODO:
//
// It should be possible to create an initially 'disconnected' Session that
// can execute general commands using a SessionCommandInterpreter.
//
// EXAMPLES: Open a Session to start a Server, add/remove
//           databases hosted by an existing Server, connect to a
//           Database...
//
// REQUIRES:  auth scheme independent of any particular Database instance
//            e.g. provide service to use /etc/passwd and /etc/groups,
//                 JAAS-plugin, etc.

    /**
     *  Binds the specified Session object into this SessionManager's active
     *  Session registry. This method is typically called internally from
     * {@link
     *  Database#connect(String,String) Database.connect(username,password)}
     *  as the final step, when a successful connection has been made.
     *
     * @param db the database to which the new Session is initially connected
     * @param user the initial Session User
     * @param readonly the initial ReadOnly attribute for the new Session
     */
    public Session newSession(Database db, User user, boolean readonly) {

        Session s = new Session(db, user, true, readonly, sessionIdCount);

        sessionMap.put(sessionIdCount, s);

        sessionIdCount++;

        return s;
    }

// TODO:
// sig change should be either:  getSysSession(Database) or getSysSession(dbID)

    /**
     * Retrieves the special SYS Session.
     *
     * @return the special SYS Session
     */
    public Session getSysSession() {
        return sysSession;
    }

// TODO:
// sig change should be either:  closeAllSessions(Database) or closeAllSessions(dbID)

    /**
     * Closes all Sessions registered with this SessionManager.
     */
    public void closeAllSessions() {

        // don't disconnect system user; need it to save database
        Iterator it = sessionMap.values().iterator();

        for (; it.hasNext(); ) {
            Session s = (Session) it.next();

            if (s != sysSession) {
                it.remove();
                s.close();
            }
        }
    }

    /**
     *  Removes the session from management and disconnects.
     *
     * @param  session to disconnect
     */
    void removeSession(Session session) {
        sessionMap.remove(session.getId());
    }

    /**
     * Removes all Sessions registered with this SessionManager.
     */
    void clearAll() {
        sessionMap.clear();
    }

    /**
     * Retrieves a list of the Sessions in this container that
     * are visible to the specified Session, given the access rights of
     * the Session User.
     *
     * @param session The Session determining visibility
     * @return the Sessions visible to the specified Session
     */
    Session[] getVisibleSessions(Session session) {
        return session.isAdmin() ? getAllSessions()
                                 : new Session[]{ session };
    }

    /**
     * Retrieves the Session with the specified Session identifier or null
     * if no such Session is registered with this SessionManager.
     */
    Session getSession(int id) {
        return (Session) sessionMap.get(id);
    }

    Session[] getAllSessions() {

        Session[] sessions = new Session[sessionMap.size()];
        Iterator  it       = sessionMap.values().iterator();

        for (int i = 0; it.hasNext(); i++) {
            sessions[i] = (Session) it.next();
        }

        return sessions;
    }
}
