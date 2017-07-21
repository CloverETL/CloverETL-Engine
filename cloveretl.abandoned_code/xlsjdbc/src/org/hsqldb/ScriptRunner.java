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

import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.StopWatch;
import org.hsqldb.scriptio.ScriptReaderBase;

/**
 * Restores the state of a Database instance from an SQL script. <p>
 *
 * The script file may be in one of several allowed encodings, currently
 * including plain text, binary format and compressed binary format.
 *
 * @author fredt@users
 * @version 1.7.2
 */
public class ScriptRunner {

    /**
     *  This is used to read the *.log file and manage any necessary
     *  transaction rollback.
     *
     * @throws  HsqlException
     */
    public static void runScript(Database database, String logFilename,
                                 int logType) throws HsqlException {

        IntKeyHashMap sessionMap = new IntKeyHashMap();
        Session sysSession = database.getSessionManager().getSysSession();
        Session       current    = sysSession;
        int           currentId  = 0;

        database.setReferentialIntegrity(false);

        ScriptReaderBase scr = null;

        try {
            StopWatch sw = new StopWatch();

            scr = ScriptReaderBase.newScriptReader(database, logFilename,
                                                   logType);

            while (true) {
                String s = scr.readLoggedStatement();

                if (s == null) {
                    break;
                }

                if (s.startsWith("/*C")) {
                    currentId = Integer.parseInt(s.substring(3,
                            s.indexOf('*', 4)));
                    current = (Session) sessionMap.get(currentId);

                    if (current == null) {
                        current =
                            database.getSessionManager().newSession(database,
                                sysSession.getUser(), false);

                        sessionMap.put(currentId, current);
                    }

                    s = s.substring(s.indexOf('/', 1) + 1);
                }

                if (s.length() != 0 &&!current.isClosed()) {
                    Result result = current.sqlExecuteDirectNoPreChecks(s);

                    if (result != null
                            && result.mode == ResultConstants.ERROR) {

                        // catch out-of-memory errors and terminate
                        if (result.getStatementID() == Trace.OUT_OF_MEMORY) {
                            Trace.printSystemOut("out of memory processing "
                                                 + logFilename + " line: "
                                                 + scr.getLineNumber());

                            throw Trace.error(result);
                        }

/** @todo fredt - must display the error through different method as printSystemOut does not normally print */

                        // stop processing on bad log line
                        Trace.printSystemOut("error in " + logFilename
                                             + " line: "
                                             + scr.getLineNumber());
                        Trace.printSystemOut(result.getMainString());

                        break;
                    }

                    if (current.isClosed()) {
                        sessionMap.remove(currentId);
                    }
                }
            }
        } catch (IOException e) {
            Trace.printSystemOut("error in " + logFilename);
        } finally {
            scr.close();
            database.getSessionManager().closeAllSessions();
            database.setReferentialIntegrity(true);
        }
    }
}
