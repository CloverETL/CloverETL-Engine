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


package org.hsqldb.jdbc;

import java.sql.SQLException;

import org.hsqldb.HsqlException;
import org.hsqldb.Result;
import org.hsqldb.Trace;

/**
 * Provides driver constants and a gateway from internal HsqlExceptions to
 * external SQLExceptions.
 *
 * @author fredt@users
 * @version 1.7.2
 * @since 1.7.2
 */
public class jdbcUtil {

    public static final int    MAJOR    = 1,
                               MINOR    = 7,
                               REVISION = 3;
    public static final String VERSION  = "1.7.3";
    public static final String PRODUCT  = "HSQL Database Engine";

    static final void throwError(HsqlException e) throws SQLException {
        throw new SQLException(e.getMessage(), e.getSQLState(),
                               e.getErrorCode());
    }

    static final void throwError(Result r) throws SQLException {
        throw new SQLException(r.getMainString(), r.getSubString(),
                               r.getStatementID());
    }

    public static final SQLException sqlException(HsqlException e) {
        return new SQLException(e.getMessage(), e.getSQLState(),
                                e.getErrorCode());
    }

    static final SQLException sqlException(int id) {
        return sqlException(Trace.error(id));
    }

    static final SQLException sqlException(int id, String message) {
        return sqlException(Trace.error(id, message));
    }

    static final SQLException sqlException(int id, int subId, Object[] add) {
        return sqlException(Trace.error(id, subId, add));
    }

    static final SQLException notSupported =
        sqlException(Trace.error(Trace.FUNCTION_NOT_SUPPORTED));
}
