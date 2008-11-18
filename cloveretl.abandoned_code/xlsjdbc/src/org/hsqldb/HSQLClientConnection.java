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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.rowio.RowInputBinary;
import org.hsqldb.rowio.RowOutputBinary;

/**
 * Base remote session proxy implementation. Uses instances of Result to
 * transmit and recieve data. This implementation utilises the updated HSQL
 * protocol.
 *
 * @author fredt@users
 * @version 1.7.2
 * @since 1.7.2
 */
public class HSQLClientConnection implements SessionInterface {

    static final int          BUFFER_SIZE = 0x1000;
    final byte[]              mainBuffer  = new byte[BUFFER_SIZE];
    private boolean           isClosed;
    private Socket            socket;
    protected OutputStream    dataOutput;
    protected DataInputStream dataInput;
    protected RowOutputBinary rowOut;
    protected RowInputBinary  rowIn;
    private Result            resultOut;
    private final int         sessionID;

//
    String  host;
    int     port;
    String  path;
    String  database;
    boolean isTLS;
    int     databaseID;

    public HSQLClientConnection(String host, int port, String path,
                                String database, boolean isTLS, String user,
                                String password) throws HsqlException {

        this.host     = host;
        this.port     = port;
        this.path     = path;
        this.database = database;
        this.isTLS    = isTLS;

        initStructures();

        Result login = new Result(ResultConstants.SQLCONNECT);

        login.mainString   = user;
        login.subString    = password;
        login.subSubString = database;

        initConnection(host, port, isTLS);

        Result resultIn = execute(login);

        if (resultIn.mode == ResultConstants.ERROR) {

/** @todo fredt - review error message */
            throw Trace.error(resultIn);
        }

        sessionID  = resultIn.sessionID;
        databaseID = resultIn.databaseID;
    }

    /**
     * resultOut is reused to trasmit all remote calls for session management.
     * Here the structure is preset for sending attributes.
     */
    private void initStructures() {

        rowOut    = new RowOutputBinary(mainBuffer);
        rowIn     = new RowInputBinary(rowOut);
        resultOut = new Result(ResultConstants.DATA, 7);
        resultOut.metaData.colNames = resultOut.metaData.colLabels =
            resultOut.metaData.tableNames = new String[] {
            "", "", "", "", "", "", ""
        };

        resultOut.add(new Object[7]);

        resultOut.metaData.colTypes = new int[] {
            Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.INTEGER,
            Types.BOOLEAN, Types.BOOLEAN, Types.BOOLEAN
        };
    }

    protected void initConnection(String host, int port,
                                  boolean isTLS) throws HsqlException {
        openConnection(host, port, isTLS);
    }

    protected void openConnection(String host, int port,
                                  boolean isTLS) throws HsqlException {

        try {
            socket = HsqlSocketFactory.getInstance(isTLS).createSocket(host,
                                                   port);
            dataOutput = new BufferedOutputStream(socket.getOutputStream());
            dataInput = new DataInputStream(
                new BufferedInputStream(socket.getInputStream()));
        } catch (Exception e) {

/** @todo fredt - change error to no connetion established */
            throw Trace.error(Trace.SOCKET_ERROR);
        }
    }

    protected void closeConnection() {

        try {
            if (socket != null) {
                socket.close();
            }
        } catch (Exception e) {}

        socket = null;
    }

    public synchronized Result execute(Result r) throws HsqlException {

        try {
            r.sessionID  = sessionID;
            r.databaseID = databaseID;

            write(r);

            return read();
        } catch (Throwable e) {
            throw Trace.error(Trace.CONNECTION_IS_BROKEN, e.getMessage());
        }
    }

    public void close() {

        if (isClosed) {
            return;
        }

        isClosed = true;

        try {
            resultOut.setResultType(ResultConstants.SQLDISCONNECT);
            execute(resultOut);
        } catch (Exception e) {}

        try {
            closeConnection();
        } catch (Exception e) {}
    }

    private Object getAttribute(int id) throws HsqlException {

        resultOut.setResultType(ResultConstants.GETSESSIONATTR);

        Result in = execute(resultOut);

        if (in.mode == ResultConstants.ERROR) {
            throw Trace.error(in);
        }

        return in.rRoot.data[id];
    }

    private void setAttribute(Object property, int id) throws HsqlException {

        resultOut.setResultType(ResultConstants.SETSESSIONATTR);
        ArrayUtil.fillArray(resultOut.rRoot.data, null);

        resultOut.rRoot.data[id] = property;

        Result resultIn = execute(resultOut);

        if (resultIn.mode == ResultConstants.ERROR) {
            throw Trace.error(resultIn);
        }
    }

    public boolean isReadOnly() throws HsqlException {

        Object info = getAttribute(Session.INFO_CONNECTION_READONLY);

        return ((Boolean) info).booleanValue();
    }

    public void setReadOnly(boolean mode) throws HsqlException {
        setAttribute(mode ? Boolean.TRUE
                          : Boolean.FALSE, Session.INFO_CONNECTION_READONLY);
    }

    public boolean isAutoCommit() throws HsqlException {

        Object info = getAttribute(Session.INFO_AUTOCOMMIT);

        return ((Boolean) info).booleanValue();
    }

    public void setAutoCommit(boolean mode) throws HsqlException {
        setAttribute(mode ? Boolean.TRUE
                          : Boolean.FALSE, Session.INFO_AUTOCOMMIT);
    }

    public boolean isClosed() {
        return isClosed;
    }

    public Session getSession() {
        return null;
    }

    public void commit() throws HsqlException {

        resultOut.setResultType(ResultConstants.SQLENDTRAN);

        resultOut.updateCount = ResultConstants.COMMIT;

        resultOut.setMainString("");
        execute(resultOut);
    }

    public void rollback() throws HsqlException {

        resultOut.setResultType(ResultConstants.SQLENDTRAN);

        resultOut.updateCount = ResultConstants.ROLLBACK;

        resultOut.setMainString("");
        execute(resultOut);
    }

    public int getId() {
        return sessionID;
    }

    protected void write(Result r) throws IOException, HsqlException {
        Result.write(r, rowOut, dataOutput);
    }

    protected Result read() throws IOException, HsqlException {

        Result r = Result.read(rowIn, dataInput);

        rowOut.setBuffer(mainBuffer);
        rowIn.resetRow(mainBuffer.length);

        return r;
    }
}
