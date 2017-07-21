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


package org.hsqldb.scriptio;

import java.io.IOException;

import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.NumberSequence;
import org.hsqldb.Table;
import org.hsqldb.rowio.RowOutputTextLog;

/**
 * Handles all logging to file operations. A log consists of three blocks:<p>
 *
 * LOG BLOCK: SQL statements logged since startup or the last CHECKPOINT<br>
 *
 * A related use for this class is for saving a current snapshot of the
 * database data to a user-defined file. This happens in the SHUTDOWN COMPACT
 * process or done as a result of the SCRIPT command. In this case, the
 * DATA block contains the CACHED table data as well.<p>
 *
 * @author fredt@users
 * @version 1.7.2
 * @since 1.7.2
 */
public class ScriptWriterText extends ScriptWriterBase {

    RowOutputTextLog rowOut;

    // todo - perhaps move this global into a lib utility class
    public static final byte[] BYTES_LINE_SEP;

    static {
        String sLineSep = System.getProperty("line.separator", "\n");

        BYTES_LINE_SEP = sLineSep.getBytes();
    }

    static final byte[] BYTES_INSERT_INTO  = "INSERT INTO ".getBytes();
    static final byte[] BYTES_VALUES       = " VALUES(".getBytes();
    static final byte[] BYTES_TERM         = ")".getBytes();
    static final byte[] BYTES_DELETE_FROM  = "DELETE FROM ".getBytes();
    static final byte[] BYTES_WHERE        = " WHERE ".getBytes();
    static final byte[] BYTES_SEQUENCE     = "ALTER SEQUENCE ".getBytes();
    static final byte[] BYTES_SEQUENCE_MID = " RESTART WITH ".getBytes();
    static final byte[] BYTES_C_ID_INIT    = "/*C".getBytes();
    static final byte[] BYTES_C_ID_TERM    = "*/".getBytes();

    public ScriptWriterText(Database db, String file,
                            boolean includeCachedData,
                            boolean newFile) throws HsqlException {
        super(db, file, includeCachedData, newFile);
    }

    protected void initBuffers() {
        rowOut = new RowOutputTextLog();
    }

    public void writeRow(int sid, Table table,
                         Object[] data) throws HsqlException, IOException {

        busyWriting = true;

        rowOut.reset();
        ((RowOutputTextLog) rowOut).setMode(RowOutputTextLog.MODE_INSERT);
        writeSessionId(sid);
        rowOut.write(BYTES_INSERT_INTO);
        rowOut.writeString(table.getName().statementName);
        rowOut.write(BYTES_VALUES);
        rowOut.writeData(data, table);
        rowOut.write(BYTES_TERM);
        rowOut.write(BYTES_LINE_SEP);
        fileStreamOut.write(rowOut.getBuffer(), 0, rowOut.size());

        byteCount += rowOut.size();

        fileStreamOut.flush();

        needsSync   = true;
        busyWriting = false;

        if (forceSync) {
            sync();
        }
    }

    protected void writeDataTerm() throws IOException {}

    protected void writeSessionId(int sid) throws IOException {

        if (sid != sessionId) {
            rowOut.write(BYTES_C_ID_INIT);
            rowOut.writeIntData(sid);
            rowOut.write(BYTES_C_ID_TERM);

            sessionId = sid;
        }
    }

    public void writeLogStatement(String s,
                                  int sid) throws IOException, HsqlException {

        busyWriting = true;

        rowOut.reset();
        writeSessionId(sid);
        rowOut.writeString(s);
        rowOut.write(BYTES_LINE_SEP);
        fileStreamOut.write(rowOut.getBuffer(), 0, rowOut.size());

        byteCount += rowOut.size();

        fileStreamOut.flush();

        needsSync   = true;
        busyWriting = false;

        if (forceSync) {
            sync();
        }
    }

    public void writeDeleteStatement(int sid, Table table,
                                     Object[] data)
                                     throws HsqlException, IOException {

        busyWriting = true;

        rowOut.reset();
        ((RowOutputTextLog) rowOut).setMode(RowOutputTextLog.MODE_DELETE);
        writeSessionId(sid);
        rowOut.write(BYTES_DELETE_FROM);
        rowOut.writeString(table.getName().statementName);
        rowOut.write(BYTES_WHERE);
        rowOut.writeData(table.getColumnCount(), table.getColumnTypes(),
                         data, table.columnList, table.getPrimaryKey());
        rowOut.write(BYTES_LINE_SEP);
        fileStreamOut.write(rowOut.getBuffer(), 0, rowOut.size());

        byteCount += rowOut.size();

        fileStreamOut.flush();

        needsSync   = true;
        busyWriting = false;

        if (forceSync) {
            sync();
        }
    }

    public void writeSequenceStatement(int sid,
                                       NumberSequence seq)
                                       throws HsqlException, IOException {

        busyWriting = true;

        rowOut.reset();
        writeSessionId(sid);
        rowOut.write(BYTES_SEQUENCE);
        rowOut.writeString(seq.getName().statementName);
        rowOut.write(BYTES_SEQUENCE_MID);
        rowOut.writeLongData(seq.peek());
        rowOut.write(BYTES_LINE_SEP);
        fileStreamOut.write(rowOut.getBuffer(), 0, rowOut.size());

        byteCount += rowOut.size();

        fileStreamOut.flush();

        needsSync   = true;
        busyWriting = false;

        if (forceSync) {
            sync();
        }
    }
}
