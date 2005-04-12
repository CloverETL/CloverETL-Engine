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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.hsqldb.Database;
import org.hsqldb.DatabaseScript;
import org.hsqldb.HsqlException;
import org.hsqldb.Result;
import org.hsqldb.Table;
import org.hsqldb.Token;
import org.hsqldb.Trace;
import org.hsqldb.index.RowIterator;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;

//import org.hsqldb.lib.StopWatch;

/**
 * Handles all logging to file operations. A log consists of three blocks:<p>
 *
 * DDL BLOCK: definition of DB objects, users and rights at startup time<br>
 * DATA BLOCK: all data for MEMORY tables at startup time<br>
 * LOG BLOCK: SQL statements logged since startup or the last CHECKPOINT<br>
 *
 * The implementation of this class and its subclasses support the formats
 * used for writing the data. In versions up to 1.7.2, this data is written
 * to the *.script file for the database. Since 1.7.2 the data can also be
 * written as binray in order to speed up shutdown and startup.<p>
 *
 * In 1.7.2, two separate files are used, one for the DDL + DATA BLOCK and
 * the other for the LOG BLOCK.<p>
 *
 * A related use for this class is for saving a current snapshot of the
 * database data to a user-defined file. This happens in the SHUTDOWN COMPACT
 * process or done as a result of the SCRIPT command. In this case, the
 * DATA block contains the CACHED table data as well.<p>
 *
 * DatabaseScriptReader and its subclasses read back the data at startup time.
 *
 * @author fredt@users
 * @version 1.7.2
 * @since 1.7.2
 */
// todo - can lock the database engine as readonly in a wrapper for this when
// used at checkpoint
public abstract class ScriptWriterBase {

    Database       db;
    String         outFile;
    OutputStream   fileStreamOut;
    FileDescriptor outDescriptor;
    int            tableRowCount;

    /**
     * this determines if the script is the normal script (false) used
     * internally by the engine or a user-initiated snapshot of the DB (true)
     */
    boolean          includeCachedData;
    long             byteCount;
    int              writeDelay;
    volatile boolean needsSync;
    volatile boolean forceSync;
    volatile boolean busyWriting;
    static final int INSERT = 0;

    /** the ID of the last session that wrote to log */
    int                          sessionId;
    public static final String[] LIST_SCRIPT_FORMATS      = new String[] {
        Token.T_TEXT, Token.T_BINARY, null, Token.T_COMPRESSED
    };
    public static final int      SCRIPT_TEXT_170          = 0;
    public static final int      SCRIPT_BINARY_172        = 1;
    public static final int      SCRIPT_ZIPPED_BINARY_172 = 3;

    public static ScriptWriterBase newScriptWriter(Database db, String file,
            boolean includeCachedData, boolean newFile,
            int scriptType) throws HsqlException {

        if (scriptType == SCRIPT_TEXT_170) {
            return new ScriptWriterText(db, file, includeCachedData, newFile);
        } else if (scriptType == SCRIPT_BINARY_172) {
            return new ScriptWriterBinary(db, file, includeCachedData,
                                          newFile);
        } else {
            return new ScriptWriterZipped(db, file, includeCachedData,
                                          newFile);
        }
    }

    ScriptWriterBase(Database db, String file, boolean includeCachedData,
                     boolean newFile) throws HsqlException {

        initBuffers();

        File newFileFile = new File(file);

        if (newFileFile.exists()) {
            if (newFile) {
                throw Trace.error(Trace.FILE_IO_ERROR, file);
            } else {
                byteCount = newFileFile.length();
            }
        }

        this.db                = db;
        this.includeCachedData = includeCachedData;
        outFile                = file;

        openFile();
    }

    abstract protected void initBuffers();

    /**
     *  Not used in current implementation.
     */
    public void setWriteDelay(int delay) {
        writeDelay = delay;
    }

    /**
     *  Called externally in write delay intervals.
     */
    public synchronized void sync() {

        if (needsSync) {
            if (busyWriting) {
                forceSync = true;

                return;
            }

            try {

                // Group One
                if (fileStreamOut != null) {
                    fileStreamOut.flush();
                }

                outDescriptor.sync();
            } catch (IOException e) {
                Trace.printSystemOut("flush() or sync() error: "
                                     + e.getMessage());
            }

            needsSync = false;
            forceSync = false;
        }
    }

    public void close() throws HsqlException {

        try {

            // Group One
            if (fileStreamOut != null) {
                fileStreamOut.flush();
                fileStreamOut.close();
            }
        } catch (IOException e) {
            throw Trace.error(Trace.FILE_IO_ERROR);
        }
    }

    public long size() {
        return byteCount;
    }

    public void writeAll() throws HsqlException {

        try {
            writeDDL();
            writeExistingData();
            finishStream();
        } catch (IOException e) {
            throw Trace.error(Trace.FILE_IO_ERROR);
        }
    }

    /**
     *  File is opened in append mode although in current usage the file
     *  never pre-exists
     */
    protected void openFile() throws HsqlException {

        try {
            FileOutputStream fos = new FileOutputStream(outFile, true);

            outDescriptor = fos.getFD();
            fileStreamOut = new BufferedOutputStream(fos, 2 << 12);
        } catch (IOException e) {
            throw Trace.error(Trace.FILE_IO_ERROR, Trace.Message_Pair,
                              new Object[] {
                e.getMessage(), outFile
            });
        }
    }

    /**
     * This is not really useful in the current usage but may be if this
     * class is used in a different way.
     */
    protected void finishStream() throws IOException {}

    protected void writeDDL() throws IOException, HsqlException {

        Result ddlPart = DatabaseScript.getScript(db, !includeCachedData);

        writeSingleColumnResult(ddlPart);
    }

    protected void writeExistingData() throws HsqlException, IOException {

        boolean       wroteTable = false;
        HsqlArrayList tables     = db.getTables();

        for (int i = 0, size = tables.size(); i < size; i++) {
            Table t = (Table) tables.get(i);

            // write all memory table data
            // write cached table data unless index roots have been written
            // write all text table data apart from readonly text tables
            // unless index roots have been written
            boolean script = false;

            switch (t.getTableType()) {

                case Table.MEMORY_TABLE :
                    script = true;
                    break;

                case Table.CACHED_TABLE :
                    script = includeCachedData;
                    break;

                case Table.TEXT_TABLE :
                    script = includeCachedData &&!t.isReadOnly();
                    break;
            }

            try {
                if (script) {
                    writeTableInit(t);

                    RowIterator it = t.rowIterator();

                    while (it.hasNext()) {
                        writeRow(0, t, it.next().getData());
                    }

                    writeTableTerm(t);
                }
            } catch (Exception e) {
                throw Trace.error(Trace.ASSERT_FAILED, e.getMessage());
            }
        }

        writeDataTerm();
    }

    protected void writeTableInit(Table t)
    throws HsqlException, IOException {}

    protected void writeTableTerm(Table t) throws HsqlException, IOException {

        if (t.isDataReadOnly() &&!t.isTemp() &&!t.isText()) {
            StringBuffer a = new StringBuffer("SET TABLE ");

            a.append(t.getName().statementName);
            a.append(" READONLY TRUE");
            writeLogStatement(a.toString(), sessionId);
        }
    }

    protected void writeSingleColumnResult(Result r)
    throws HsqlException, IOException {

        Iterator it = r.iterator();

        while (it.hasNext()) {
            Object[] data = (Object[]) it.next();

            writeLogStatement((String) data[0], sessionId);
        }
    }

    abstract void writeRow(int sid, Table table,
                           Object[] data) throws HsqlException, IOException;

    abstract protected void writeDataTerm() throws IOException;

    abstract protected void writeSessionId(int sid) throws IOException;

    abstract void writeLogStatement(String s,
                                    int sid)
                                    throws IOException, HsqlException;
}
