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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.Session;

abstract public class ScriptReaderBase {

    public static ScriptReaderBase newScriptReader(Database db, String file,
            int scriptType) throws HsqlException, IOException {

        if (scriptType == ScriptWriterBase.SCRIPT_TEXT_170) {
            return new ScriptReaderText(db, file);
        } else if (scriptType == ScriptWriterBase.SCRIPT_BINARY_172) {
            return new ScriptReaderBinary(db, file);
        } else {
            return new ScriptReaderZipped(db, file);
        }
    }

    BufferedInputStream dataStreamIn;
    Database            db;
    int                 lineCount;
    String              lastLine;

//    int         byteCount;
    String fileName;

    ScriptReaderBase(Database db,
                     String file) throws HsqlException, IOException {

        this.db  = db;
        fileName = file;

        openFile();
    }

    protected void openFile() throws IOException {

        // canonical path for "res:" type databases always starts with "/"
        // so we don't need to use getClassLoader.getResourceAsStream here
        // or anywhere else.
        // In fact, getClass().getResourceAsStream() is preferred, as
        // it is not subject to the same security restrictions
        dataStreamIn = db.isFilesInJar()
                       ? new BufferedInputStream(
                           getClass().getResourceAsStream(fileName), 1 << 13)
                       : new BufferedInputStream(
                           new FileInputStream(fileName), 1 << 13);
    }

    public void readAll(Session session) throws IOException, HsqlException {
        readDDL(session);
        readExistingData(session);
    }

    abstract protected void readDDL(Session session)
    throws IOException, HsqlException;

    abstract protected void readExistingData(Session session)
    throws IOException, HsqlException;

    abstract public String readLoggedStatement() throws IOException;

    public int getLineNumber() {
        return lineCount;
    }

    public void close() {

        try {
            dataStreamIn.close();
        } catch (Exception e) {}
    }
}
