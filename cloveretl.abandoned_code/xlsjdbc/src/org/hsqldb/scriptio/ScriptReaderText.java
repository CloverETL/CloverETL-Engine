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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.Result;
import org.hsqldb.ResultConstants;
import org.hsqldb.Session;
import org.hsqldb.Trace;
import org.hsqldb.lib.StringConverter;

/**
 * Handles operations involving reading back a script or log file written
 * out by DatabaseScriptWriter. This implementation and its subclasses
 * correspond to DatabaseScriptWriter and its subclasses for the supported
 * formats.
 *
 *  @author fredt@users
 *  @since 1.7.2
 *  @version 1.7.2
 */
public class ScriptReaderText extends ScriptReaderBase {

    // this is used only to enable reading one logged line at a time
    BufferedReader d;

    ScriptReaderText(Database db,
                     String file) throws HsqlException, IOException {
        super(db, file);
    }

    protected void readDDL(Session session)
    throws IOException, HsqlException {

        for (;;) {
            lastLine = readLoggedStatement();

            if (lastLine == null || lastLine.startsWith("INSERT INTO ")) {
                break;
            }

            Result result = session.sqlExecuteDirectNoPreChecks(lastLine);

            if (result != null && result.mode == ResultConstants.ERROR) {
                throw Trace.error(Trace.ERROR_IN_SCRIPT_FILE,
                                  Trace.DatabaseScriptReader_readDDL,
                                  new Object[] {
                    new Integer(lineCount), result.getMainString()
                });
            }
        }
    }

    protected void readExistingData(Session session)
    throws IOException, HsqlException {

        // fredt - needed for forward referencing FK constraints
        db.setReferentialIntegrity(false);

        if (lastLine == null) {
            lastLine = readLoggedStatement();
        }

        for (;;) {
            if (lastLine == null) {
                break;
            }

            Result result = session.sqlExecuteDirectNoPreChecks(lastLine);

            if (result != null && result.mode == ResultConstants.ERROR) {
                throw Trace.error(Trace.ERROR_IN_SCRIPT_FILE,
                                  Trace.DatabaseScriptReader_readExistingData,
                                  new Object[] {
                    new Integer(lineCount), result.getMainString()
                });
            }

            lastLine = readLoggedStatement();
        }

        db.setReferentialIntegrity(true);
    }

    public String readLoggedStatement() throws IOException {

        //fredt temporary solution - should read bytes directly from buffer
        String s = d.readLine();

        lineCount++;

        return StringConverter.asciiToUnicode(s);
    }

    /**
     * openFile
     *
     * @throws IOException
     * @todo Implement this org.hsqldb.scriptio.ScriptReaderBase method
     */
    protected void openFile() throws IOException {

        super.openFile();

        d = new BufferedReader(new InputStreamReader(dataStreamIn));
    }

    public void close() {

        try {
            d.close();
        } catch (Exception e) {}
    }
}
