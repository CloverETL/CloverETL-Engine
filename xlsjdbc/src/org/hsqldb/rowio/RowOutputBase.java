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


package org.hsqldb.rowio;

import java.io.IOException;
import java.math.BigDecimal;

import org.hsqldb.Column;
import org.hsqldb.HsqlException;
import org.hsqldb.Table;
import org.hsqldb.Trace;
import org.hsqldb.Types;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.types.Binary;
import org.hsqldb.types.JavaObject;

/**
 * Base class for writing the data for a database row in different formats.
 * Defines the methods that are independent of storage format and declares
 * the format-dependent methods that subclasses should define.
 *
 * @author sqlbob@users (RMP)
 * @author fredt@users
 * @version 1.7.2
 * @since 1.7.0
 */
public abstract class RowOutputBase extends HsqlByteArrayOutputStream
implements RowOutputInterface {

    public static final int CACHED_ROW_160 = 0;
    public static final int CACHED_ROW_170 = 1;

    // the last column in a table is an ID that should not be written to file
    protected boolean skipSystemId = false;

    public static RowOutputInterface newRowOutput(int cachedRowType)
    throws HsqlException {

        try {
            if (cachedRowType == CACHED_ROW_170) {
                return new RowOutputBinary();
            } else {
                Class c = Class.forName("org.hsqldb.rowio.RowOutputLegacy");

                return (RowOutputInterface) c.newInstance();
            }
        } catch (Exception e) {
            throw Trace.error(Trace.MISSING_SOFTWARE_MODULE,
                              Trace.DatabaseRowOutput_newDatabaseRowOutput);
        }
    }

    /**
     *  Constructor used for persistent storage of a Table row
     *
     * @exception  IOException when an IO error is encountered
     */
    public RowOutputBase() {
        super();
    }

    /**
     *  Constructor used for result sets
     *
     * @exception  IOException when an IO error is encountered
     */
    public RowOutputBase(int initialSize) {
        super(initialSize);
    }

    /**
     *  Constructor used for network transmission of result sets
     *
     * @exception  IOException when an IO error is encountered
     */
    public RowOutputBase(byte[] buffer) {
        super(buffer);
    }

// fredt@users - comment - methods for writing Result column type, name and data size
    public abstract void writeEnd() throws IOException;

    public abstract void writeSize(int size) throws IOException;

    public abstract void writeType(int type) throws IOException;

    public abstract void writeIntData(int i) throws IOException;

    public abstract void writeIntData(int i, int position) throws IOException;

    public abstract void writeString(String s) throws IOException;

// fredt@users - comment - methods used for writing each SQL type
    protected void writeFieldPrefix() throws IOException {}

    protected abstract void writeFieldType(int type) throws IOException;

    protected abstract void writeNull(int type) throws IOException;

    protected abstract void writeChar(String s, int t) throws IOException;

    protected abstract void writeSmallint(Number o)
    throws IOException, HsqlException;

    protected abstract void writeInteger(Number o)
    throws IOException, HsqlException;

    protected abstract void writeBigint(Number o)
    throws IOException, HsqlException;

    protected abstract void writeReal(Double o,
                                      int type)
                                      throws IOException, HsqlException;

    protected abstract void writeDecimal(java.math.BigDecimal o)
    throws IOException, HsqlException;

    protected abstract void writeBit(Boolean o)
    throws IOException, HsqlException;

    protected abstract void writeDate(java.sql.Date o)
    throws IOException, HsqlException;

    protected abstract void writeTime(java.sql.Time o)
    throws IOException, HsqlException;

    protected abstract void writeTimestamp(java.sql.Timestamp o)
    throws IOException, HsqlException;

    protected abstract void writeOther(JavaObject o)
    throws IOException, HsqlException;

    protected abstract void writeBinary(Binary o,
                                        int t)
                                        throws IOException, HsqlException;

    public void writeRow(Object data[],
                         Table t) throws IOException, HsqlException {

        writeSize(0);
        writeData(data, t);
        writeIntData(size(), 0);
    }

    /**
     *  This method is called to write data for a table.
     *
     * @param  data
     * @param  t
     * @throws  IOException
     */
    public void writeData(Object data[],
                          Table t) throws IOException, HsqlException {

        int[] types = t.getColumnTypes();
        int   l     = t.getColumnCount();

        writeData(l, types, data, null, null);
    }

    /**
     *  This method is called to write data for a Result.
     *
     * @param  l
     * @param  types
     * @param  data
     * @param cols
     * @param primarykeys
     * @throws  IOException
     */
    public void writeData(int l, int types[], Object data[],
                          HashMappedList cols,
                          int[] primaryKeys)
                          throws IOException, HsqlException {

        int limit = primaryKeys == null ? l
                                        : primaryKeys.length;

        for (int i = 0; i < limit; i++) {
            int    j = primaryKeys == null ? i
                                           : primaryKeys[i];
            Object o = data[j];
            int    t = types[j];

            if (cols != null) {
                Column col = (Column) cols.get(j);

                writeFieldPrefix();
                writeString(col.columnName.statementName);
            }

            if (o == null) {
                writeNull(t);

                continue;
            }

            writeFieldType(t);

            switch (t) {

                case Types.NULL :
                case Types.CHAR :
                case Types.VARCHAR :
                case Types.VARCHAR_IGNORECASE :
                case Types.LONGVARCHAR :
                    writeChar((String) o, t);
                    break;

                case Types.TINYINT :
                case Types.SMALLINT :
                    writeSmallint((Number) o);
                    break;

                case Types.INTEGER :
                    writeInteger((Number) o);
                    break;

                case Types.BIGINT :
                    writeBigint((Number) o);
                    break;

                case Types.REAL :
                case Types.FLOAT :
                case Types.DOUBLE :
                    writeReal((Double) o, t);
                    break;

                case Types.NUMERIC :
                case Types.DECIMAL :
                    writeDecimal((BigDecimal) o);
                    break;

                case Types.BOOLEAN :
                    writeBit((Boolean) o);
                    break;

                case Types.DATE :
                    writeDate((java.sql.Date) o);
                    break;

                case Types.TIME :
                    writeTime((java.sql.Time) o);
                    break;

                case Types.TIMESTAMP :
                    writeTimestamp((java.sql.Timestamp) o);
                    break;

                case Types.OTHER :
                    writeOther((JavaObject) o);
                    break;

                case Types.BINARY :
                case Types.VARBINARY :
                case Types.LONGVARBINARY :
                    writeBinary((Binary) o, t);
                    break;

                default :
                    throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED,
                                      Types.getTypeString(t));
            }
        }
    }

    // returns the underlying HsqlByteArrayOutputStream
    public HsqlByteArrayOutputStream getOutputStream() {
        return this;
    }
}
