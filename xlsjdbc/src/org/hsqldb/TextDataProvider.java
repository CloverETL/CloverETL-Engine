/*
 * Created on 4.4.2005
 *
 */
package org.hsqldb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.rowio.RowInputText;
import org.hsqldb.rowio.RowInputTextQuoted;
import org.hsqldb.rowio.RowOutputText;
import org.hsqldb.rowio.RowOutputTextQuoted;
import org.hsqldb.scriptio.ScriptWriterText;

/**
 * @author vitaz
 *
 */
public class TextDataProvider extends TextCache.DataProvider {

    public static final String NL = System.getProperty("line.separator");
    
    private String fs;
    private String vs;
    private String lvs;
    private String stringEncoding;
    protected boolean isQuoted;
    protected boolean isAllQuoted;
    protected boolean ignoreFirst;
    protected String ignoredFirst = NL;
    
    /**
     * 
     * @param cache
     */
    public TextDataProvider(TextCache cache) {
        super(cache);
    }
    
    /**
     * 
     * @see org.hsqldb.TextCache.DataProvider#initParams()
     */
    protected void initParams() throws HsqlException {
        // fredt - write rows as soon as they are inserted
        setStoreOnInsert(true);
        
        HsqlProperties tableprops =
            HsqlProperties.delimitedArgPairsToProps(getName(), "=", ";", null);
        
        //-- Get file name
        switch (tableprops.errorCodes.length) {
        
        case 0 :
            throw Trace.error(Trace.TEXT_TABLE_SOURCE,
                    Trace.TEXT_TABLE_SOURCE_FILENAME);
        case 1 :
            
            // source file name is the only key without a value
            setName(tableprops.errorKeys[0].trim());
            break;
            
        default :
            throw Trace.error(Trace.TEXT_TABLE_SOURCE,
                    Trace.TEXT_TABLE_SOURCE_VALUE_MISSING,
                    tableprops.errorKeys[1]);
        }
        
        //-- Get separators:
        HsqlProperties dbProps = getDatabaseProperties(); 
        fs = translateSep(tableprops.getProperty("fs",
                dbProps.getProperty("textdb.fs", ",")));
        vs = translateSep(tableprops.getProperty("vs",
                dbProps.getProperty("textdb.vs", fs)));
        lvs = translateSep(tableprops.getProperty("lvs",
                dbProps.getProperty("textdb.lvs", fs)));
        
        if (fs.length() == 0 || vs.length() == 0 || lvs.length() == 0) {
            throw Trace.error(Trace.TEXT_TABLE_SOURCE,
                    Trace.TEXT_TABLE_SOURCE_SEPARATOR);
        }
        
        //-- Get booleans
        ignoreFirst = tableprops.isPropertyTrue("ignore_first",
                dbProps.isPropertyTrue("textdb.ignore_first", false));
        isQuoted =
            tableprops.isPropertyTrue("quoted",
                    dbProps.isPropertyTrue("textdb.quoted",
                            true));
        isAllQuoted = tableprops.isPropertyTrue("all_quoted",
                dbProps.isPropertyTrue("textdb.all_quoted", false));
        
        //-- Get encoding
        stringEncoding = translateSep(tableprops.getProperty("encoding",
                dbProps.getProperty("textdb.encoding", "ASCII")));
        
        //-- Get size and scale
        setCacheScale(tableprops.getIntegerProperty("cache_scale",
                dbProps.getIntegerProperty("textdb.cache_scale", 10, 8, 16)));
        setCacheSizeScale(tableprops.getIntegerProperty("cache_size_scale",
                dbProps.getIntegerProperty("textdb.cache_size_scale", 12, 8,
                        20)));
        
        try {
            if (isQuoted || isAllQuoted) {
                setRowInput(new RowInputTextQuoted(fs, vs, lvs, isAllQuoted));
                setRowOutput(new RowOutputTextQuoted(fs, vs, lvs, isAllQuoted,
                        stringEncoding));
            } else {
                setRowInput(new RowInputText(fs, vs, lvs, false));
                setRowOutput(new RowOutputText(fs, vs, lvs, false,
                        stringEncoding));
            }
        } catch (IOException e) {
            
            // no exception expected here the IOException is vestigial
            throw (Trace.error(Trace.TEXT_TABLE_SOURCE,
                    "invalid file: " + e));
        }
    }
    
    /**
     * 
     * @see org.hsqldb.TextCache.DataProvider#initBuffers()
     */
    protected void initBuffers() throws HsqlException {
        // empty
    }

    /**
     * 
     * @see org.hsqldb.TextCache.DataProvider#open(boolean)
     */
    protected void open(boolean readonly) throws HsqlException {

        try {
            setDataFile(ScaledRAFile.newScaledRAFile(getName(), readonly, 1,
                    ScaledRAFile.DATA_FILE_RAF));
            setFileFreePosition((int) getDataFile().length());

            if ((getFileFreePosition() == 0) && ignoreFirst) {
                byte[] buf = null;

                try {
                    buf = ignoredFirst.getBytes(stringEncoding);
                } catch (UnsupportedEncodingException e) {
                    buf = ignoredFirst.getBytes();
                }

                getDataFile().write(buf, 0, buf.length);

                setFileFreePosition(ignoredFirst.length());
            }
        } catch (Exception e) {
            throw Trace.error(Trace.FILE_IO_ERROR,
                              Trace.TextCache_openning_file_error,
                              new Object[] {
                getName(), e
            });
        }

        setReadOnly(readonly);
    }
    
    protected void close() throws HsqlException {
        final ScaledRAFile dataFile = getDataFile();  
        if (dataFile == null) {
            return;
        }

        try {
            saveAll();

            boolean empty = (dataFile.length() <= NL.length());

            dataFile.close();

            setDataFile(null);

            if (empty && !isReadOnly()) {
                FileUtil.delete(getName());
            }
        } catch (Exception e) {
            throw Trace.error(Trace.FILE_IO_ERROR,
                              Trace.TextCache_closing_file_error,
                              new Object[] {
                getName(), e
            });
        }
    }

    /**
     * 
     * @see org.hsqldb.TextCache.DataProvider#free(org.hsqldb.CachedRow)
     */
    protected void free(CachedRow r) throws HsqlException {

        if (isStoreOnInsert() && !isIndexingSource()) {
            int pos = r.iPos;
            int length = r.storageSize
                         - ScriptWriterText.BYTES_LINE_SEP.length;

            final RowOutputText rowOut = (RowOutputText) getRowOutput();
            final ScaledRAFile dataFile = getDataFile();
            
            rowOut.reset();

            HsqlByteArrayOutputStream out = rowOut.getOutputStream();

            try {
                out.fill(' ', length);
                out.write(ScriptWriterText.BYTES_LINE_SEP);
                dataFile.seek(pos);
                dataFile.write(out.getBuffer(), 0, out.size());
            } catch (IOException e) {
                throw (Trace.error(Trace.FILE_IO_ERROR, e.toString()));
            }
        }

        remove(r);
    }

    /**
     * 
     * @see org.hsqldb.TextCache.DataProvider#makeRow(int, org.hsqldb.Table)
     */
    protected CachedRow makeRow(int pos, Table t) throws HsqlException {

        final ScaledRAFile dataFile = getDataFile();
        final RowInputText rowIn = (RowInputText) getRowInput();
        CachedRow r = null;

        try {

            // HsqlStringBuffer buffer   = new HsqlStringBuffer(80);
            ByteArray buffer   = new ByteArray(80);
            boolean   blank    = true;
            boolean   complete = false;

            try {

                // char c;
                int c;
                int next;

                dataFile.seek(pos);

                //-- The following should work for DOS, MAC, and Unix line
                //-- separators regardless of host OS.
                while (true) {
                    next = dataFile.read();

                    if (next == -1) {
                        break;
                    }

                    // c = (char) (next & 0xff);
                    c = next;

                    //-- Ensure line is complete.
                    if (c == '\n') {
                        buffer.append('\n');

                        //-- Store first line.
                        if (ignoreFirst && pos == 0) {
                            ignoredFirst = buffer.toString();
                            blank        = true;
                        }

                        //-- Ignore blanks
                        if (!blank) {
                            complete = true;

                            break;
                        } else {
                            pos += buffer.length();

                            buffer.setLength(0);

                            blank = true;

                            rowIn.skippedLine();

                            continue;
                        }
                    }

                    if (c == '\r') {

                        //-- Check for newline
                        try {
                            next = dataFile.read();

                            if (next == -1) {
                                break;
                            }

                            // c = (char) (next & 0xff);
                            c = next;

                            if (c == '\n') {
                                buffer.append('\n');
                            }
                        } catch (Exception e2) {
                            ;
                        }

                        buffer.append('\n');

                        //-- Store first line.
                        //-- Currently this is of no use as it is lost in
                        // shutdown compact. We might want to store this in
                        // the *.script file at that point to be reused for
                        // reconstructing the data source file.
                        //
                        if (ignoreFirst && pos == 0) {
                            ignoredFirst = buffer.toString();
                            blank        = true;
                        }

                        //-- Ignore blanks.
                        if (!blank) {
                            complete = true;

                            break;
                        } else {
                            pos += buffer.length();

                            buffer.setLength(0);

                            blank = true;

                            rowIn.skippedLine();

                            continue;
                        }
                    }

                    if (c != ' ') {
                        blank = false;
                    }

                    buffer.append(c);
                }
            } catch (Exception e) {
                complete = false;
            }

            if (complete) {

                // rowIn.setSource(buffer.toString(), pos);
                rowIn.setSource(buffer.toString(), pos, buffer.length());

                if (isIndexingSource()) {
                    r = new PointerCachedDataRow(t, rowIn);
                } else {
                    r = new CachedDataRow(t, rowIn);
                }
            }
        } catch (Exception e) {
            throw Trace.error(Trace.TEXT_FILE, e);
        }

        return r;
    }

    /**
     * 
     * @see org.hsqldb.TextCache.DataProvider#setStorageSize(org.hsqldb.CachedRow)
     */
    protected void setStorageSize(CachedRow r) {
        r.storageSize = getRowOutput().getSize(r);
    }
    
    private String translateSep(String sep) {
        return translateSep(sep, false);
    }

    /**
     * Translates the escaped characters in a separator string and returns
     * the non-escaped string.
     */
    private String translateSep(String sep, boolean isProperty) {

        if (sep == null) {
            return (null);
        }

        int next = 0;

        if ((next = sep.indexOf('\\')) != -1) {
            int          start      = 0;
            char         sepArray[] = sep.toCharArray();
            char         ch         = 0;
            int          len        = sep.length();
            StringBuffer realSep    = new StringBuffer(len);

            do {
                realSep.append(sepArray, start, next - start);

                start = ++next;

                if (next >= len) {
                    realSep.append('\\');

                    break;
                }

                if (!isProperty) {
                    ch = sepArray[next];
                }

                if (ch == 'n') {
                    realSep.append('\n');

                    start++;
                } else if (ch == 'r') {
                    realSep.append('\r');

                    start++;
                } else if (ch == 't') {
                    realSep.append('\t');

                    start++;
                } else if (ch == '\\') {
                    realSep.append('\\');

                    start++;
                } else if (ch == 'u') {
                    start++;

                    realSep.append(
                        (char) Integer.parseInt(
                            sep.substring(start, start + 4), 16));

                    start += 4;
                } else if (sep.startsWith("semi", next)) {
                    realSep.append(';');

                    start += 4;
                } else if (sep.startsWith("space", next)) {
                    realSep.append(' ');

                    start += 5;
                } else if (sep.startsWith("quote", next)) {
                    realSep.append('\"');

                    start += 5;
                } else if (sep.startsWith("apos", next)) {
                    realSep.append('\'');

                    start += 4;
                } else {
                    realSep.append('\\');
                    realSep.append(sepArray[next]);

                    start++;
                }
            } while ((next = sep.indexOf('\\', start)) != -1);

            realSep.append(sepArray, start, len - start);

            sep = realSep.toString();
        }

        return sep;
    }
    
    private class ByteArray {

        private byte[] buffer;
        private int    buflen;

        public ByteArray(int n) {
            buffer = new byte[n];
            buflen = 0;
        }

        public void append(int c) {

            if (buflen >= buffer.length) {
                byte[] newbuf = new byte[buflen + 80];

                System.arraycopy(buffer, 0, newbuf, 0, buflen);

                buffer = newbuf;
            }

            buffer[buflen] = (byte) c;

            buflen++;
        }

        public int length() {
            return buflen;
        }

        public void setLength(int l) {
            buflen = l;
        }

        public String toString() {

            try {
                return new String(buffer, 0, buflen, stringEncoding);
            } catch (UnsupportedEncodingException e) {
                return new String(buffer, 0, buflen);
            }
        }
    }

    /**
     * @see org.hsqldb.TextCache.DataProvider#getLineNumber()
     */
    protected int getLineNumber() {
        return ((RowInputText) getRowInput()).getLineNumber();
    }

    /**
     * @see org.hsqldb.TextCache.DataProvider#reopen()
     */
    protected void reopen() throws HsqlException {
        open(isReadOnly());
        ((RowInputText) getRowInput()).reset();
    }
}
