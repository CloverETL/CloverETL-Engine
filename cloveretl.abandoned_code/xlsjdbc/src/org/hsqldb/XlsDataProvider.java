/*
 * Created on 1.4.2005
 *
 */
package org.hsqldb;

import java.io.IOException;
import java.sql.SQLException;

import jxl.write.WritableSheet;

import org.hsqldb.xls.RowInputXls;
import org.hsqldb.xls.RowOutputXls;

import cz.opentech.jdbc.xlsdriver.db.XlsDB;
import cz.opentech.jdbc.xlsdriver.db.XlsWorkbook;
import cz.opentech.jdbc.xlsdriver.metadata.TableMetadata;

/**
 * @author vitaz
 */
public class XlsDataProvider extends TextCache.DataProvider {

    // TODO implement cacheable
    private boolean isSavingAll;

    private TableMetadata metadata;

    private XlsWorkbook writableWorkbook;

    private WritableSheet sheet;
    
    /**
     * 
     * @param cache
     */
    public XlsDataProvider(TextCache cache) {
        super(cache);
    }

    /**
     * 
     * @see org.hsqldb.TextCache.DataProvider#open(boolean)
     */
    protected void open(boolean readonly) throws HsqlException {

        try {
            readData();
        } catch (IOException e) {
            throw Trace.error(Trace.FILE_IO_ERROR,
                    Trace.TextCache_openning_file_error, new Object[] {
                            getName(), e });
        }

        setReadOnly(readonly);
    }

    /**
     * @see org.hsqldb.TextCache.DataProvider#close()
     */
    protected void close() throws HsqlException {
        closeFile();
    }

    /**
     * @see org.hsqldb.TextCache.DataProvider#closeFile()
     */
    protected void closeFile() throws HsqlException {
        if (writableWorkbook == null) {
            return;
        }

        try {
            saveAll();
            writableWorkbook.close();
            writableWorkbook = null;
        } catch (IOException e) {
            throw Trace.error(Trace.FILE_IO_ERROR,
                    Trace.TextCache_closing_file_error, new Object[] {
                            getName(), e });
        }
    }

    private void readData() throws IOException {
        writableWorkbook = XlsDB.loadWorkbook(getName());
        sheet = writableWorkbook.getWWB().getSheet(metadata.getSheet());
        final RowOutputXls rowOut = (RowOutputXls) getRowOutput();
        rowOut.setSheet(sheet);

        setFileFreePosition(sheet.getRows() * sheet.getColumns());
    }

    private void flushData() throws IOException {
    	final RowOutputXls rowOut = (RowOutputXls) getRowOutput();
    	rowOut.defrag();
        writableWorkbook.flush();
        readData();
    }

    /**
     * @see org.hsqldb.TextCache.DataProvider#defrag()
     */
    protected void defrag() throws HsqlException {

        final boolean readOnly = getCache().cacheReadonly; 
        close();
        open(readOnly);
    }

    /**
     * @see org.hsqldb.TextCache.DataProvider#free(org.hsqldb.CachedRow)
     */
    protected void free(CachedRow r) throws HsqlException {
        if (isStoreOnInsert() && !isIndexingSource()) {
           	((RowOutputXls) getRowOutput()).removeRow(r.iPos);
        }
        
        super.free(r);
    }

    /**
     * @see org.hsqldb.TextCache.DataProvider#initBuffers()
     */
    protected void initBuffers() throws HsqlException {
        // empty
    }

    /**
     * @see org.hsqldb.TextCache.DataProvider#initParams()
     */
    protected void initParams() throws HsqlException {

        setStoreOnInsert(true);

        try {
            String url = getName();
            int idx = url.indexOf(';');
            String name = null;
            if (idx != -1) {
                name = url.substring(0, idx);
                url = url.substring(idx + 1);
            }
            metadata = TableMetadata.parseTable(url, new TableMetadata(null));
            setName(name != null ? name : metadata.getFile());
        } catch (SQLException e) {
            metadata = null;
            throw (Trace.error(Trace.TEXT_TABLE_SOURCE, "invalid properties: "
                    + e.getMessage()));
        }
        // implement the following
        //-- Get size and scale
        HsqlProperties dbProps = getDatabaseProperties();
//        setCacheScale(dbProps.getIntegerProperty("textdb.cache_scale", 10, 8, 16));
//        setCacheSizeScale(dbProps.getIntegerProperty("textdb.cache_size_scale", 12, 8, 20));
        setCacheScale(8);
        setCacheSizeScale(8);

        setRowInput(new RowInputXls(metadata));
        setRowOutput(new RowOutputXls(metadata));
    }

    /**
     * @see org.hsqldb.TextCache.DataProvider#makeRow(int, org.hsqldb.Table)
     */
    protected CachedRow makeRow(int pos, Table t) throws HsqlException {
        final RowInputXls rowIn = (RowInputXls) getRowInput();
        CachedRow r = null;

        try {
            System.out.println("Line " + pos + " read.");
            rowIn.setSource(sheet, pos);
            if (!rowIn.isEol()) {
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
     * @see org.hsqldb.TextCache.DataProvider#saveRow(org.hsqldb.CachedRow)
     */
    protected void saveRow(CachedRow r) throws IOException, HsqlException {
        final RowOutputXls rowOut = (RowOutputXls) getRowOutput();

        rowOut.setRow(r.iPos);
        r.write(rowOut);
        System.out.println("Line " + r.iPos + " written.");

        if (!isSavingAll) {
	        // make the cache dirty
	        getCache().fileModified = true;
        }
    }

    /**
     * @see org.hsqldb.TextCache.DataProvider#saveAll()
     */
    protected void saveAll() throws HsqlException {
        isSavingAll = true;
        try {
            super.saveAll();
            flushData();
        } catch (IOException e) {
            throw Trace.error(Trace.FILE_IO_ERROR, Trace.Cache_saveAll,
                    new Object[] { e });
        } finally {
            isSavingAll = false;
        }
    }

    /**
     * @see org.hsqldb.TextCache.DataProvider#getLineNumber()
     */
    protected int getLineNumber() {
        return ((RowInputXls) getRowInput()).getLineNumber();
    }

    /**
     * @see org.hsqldb.TextCache.DataProvider#reopen()
     */
    protected void reopen() throws HsqlException {
        open(isReadOnly());
        ((RowInputXls) getRowInput()).reset();
    }

    /**
     * 
     * @see org.hsqldb.TextCache.DataProvider#setStorageSize(org.hsqldb.CachedRow)
     */
    protected void setStorageSize(CachedRow r) {
        r.storageSize = getRowOutput().getSize(r);
    }
}
