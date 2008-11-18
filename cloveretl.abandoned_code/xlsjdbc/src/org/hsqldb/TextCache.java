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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.hsqldb.lib.FileUtil;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

// Ito Kazumitsu 20030328 - patch 1.7.2 - character encoding support

/** @todo fredt - file error messages to Trace */

/**
 * Acts as a buffer manager for a single TEXT table with respect its Row data.
 *
 * Handles read/write operations on the table's text format data file using a
 * compatible pair of org.hsqldb.rowio input/output class instances.
 *
 * @author sqlbob@users (RMP)
 * @version 1.7.2
 */
public class TextCache extends DataFileCache {

    //state of Cache
    private boolean isIndexingSource;
    private boolean readOnly;
    private RowInputInterface rowIn;
    private Table table;
    
    // data provider
    private DataProvider dataProvider;
    
    
    /**
     *  The source string for a cached table is evaluated and the parameters
     *  are used to open the source file.<p>
     *
     *  Settings are used in this order: (1) settings specified in the
     *  source string for the table (2) global database settings in
     *  *.properties file (3) program defaults
     */
    TextCache(String name, Table table) throws HsqlException {

        super(name, table.database);

        this.table = table;
    }

    public static abstract class DataProvider {
        private final TextCache cache;
        protected DataProvider(TextCache cache) {
            this.cache = cache;
        }
        // Properties
        protected TextCache getCache() {
            return cache;
        }
        protected boolean isStoreOnInsert() {
            return cache.storeOnInsert;
        }
        protected void setStoreOnInsert(boolean storeOnInsert) {
            cache.storeOnInsert = storeOnInsert;
        }
        protected boolean isIndexingSource() {
            return cache.isIndexingSource;
        }
        protected void setIndexingSource(boolean indexingSource) {
            cache.isIndexingSource = indexingSource;
        }
        protected RowInputInterface getRowInput() {
            return cache.rowIn;
        }
        protected void setRowInput(RowInputInterface rowInput) {
            cache.rowIn = rowInput;
        }
        protected RowOutputInterface getRowOutput() {
            return (RowOutputInterface) cache.rowOut;
        }
        protected abstract int getLineNumber();
        protected abstract void reopen() throws HsqlException;
        protected void setRowOutput(RowOutputInterface rowOutput) {
            cache.rowOut = rowOutput;
        }
        protected ScaledRAFile getDataFile() {
            return cache.dataFile;
        }
        protected void setDataFile(ScaledRAFile newDataFile) {
            cache.dataFile = newDataFile;
        }
        protected int getFileFreePosition() {
            return cache.fileFreePosition;
        }
        protected void setFileFreePosition(int newFileFreePosition) {
            cache.fileFreePosition = newFileFreePosition;
        }
        protected String getName() {
            return cache.sName;
        }
        protected void setName(String newName) {
            cache.sName = newName;
        }
        protected boolean isReadOnly() {
            return cache.readOnly;
        }
        protected void setReadOnly(boolean readOnly) {
            cache.readOnly = readOnly;
        }
        protected HsqlProperties getDatabaseProperties() {
            return cache.dbProps;
        }
        protected int getCacheScale() {
            return cache.cacheScale;
        }
        protected void setCacheScale(int newScale) {
            cache.cacheScale = newScale;
        }
        protected int getCacheSizeScale() {
            return cache.cacheSizeScale;
        }
        protected void setCacheSizeScale(int newScale) {
            cache.cacheSizeScale = newScale;
        }
        // Methods
        /**
         * @param r
         * @throws org.hsqldb.HsqlException
         */
        protected void add(CachedRow r) throws HsqlException {
            cache.internalAdd(r);
        }
        /**
         * @param newName
         * @throws org.hsqldb.HsqlException
         */
        protected void backup(String newName) throws HsqlException {
            cache.internalBackup(newName);
        }
        /**
         * @throws org.hsqldb.HsqlException
         */
        protected void close() throws HsqlException {
            cache.internalClose();
        }
        /**
         * @throws org.hsqldb.HsqlException
         */
        protected void closeFile() throws HsqlException {
            cache.internalCloseFile();
        }
        /**
         * @throws org.hsqldb.HsqlException
         */
        protected void defrag() throws HsqlException {
            cache.internalDefrag();
        }
        /**
         * @param r
         * @throws org.hsqldb.HsqlException
         */
        protected void free(CachedRow r) throws HsqlException {
            cache.internalFree(r);
        }
        /**
         * @return
         */
        protected int getCachedCount() {
            return cache.internalGetCachedCount();
        }
        /**
         * @return
         */
        protected int getFreePos() {
            return cache.internalGetFreePos();
        }
        /**
         * @param pos
         * @return
         */
        protected CachedRow getRow(int pos) {
            return cache.internalGetRow(pos);
        }
        /**
         * @param pos
         * @param t
         * @return
         * @throws org.hsqldb.HsqlException
         */
        protected CachedRow getRow(int pos, Table t) throws HsqlException {
            return cache.internalGetRow(pos, t);
        }
        /**
         * 
         */
        protected void init() {
            cache.internalInit();
        }
        /**
         * @throws org.hsqldb.HsqlException
         */
        protected void initBuffers() throws HsqlException {
            cache.internalInitBuffers();
        }
        /**
         * @throws org.hsqldb.HsqlException
         */
        protected void initParams() throws HsqlException {
            cache.internalInitParams();
        }
        /**
         * @param pos
         * @param t
         * @return
         * @throws org.hsqldb.HsqlException
         */
        protected CachedRow makeRow(int pos, Table t) throws HsqlException {
            return cache.internalMakeRow(pos, t);
        }
        /**
         * @param readonly
         * @throws org.hsqldb.HsqlException
         */
        protected void open(boolean readonly) throws HsqlException {
            cache.internalOpen(readonly);
        }
        /**
         * @param r
         * @return
         * @throws org.hsqldb.HsqlException
         */
        protected CachedRow remove(CachedRow r) throws HsqlException {
            return cache.internalRemove(r);
        }
        /**
         * @param t
         * @throws org.hsqldb.HsqlException
         */
        protected void remove(Table t) throws HsqlException {
            cache.internalRemove(t);
        }
        /**
         * @throws org.hsqldb.HsqlException
         */
        protected void saveAll() throws HsqlException {
            cache.internalSaveAll();
        }
        /**
         * @param r
         * @throws java.io.IOException
         * @throws org.hsqldb.HsqlException
         */
        protected void saveRow(CachedRow r) throws IOException, HsqlException {
            cache.internalSaveRow(r);
        }
        /**
         * @param r
         * @return
         * @throws org.hsqldb.HsqlException
         */
        protected int setFilePos(CachedRow r) throws HsqlException {
            return cache.internalSetFilePos(r);
        }
        /**
         * @param r
         * @throws org.hsqldb.HsqlException
         */
        protected void setStorageSize(CachedRow r) throws HsqlException {
            cache.internalSetStorageSize(r);
        }
    }

    /**
     * @param r
     * @throws org.hsqldb.HsqlException
     */
    protected void add(CachedRow r) throws HsqlException {
        dataProvider.add(r);
    }
    private void internalAdd(CachedRow r) throws HsqlException {
        super.add(r);
    }
    /**
     * @param newName
     * @throws org.hsqldb.HsqlException
     */
    protected void backup(String newName) throws HsqlException {
        dataProvider.backup(newName);
    }
    private void internalBackup(String newName) throws HsqlException {
        super.backup(newName);
    }
    /**
     * @throws org.hsqldb.HsqlException
     */
    protected void close() throws HsqlException {
        dataProvider.close();
    }
    private void internalClose() throws HsqlException {
        super.close();
    }
    /**
     * @throws org.hsqldb.HsqlException
     */
    protected void closeFile() throws HsqlException {
        dataProvider.closeFile();
    }
    private void internalCloseFile() throws HsqlException {
        super.closeFile();
    }
    /**
     * @throws org.hsqldb.HsqlException
     */
    protected void defrag() throws HsqlException {
        dataProvider.defrag();
    }
    private void internalDefrag() throws HsqlException {
        super.defrag();
    }
    /**
     * @param r
     * @throws org.hsqldb.HsqlException
     */
    protected void free(CachedRow r) throws HsqlException {
        dataProvider.free(r);
    }
    private void internalFree(CachedRow r) throws HsqlException {
        super.free(r);
    }
    /**
     * @return
     */
    protected int getCachedCount() {
        return dataProvider.getCachedCount();
    }
    private int internalGetCachedCount() {
        return super.getCachedCount();
    }
    /**
     * @return
     */
    protected int getFreePos() {
        return dataProvider.getFreePos();
    }
    private int internalGetFreePos() {
        return super.getFreePos();
    }
    /**
     * @param pos
     * @return
     */
    protected CachedRow getRow(int pos) {
        return dataProvider.getRow(pos);
    }
    private CachedRow internalGetRow(int pos) {
        return super.getRow(pos);
    }
    /**
     * @param pos
     * @param t
     * @return
     * @throws org.hsqldb.HsqlException
     */
    protected CachedRow getRow(int pos, Table t) throws HsqlException {
        return dataProvider.getRow(pos, t);
    }
    private CachedRow internalGetRow(int pos, Table t) throws HsqlException {
        return super.getRow(pos, t);
    }
    /**
     * 
     */
    protected void init() {
        dataProvider.init();
    }
    private void internalInit() {
        super.init();
    }
    /**
     * @throws org.hsqldb.HsqlException
     */
    protected void initBuffers() throws HsqlException {
        dataProvider.initBuffers();
    }
    private void internalInitBuffers() throws HsqlException {
        super.initBuffers();
    }
    /**
     * @throws org.hsqldb.HsqlException
     */
    protected void initParams() throws HsqlException {
        dataProvider = createDataProvider(sName);
        dataProvider.initParams();
    }
    private void internalInitParams() throws HsqlException {
        super.initParams();
    }
    /**
     * @param pos
     * @param t
     * @return
     * @throws org.hsqldb.HsqlException
     */
    protected CachedRow makeRow(int pos, Table t) throws HsqlException {
        return dataProvider.makeRow(pos, t);
    }
    protected CachedRow internalMakeRow(int pos, Table t) throws HsqlException {
        return super.makeRow(pos, t);
    }
    /**
     * @param readonly
     * @throws org.hsqldb.HsqlException
     */
    protected void open(boolean readonly) throws HsqlException {
        dataProvider.open(readonly);
    }
    private void internalOpen(boolean readonly) throws HsqlException {
        super.open(readonly);
    }
    /**
     * @param r
     * @return
     * @throws org.hsqldb.HsqlException
     */
    protected CachedRow remove(CachedRow r) throws HsqlException {
        return dataProvider.remove(r);
    }
    private CachedRow internalRemove(CachedRow r) throws HsqlException {
        return super.remove(r);
    }
    /**
     * @param t
     * @throws org.hsqldb.HsqlException
     */
    protected void remove(Table t) throws HsqlException {
        dataProvider.remove(t);
    }
    private void internalRemove(Table t) throws HsqlException {
        super.remove(t);
    }
    /**
     * @throws org.hsqldb.HsqlException
     */
    protected void saveAll() throws HsqlException {
        dataProvider.saveAll();
    }
    private void internalSaveAll() throws HsqlException {
        super.saveAll();
    }
    /**
     * @param r
     * @throws java.io.IOException
     * @throws org.hsqldb.HsqlException
     */
    protected void saveRow(CachedRow r) throws IOException, HsqlException {
        dataProvider.saveRow(r);
    }
    private void internalSaveRow(CachedRow r) throws IOException, HsqlException {
        super.saveRow(r);
    }
    /**
     * @param r
     * @return
     * @throws org.hsqldb.HsqlException
     */
    protected int setFilePos(CachedRow r) throws HsqlException {
        return dataProvider.setFilePos(r);
    }
    private int internalSetFilePos(CachedRow r) throws HsqlException {
        return super.setFilePos(r);
    }
    /**
     * @param r
     * @throws org.hsqldb.HsqlException
     */
    protected void setStorageSize(CachedRow r) throws HsqlException {
        dataProvider.setStorageSize(r);
    }
    private void internalSetStorageSize(CachedRow r) throws HsqlException {
        super.setStorageSize(r);
    }
    
    // DATA PROVIDERS ---------------------------------------------------------
    private static final HashMap providers = new HashMap();
    static {
        InputStream i = TextCache.class.getResourceAsStream("DataProviders.properties");
        if (i != null) try {
            Properties tmp = new Properties();
            tmp.load(i);
            for (Iterator it = tmp.entrySet().iterator(); it.hasNext();) {
                Map.Entry entry = (Map.Entry)it.next();
                providers.put(entry.getKey().toString().trim().toLowerCase(),
                        entry.getValue().toString().trim());
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
    
    public static void registerDataProvider(String ext, Class cls) {
        providers.put(ext.trim().toLowerCase(), cls.getName());
    }
    public static void unregisterDataProvider(String ext) {
        providers.remove(ext.trim().toLowerCase());
    }
    protected DataProvider createDataProvider(String path) {
        int idx = path.indexOf(';');
        if (idx != -1) path = path.substring(0, idx);
        idx = path.lastIndexOf('.');
        String ext = (idx == -1) ? "csv" : path.substring(idx + 1);
        String clsName = (String) providers.get(ext.trim().toLowerCase());
        if (clsName == null) {
            clsName = TextDataProvider.class.getName();
        }
        try {
            Class cls = Class.forName(clsName);
            Constructor c = cls.getConstructor(new Class[] {TextCache.class});
            return (DataProvider) c.newInstance(new Object[] {this});
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage());
        }
    }
    int getLineNumber() {
        return dataProvider.getLineNumber();
    }

    void setSourceIndexing(boolean mode) {
        isIndexingSource = mode;
    }
    /**
     * Closes the source file and deletes it if it is not read-only.
     */
    void purge() throws HsqlException {

        if (dataFile == null) {
            return;
        }

        try {
            if (readOnly) {
                close();
            } else {
                dataFile.close();

                dataFile = null;

                FileUtil.delete(sName);
            }
        } catch (Exception e) {
            throw Trace.error(Trace.FILE_IO_ERROR,
                              Trace.TextCache_purging_file_error,
                              new Object[] {
                sName, e
            });
        }
    }
    void reopen() throws HsqlException {
        dataProvider.reopen();
    }
}
