/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP, 
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals 
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2004, The HSQL Development Group
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

import org.hsqldb.lib.ArrayCounter;
import org.hsqldb.lib.ObjectComparator;
import org.hsqldb.lib.Sort;
import org.hsqldb.rowio.RowInputBase;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputBase;
import org.hsqldb.rowio.RowOutputBinary;
import org.hsqldb.rowio.RowOutputInterface;

// fredt@users 20011220 - patch 437174 by hjbusch@users - cache update
// most changes and comments by HJB are kept unchanged
// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP) - cache update
// fredt@users 20020320 - doc 1.7.0 by boucherb@users - doc update
// fredt@users 20021105 - patch 1.7.2 - refactoring and enhancements
// fredt@users 20021215 - doc 1.7.2 - javadoc comments rewritten

/** @todo - string literal - PrintSystemOut()*/

/** @todo -
 * fredt20021022
 * Check for MAX_INTEGER size.
 * Optional file size limit below MAX_INTEGER.
 * Optional defrag when limit reached.
 * Store total amount of free.
 */

/**
 *
 * Handles cached table persistence with a *.data file and memory cache.<p>
 *
 * All CACHED tables are stored in a *.data file. The Cache object provides
 * buffered access to the rows. The buffer is a hash index implementation,
 * with separate chaining. The maximum number of elements is fixed.<p>
 *
 * Chains of elements for each index slot are linked to the next
 * and previous chain to form a circular double-linked list of all the
 * cached elements. When the maximum number of elements is reached and new
 * rows are added to the cache, either due to INSERT commands or reading from
 * the disk, some elements of the cache are released. These elements are
 * selected from the least frequently-accessed rows. If any of these rows
 * have been modified, they are written to disk before being freed.<p>
 *
 * In 1.7.2 a new cleanUp() method allows the rows to be freed to be selected
 * precisely according to their last access sequence number. The double-linked
 * list is traversed once and all the access count values are copied to an
 * int array. A statistical function calculates the access sequence number
 * corresponding to the given percentile. This value is used in another
 * traversal of the double-linked list to put the given percentile of rows.
 * Modified rows are put in an array and sorted according to their file
 * position before they are saved to disk and removed from the array.<p>
 *
 * A separate linked list of free slots in the *.data file is also kept. This
 * list is formed when rows are deleted from the database, and is used for
 * allocating space to newly created rows.<p>
 *
 * The maximum number of rows in the Cache is three times the size of the
 * Cache array.<p>
 *
 * The algorithm for freeing rows from the cache has changed in version
 * 1.7.2 to better reflect row usage. The trigger for CleanUp() is now
 * internal to Cache and fired exactly when the maximum size is reached.<p>
 *
 * Subclasses of Cache are used for TEXT tables, where each TextCache Object
 * corresponds to a single table in the database and accesses that table's
 * data source.<p>
 *
 * Maximum size of the all data in cache is not yet enforced (1_7_2_alpha_i)
 * but will be implemented via the cache_size_scale property.
 *
 *
 * @version    1.7.2
 * @see        CachedRow
 * @see        CacheFree
 */
abstract class Cache {

    static final int CACHE_TYPE_DATA         = 0;
    static final int CACHE_TYPE_TEXT         = 1;
    static final int CACHE_TYPE_REVERSE_TEXT = 2;

    // cached row access counter
    int currentAccessCount;
    int firstAccessCount;

    // pre openning fields
    protected Database dDatabase;

// boucherb@users - access changed for metadata 1.7.2
    protected HsqlDatabaseProperties dbProps;
    protected String                 sName;

// --------------------------------------------------
    // cache operation mode
// boucherb@users - access changed for metadata 1.7.2
    protected boolean storeOnInsert;

// --------------------------------------------------
    // post openning constant fields
    boolean cacheReadonly;

    // this flag is used externally to determine if a backup is required
    boolean fileModified;
    int     maxNioScale;

    // package access to all variables below allowed only for metadata
    int                         cacheScale;
    int                         cacheSizeScale;
    int                         cacheFileScale;
    protected int               cachedRowPadding = 8;
    int                         cachedRowType = RowOutputBase.CACHED_ROW_160;
    protected int               rowStoreExtra;
    int                         maxCacheSize;     // number of Rows
    long                        maxCacheBytes;    // number of bytes
    int                         multiplierMask;
    private CachedRowComparator rowComparator;
    private CachedRow[]         rowTable;
    private CachedRow[]         rData;
    private int[]               accessCount;

    // file format fields
    static final int FREE_POS_POS        = 16;    // where iFreePos is saved
    static final int INITIAL_FREE_POS    = 32;
    static final int ROW_STORE_EXTRA_160 = 8;
    static final int ROW_STORE_EXTRA_170 = 4;

    // variable fields
// boucherb@users - access changed for metadata 1.7.2
    protected ScaledRAFile dataFile;
    protected int          fileFreePosition;

// ---------------------------------------------------
    //
    private CachedRow rFirst;                     // must point to one of rData[]

    // outside access allowed to all below only for system table
    CacheFree fRoot;
    int       iFreeCount;
    int       iCacheSize;
    long      cacheBytesLength;

    // reusable input / output streams
    RowInputInterface            rowIn;
    protected RowOutputInterface rowOut;

    // for testing
//    StopWatch saveAllTimer = new StopWatch(false);
//    StopWatch makeRowTimer = new StopWatch(false);
//    StopWatch sortTimer    = new StopWatch(false);
//    StopWatch rankTimer    = new StopWatch(false);
    int makeRowCount = 0;
    int saveRowCount = 0;

    Cache(String name, Database db) throws HsqlException {

        sName     = name;
        dDatabase = db;
        dbProps   = db.getProperties();

        initParams();
        init();
    }

    /**
     * initial external parameters are set here.
     */
    protected void initParams() throws HsqlException {

        cacheScale = dbProps.getIntegerProperty("hsqldb.cache_scale", 14, 8,
                18);
        cacheSizeScale = dbProps.getIntegerProperty("hsqldb.cache_size_scale",
                10, 6, 20);

        // fredt - this is not yet supported
        cacheFileScale = dbProps.getIntegerProperty("hsqldb.cache_file_scale",
                1, 1, 8);

        if (cacheFileScale != 8) {
            cacheFileScale = 1;
        }

        Trace.printSystemOut("cache_scale: " + cacheScale);
        Trace.printSystemOut("cache_size_scale: " + cacheSizeScale);
    }

    /**
     *  Structural initialisations take place here. This allows the Cache to
     *  be resized while the database is in operation.
     */
    protected void init() {

        cacheReadonly = dDatabase.isFilesReadOnly();

        int lookupTableLength = 1 << cacheScale;
        int avgRowBytes       = 1 << cacheSizeScale;

        // HJB-2001-06-21: let the cache be larger than the array
        maxCacheSize     = lookupTableLength * 3;
        maxCacheBytes    = maxCacheSize * avgRowBytes;
        multiplierMask   = lookupTableLength - 1;
        rowComparator    = new CachedRowComparator();
        accessCount      = new int[maxCacheSize];
        rowTable         = new CachedRow[maxCacheSize];
        rData            = new CachedRow[lookupTableLength];
        rFirst           = null;
        fileFreePosition = 0;
        fRoot            = null;
        iFreeCount       = 0;
        iCacheSize       = 0;
        cacheBytesLength = 0;
    }

    protected void initBuffers() throws HsqlException {

        rowOut = RowOutputBase.newRowOutput(cachedRowType);
        rowIn  = RowInputBase.newRowInput(cachedRowType);
        rowStoreExtra = rowOut instanceof RowOutputBinary
                        ? ROW_STORE_EXTRA_170
                        : ROW_STORE_EXTRA_160;
    }

    abstract void open(boolean readonly) throws HsqlException;

    abstract void close() throws HsqlException;

    abstract void defrag() throws HsqlException;

    abstract void closeFile() throws HsqlException;

    abstract void free(CachedRow r) throws HsqlException;

    protected abstract void setStorageSize(CachedRow r) throws HsqlException;

    /**
     * Adds a new Row to the Cache. This is used when a new database row is
     * created<p>
     */
    void add(CachedRow r) throws HsqlException {

        fileModified = true;

        if (iCacheSize >= maxCacheSize
                || r.storageSize + cacheBytesLength > maxCacheBytes) {
            cleanUp();
        }

        setStorageSize(r);
        resetAccessCount();

        r.iLastAccess = currentAccessCount++;

        int i = setFilePos(r);

        // HJB-2001-06-21
        int       k      = (i >> 3) & multiplierMask;
        CachedRow before = rData[k];

        if (before == null) {
            before = rFirst;
        }

        r.insert(before);

        try {

            // for text tables
            if (storeOnInsert) {
                saveRow(r);
            }
        } catch (IOException e) {
            throw Trace.error(Trace.FILE_IO_ERROR);
        }

        iCacheSize++;

        cacheBytesLength += r.storageSize;
        rData[k]         = r;
        rFirst           = r;
    }

    abstract int setFilePos(CachedRow r) throws HsqlException;

    abstract protected CachedRow makeRow(int pos,
                                         Table t) throws HsqlException;

    /**
     * Reads a Row object from this Cache that corresponds to the
     * (pos) file offset in .data file.
     */
    CachedRow getRow(int pos, Table t) throws HsqlException {

        CachedRow r = getRow(pos);

        if (r != null) {
            resetAccessCount();

            r.iLastAccess = currentAccessCount++;

            return r;
        }

        if (iCacheSize >= maxCacheSize) {
            cleanUp();
        }

//        makeRowTimer.start();
        //-- makeRow in text tables may change iPos because of blank lines
        //-- row can be null at the end of csv file
        r = makeRow(pos, t);

//        makeRowTimer.stop();
        if (r == null) {
            return r;
        }

        int       k      = (r.iPos >> 3) & multiplierMask;
        CachedRow before = rData[k];

        if (before == null) {
            before = rFirst;
        }

        r.insert(before);

        iCacheSize++;

        cacheBytesLength += r.storageSize;
        rData[k]         = r;
        rFirst           = r;

        resetAccessCount();

        r.iLastAccess = currentAccessCount++;

        if (cacheBytesLength > maxCacheBytes) {
            cleanUp();
        }

        return r;
    }

    CachedRow getRow(int pos) {

        // HJB-2001-06-21
        int       k     = (pos >> 3) & multiplierMask;
        CachedRow r     = rData[k];
        CachedRow start = r;
        int       p     = 0;

        while (r != null) {
            p = r.iPos;

            if (p == pos) {
                return r;
            } else if (((p >> 3) & multiplierMask) != k) {

                // HJB-2001-06-21 - check the row belongs to this bucket
                break;
            }

            r = r.rNext;

            if (r == start) {
                break;
            }
        }

        return null;
    }

    /**
     * Reduces the number of rows held in this Cache object. <p>
     *
     * Cleanup is done by checking the accessCount of the Rows and removing
     * the rows with the lowest access count.
     *
     * Index operations require that up to 5 recently accessed rows remain
     * in the cache.
     *
     */
    private void cleanUp() throws HsqlException {

        // put access count in the array
        for (int i = 0; i < iCacheSize; i++) {
            accessCount[i] = rFirst.iLastAccess;
            rFirst         = rFirst.rNext;
        }

//        rankTimer.start();
        // It is possible the target access count is shared
        // by many elements due to resetting of all access counts. So remove
        // the next access count to account for this condition.
        firstAccessCount =
            ArrayCounter.rank(
                accessCount, iCacheSize / 4, firstAccessCount,
                currentAccessCount, iCacheSize / 512) + 1;

//        rankTimer.stop();
        // put all low rows in the array
        int removecount = 0;

        for (int i = 0; i < iCacheSize; i++) {
            if (rFirst.iLastAccess < firstAccessCount) {
                rowTable[removecount++] = rFirst;
            }

            rFirst = rFirst.rNext;
        }

        rowComparator.setType(rowComparator.COMPARE_POSITION);

//        sortTimer.start();
        Sort.sort(rowTable, rowComparator, 0, removecount - 1);

//        sortTimer.stop();
//        saveAllTimer.start();
        int removedRows = 0;

        for (int i = 0; i < removecount; i++) {
            CachedRow r = rowTable[i];

            try {
                if (r.hasChanged()) {
                    saveRow(r);

                    saveRowCount++;
                }

                if (!r.isRoot()) {
                    remove(r);

                    removedRows++;
                }
            } catch (Exception e) {
                throw Trace.error(Trace.FILE_IO_ERROR, Trace.Cache_cleanUp,
                                  new Object[]{ e });
            }

            // all rows must be cleard from array - even those that remain in cache
            rowTable[i] = null;
        }

//        saveAllTimer.stop();
        // Trace.printSystemOut("cache.cleanup() min access: ", tempfirstaccess);
        // Trace.printSystemOut("cache.cleanup() current access: ", currentAccessCount);
        // Trace.printSystemOut("cache.cleanup() total saveRowCount: ", saveRowCount);
        // Trace.printSystemOut("cache.cleanup() removed row count for this call: ", removedRows);
        initBuffers();
    }

    /**
     * Removes all Row objects for a table from this Cache object. This is
     * done when a table is dropped. Necessary because the Rows corresponding
     * to index roots will never be removed otherwise. This doesn't add the
     * Rows to the free list and does not by itself modify the *.data file.
     */
    protected void remove(Table t) throws HsqlException {

        CachedRow row = rFirst;

        for (int i = 0; i < iCacheSize; i++) {
            if (row.tTable == t) {
                row = remove(row);
            } else {
                row = row.rNext;
            }
        }
    }

    /**
     * Removes a Row from this Cache object. This is done when there is no
     * room for extra rows to be read from the disk.
     */
    protected CachedRow remove(CachedRow r) throws HsqlException {

        // r.hasChanged() == false unless called from Cache.remove(Table)
        // make sure rData[k] does not point here
        // HJB-2001-06-21
        int k = (r.iPos >> 3) & multiplierMask;

        if (rData[k] == r) {
            CachedRow n = r.rNext;

            rFirst = n;

            if (n == r || ((n.iPos >> 3) & multiplierMask) != k) {    // HJB-2001-06-21
                n = null;
            }

            rData[k] = n;
        }

        // make sure rFirst does not point here
        if (r == rFirst) {
            rFirst = rFirst.rNext;

            if (r == rFirst) {
                rFirst = null;
            }
        }

        iCacheSize--;

        cacheBytesLength -= r.storageSize;

        return r.free();
    }

    /**
     * Scales down all iLastAccess values to avoid negative numbers.
     *
     */
    private void resetAccessCount() throws HsqlException {

        if (currentAccessCount != Integer.MAX_VALUE) {
            return;
        }

        currentAccessCount >>= 2;
        firstAccessCount   >>= 2;

        int i = iCacheSize;

        while (i-- > 0) {
            rFirst.iLastAccess >>= 2;
            rFirst             = rFirst.rNext;
        }
    }

    /**
     * Writes out all modified cached Rows.
     */
    protected void saveAll() throws HsqlException {

//        saveAllTimer.start();
        if (rFirst == null) {
            return;
        }

        int j = 0;

        for (int i = 0; i < iCacheSize; i++) {
            if (rFirst.hasChanged) {
                rowTable[j++] = rFirst;
            }

            rFirst = rFirst.rNext;
        }

        // sort by file position
        rowComparator.setType(rowComparator.COMPARE_POSITION);

        if (j != 0) {
            Sort.sort(rowTable, rowComparator, 0, j - 1);
        }

        for (int i = 0; i < j; i++) {
            CachedRow r = (CachedRow) rowTable[i];

            try {
                saveRow(r);

                saveRowCount++;

                rowTable[i] = null;
            } catch (Exception e) {
                throw Trace.error(Trace.FILE_IO_ERROR, Trace.Cache_saveAll,
                                  new Object[]{ e });
            }
        }

//        saveAllTimer.stop();
/*
        Trace.printSystemOut(
            saveAllTimer.elapsedTimeToMessage(
                "Cache.saveRow() total row save time"));
        Trace.printSystemOut("Cache.saveRow() total row save count = "
                             + saveRowCount);
        Trace.printSystemOut(
            makeRowTimer.elapsedTimeToMessage(
                "Cache.makeRow() total row load time"));
        Trace.printSystemOut("Cache.makeRow() total row load count = "
                             + makeRowCount);
        Trace.printSystemOut(
            sortTimer.elapsedTimeToMessage("Cache.sort() total time"));
        Trace.printSystemOut(
            rankTimer.elapsedTimeToMessage("Cache.rank() total time"));
 */
    }

    abstract protected void saveRow(CachedRow r)
    throws IOException, HsqlException;

    abstract void backup(String newName) throws HsqlException;

    /**
     * Getter for iFreePos member
     */
    int getFreePos() {
        return fileFreePosition;
    }

    static class CachedRowComparator implements ObjectComparator {

        static final int COMPARE_LAST_ACCESS = 0;
        static final int COMPARE_POSITION    = 1;
        static final int COMPARE_SIZE        = 2;
        private int      compareType;

        CachedRowComparator() {}

        void setType(int type) {
            compareType = type;
        }

        public int compare(Object a, Object b) {

            switch (compareType) {

                case COMPARE_LAST_ACCESS :
                    return ((CachedRow) a).iLastAccess
                           - ((CachedRow) b).iLastAccess;

                case COMPARE_POSITION :
                    return ((CachedRow) a).iPos - ((CachedRow) b).iPos;

                case COMPARE_SIZE :
                    return ((CachedRow) a).storageSize
                           - ((CachedRow) b).storageSize;

                default :
                    return 0;
            }
        }
    }
}
