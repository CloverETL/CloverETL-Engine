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

import java.io.File;
import java.io.IOException;

import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.ZipUnzipFile;
import org.hsqldb.rowio.RowOutputBase;

/**
 * Acts as a buffer manager for CACHED table Row data and Index structures.
 *
 * @version 1.7.2
 */
public class DataFileCache extends Cache {

    private static final int MAX_FREE_COUNT = 1024;

    DataFileCache(String name, Database db) throws HsqlException {
        super(name, db);
    }

    /**
     * Opens the *.data file for this cache, setting the variables that
     * allow accesse to the particular database version of the *.data file.
     */
    void open(boolean readonly) throws HsqlException {

        try {
            boolean exists = false;
            File    f      = new File(sName);

            if (f.exists() && f.length() > FREE_POS_POS) {
                exists = true;
            }

            boolean isNio = this.dDatabase.getProperties().isPropertyTrue(
                "hsqldb.nio_data_file");
            int fileType = isNio ? ScaledRAFile.DATA_FILE_NIO
                                 : ScaledRAFile.DATA_FILE_RAF;

            dataFile = ScaledRAFile.newScaledRAFile(sName, readonly, 1,
                    fileType);

            if (exists) {
                dataFile.seek(FREE_POS_POS);

                fileFreePosition = dataFile.readInt();
            } else {

// erik - iFreePos = INITIAL_FREE_POS / cacheFileScale;
                fileFreePosition = INITIAL_FREE_POS;

                dbProps.setProperty("hsqldb.cache_version", "1.7.0");
            }

            String cacheVersion = dbProps.getProperty("hsqldb.cache_version",
                "1.6.0");

            if (cacheVersion.equals("1.7.0")) {
                cachedRowType = RowOutputBase.CACHED_ROW_170;
            }

            initBuffers();

            fileModified = false;
        } catch (Exception e) {
            throw Trace.error(Trace.FILE_IO_ERROR, Trace.DataFileCache_open,
                              new Object[] {
                e, sName
            });
        }
    }

    /**
     *  Writes out all cached rows that have been modified and the free
     *  position pointer for the *.data file and then closes the file.
     */
    void close() throws HsqlException {

        if (dataFile == null || dataFile.isReadOnly()) {
            return;
        }

        try {
            dataFile.seek(FREE_POS_POS);
            dataFile.writeInt(fileFreePosition);
            saveAll();
            dataFile.close();

            dataFile = null;

            boolean empty = fileFreePosition == INITIAL_FREE_POS;

            if (empty) {
                new File(sName).delete();
            }
        } catch (Exception e) {
            throw Trace.error(Trace.FILE_IO_ERROR, Trace.DataFileCache_close,
                              new Object[] {
                e, sName
            });
        }
    }

    /** @todo fredt - better error message */

    /**
     *  Writes out all the rows to a new file without fragmentation and
     *  returns an ArrayList containing new positions for index roots.
     *  Is called with the cache file closed.
     *
     *  Not possible with nio .data file as it can't be overwritten
     */
    void defrag() throws HsqlException {

        close();

        // return here if *.data file was deleted because it was empty
        if (!FileUtil.exists(sName)) {
            init();
            open(cacheReadonly);
            Trace.printSystemOut("opened empty chache");

            return;
        }

        HsqlArrayList indexRoots = null;

        try {

            // open as readonly
            open(true);

            boolean        wasNio = dataFile.wasNio();
            DataFileDefrag dfd    = new DataFileDefrag();

            indexRoots = dfd.defrag(dDatabase, sName);

            closeFile();
            Trace.printSystemOut("closed old cache");

            if (wasNio) {
                System.gc();
                FileUtil.renameOverwrite(sName, sName + ".old");

                File oldfile = new File(sName + ".old");

                oldfile.delete();
                FileUtil.deleteOnExit(oldfile);
            }

            FileUtil.renameOverwrite(sName + ".new", sName);

            String backupName = dDatabase.getPath() + ".backup";

            backup(backupName + ".new");
            FileUtil.renameOverwrite(backupName + ".new", backupName);
            dbProps.setProperty("hsqldb.cache_version", "1.7.0");

            for (int i = 0; i < indexRoots.size(); i++) {
                int[] roots = (int[]) indexRoots.get(i);

                if (roots != null) {
                    Trace.printSystemOut(
                        org.hsqldb.lib.StringUtil.getList(roots, " ", ""));
                }
            }
        } catch (Exception e) {

//            e.printStackTrace();
            throw Trace.error(Trace.FILE_IO_ERROR,
                              Trace.DataFileCache_defrag, new Object[] {
                e, sName
            });
        } finally {
            init();
            open(cacheReadonly);

            if (indexRoots != null) {
                DataFileDefrag.updateTableIndexRoots(dDatabase.getTables(),
                                                     indexRoots);
            }

            Trace.printSystemOut("opened cache");
        }
    }

    /**
     *  Closes this object's database file without flushing pending writes.
     */
    void closeFile() throws HsqlException {

        Trace.printSystemOut("DataFileCache.closeFile()");

        if (dataFile == null) {
            return;
        }

        try {
            dataFile.close();

            dataFile = null;
        } catch (Exception e) {
            throw Trace.error(Trace.FILE_IO_ERROR,
                              Trace.DataFileCache_closeFile, new Object[] {
                sName, e
            });
        }
    }

    /**
     * Used when a row is deleted as a result of some DML or DDL command.
     * Adds the file space for the row to the list of free positions.
     * If there exists more than MAX_FREE_COUNT free positions,
     * then they are probably all too small, so we start a new list. <p>
     * todo: This is wrong when deleting lots of records <p>
     * Then remove the row from the cache data structures.
     */
    void free(CachedRow r) throws HsqlException {

        fileModified = true;

        iFreeCount++;

        CacheFree n = new CacheFree();

        n.iPos    = r.iPos;
        n.iLength = r.storageSize;

        if (iFreeCount > MAX_FREE_COUNT) {
            iFreeCount = 0;
        } else {
            n.fNext = fRoot;
        }

        fRoot = n;

        // it's possible to remove roots too
        // a newer copy of the row may be in cache
        r = getRow(r.iPos);

        if (r != null) {
            remove(r);
        }
    }

    /**
     * Allocates file space for the row. <p>
     *
     * A Row is added by walking the list of CacheFree objects to see if
     * there is available space to store it, reusing space if it exists.
     * Otherwise the file is grown to accommodate it.
     */
    int setFilePos(CachedRow r) throws HsqlException {

        int       rowSize = r.storageSize;
        int       size    = rowSize;
        CacheFree f       = fRoot;
        CacheFree last    = null;
        int       i       = fileFreePosition;

        while (f != null) {

            // first that is long enough
            if (f.iLength >= size) {
                i    = f.iPos;
                size = f.iLength - size;

                if (size < 32) {

                    // remove almost empty blocks
                    if (last == null) {
                        fRoot = f.fNext;
                    } else {
                        last.fNext = f.fNext;
                    }

                    iFreeCount--;
                } else {
                    f.iLength = size;

// erik  f.iPos += rowSize / cacheFileScale
                    f.iPos += rowSize;
                }

                break;
            }

            last = f;
            f    = f.fNext;
        }

        if (i == fileFreePosition) {

// erik  iFreePs += size / cacheFileScale
            fileFreePosition += size;
        }

        r.setPos(i);

        return i;
    }

    /**
     * Constructs a new Row for the specified table, using row data read
     * at the specified position (pos) in this object's database file.
     */
    protected CachedRow makeRow(int pos, Table t) throws HsqlException {

        CachedRow r = null;

        makeRowCount++;

        try {

// erik -  rFile.readSeek(pos*cacheFileScale);
            dataFile.seek(pos);

            int size = dataFile.readInt();

            rowIn.resetRow(pos, size);
            dataFile.read(rowIn.getBuffer(), 4, size - 4);

            r = new CachedRow(t, rowIn);
        } catch (IOException e) {
            throw Trace.error(Trace.FILE_IO_ERROR,
                              Trace.DataFileCache_makeRow, new Object[] {
                e, sName
            });
        }

        return r;
    }

    /**
     * Writes out the specified Row. Will write only the Nodes or both Nodes
     * and table row data depending on what is not already persisted to disk.
     */
    protected void saveRow(CachedRow r) throws IOException, HsqlException {

        rowOut.reset();

// erik - multiply position by cacheFileScale   rFile.seek(r.iPos * cacheFileScale);
        dataFile.seek(r.iPos);
        r.write(rowOut);
        dataFile.write(rowOut.getOutputStream().getBuffer(), 0,
                       rowOut.getOutputStream().size());
    }

    /**
     *  Saves the *.data file as compressed *.backup.
     *
     * @throws  HsqlException
     */
    void backup(String newName) throws HsqlException {

        try {

            // create a '.new' file; rename later
            ZipUnzipFile.compressFile(sName, newName);
        } catch (Exception e) {
            throw Trace.error(Trace.FILE_IO_ERROR,
                              Trace.DataFileCache_backup + newName);
        }
    }

    static void resetFreePos(String filename) {

        ScaledRAFile raFile = null;

        try {
            raFile = ScaledRAFile.newScaledRAFile(filename, false, 1,
                                                  ScaledRAFile.DATA_FILE_RAF);

            raFile.seek(Cache.FREE_POS_POS);
            raFile.writeInt(Cache.INITIAL_FREE_POS);
        } catch (IOException e) {}
        finally {
            if (raFile != null) {
                try {
                    raFile.close();
                } catch (IOException e) {}
            }
        }
    }

    /**
     * Calculates the number of bytes required to store a Row in this object's
     * database file.
     */
    protected void setStorageSize(CachedRow r) throws HsqlException {

        // iSize = 4 bytes, each index = 32 bytes
        Table t    = r.getTable();
        int   size = rowStoreExtra + 16 * t.getIndexCount();

        size += rowOut.getSize(r);
        size = ((size + cachedRowPadding - 1) / cachedRowPadding)
               * cachedRowPadding;    // align to 8 byte blocks
        r.storageSize = size;
    }

    int getCachedCount() {
        return this.iCacheSize;
    }
}
