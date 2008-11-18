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

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.hsqldb.lib.DoubleIntTable;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.StopWatch;
import org.hsqldb.rowio.RowOutputBinary;

/**
 *  Routine to defrag the *.data file.
 *
 *  This method iterates over the primary index of a table to find the
 *  disk position for each row and stores it, together with the new position
 *  in an array.
 *
 *  A second pass over the primary index writes each row to the new disk
 *  image after translating the old pointers to the new.
 *
 * @version    1.7.2
 * @author     fredt@users
 */
class DataFileDefrag {

    BufferedOutputStream fileStreamOut;
    long                 filePos;
    StopWatch            stopw = new StopWatch();

    HsqlArrayList defrag(Database db,
                         String filename) throws IOException, HsqlException {

        Trace.printSystemOut("Defrag Transfer begins");

        HsqlArrayList    rootsList = new HsqlArrayList();
        HsqlArrayList    tTable    = db.getTables();
        RandomAccessFile dest      = null;

        try {
            FileOutputStream fos = new FileOutputStream(filename + ".new",
                false);

            fileStreamOut = new BufferedOutputStream(fos, 1 << 12);

            for (int i = 0; i < DataFileCache.INITIAL_FREE_POS; i++) {
                fileStreamOut.write(0);
            }

            filePos = DataFileCache.INITIAL_FREE_POS;

            for (int i = 0, tSize = tTable.size(); i < tSize; i++) {
                Table t = (Table) tTable.get(i);

                if (t.tableType == Table.CACHED_TABLE) {
                    int[] rootsArray = writeTableToDataFile(t);

                    rootsList.add(rootsArray);
                } else {
                    rootsList.add(null);
                }

                Trace.printSystemOut(t.getName().name + " complete");
            }

            fileStreamOut.close();

            // write out the end of file position
            dest = new RandomAccessFile(filename + ".new", "rw");

            dest.seek(DataFileCache.FREE_POS_POS);
            dest.writeInt((int) filePos);

            for (int i = 0, size = rootsList.size(); i < size; i++) {
                int[] roots = (int[]) rootsList.get(i);

                if (roots != null) {
                    Trace.printSystemOut(
                        org.hsqldb.lib.StringUtil.getList(roots, ",", ""));
                }
            }
        } catch (IOException e) {
            throw Trace.error(Trace.FILE_IO_ERROR, filename + ".new");
        } finally {
            if (fileStreamOut != null) {
                fileStreamOut.close();
            }

            if (dest != null) {
                dest.close();
            }
        }

        //Trace.printSystemOut("Transfer complete: ", stopw.elapsedTime());
        return rootsList;
    }

    static void updateTableIndexRoots(org.hsqldb.lib.HsqlArrayList tTable,
                                      HsqlArrayList rootsList)
                                      throws HsqlException {

        for (int i = 0, size = tTable.size(); i < size; i++) {
            Table t = (Table) tTable.get(i);

            if (t.tableType == Table.CACHED_TABLE) {
                int[] rootsArray = (int[]) rootsList.get(i);

                t.setIndexRoots(rootsArray);
            }
        }
    }

    /** @todo fredt - for pointerLookup use an upward estimate of number of rows based on Index */
    int[] writeTableToDataFile(Table table)
    throws IOException, HsqlException {

        RowOutputBinary rowOut        = new RowOutputBinary();
        DoubleIntTable  pointerLookup = new DoubleIntTable(1000000);
        int[]           rootsArray    = table.getIndexRootsArray();
        Index           index         = table.getPrimaryIndex();
        long            pos           = filePos;
        int             count         = 0;

        Trace.printSystemOut("lookup begins: " + stopw.elapsedTime());

        for (Node n = index.first(); n != null; count++) {
            CachedRow row = (CachedRow) n.getRow();

            pointerLookup.add(row.iPos, (int) pos);

            if (count % 50000 == 0) {
                Trace.printSystemOut("pointer pair for row " + count + " "
                                     + row.iPos + " " + pos);
            }

            pos += row.storageSize;
            n   = index.next(n);
        }

        Trace.printSystemOut(table.getName().name + " list done ",
                             stopw.elapsedTime());

        count = 0;

        for (Node n = index.first(); n != null; count++) {
            CachedRow row = (CachedRow) n.getRow();

            rowOut.reset();

// code should be moved to CachedRow.java
            rowOut.writeSize(row.storageSize);

            Node rownode = row.nPrimaryNode;

            while (rownode != null) {
                ((DiskNode) rownode).writeTranslate(rowOut, pointerLookup);

                rownode = rownode.nNext;
            }

            rowOut.writeData(row.getData(), row.getTable());
            rowOut.writeEnd();

// end
            fileStreamOut.write(rowOut.getOutputStream().getBuffer(), 0,
                                rowOut.size());

            filePos += row.storageSize;

            if ((count) % 50000 == 0) {
                Trace.printSystemOut(count + " rows " + stopw.elapsedTime());
            }

            n = index.next(n);
        }

        for (int i = 0; i < rootsArray.length; i++) {
            if (rootsArray[i] == -1) {
                continue;
            }

            int lookupIndex = pointerLookup.find(0, rootsArray[i]);

            if (lookupIndex == -1) {
                throw Trace.error(Trace.DataFileDefrag_writeTableToDataFile);
            }

            rootsArray[i] = pointerLookup.get(lookupIndex, 1);
        }

        Trace.printSystemOut(table.getName().name + " : table converted");

        return rootsArray;
    }
}
