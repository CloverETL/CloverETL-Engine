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

import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;

// fredt@users 20020221 - patch 513005 by sqlbob@users (RMP)
// fredt@users 20020920 - patch 1.7.1 - refactoring to cut memory footprint
// fredt@users 20021205 - patch 1.7.2 - enhancements
// fredt@users 20021215 - doc 1.7.2 - javadoc comments
// boucherb@users - 20040411 - doc 1.7.2 - javadoc comments

/**
 *  In-memory representation of a disk-based database row object with  methods
 *  for serialization and de-serialization. <p>
 *
 *  A CachedRow is normally part of a circular double linked list which
 *  contains all of the Rows currently in the Cache for the database. It is
 *  unlinked from this list when it is freed from the Cache to make way for
 *  other rows.
 *
 * @version 1.7.2
 */
public class CachedRow extends Row {

    static final int NO_POS = -1;
    protected Table  tTable;
    int              iLastAccess;
    CachedRow        rLast, rNext;
    int              iPos = NO_POS;
    int              storageSize;

    /**
     *  Flag indicating any change to Node data.
     */
    protected boolean hasChanged;

    /**
     *  Flag indicating both Row and Node data has changed.
     */
    protected boolean hasDataChanged;

    /**
     *  Default constructor used only in subclasses.
     */
    CachedRow() {}

    /**
     *  Constructor for new Rows.  Variable hasDataChanged is set to true in
     *  order to indicate the data needs saving.
     *
     * @param t table
     * @param o row data
     * @throws HsqlException if a database access error occurs
     */
    public CachedRow(Table t, Object o[]) throws HsqlException {

        tTable = t;

        int indexcount = t.getIndexCount();

        nPrimaryNode = Node.newNode(this, 0, t);

        Node n = nPrimaryNode;

        for (int i = 1; i < indexcount; i++) {
            n.nNext = Node.newNode(this, i, t);
            n       = n.nNext;
        }

        oData      = o;
        hasChanged = hasDataChanged = true;

        t.addRowToStore(this);
    }

    /**
     *  Constructor when read from the disk into the Cache.
     *
     * @param t table
     * @param in data source
     * @throws IOException
     * @throws HsqlException
     */
    public CachedRow(Table t,
                     RowInputInterface in) throws IOException, HsqlException {

        tTable      = t;
        iPos        = in.getPos();
        storageSize = in.getSize();

        int indexcount = t.getIndexCount();

        nPrimaryNode = Node.newNode(this, in, 0, t);

        Node n = nPrimaryNode;

        for (int i = 1; i < indexcount; i++) {
            n.nNext = Node.newNode(this, in, i, t);
            n       = n.nNext;
        }

        oData = in.readData(tTable.getColumnTypes(), tTable.columnCount);

        setPos(iPos);

        // change from 1.7.0 format - the check is no longer read or written
        // Trace.check(in.readIntData() == iPos, Trace.INPUTSTREAM_ERROR);
    }

    /**
     *  This method is called only when the Row is deleted from the database
     *  table. The links with all the other objects apart from the data
     *  are removed.
     *
     * @throws HsqlException
     */
    void delete() throws HsqlException {

        Record.memoryRecords++;

        hasChanged = false;

        tTable.removeRow(this);

        rNext        = null;
        rLast        = null;
        tTable       = null;
        nPrimaryNode = null;
    }

    /**
     * Sets the file position for the row and registers the row with
     * the table.
     *
     * @param pos position in data file
     */
    void setPos(int pos) {

        iPos = pos;

        tTable.registerRow(this);
    }

    /**
     * Sets flag for Node data change.
     */
    void setChanged() {
        hasChanged = true;
    }

    /**
     * Returns true if Node data has changed.
     *
     * @return boolean
     */
    boolean hasChanged() {
        return hasChanged;
    }

    /**
     * Sets flag for Row data change.
     */
    void setDataChanged() {
        hasDataChanged = true;
    }

    /**
     * Returns true if either Row or Node data.
     *
     * @return boolean
     */
    boolean hasDataChanged() {
        return hasDataChanged;
    }

    /**
     * Returns the Table to which this Row belongs.
     *
     * @return Table
     */
    public Table getTable() {
        return tTable;
    }

    /**
     * Returns true if any of the Nodes for this row is a root node.
     * Used only in Cache.java to avoid removing the row from the cache.
     *
     * @return boolean
     * @throws HsqlException
     */
    boolean isRoot() throws HsqlException {

        Node n = nPrimaryNode;

        while (n != null) {
            if (Trace.DOASSERT) {
                Trace.doAssert(n.getBalance() != -2);
            }

            if (n.isRoot()) {
                return true;
            }

            n = n.nNext;
        }

        return false;
    }

    /**
     *  Using the internal reference to the Table, returns the current cached
     *  Row. Valid for deleted rows only before any subsequent insert or
     *  update on any cached table.<p>
     *
     *  Access to tables while performing the internal operations for an
     *  SQL statement result in CachedRow objects to be cleared from the cache.
     *  This method returns the CachedRow, loading it to the cache if it is not
     *  there.
     * @return the current Row in Cache for this Object
     * @throws HsqlException
     */
    Row getUpdatedRow() throws HsqlException {
        return tTable == null ? null
                              : tTable.getRow(iPos, null);
    }

    /**
     *  Used exclusively by Cache to save the row to disk. New implementation
     *  in 1.7.2 writes out only the Node data if the table row data has not
     *  changed. This situation accounts for the majority of invocations as
     *  for each row deleted or inserted, the Nodes for several other rows
     *  will change.
     *
     * @param output data source
     * @throws IOException
     * @throws HsqlException
     */
    void write(RowOutputInterface out) throws IOException, HsqlException {

        writeNodes(out);

        if (hasDataChanged) {
            out.writeData(oData, tTable);
            out.writeEnd();
        }

        hasDataChanged = false;
    }

    /**
     *  Writes the Nodes, immediately after the row size.
     *
     * @param out
     *
     * @throws IOException
     * @throws HsqlException
     */
    private void writeNodes(RowOutputInterface out)
    throws IOException, HsqlException {

        out.writeSize(storageSize);

        Node n = nPrimaryNode;

        while (n != null) {
            n.write(out);

            n = n.nNext;
        }

        hasChanged = false;
    }

    /**
     * Used to insert the Row into the linked list that includes all the rows
     * currently in the Cache.
     *
     * @param before the row before which to insert
     */
    void insert(CachedRow before) {

        Record.memoryRecords++;

        if (before == null) {
            rNext = this;
            rLast = this;
        } else {
            rNext        = before;
            rLast        = before.rLast;
            before.rLast = this;
            rLast.rNext  = this;
        }
    }

    /**
     *  Removes the Row from the linked list of Rows in the Cache.
     *
     * @return the next Row in the linked list
     * @throws HsqlException never
     */
    CachedRow free() throws HsqlException {

        CachedRow nextrow = rNext;

        rLast.rNext = rNext;
        rNext.rLast = rLast;
        rNext       = rLast = null;

        if (nextrow == this) {
            return null;
        }

        return nextrow;
    }

    /**
     * Lifetime scope of this method depends on the operations performed on
     * any cached tables since this row or the parameter were constructed.
     * If only deletes or only inserts have been performed, this method
     * remains valid. Otherwise it can return invalid results.
     *
     * @param obj row to compare
     * @return boolean
     */
    public boolean equals(Object obj) {

        if (obj == this) {
            return true;
        }

        if (obj instanceof CachedRow) {
            return ((CachedRow) obj).iPos == iPos;
        }

        return false;
    }

    /**
     * Hash code is valid only until a modification to the cache
     *
     * @return file position of row
     */
    public int hashCode() {
        return iPos;
    }
}
