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

import java.util.NoSuchElementException;

import org.hsqldb.index.RowIterator;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.HsqlNameManager.HsqlName;

// fredt@users 20020221 - patch 513005 by sqlbob@users - corrections
// fredt@users 20020225 - patch 1.7.0 - cascading deletes
// a number of changes to support this feature
// tony_lai@users 20020820 - patch 595052 - better error message
// fredt@users 20021205 - patch 1.7.2 - changes to method signature

/**
 * Implementation of an AVL tree with parent pointers in nodes. Subclasses
 * of Node implement the tree node objects for memory or disk storage. An
 * Index has a root Node that is linked with other nodes using Java Object
 * references or file pointers, depending on Node implementation.<p>
 * An Index object also holds information on table columns (in the form of int
 * indexes) that are covered by it.(fredt@users)
 *
 * @version 1.7.2
 */
public class Index {

    // types of index
    static final int MEMORY_INDEX  = 0;
    static final int DISK_INDEX    = 1;
    static final int POINTER_INDEX = 2;

    // fields
    private final HsqlName     indexName;
    final boolean[]            colCheck;
    private final int[]        colIndex;
    private final int[]        colType;
    private final boolean      isUnique;                 // DDL uniqueness
    boolean                    isConstraint;
    boolean                    isForward;
    private final boolean      isExact;
    private final int          visibleColumns;
    private final int          colIndex_0, colType_0;    // just for tuning
    private Node               root;
    private int                depth;
    private static RowIterator emtyIterator = new IndexRowIterator(null);

    /**
     * Constructor declaration
     *
     *
     * @param name HsqlName of the index
     * @param table table of the index
     * @param column array of column indexes
     * @param type array of column types
     * @param unique is this a unique index
     * @param constraint does this index belonging to a constraint
     * @param forward is this an auto-index for an FK that refers to a table defined after this table
     * @param visColumns count of visible columns
     */
    Index(HsqlName name, Table table, int column[], int type[],
            boolean unique, boolean constraint, boolean forward,
            int visColumns) {

        indexName      = name;
        colIndex       = column;
        colType        = type;
        isUnique       = unique;
        isConstraint   = constraint;
        isForward      = forward;
        colIndex_0     = colIndex[0];
        colType_0      = colType[0];
        visibleColumns = visColumns;
        isExact        = colIndex.length == visibleColumns;
        colCheck       = table.getNewColumnCheckList();

        ArrayUtil.intIndexesToBooleanArray(colIndex, colCheck);
    }

    /**
     * Returns the root node
     */
    Node getRoot() {
        return root;
    }

    /**
     * Set the root node
     */
    void setRoot(Node r) {
        root  = r;
        depth = 0;
    }

    /**
     * Returns the HsqlName object
     */
    HsqlName getName() {
        return indexName;
    }

    /**
     * Changes index name. Used by 'alter index rename to'. Argument isquoted
     * is true if the name was quoted in the DDL.
     */
    void setName(String name, boolean isquoted) throws HsqlException {
        indexName.rename(name, isquoted);
    }

    /**
     * Returns the count of visible columns used
     */
    int getVisibleColumns() {
        return visibleColumns;
    }

    /**
     * Is this a UNIQUE index?
     */
    boolean isUnique() {
        return isUnique;
    }

    /**
     * Does this index belong to a constraint?
     */
    boolean isConstraint() {
        return isConstraint;
    }

    /**
     * Returns the array containing column indexes for index
     */
    int[] getColumns() {
        return colIndex;    // todo: this gives back also primary key field!
    }

    /**
     * Returns the node count.
     */
    int size() throws HsqlException {

        int  count = 0;
        Node n     = first();

        while (n != null) {
            n = next(n);

            count++;
        }

        return count;
    }

    void clearAll() {
        root  = null;
        depth = 0;
    }

    /**
     * Insert a node into the index
     */
    void insert(Node i) throws HsqlException {

        Object  data[]  = i.getData();
        Node    n       = root,
                x       = n;
        boolean isleft  = true;
        int     compare = -1;

        while (true) {
            if (n == null) {
                if (x == null) {
                    root = i;

                    return;
                }

                set(x, isleft, i);

                break;
            }

            Object nData[] = n.getData();

            compare = compareRowForInsert(data, nData);

            if (compare == 0) {
                throw Trace.error(Trace.VIOLATION_OF_UNIQUE_INDEX,
                                  indexName.name);
            }

            isleft = compare < 0;
            x      = n;
            n      = child(x, isleft);
        }

        balance(x, isleft);
    }

    /**
     * Balances part of the tree after an alteration to the index.
     */
    private void balance(Node x, boolean isleft) throws HsqlException {

        while (true) {
            int sign = isleft ? 1
                              : -1;

            switch (x.getBalance() * sign) {

                case 1 :
                    x.setBalance(0);

                    return;

                case 0 :
                    x.setBalance(-sign);
                    break;

                case -1 :
                    Node l = child(x, isleft);

                    if (l.getBalance() == -sign) {
                        replace(x, l);
                        set(x, isleft, child(l, !isleft));
                        set(l, !isleft, x);
                        x.setBalance(0);
                        l.setBalance(0);
                    } else {
                        Node r = child(l, !isleft);

                        replace(x, r);
                        set(l, !isleft, child(r, isleft));
                        set(r, isleft, l);
                        set(x, isleft, child(r, !isleft));
                        set(r, !isleft, x);

                        int rb = r.getBalance();

                        x.setBalance((rb == -sign) ? sign
                                                   : 0);
                        l.setBalance((rb == sign) ? -sign
                                                  : 0);
                        r.setBalance(0);
                    }

                    return;
            }

            if (x.equals(root)) {
                return;
            }

            isleft = x.isFromLeft();
            x      = x.getParent();
        }
    }

    /**
     * Delete a node from the index
     */
    void delete(Node x) throws HsqlException {

        if (x == null) {
            return;
        }

        Node n;

        if (x.getLeft() == null) {
            n = x.getRight();
        } else if (x.getRight() == null) {
            n = x.getLeft();
        } else {
            Node d = x;

            x = x.getLeft();

/*
            // todo: this can be improved

            while (x.getRight() != null) {
                if (Trace.STOP) {
                    Trace.stop();
                }

                x = x.getRight();
            }
*/
            for (Node temp = x; (temp = temp.getRight()) != null; ) {
                x = temp;
            }

            // x will be replaced with n later
            n = x.getLeft();

            // swap d and x
            int b = x.getBalance();

            x.setBalance(d.getBalance());
            d.setBalance(b);

            // set x.parent
            Node xp = x.getParent();
            Node dp = d.getParent();

            if (d == root) {
                root = x;
            }

            x.setParent(dp);

            if (dp != null) {
                if (dp.getRight().equals(d)) {
                    dp.setRight(x);
                } else {
                    dp.setLeft(x);
                }
            }

            // for in-memory tables we could use: d.rData=x.rData;
            // but not for cached tables
            // relink d.parent, x.left, x.right
            if (xp == d) {
                d.setParent(x);

                if (d.getLeft().equals(x)) {
                    x.setLeft(d);
                    x.setRight(d.getRight());
                } else {
                    x.setRight(d);
                    x.setLeft(d.getLeft());
                }
            } else {
                d.setParent(xp);
                xp.setRight(d);
                x.setRight(d.getRight());
                x.setLeft(d.getLeft());
            }

            x.getRight().setParent(x);
            x.getLeft().setParent(x);

            // set d.left, d.right
            d.setLeft(n);

            if (n != null) {
                n.setParent(d);
            }

            d.setRight(null);

            x = d;
        }

        boolean isleft = x.isFromLeft();

        replace(x, n);

        n = x.getParent();

        x.delete();

        while (n != null) {
            x = n;

            int sign = isleft ? 1
                              : -1;

            switch (x.getBalance() * sign) {

                case -1 :
                    x.setBalance(0);
                    break;

                case 0 :
                    x.setBalance(sign);

                    return;

                case 1 :
                    Node r = child(x, !isleft);
                    int  b = r.getBalance();

                    if (b * sign >= 0) {
                        replace(x, r);
                        set(x, !isleft, child(r, isleft));
                        set(r, isleft, x);

                        if (b == 0) {
                            x.setBalance(sign);
                            r.setBalance(-sign);

                            return;
                        }

                        x.setBalance(0);
                        r.setBalance(0);

                        x = r;
                    } else {
                        Node l = child(r, isleft);

                        replace(x, l);

                        b = l.getBalance();

                        set(r, isleft, child(l, !isleft));
                        set(l, !isleft, r);
                        set(x, !isleft, child(l, isleft));
                        set(l, isleft, x);
                        x.setBalance((b == sign) ? -sign
                                                 : 0);
                        r.setBalance((b == -sign) ? sign
                                                  : 0);
                        l.setBalance(0);

                        x = l;
                    }
            }

            isleft = x.isFromLeft();
            n      = x.getParent();
        }
    }

    /**
     * Finds a foreign key referencing rows (in child table)
     *
     * @param rowdata array containing data for the index columns
     * @param rowColMap map of the data to columns
     * @param first true if the first matching node is required, false if any node
     * @return matching node or null
     * @throws HsqlException
     */
    Node findNotNull(Object rowdata[], int[] rowColMap,
                     boolean first) throws HsqlException {

        Node x      = root, n;
        Node result = null;

        if (isNull(rowdata, rowColMap)) {
            return null;
        }

        while (x != null) {
            int i = this.compareRowNonUnique(rowdata, rowColMap, x.getData());

            if (i == 0) {
                if (first == false) {
                    result = x;

                    break;
                } else if (result == x) {
                    break;
                }

                result = x;
                n      = x.getLeft();
            } else if (i > 0) {
                n = x.getRight();
            } else {
                n = x.getLeft();
            }

            if (n == null) {
                break;
            }

            x = n;
        }

        return result;
    }

    /**
     * Finds any row that matches the rowdata. Use rowColMap to map index
     * columns to rowdata. Limit to visible columns of data.
     *
     * @param rowdata array containing data for the index columns
     * @param rowColMap map of the data to columns
     * @return node matching node
     * @throws HsqlException
     */
    Node find(Object rowdata[], int[] rowColMap) throws HsqlException {

        Node x = root;

        while (x != null) {
            int c = compareRowNonUnique(rowdata, rowColMap, x.getData());

            if (c == 0) {
                return x;
            } else if (c < 0) {
                x = x.getLeft();
            } else {
                x = x.getRight();
            }
        }

        return null;
    }

    /**
     * Determines if a table row has a null column for any of the columns given
     * in the rowColMap array.
     */
    static boolean isNull(Object row[], int[] rowColMap) {

        int count = rowColMap.length;

        for (int i = 0; i < count; i++) {
            if (row[rowColMap[i]] == null) {
                return true;
            }
        }

        return false;
    }

    /**
     * Determines if a table row has a null column for any of the indexed
     * columns.
     */
    boolean isNull(Object row[]) {

        int count = colIndex.length;

        for (int i = 0; i < count; i++) {
            int j = colIndex[i];

            if (j < visibleColumns && row[j] == null) {
                return true;
            }
        }

        return false;
    }

    /**
     * Return the first node equal to the rowdata object. Use visible columns
     * only. The rowdata has the same column mapping as this table.
     *
     * @param rowdata array containing table row data
     * @return matching node
     * @throws HsqlException
     */
    Node findFirst(Object rowdata[]) throws HsqlException {

        Node    x      = root;
        Node    found  = null;
        boolean unique = isUnique &&!isNull(rowdata);

        while (x != null) {
            int c = compareRowNonUnique(rowdata, colIndex, x.getData());

            if (c == 0) {
                found = x;

                if (unique) {
                    break;
                }

                x = x.getLeft();
            } else if (c < 0) {
                x = x.getLeft();
            } else {
                x = x.getRight();
            }
        }

        return found;
    }

    /**
     * Finds the first node that is larger or equal to the given one based
     * on the first column of the index only.
     *
     * @param value value to match
     * @param compare comparison Expression type
     *
     * @return matching node
     *
     * @throws HsqlException
     */
    Node findFirst(Object value, int compare) throws HsqlException {

        boolean isEqual = compare == Expression.EQUAL
                          || compare == Expression.IS_NULL;
        Node x     = root;
        int  iTest = 1;

        if (compare == Expression.BIGGER) {
            iTest = 0;
        }

        if (value == null &&!isEqual) {
            return null;
        }

/*
        // this method returns the correct node only with the following conditions
        boolean check = compare == Expression.BIGGER
                        || compare == Expression.EQUAL
                        || compare == Expression.BIGGER_EQUAL;

        if (!check) {
            Trace.doAssert(false, "Index.findFirst");
        }
*/
        while (x != null) {
            boolean t =
                Column.compare(value, x.getData()[colIndex_0], colType_0)
                >= iTest;

            if (t) {
                Node r = x.getRight();

                if (r == null) {
                    break;
                }

                x = r;
            } else {
                Node l = x.getLeft();

                if (l == null) {
                    break;
                }

                x = l;
            }
        }

/*
        while (x != null
                && Column.compare(value, x.getData()[colIndex_0], colType_0)
                   >= iTest) {
            x = next(x);
        }
*/
        while (x != null) {
            Object colvalue = x.getData()[colIndex_0];
            int    result   = Column.compare(value, colvalue, colType_0);

            if (result >= iTest) {
                x = next(x);
            } else {
                if (isEqual) {
                    if (result != 0) {
                        x = null;
                    }
                } else if (colvalue == null) {
                    x = next(x);

                    continue;
                }

                break;
            }
        }

        return x;
    }

    /**
     * Finds the first node where the data is not null.
     *
     * @return matching node
     *
     * @throws HsqlException
     */
    Node findFirstNotNull() throws HsqlException {

        Node x = root;

        while (x != null) {
            boolean t =
                Column.compare(null, x.getData()[colIndex_0], colType_0) >= 0;

            if (t) {
                Node r = x.getRight();

                if (r == null) {
                    break;
                }

                x = r;
            } else {
                Node l = x.getLeft();

                if (l == null) {
                    break;
                }

                x = l;
            }
        }

        while (x != null) {
            Object colvalue = x.getData()[colIndex_0];

            if (colvalue == null) {
                x = next(x);
            } else {
                break;
            }
        }

        return x;
    }

    /**
     * Returns the first node of the index
     *
     * @return first node
     *
     * @throws HsqlException
     */
    Node first() throws HsqlException {

        depth = 0;

        Node x = root,
             l = x;

        while (l != null) {
            x = l;
            l = x.getLeft();

            depth++;
        }

        return x;
    }

    RowIterator firstRow() throws HsqlException {
        return new IndexRowIterator(this);
    }

    /**
     * Returns the node after the given one
     *
     * @param x node
     *
     * @return next node
     *
     * @throws HsqlException
     */
    Node next(Node x) throws HsqlException {

        if (x == null) {
            return null;
        }

        Node r = x.getRight();

        if (r != null) {
            x = r;

            Node l = x.getLeft();

            while (l != null) {
                x = l;
                l = x.getLeft();
            }

            return x;
        }

        Node ch = x;

        x = x.getParent();

        while (x != null && ch.equals(x.getRight())) {
            ch = x;
            x  = x.getParent();
        }

        return x;
    }

    /**
     * Returns either child node
     *
     * @param x node
     * @param isleft boolean
     *
     * @return child node
     *
     * @throws HsqlException
     */
    private Node child(Node x, boolean isleft) throws HsqlException {
        return isleft ? x.getLeft()
                      : x.getRight();
    }

    /**
     * Replace two nodes
     *
     * @param x node
     * @param n node
     *
     * @throws HsqlException
     */
    private void replace(Node x, Node n) throws HsqlException {

        if (x.equals(root)) {
            root = n;

            if (n != null) {
                n.setParent(null);
            }
        } else {
            set(x.getParent(), x.isFromLeft(), n);
        }
    }

    /**
     * Set a node as child of another
     *
     * @param x parent node
     * @param isleft boolean
     * @param n child node
     *
     * @throws HsqlException
     */
    private void set(Node x, boolean isleft, Node n) throws HsqlException {

        if (isleft) {
            x.setLeft(n);
        } else {
            x.setRight(n);
        }

        if (n != null) {
            n.setParent(x);
        }
    }

    /**
     * Find a node with matching data
     *
     * @param d row data
     *
     * @return matching node
     *
     * @throws HsqlException
     */
    Node search(Object d[]) throws HsqlException {

        Node x = root;

        while (x != null) {
            int c = compareRow(d, x.getData());

            if (c == 0) {
                return x;
            } else if (c < 0) {
                x = x.getLeft();
            } else {
                x = x.getRight();
            }
        }

        return null;
    }

    /**
     * Compares two table rows based on the columns of the index. The aColIndex
     * parameter specifies which columns of the other table are to be compared
     * with the colIndex columns of this index. The aColIndex can cover all
     * or only some columns of this index.
     *
     * @param a row from another table
     * @param rowColMap column indexes in the other table
     * @param b a full row in this table
     *
     * @return comparison result, -1,0,+1
     * @throws HsqlException
     */
    int compareRowNonUnique(Object a[], int[] rowColMap,
                            Object b[]) throws HsqlException {

        int i = Column.compare(a[rowColMap[0]], b[colIndex_0], colType_0);

        if (i != 0) {
            return i;
        }

        int fieldcount = rowColMap.length;

        if (fieldcount > visibleColumns) {
            fieldcount = visibleColumns;
        }

        for (int j = 1; j < fieldcount; j++) {
            i = Column.compare(a[rowColMap[j]], b[colIndex[j]], colType[j]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    /**
     * Compares two full table rows based on the columns of the index
     *
     * @param a a full row
     * @param b a full row
     *
     * @return comparison result, -1,0,+1
     * @throws HsqlException
     */
    private int compareRow(Object a[], Object b[]) throws HsqlException {

        int i = Column.compare(a[colIndex_0], b[colIndex_0], colType_0);

        if (i != 0) {
            return i;
        }

        int fieldcount = colIndex.length;

        for (int j = 1; j < fieldcount; j++) {
            i = Column.compare(a[colIndex[j]], b[colIndex[j]], colType[j]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    /**
     * compares two full table rows based on a set of columns
     *
     * @param a a full row
     * @param b a full row
     * @param cols array of column indexes to compare
     *
     * @return comparison result, -1,0,+1
     * @throws HsqlException
     */
    static int compareRows(Object a[], Object b[],
                           int[] cols) throws HsqlException {

        int fieldcount = cols.length;

        for (int j = 0; j < fieldcount; j++) {
            int i = Column.compare(a[cols[j]], b[cols[j]], cols[j]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    /**
     * Compare two rows of the table for inserting rows into unique indexes
     *
     * @param a data
     * @param b data
     *
     * @return comparison result, -1,0,+1
     *
     * @throws HsqlException
     */
    private int compareRowForInsert(Object a[],
                                    Object b[]) throws HsqlException {

        Object value = a[colIndex_0];
        int    i     = Column.compare(value, b[colIndex_0], colType_0);
        int    j     = 1;

        if (i != 0) {
            return i;
        }

        for (; j < visibleColumns; j++) {
            Object currentvalue = a[colIndex[j]];

            i = Column.compare(currentvalue, b[colIndex[j]], colType[j]);

            if (i != 0) {
                return i;
            }

            if (currentvalue == null) {
                value = null;
            }
        }

        if (isExact || (isUnique && value != null)) {
            return 0;
        }

        for (; j < colIndex.length; j++) {
            Object currentvalue = a[colIndex[j]];

            i = Column.compare(currentvalue, b[colIndex[j]], colType[j]);

            if (i != 0) {
                return i;
            }
        }

        return 0;
    }

    /**
     * Returns a value indicating the order of different types of index in
     * the list of indexes for a table. The position of the groups of Indexes
     * in the list in ascending order is as follows:
     *
     * primary key index
     * unique constraint indexes
     * autogenerated foreign key indexes for FK's that reference this table or
     *  tables created before this table
     * user created indexes (CREATE INDEX)
     * autogenerated foreign key indexes for FK's that reference tables created
     *  after this table
     *
     * Among a group of indexes, the order is based on the order of creation
     * of the index.
     *
     * @return ordinal value
     */
    int getIndexOrderValue() {

        int value = 0;

        if (isConstraint) {
            return isForward ? 4
                             : isUnique ? 0
                                        : 1;
        } else {
            return 2;
        }
    }

    public static class IndexRowIterator implements RowIterator {

        Index index;
        Node  next;

        private IndexRowIterator(Index index) {

            this.index = index;

            if (index != null) {
                try {
                    next = index.first();
                } catch (HsqlException e) {}
            }
        }

        public boolean hasNext() {
            return next != null;
        }

        public Row next() {

            if (hasNext()) {
                try {
                    Row row = next.getRow();

                    next = index.next(next);

                    return row;
                } catch (Exception e) {
                    throw new NoSuchElementException();
                }
            }

            throw new NoSuchElementException();
        }
    }
}
