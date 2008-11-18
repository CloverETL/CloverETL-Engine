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


package org.hsqldb.lib;

/**
 * Minimal class for maintaining and searching an int to int sorted lookup
 * table. This is suitable for very large lookups where memory footprint
 * concerns are paramount.<p>
 *
 * This class does not distinguish between the key and the value until the
 * find() method is invoked. At this point, the table is sorted on the
 * specified column. Invoking find() on the other column will result in a new
 * sort. If the values in the key list are not unique, any index with the
 * target key value will be returned. The adjacent value pairs may hold the
 * same key.<p>
 *
 * The arrays grow to twice the size when the current size is reached.
 * Methods in this class throw the normal Java exceptions if invalid arguments
 * are used (e.g. out of bounds values).<p>
 *
 * The arrays are sorted on the key values prior
 * to each lookup unless no keys are modified or added since the last lookup.
 * The find() method uses a binary search each time it is invoked.<p>
 *
 * This class is slower than IntKeyIntValueHashMap and should be used in
 * preference only when memory use is more important than speed.<p>
 *
 * The search and sort methods are from UnifiedTable, originally written
 * by Tony Lai.
 *
 * @author fredt@users
 * @author tony_lai@users
 *
 * @version 1.7.2
 * @since 1.7.2
 */
public class DoubleIntTable {

    private int[][] data;
    private int     count        = 0;
    private int     sortedColumn = -1;

    public DoubleIntTable(int size) {

        data = new int[][] {
            new int[size], new int[size]
        };
    }

    public int size() {
        return count;
    }

    public int get(int index, int column) {
        return data[column][index];
    }

    /**
     * Puts a single value into the table.
     *
     * @param index index into the array
     * @param column the column to insert (0 or 1)
     * @param value the value to insert
     */
    public void putSingle(int index, int column, int value) {

        data[column][index] = value;

        if (column == sortedColumn) {
            sortedColumn = -1;
        }
    }

    /**
     * Puts a key, value pair into the table at a given index.
     *
     * @param index index into the array
     * @param value1 value for column 0
     * @param value2 value for column 1
     */
    public void putPair(int index, int value1, int value2) {

        data[0][index] = value1;
        data[1][index] = value2;
        sortedColumn   = -1;
    }

    /**
     * Adds a key, value pair into the table.
     *
     * @param index index into the array
     * @param value1 value for column 0
     * @param value2 value for column 1
     */
    public void add(int value1, int value2) {

        if (count == data[0].length) {
            int[][] newdata = new int[][] {
                new int[data[0].length * 2], new int[data[0].length * 2]
            };

            System.arraycopy(data[0], 0, newdata[0], 0, count);
            System.arraycopy(data[1], 0, newdata[1], 0, count);

            data = newdata;
        }

        data[0][count] = value1;
        data[1][count] = value2;

        count++;

        sortedColumn = -1;
    }

    private int targetSearchColumn;
    private int targetSearchValue;

    synchronized public int find(int column, int value) {

        targetSearchColumn = column;
        targetSearchValue  = value;

        if (sortedColumn != targetSearchColumn) {
            fastQuickSort();

            sortedColumn = targetSearchColumn;
        }

        return binarySearch();
    }

// fredt - patched - this actually compared the table[rowCount] column
    private int binarySearch() {

        int low  = 0;
        int high = size();
        int mid  = 0;

// fredt - patched - changed from while (low <= high)
        while (low < high) {
            mid = (low + high) / 2;

            if (greaterThan(mid)) {

// fredt - patched - changed from high = mid -1
                high = mid;
            } else {
                if (lessThan(mid)) {
                    low = mid + 1;
                } else {
                    return mid;    // found
                }
            }
        }

        return -1;
    }

    private void fastQuickSort() {
        quickSort(0, size() - 1);
        insertionSort(0, size() - 1);
    }

    private void quickSort(int l, int r) {

        int M = 4;
        int i;
        int j;
        int v;

        if ((r - l) > M) {
            i = (r + l) / 2;

            if (lessThan(i, l)) {
                swap(l, i);    // Tri-Median Methode!
            }

            if (lessThan(r, l)) {
                swap(l, r);
            }

            if (lessThan(r, i)) {
                swap(i, r);
            }

            j = r - 1;

            swap(i, j);

            i = l;
            v = j;

            for (;;) {
                while (lessThan(++i, v)) {
                    ;
                }

                while (lessThan(v, --j)) {
                    ;
                }

                if (j < i) {
                    break;
                }

                swap(i, j);
            }

            swap(i, r - 1);
            quickSort(l, j);
            quickSort(i + 1, r);
        }
    }

    private void insertionSort(int lo0, int hi0) {

        int i;
        int j;

        for (i = lo0 + 1; i <= hi0; i++) {
            j = i;

            while ((j > lo0) && lessThan(i, j - 1)) {
                j--;
            }

            if (i != j) {
                int col1 = data[0][i];
                int col2 = data[1][i];

                moveRows(j, j + 1, i - j);
                putPair(j, col1, col2);
            }
        }
    }

    /**
     * Check if row indexed i is less than row indexed j
     */
    private boolean lessThan(int i, int j) {
        return data[targetSearchColumn][i] < data[targetSearchColumn][j];
    }

    /**
     * Check if targeted column value in the row indexed i is less than the
     * search target object.
     */
    private boolean lessThan(int i) {
        return targetSearchValue > data[targetSearchColumn][i];
    }

    /**
     * Check if targeted column value in the row indexed i is greater than the
     * search target object.
     * @see setSearchTarget(Object)
     */
    private boolean greaterThan(int i) {
        return targetSearchValue < data[targetSearchColumn][i];
    }

    void swap(int i1, int i2) {

        int col1 = data[0][i1];
        int col2 = data[1][i1];

        data[0][i1] = data[0][i2];
        data[1][i1] = data[1][i2];
        data[0][i2] = col1;
        data[1][i2] = col2;
    }

    void moveRows(int fromIndex, int toIndex, int rows) {
        System.arraycopy(data[0], fromIndex, data[0], toIndex, rows);
        System.arraycopy(data[1], fromIndex, data[1], toIndex, rows);
    }
}
