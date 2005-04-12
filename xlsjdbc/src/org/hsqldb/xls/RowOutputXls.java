package org.hsqldb.xls;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import jxl.JXLException;
import jxl.write.Blank;
import jxl.write.WritableSheet;

import org.hsqldb.CachedRow;
import org.hsqldb.HsqlException;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.rowio.RowOutputBase;
import org.hsqldb.types.Binary;
import org.hsqldb.types.JavaObject;

import cz.opentech.jdbc.xlsdriver.db.util.Blocks;
import cz.opentech.jdbc.xlsdriver.metadata.TableMetadata;

/**
 *  @author vitaz
 */
public class RowOutputXls extends RowOutputBase {

    private final TableMetadata metadata;
    private final int rowSize;
    private WritableSheet sheet;
    private Iterator rowIterator;
    private int rowIdx;
    private int rowPos;
    private Iterator colIterator;
    private int colIdx;
    private final ArrayList freedRows = new ArrayList();

    
    public RowOutputXls(TableMetadata metadata) {
        this.metadata = metadata;
        this.rowSize = metadata.getColumnsCount();
    }
    
    public void setSheet(WritableSheet sheet) {
        this.sheet = sheet;
        rowIterator = metadata.getRows().iterator();
        if (rowIterator.hasNext()) {
            rowIdx = ((Integer) rowIterator.next()).intValue();
            rowPos = 0;
        } else {
            rowIdx = -1;
            rowPos = -1;
        }
        reset();
    }
    
    private void nextCell() throws IOException {
        if (!colIterator.hasNext()) {
            throw new IllegalStateException("Bad column index.");
        }
        colIdx = ((Integer) colIterator.next()).intValue();
        while (rowIdx >= sheet.getRows()) try {
            sheet.addCell(new Blank(0, rowIdx));
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }
    
    /**
     * @see org.hsqldb.rowio.RowOutputInterface#writeEnd()
     */
    public void writeEnd() throws IOException {
        // empty
    }

    /**
     * @see org.hsqldb.rowio.RowOutputInterface#writeSize(int)
     */
    public void writeSize(int size) throws IOException {
        // empty
    }

    /**
     * @see org.hsqldb.rowio.RowOutputInterface#writeType(int)
     */
    public void writeType(int type) throws IOException {
        // empty
    }

    /**
     * @see org.hsqldb.rowio.RowOutputInterface#writeString(java.lang.String)
     */
    public void writeString(String value) throws IOException {
        nextCell();
        try {
            CellHelper.setCell(sheet, colIdx, rowIdx, value);
        } catch (JXLException e) {
            throw new IOException(e.getMessage());
        }
    }

    /**
     * @see org.hsqldb.rowio.RowOutputInterface#writeIntData(int)
     */
    public void writeIntData(int i) throws IOException {
        nextCell();
        try {
            CellHelper.setCell(sheet, colIdx, rowIdx, new Integer(i));
        } catch (JXLException e) {
            throw new IOException(e.getMessage());
        }
    }

    /**
     * @see org.hsqldb.rowio.RowOutputInterface#writeIntData(int, int)
     */
    public void writeIntData(int i, int position) throws IOException {
        throw new UnsupportedOperationException();
//        nextCell();
//        try {
//            CellHelper.setCell(sheet, position, row, new Integer(i));
//        } catch (JXLException e) {
//            throw new IOException(e.getMessage());
//        }
    }

    /**
     * @see org.hsqldb.rowio.RowOutputInterface#writeLongData(long)
     */
    public void writeLongData(long i) throws IOException {
        nextCell();
        try {
            CellHelper.setCell(sheet, colIdx, rowIdx, new Long(i));
        } catch (JXLException e) {
            throw new IOException(e.getMessage());
        }
    }

    /**
     * @see org.hsqldb.rowio.RowOutputInterface#getSize(org.hsqldb.CachedRow)
     */
    public int getSize(CachedRow row) {
        return size();
    }

    /**
     * @see org.hsqldb.rowio.RowOutputInterface#getOutputStream()
     */
    public HsqlByteArrayOutputStream getOutputStream() {
        throw new UnsupportedOperationException();
    }

    /**
     * @see org.hsqldb.rowio.RowOutputInterface#reset()
     */
    public void reset() {
        colIterator = metadata.getCols().iterator();
        colIdx = -1;
    }

    /**
     * @see org.hsqldb.rowio.RowOutputInterface#size()
     */
    public int size() {
        return rowSize;
    }

    /**
     * @see org.hsqldb.rowio.RowOutputBase#writeFieldType(int)
     */
    protected void writeFieldType(int type) throws IOException {
        // empty
    }

    /**
     * @see org.hsqldb.rowio.RowOutputBase#writeNull(int)
     */
    protected void writeNull(int type) throws IOException {
        nextCell();
        try {
            CellHelper.setCellNull(sheet, colIdx, rowIdx);
        } catch (JXLException e) {
            throw new IOException(e.getMessage());
        }
    }

    /**
     * @see org.hsqldb.rowio.RowOutputBase#writeChar(java.lang.String, int)
     */
    protected void writeChar(String s, int t) throws IOException {
        writeString(s);
    }
    
    private void writeNumber(Number o) throws IOException {
        nextCell();
        try {
            CellHelper.setCell(sheet, colIdx, rowIdx, o);
        } catch (JXLException e) {
            throw new IOException(e.getMessage());
        }
    }

    /**
     * @see org.hsqldb.rowio.RowOutputBase#writeSmallint(java.lang.Number)
     */
    protected void writeSmallint(Number o) throws IOException, HsqlException {
        writeNumber(o);
    }

    /**
     * @see org.hsqldb.rowio.RowOutputBase#writeInteger(java.lang.Number)
     */
    protected void writeInteger(Number o) throws IOException, HsqlException {
        writeNumber(o);
    }

    /**
     * @see org.hsqldb.rowio.RowOutputBase#writeBigint(java.lang.Number)
     */
    protected void writeBigint(Number o) throws IOException, HsqlException {
        writeNumber(o);
    }

    /**
     * @see org.hsqldb.rowio.RowOutputBase#writeReal(java.lang.Double, int)
     */
    protected void writeReal(Double o, int type) throws IOException, HsqlException {
        writeNumber(o);
    }

    /**
     * @see org.hsqldb.rowio.RowOutputBase#writeDecimal(java.math.BigDecimal)
     */
    protected void writeDecimal(BigDecimal o) throws IOException, HsqlException {
        writeNumber(o);
    }

    /**
     * @see org.hsqldb.rowio.RowOutputBase#writeBit(java.lang.Boolean)
     */
    protected void writeBit(Boolean o) throws IOException, HsqlException {
        nextCell();
        try {
            CellHelper.setCell(sheet, colIdx, rowIdx, o);
        } catch (JXLException e) {
            throw new IOException(e.getMessage());
        }
    }

    private void internalWriteDate(java.util.Date d) throws IOException {
        nextCell();
        try {
            CellHelper.setCell(sheet, colIdx, rowIdx, d);
        } catch (JXLException e) {
            throw new IOException(e.getMessage());
        }
    }
    
    /**
     * @see org.hsqldb.rowio.RowOutputBase#writeDate(java.sql.Date)
     */
    protected void writeDate(Date o) throws IOException, HsqlException {
        internalWriteDate(o);
    }

    /**
     * @see org.hsqldb.rowio.RowOutputBase#writeTime(java.sql.Time)
     */
    protected void writeTime(Time o) throws IOException, HsqlException {
        internalWriteDate(o);
    }

    /**
     * @see org.hsqldb.rowio.RowOutputBase#writeTimestamp(java.sql.Timestamp)
     */
    protected void writeTimestamp(Timestamp o) throws IOException, HsqlException {
        internalWriteDate(o);
    }

    /**
     * @see org.hsqldb.rowio.RowOutputBase#writeOther(org.hsqldb.types.JavaObject)
     */
    protected void writeOther(JavaObject o) throws IOException, HsqlException {
        writeString(StringConverter.byteToHex(o.getBytes()));
    }

    /**
     * @see org.hsqldb.rowio.RowOutputBase#writeBinary(org.hsqldb.types.Binary, int)
     */
    protected void writeBinary(Binary o, int t) throws IOException, HsqlException {
        writeString(StringConverter.byteToHex(o.getBytes()));
    }

    /**
     * @param pos
     */
    public void setRow(int pos) {
        freedRows.remove(new Integer(pos));
        internalSetRow(pos);
    }
    private void internalSetRow(int pos) {
        final int newRpos = pos / rowSize;
        final Blocks rb = metadata.getRows();
        final int rbSize = rb.size();
        // extent row blocks
        if (newRpos - rbSize >= 0) {
            rb.add(rbSize, (newRpos - rowPos) + rowIdx);
        }
        // if rowIdx differs from the requested, recount iterator
        if (rowPos != newRpos) {
            if (rowPos > newRpos) {
	            rowIterator = metadata.getRows().iterator();
	            rowIdx = ((Integer) rowIterator.next()).intValue();
	            rowPos = rowIdx = 0;
            }
            for (; rowPos < newRpos; rowPos++) {
                rowIdx = ((Integer) rowIterator.next()).intValue();
            }
        }
        reset();
    }

    /**
     * 
     */
    public void removeRow(int pos) {
        System.out.println("Line " + pos + " added to free rows.");
        freedRows.add(new Integer(pos));
    }
    
    /**
     * After you call this method you cannot use the sheet
     * with current state of the cache (cashed rows have
     * bad positions).
     *
     */
    public void defrag() {
    	Collections.sort(freedRows, new Comparator() {
            public int compare(Object o1, Object o2) {
                // sort the position descending
                return ((Integer) o2).intValue() - ((Integer) o1).intValue();
            }
    	});
        for (Iterator it = freedRows.iterator(); it.hasNext();) {
            final int pos = ((Integer) it.next()).intValue();
        	internalSetRow(pos);
            System.out.println("Line " + pos + " removed.");
        	sheet.removeRow(rowIdx);
        }
        freedRows.clear();
    }
}
