package org.hsqldb.xls;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.NoSuchElementException;

import jxl.Cell;
import jxl.CellType;
import jxl.write.WritableSheet;

import org.hsqldb.Column;
import org.hsqldb.HsqlException;
import org.hsqldb.Types;
import org.hsqldb.rowio.RowInputBase;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.types.Binary;
import org.hsqldb.types.JavaObject;

import cz.opentech.jdbc.xlsdriver.db.util.Blocks;
import cz.opentech.jdbc.xlsdriver.metadata.TableMetadata;

/**
 *
 * @author vitaz
 */
public class RowInputXls extends RowInputBase implements RowInputInterface {

    private static final Cell[] EMPTY_ROW = new Cell[0];
    private static final String DATE_FORMAT = "yyyyMMdd"; 
    
    private final TableMetadata metadata;
    private final int rowSize;
    private WritableSheet sheet;
    private Iterator rowIterator;
    private Iterator colIterator;
    // physical position in the sheet 
    private int rowIdx;
    private int colIdx;
    // logical position
    private int rowPos;
    // current cell
    private Cell cell;
    
    /**
     */
    public RowInputXls(TableMetadata metadata) {
        super();
        this.metadata = metadata;
        this.rowSize = metadata.getColumnsCount();
        reset();
    }

    public void setSource(WritableSheet sheet, int pos) {
        this.sheet = sheet; // should not change
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
	            rowPos = rowIdx = 0;
            }
            for (; rowPos < newRpos; rowPos++) {
                rowIdx = ((Integer) rowIterator.next()).intValue();
            }
        }
        colIterator = metadata.getCols().iterator();
        filePos = pos;
        nextPos = pos + rowSize;
        getCellAndLoadNext(); // loads the first cell in the row
    }
    
    /**
     * Is end of lines.
     * 
     * @return
     */
    public boolean isEol() {
        return sheet.getRows() <= rowIdx;
    }

    
    private Cell getCellAndLoadNext() {
        Cell ret = cell;
        if (colIterator.hasNext()) {
            colIdx = ((Integer) colIterator.next()).intValue();
            cell = sheet.getCell(colIdx, rowIdx);
        } else {
            cell = null;
        }
        return ret;
    }

    public String readString() throws IOException {
        return CellHelper.stringValue(getCellAndLoadNext());
    }

    public int readIntData() {
        Double ret = CellHelper.numberValue(getCellAndLoadNext());
        return (ret != null) ? ret.intValue() : 0;
    }

    public long readLongData() {
        Double ret = CellHelper.numberValue(getCellAndLoadNext());
        return (ret != null) ? ret.longValue() : 0L;
    }

    public int readType() {
        return 0;
    }

    protected boolean checkNull() {
        return cell == null	|| cell.getType() == CellType.EMPTY;
    }

    protected String readChar(int type) throws IOException {
        switch (type) {
            case Types.CHAR :
            case Types.VARCHAR :
            case Types.VARCHAR_IGNORECASE :
            case Types.LONGVARCHAR :
            default :
                return readString();
        }
    }

    protected Integer readSmallint() {
        return readInteger();
    }

    protected Integer readInteger() {
        Double ret = CellHelper.numberValue(getCellAndLoadNext());
        return ret != null ? new Integer(ret.intValue()) : null;
    }

    protected Long readBigint() {
        Double ret = CellHelper.numberValue(getCellAndLoadNext());
        return ret != null ? new Long(ret.longValue()) : null;
    }

    protected Double readReal(int type) {
        return CellHelper.numberValue(getCellAndLoadNext());
    }

    protected java.math.BigDecimal readDecimal() {
        Double ret = CellHelper.numberValue(getCellAndLoadNext());
        return ret != null ? new BigDecimal(ret.doubleValue()) : null;
    }

    protected Time readTime() {
        Date ret = CellHelper.dateValue(getCellAndLoadNext(), DATE_FORMAT);
        return ret != null ? new Time(ret.getTime()) : null;
    }

    protected Date readDate() {
        return CellHelper.dateValue(getCellAndLoadNext(), DATE_FORMAT);
    }

    protected Timestamp readTimestamp() {
        Date ret = CellHelper.dateValue(getCellAndLoadNext(), DATE_FORMAT);
        return ret != null ? new Timestamp(ret.getTime()) : null;
    }

    protected Boolean readBit() {
        return CellHelper.boolValue(getCellAndLoadNext());
    }

    protected Object readOther() throws IOException, HsqlException {
        byte[] data;
        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        data = Column.hexToByteArray(s);

        return new JavaObject(data);
    }

    protected Binary readBinary(int type) throws IOException, HsqlException {

        String s = readString();

        if (s == null) {
            return null;
        }

        s = s.trim();

        if (s.length() == 0) {
            return null;
        }

        return new Binary(Column.hexToByteArray(s), false);
    }

    public int getLineNumber() {
        return rowPos;
    }

    public void setNextPos(int pos) {
        nextPos = pos;
    }

    public void skippedLine() {
        setSource(this.sheet, rowPos + rowSize);
    }

    public void reset() {
        filePos = -1;
        nextPos = -1;
        rowIterator = metadata.getRows().iterator();
        rowPos = -1;
        rowIdx = -1;
        colIterator = metadata.getCols().iterator();
        colIdx = -1;
        cell = null;
        sheet = null;
    }
    
    static final Iterator EMPTY_ITER = new Iterator() {
        public boolean hasNext() {
            return false;
        }
        public Object next() {
            throw new NoSuchElementException();
        }
        public void remove() {
            throw new NoSuchElementException();
        }
    };
}
