/*
 * Created on 5.4.2005
 *
 */
package cz.opentech.jdbc.xlsdriver.db;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;

import jxl.Cell;
import jxl.CellType;
import jxl.Sheet;
import jxl.write.WritableSheet;
import cz.opentech.jdbc.xlsdriver.db.util.Blocks;
import cz.opentech.jdbc.xlsdriver.db.util.CellHelper;
import cz.opentech.jdbc.xlsdriver.metadata.ColumnMetadata;
import cz.opentech.jdbc.xlsdriver.metadata.TableMetadata;

/**
 * @author vitaz
 *
 */
public class XlsTableAnalyzer {
    
    /**
     * 
     * @param sheet
     * @param meta
     * @return
     * @throws SQLException
     */
    public static void analyzeHeader(Sheet sheet, TableMetadata meta)
    		throws SQLException {
        // find the header
        int rowIdx = 0;
        int maxRow = sheet.getRows() - 1;
        Cell[] row = null;
        for (; rowIdx <= maxRow && isRowNull(row = sheet.getRow(rowIdx)); rowIdx++);
        if (isRowNull(row)) {
            if (meta.getColumnsCount() == 0) {
                System.out.println("WARNING: Sheet " + sheet.getName() + " not loaded. Not header found.");
            } else {
                meta.getCols().add(0, meta.getColumnsCount() - 1);
                meta.getRows().add(0, Blocks.INF);
            }
            return;
        }
        boolean noHeader;
        if (meta.getColumnsCount() == 0) {
            meta.getHead().add(rowIdx); // set head only if no signature was provided
            noHeader = false;
        } else {
            noHeader = true;
        }
        // analyze the columns of the header
        Blocks cols = meta.getCols();
        int maxCol = sheet.getColumns() - 1;
        if (meta.getColumnsCount() > 0) {
            maxCol = Math.min(maxCol, meta.getColumnsCount() - 1); 
        }
        for (int colIdx = 0; colIdx <= maxCol; colIdx++) {
            Cell cell = sheet.getCell(colIdx, rowIdx);
            String hv = CellHelper.stringValue(cell);
            if (hv != null) {
                cols.add(colIdx);
                if (meta.getColumnsCount() <= colIdx) {
                    ColumnMetadata colmeta = meta.addColumn();
                    colmeta.setName(hv);
                }
            }
        }
        // analyze the rows
        if (!noHeader) rowIdx += 1;  
        while (rowIdx <= maxRow && isRowNull(row = sheet.getRow(rowIdx))) rowIdx++;
        meta.getRows().add(rowIdx, Blocks.INF);
    }
    
    /**
     * Renames duplicit and empty column names.
     * 
     * @param header
     */
    public static void normalizeHeader(TableMetadata meta) {
        final int headSize = meta.getColumnsCount();
        HashSet headers = new HashSet(headSize);
        headers.add("");
        for (int i = 0; i < headSize; i++) {
            ColumnMetadata col = meta.getColumn(i);
            String head = col.getName().trim();
            int idx = 0;
            String s = head; 
            while (headers.contains(s)) {
                s = head + "$" + ++idx;
            }
            headers.add(s);
            col.setName(s);
        }
    }    

    public static void loadHeader(WritableSheet sheet, TableMetadata meta)
    		throws SQLException {
        Blocks headblcks = meta.getHead();
        Blocks colsblcks = meta.getCols();
        int headSize = colsblcks.size();
        if (headSize == Integer.MAX_VALUE) {
            headSize = sheet.getColumns();
        }
        
        for (Iterator it = headblcks.iterator(); it.hasNext();) {
            int row = ((Integer) it.next()).intValue();
            int idx = 0;
            int maxCol = sheet.getColumns() - 1;
            for (Iterator it2 = colsblcks.iterator(); it2.hasNext(); idx++) {
                ColumnMetadata colmeta = meta.getColumn(idx);
                int col = ((Integer) it2.next()).intValue();
                if (col > maxCol) break;
                Cell cell = sheet.getCell(col, row);
                String cellStr = CellHelper.stringValue(cell);
                if (cellStr != null) {
                    String colName = colmeta.getName();
                    if (colName.length() > 0) {
                        colName += " ";
                    }
                    colName += cellStr.trim();
                    colmeta.setName(colName);
                }
            }
        }
    }
    
    /**
     * 
     * @param sheet
     * @param meta
     * @param types
     * @return
     */
    public static void analyzeTypes(Sheet sheet, TableMetadata meta) {
        final Blocks cols = meta.getCols();
        final int colsCnt = cols.size();
        final Blocks rows = meta.getRows();
        
        final int TYPES_CNT = 4; // 0-text, 1-numeric, 3-bool, 4-date
        int[] typesStat = new int[TYPES_CNT*colsCnt];
		int rowsScanned = meta.getRowsScanned();
		// rowscanned == 0 means scan all rows
		if (rowsScanned == 0) rowsScanned = Integer.MAX_VALUE;
		int maxRow = sheet.getRows() - 1;
        for (Iterator it = rows.iterator(); rowsScanned > 0 && it.hasNext();
				rowsScanned--) {
            int row = ((Integer) it.next()).intValue();
            if (row > maxRow) break;
            int idx = 0;
			int maxCol = sheet.getColumns() - 1;
            for (Iterator it2 = cols.iterator(); it2.hasNext(); idx++) {
                int col = ((Integer) it2.next()).intValue();
				if (col > maxCol) break;
                Cell cell = sheet.getCell(col, row);
                if (cell == null) continue;
				CellType cellType = cell.getType();
                if (cellType == CellType.ERROR
                		|| cellType == CellType.LABEL
						|| cellType == CellType.FORMULA_ERROR
						|| cellType == CellType.BOOLEAN_FORMULA
						|| cellType == CellType.DATE_FORMULA
						|| cellType == CellType.NUMBER_FORMULA
						|| cellType == CellType.STRING_FORMULA) {
                    typesStat[TYPES_CNT*idx] += 1;
                } else if (cellType == CellType.NUMBER) {
                    typesStat[TYPES_CNT*idx + 1] += 1;
                } else if (cellType == CellType.BOOLEAN) {
                    typesStat[TYPES_CNT*idx + 2] += 1;
                } else if (cellType == CellType.DATE) {
                    typesStat[TYPES_CNT*idx + 3] += 1;
                }
            }
        }
        
        // select the most used types in scanned rows
        for (int i = 0; i < colsCnt; i++) {
            final ColumnMetadata colmeta = meta.getColumn(i);
            if (colmeta.getType() == null) {
	            final int maxIdx = maxIdx(typesStat, TYPES_CNT*i, TYPES_CNT);
	            switch (maxIdx) {
	            case 0: colmeta.setType(XlsType.TEXT); break;
	            case 1: colmeta.setType(XlsType.NUMERIC); break;
	            case 2: colmeta.setType(XlsType.BOOL); break;
	            case 3: colmeta.setType(XlsType.DATE); break;
	            default: throw new IllegalStateException();
	            }
            }
        }
    }
    private static int maxIdx(int[] buff, int offset, int len) {
        int max = Integer.MIN_VALUE;
        int idx = -1;
        for (int i = 0; i < len; i++) {
            int num = buff[offset++];
            if (num > max) {
                max = num;
                idx = i;
            }
        }
        return idx;
    }

    static boolean isRowNull(Cell[] row) {
    	if (row == null || row.length == 0) {
    		return true;
    	}
    	for (int i = 0; i < row.length; i++) {
    		if (row[i] != null && row[i].getType() != CellType.EMPTY) {
    			return false;
    		}
    	}
    	return true;
    }
    
}
