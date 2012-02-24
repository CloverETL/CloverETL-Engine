/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.util;

import java.awt.Point;

import org.apache.poi.ss.util.CellReference;
import org.jetel.data.parser.XLSMapping;



/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 30 Aug 2011
 */
public class SpreadsheetUtils {
	
	/**
	 * Only private constructor, this class should not be instantiated at all.
	 */
	private SpreadsheetUtils() {
	}

	static final String INVALID_SHEET_CHARACTERS = ":\\\\/\\[\\]";
	static final String INVALID_UNESCAPED_CHARACTERS = INVALID_SHEET_CHARACTERS + "\\,\\-";
	private static final String INVALID_UNESCAPED_SHEET_NAME = ".*[" + INVALID_UNESCAPED_CHARACTERS + "].*|[0-9]+";
	
	private static final char A = 'A';
	private static final char Z = 'Z';
	private static final int CHAR_COUNT = Z - A + 1;
	
	public static int getColumnIndex(String cellReference) {
		// TODO maybe use CellReference.convertColStringToIndex() instead
		int columnIndex = 0;
		char c;
		for (int i = 0; i < cellReference.length(); i++) {
			 c = cellReference.charAt(i);
			 if (c < A || c > Z) {
				 break;
			 }
			columnIndex *= CHAR_COUNT;
			columnIndex += c - A + 1;
		}
		return columnIndex - 1;
	}
	
	
	public static Point getCellCoordinates(String cellReference) {
		int columnIndex = 0;
		char c;
		int i = 0;
		for (; i < cellReference.length(); i++) {
			 c = cellReference.charAt(i);
			 if (c < A || c > Z) {
				 break;
			 }
			columnIndex *= CHAR_COUNT;
			columnIndex += c - A + 1;
		}
		int row;
		try {
			row = Integer.valueOf(cellReference.substring(i, cellReference.length())) - 1;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid cell reference '" + cellReference + "'", e);
		}
		return new Point(columnIndex - 1, row);
	}
	
	/**
	 * Converts 0-based cell coordinates into Excel style cell reference (e.g. B2).
	 * @param column 0-based column index
	 * @param row 0-based row number
	 * @return cell reference (e.g. "C5" for coordinates (3,4))
	 */
	public static String getCellReference(int column, int row) {
		return getColumnReference(column) + (row + 1);
	}
	
	public static String getColumnReference(int columnIndex) {
		return CellReference.convertNumToColString(columnIndex);
	}
	
	public static String quoteSheetNameIfNeeded(String sheetName) {
		if (sheetName.matches(INVALID_UNESCAPED_SHEET_NAME)) {
			return quoteSheetName(sheetName);
		} else {
			return sheetName;
		}
	}

	public static String quoteSheetNameIfNotQuoted(String sheetName) {
		if (sheetName.startsWith(""+XLSMapping.ESCAPE_START) && sheetName.endsWith(""+XLSMapping.ESCAPE_END)) {
			return sheetName;
		} else {
			return quoteSheetName(sheetName);
		}
	}


	private static String quoteSheetName(String sheetName) {
		StringBuilder quotedSheetNameBuilder = new StringBuilder();
		quotedSheetNameBuilder.append(XLSMapping.ESCAPE_START);
		quotedSheetNameBuilder.append(sheetName);
		quotedSheetNameBuilder.append(XLSMapping.ESCAPE_END);
		return quotedSheetNameBuilder.toString();
	}

	
	public static enum SpreadsheetAttitude {

        IN_MEMORY,
        STREAM;

        public static SpreadsheetAttitude valueOfIgnoreCase(String string) {
            for (SpreadsheetAttitude parserType : values()) {
                if (parserType.name().equalsIgnoreCase(string)) {
                    return parserType;
                }
            }

            return IN_MEMORY;
        }
    }

	public static enum SpreadsheetWriteMode {

		OVERWRITE_SHEET_IN_MEMORY,
		INSERT_INTO_SHEET_IN_MEMORY,
		APPEND_TO_SHEET_IN_MEMORY,
		CREATE_FILE_IN_STREAM,
		CREATE_FILE_IN_MEMORY;

        public static SpreadsheetWriteMode valueOfIgnoreCase(String string) {
            for (SpreadsheetWriteMode parserType : values()) {
                if (parserType.name().equalsIgnoreCase(string)) {
                    return parserType;
                }
            }

            return OVERWRITE_SHEET_IN_MEMORY;
        }
        
        public boolean isStreamed() {
        	return (this == CREATE_FILE_IN_STREAM);
        }
        
        public boolean isAppend() {
        	return (this == APPEND_TO_SHEET_IN_MEMORY);
        }
        
        public boolean isInsert() {
        	return (this == INSERT_INTO_SHEET_IN_MEMORY);
        }
        
        public boolean isOverwritingSheetOrFile() {
        	return (this == OVERWRITE_SHEET_IN_MEMORY || isCreatingNewFile());
        }
        
        public boolean isCreatingNewFile() {
        	return (this == CREATE_FILE_IN_MEMORY || this == CREATE_FILE_IN_STREAM);
        }
    }

	public static enum SpreadsheetExistingSheetsActions {

		DO_NOTHING,
		CLEAR_SHEETS,
		REMOVE_SHEETS;

        public static SpreadsheetExistingSheetsActions valueOfIgnoreCase(String string) {
            for (SpreadsheetExistingSheetsActions parserType : values()) {
                if (parserType.name().equalsIgnoreCase(string)) {
                    return parserType;
                }
            }

            return DO_NOTHING;
        }
        
        public boolean isRemovingAllRows() {
        	return (this == CLEAR_SHEETS);
        }
        
        public boolean isRemovingAllSheets() {
        	return (this == REMOVE_SHEETS);
        }
    }

	public static enum SpreadsheetFormat {

		XLS, XLSX, AUTO;

		public static SpreadsheetFormat valueOfIgnoreCase(String string) {
			for (SpreadsheetFormat format : values()) {
				if (format.name().equalsIgnoreCase(string)) {
					return format;
				}
			}

			return XLSX;
		}

	}

}
