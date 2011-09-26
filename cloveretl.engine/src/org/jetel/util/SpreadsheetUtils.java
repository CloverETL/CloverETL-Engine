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

	private static final char A = 'A';
	private static final char Z = 'Z';
	private static final int CHAR_COUNT = Z - A + 1;
	
	public static int getColumnIndex(String cellReference) {
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
		return new Point(columnIndex - 1, Integer.valueOf(cellReference.substring(i, cellReference.length())) - 1);
	}
	
	public static String getColumnReference(int columnIndex) {
		if (columnIndex < CHAR_COUNT) {
			char c = A;
			c += columnIndex;
			return String.valueOf(c);
		} else {
			StringBuilder sb = new StringBuilder();
			while (columnIndex > 0) {
				char c = A;
				c += columnIndex % CHAR_COUNT;
				sb.insert(0, c);
				columnIndex /= CHAR_COUNT;
			}
			return sb.toString();
		}
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
