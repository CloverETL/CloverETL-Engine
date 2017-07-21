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
package org.jetel.data.formatter.spreadsheet;

import org.jetel.data.parser.XLSMapping;
import org.jetel.data.parser.XLSMapping.HeaderRange;
import org.jetel.data.parser.XLSMapping.SpreadsheetOrientation;

public class CoordsTransformations {

	SpreadsheetOrientation orientation;
	
	public CoordsTransformations(SpreadsheetOrientation orientation) {
		this.orientation = orientation;
	}
	
	public int maximum(int value1, int value2) {
		return ((value1>value2) ? value1 : value2);
	}

	public int minimum(int value1, int value2) {
		return ((value1<value2) ? value1 : value2);
	}
	
	public int getY2fromRange(HeaderRange headerRange) {
		if (orientation==XLSMapping.HEADER_ON_TOP) {
			return headerRange.getRowEnd();
		} else {
			return headerRange.getColumnEnd();
		}
	}
	
	public int getX1fromRange(HeaderRange headerRange) {
		if (orientation==XLSMapping.HEADER_ON_TOP) {
			return headerRange.getColumnStart();
		} else {
			return headerRange.getRowStart();
		}
	}
	

	public int getY1fromRange(HeaderRange headerRange) {
		if (orientation==XLSMapping.HEADER_ON_TOP) {
			return headerRange.getRowStart();
		} else {
			return headerRange.getColumnStart();
		}
	}
	
	public int getX2fromRange(HeaderRange headerRange) {
		if (orientation==XLSMapping.HEADER_ON_TOP) {
			return headerRange.getColumnEnd();
		} else {
			return headerRange.getRowEnd();
		}
	}
	
	public int getMaxY(int y, HeaderRange headerRange, int skip) {
		return maximum(y, getY2fromRange(headerRange) + skip);
	}
	
	public int getMinY(int y, HeaderRange headerRange, int skip) {
		return minimum(y, getY2fromRange(headerRange) + skip);
	}
	
	public int getMaxX(int x, HeaderRange headerRange) {
		return maximum(x, getX2fromRange(headerRange));
	}
	
	public int getMinY(int y, HeaderRange headerRange) {
		return minimum(y, getY1fromRange(headerRange));
	}
	
	public int getMinX(int x, HeaderRange headerRange) {
		return minimum(x, getX1fromRange(headerRange));
	}
	
	public int translateXYtoRowNumber(int x, int y) {
		if (orientation==XLSMapping.HEADER_ON_TOP) {
			return y;
		} else {
			return x;
		}
	}
	
	public int translateXYtoColumnNumber(int x, int y) {
		if (orientation == XLSMapping.HEADER_ON_TOP) {
			return x;
		} else {
			return y;
		}
	}
	
}