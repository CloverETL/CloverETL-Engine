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
package org.jetel.util.spreadsheet;

/* ====================================================================
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==================================================================== */

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * This class is a slightly modified copy of Apache POI 3.8b3 org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler class.
 * Notable differences:
 *  - ignores formula "f" tags. Cell with formula is treated as any other cell with value;
 *  - in {@link SheetContentsHandler#cell(String, int, String, int)}, cell value type and cell style index is available.
 *  - handles missing "r" attribute in cell element "c" and row element "row".
 *  
 * @author tkramolis
 * 
 * -- Original javaDoc --
 * This class handles the processing of a sheet#.xml sheet part of a XSSF .xlsx file,
 * and generates row and cell events for it.
 */
public class XSSFSheetXMLHandler extends DefaultHandler {
	/**
	 * These are the different kinds of cells we support. We keep track of the current one between the start and end.
	 */
	enum XSSFDataType {
		BOOLEAN, ERROR, INLINE_STRING, SST_STRING, NUMBER, FORMULA,
	}

	public static final String CELL_VALUE_TRUE = "TRUE";
	public static final String CELL_VALUE_FALSE = "FALSE";
	
	private ReadOnlySharedStringsTable sharedStringsTable;

	/**
	 * Where our text is going
	 */
	private final SheetContentsHandler output;

	// Set when V start element is seen
	private boolean vIsOpen;
	// Set when F start element is seen
	//private boolean fIsOpen;
	// Set when an Inline String "is" is seen
	private boolean isIsOpen;
	// Set when a header/footer element is seen
	private boolean hfIsOpen;

	// Set when cell start element is seen;
	// used when cell close element is seen.
	private XSSFDataType nextDataType;

	// Used to format numeric cell values.
	private int styleIndex;
	private boolean formulasNotResults;
	private String cellRef;

	// Gathers characters as they are seen.
	private StringBuffer value = new StringBuffer();
	private StringBuffer formula = new StringBuffer();
	private StringBuffer headerFooter = new StringBuffer();
	
	private int formulaType; // Cell.CELL_TYPE* of cached formula value
	
	private int lastRowNum = -1;
	
	/**
	 * Accepts objects needed while parsing.
	 * 
	 * @param styles
	 *            Table of styles
	 * @param strings
	 *            Table of shared strings
	 */
	public XSSFSheetXMLHandler(ReadOnlySharedStringsTable strings, SheetContentsHandler sheetContentsHandler) {
		this.sharedStringsTable = strings;
		this.output = sheetContentsHandler;
		this.nextDataType = XSSFDataType.NUMBER;
	}
	
	private boolean isTextTag(String name) {
		if ("v".equals(name)) {
			// Easy, normal v text tag
			return true;
		}
		if ("inlineStr".equals(name)) {
			// Easy inline string
			return true;
		}
		if ("t".equals(name) && isIsOpen) {
			// Inline string <is><t>...</t></is> pair
			return true;
		}
		// It isn't a text tag
		return false;
	}

	@Override
	public void startElement(String uri, String localName, String name, Attributes attributes) throws SAXException {

		if (isTextTag(name)) {
			vIsOpen = true;
			// Clear contents cache
			value.setLength(0);
		} else if ("is".equals(name)) {
			// Inline string outer tag
			isIsOpen = true;
		} else if ("f".equals(name)) {
	          // Clear contents cache
	          formula.setLength(0);
	          
	          // Mark us as being a formula if not already
	          if(nextDataType == XSSFDataType.NUMBER) {
	        	 formulaType = Cell.CELL_TYPE_NUMERIC;
	             nextDataType = XSSFDataType.FORMULA;
	          }
	          
	          // Decide where to get the formula string from
	          String type = attributes.getValue("t");
	          if(type != null && type.equals("shared")) {
	             // Is it the one that defines the shared, or uses it?
	             String ref = attributes.getValue("ref");
	             //String si = attributes.getValue("si");
	             
	             if(ref != null) {
	                // This one defines it
	                // TODO Save it somewhere
	                //fIsOpen = true;
	             } else {
	                // This one uses a shared formula
	                // TODO Retrieve the shared formula and tweak it to 
	                //  match the current cell
	                if(formulasNotResults) {
	                   System.err.println("Warning - shared formulas not yet supported!");
	                } else {
	                   // It's a shared formula, so we can't get at the formula string yet
	                   // However, they don't care about the formula string, so that's ok!
	                }
	             }
	          } else {
	             //fIsOpen = true;
	          }
	    } else if ("oddHeader".equals(name) || "evenHeader".equals(name) || "firstHeader".equals(name) || "firstFooter".equals(name) || "oddFooter".equals(name) || "evenFooter".equals(name)) {
			hfIsOpen = true;
			// Clear contents cache
			headerFooter.setLength(0);
		} else if ("row".equals(name)) {
			String r = attributes.getValue("r");
			int rowNum = r != null ? Integer.parseInt(r) - 1 : lastRowNum + 1;
			lastRowNum = rowNum;
			cellRef = null;
			output.startRow(rowNum);
		} else if ("c".equals(name)) { // c => cell
			// Set up defaults.
			this.nextDataType = XSSFDataType.NUMBER;
			String r = attributes.getValue("r"); // cell reference (coordinates, e.g. "A1")
			if (r == null) { // fix of CLO-466 - cell element with no reference attribute "r" 
				if (cellRef == null) {
					// no previous cell in current row yet - assume this cell is in first column of current row
					r = "A" + lastRowNum;
				} else {
					// assume this cell is right neighbor of last cell in current row
					r = SpreadsheetUtils.getColumnReference(SpreadsheetUtils.getColumnIndex(cellRef) + 1) + lastRowNum;
				}
			}
			cellRef = r;
			String cellType = attributes.getValue("t");
			String cellStyleStr = attributes.getValue("s");
			
			if ("b".equals(cellType)) {
				nextDataType = XSSFDataType.BOOLEAN;
			} else if ("e".equals(cellType)) {
				nextDataType = XSSFDataType.ERROR;
			} else if ("inlineStr".equals(cellType)) {
				nextDataType = XSSFDataType.INLINE_STRING;
			} else if ("s".equals(cellType)) {
				nextDataType = XSSFDataType.SST_STRING;
			} else if ("str".equals(cellType)) {
				nextDataType = XSSFDataType.FORMULA;
				formulaType = Cell.CELL_TYPE_STRING; // at least I hope so...
			}
			
			if (cellStyleStr != null) {
				// Number, but almost certainly with a special style or format
				styleIndex = Integer.parseInt(cellStyleStr);
			} else {
				styleIndex = 0; // set default style
			}
		}
	}

	@Override
	public void endElement(String uri, String localName, String name) throws SAXException {
		String thisStr = null;
		int cellType;

		// v => contents of a cell
		if (isTextTag(name)) {
			vIsOpen = false;

			// Process the value contents as required, now we have it all
			switch (nextDataType) {
			case BOOLEAN:
				char first = value.charAt(0);
				thisStr = first == '0' ? CELL_VALUE_FALSE : CELL_VALUE_TRUE;
				cellType = Cell.CELL_TYPE_BOOLEAN;
				break;

			case ERROR:
				thisStr = "ERROR:" + value.toString();
				cellType = Cell.CELL_TYPE_ERROR;
				break;
				
			case FORMULA:
                if(formulasNotResults) {
                   thisStr = formula.toString();
                } else {
                   thisStr = value.toString();
                }
                cellType = Cell.CELL_TYPE_FORMULA;
                break;

			case INLINE_STRING:
				// TODO: Can these ever have formatting on them?
				XSSFRichTextString rtsi = new XSSFRichTextString(value.toString());
				thisStr = rtsi.toString();
				cellType = Cell.CELL_TYPE_STRING;
				break;

			case SST_STRING:
				String sstIndex = value.toString();
				try {
					int idx = Integer.parseInt(sstIndex);
					XSSFRichTextString rtss = new XSSFRichTextString(sharedStringsTable.getEntryAt(idx));
					thisStr = rtss.toString();
				} catch (NumberFormatException ex) {
					System.err.println("Failed to parse SST index '" + sstIndex + "': " + ex.toString());
				}
				cellType = Cell.CELL_TYPE_STRING;
				break;

			case NUMBER:
				String n = value.toString();
				thisStr = n;
				cellType = Cell.CELL_TYPE_NUMERIC;
				break;

			default:
				thisStr = "(TODO: Unexpected type: " + nextDataType + ")";
				cellType = -1;
				break;
			}

			// Output
			output.cell(cellRef, cellType, formulaType, thisStr, styleIndex);
			formulaType = -1;
		} else if ("is".equals(name)) {
			isIsOpen = false;
		} else if ("row".equals(name)) {
			output.endRow();
		} else if ("oddHeader".equals(name) || "evenHeader".equals(name) || "firstHeader".equals(name)) {
			hfIsOpen = false;
			output.headerFooter(headerFooter.toString(), true, name);
		} else if ("oddFooter".equals(name) || "evenFooter".equals(name) || "firstFooter".equals(name)) {
			hfIsOpen = false;
			output.headerFooter(headerFooter.toString(), false, name);
		}
	}

	/**
	 * Captures characters only if a suitable element is open. Originally was just "v"; extended for inlineStr also.
	 */
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		if (vIsOpen) {
			value.append(ch, start, length);
		}
		if (hfIsOpen) {
			headerFooter.append(ch, start, length);
		}
	}

	/**
	 * You need to implement this to handle the results of the sheet parsing.
	 */
	public interface SheetContentsHandler {
		/** A row with the (zero based) row number has started */
		public void startRow(int rowNum);

		/** A row with the (zero based) row number has ended */
		public void endRow();

		/** A cell, with the given unformatted value, was encountered */
		public void cell(String cellReference, int cellType, int formulaType, String value, int styleIndex);

		/** A header or footer has been encountered */
		public void headerFooter(String text, boolean isHeader, String tagName);
	}
}