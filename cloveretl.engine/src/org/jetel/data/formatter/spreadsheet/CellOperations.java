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

import java.util.Date;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Hyperlink;
import org.jetel.data.DataField;
import org.jetel.data.primitive.Decimal;
import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataFieldMetadata;

public class CellOperations {
	public static void copyCell(Cell sourceCell, Cell targetCell) {
		if (sourceCell.getCellComment()!=null) {
			targetCell.setCellComment(sourceCell.getCellComment());
		}
		if (sourceCell.getHyperlink()!=null) {
			targetCell.setHyperlink(sourceCell.getHyperlink());
		}
		targetCell.setCellStyle(sourceCell.getCellStyle());
		switch (sourceCell.getCellType()) {
		case Cell.CELL_TYPE_BLANK:
			targetCell.setCellValue("");
			targetCell.setCellType(Cell.CELL_TYPE_BLANK);
			break;
		case Cell.CELL_TYPE_STRING:
			targetCell.setCellValue(sourceCell.getStringCellValue());
			break;
		case Cell.CELL_TYPE_BOOLEAN:
			targetCell.setCellValue(sourceCell.getBooleanCellValue());
			break;
		case Cell.CELL_TYPE_ERROR:
			targetCell.setCellValue(sourceCell.getErrorCellValue());
			break;
		case Cell.CELL_TYPE_FORMULA:
			targetCell.setCellFormula(sourceCell.getCellFormula());
			break;
		case Cell.CELL_TYPE_NUMERIC:
			targetCell.setCellValue(sourceCell.getNumericCellValue());
		}
	}
	
	public static void swapCells(Cell sourceCell, Cell targetCell) {
		if (sourceCell.getCellComment()!=null) {
			Comment targetCellComment = targetCell.getCellComment();
			targetCell.setCellComment(sourceCell.getCellComment());
			sourceCell.setCellComment(targetCellComment);
		}
		if (sourceCell.getHyperlink()!=null) {
			Hyperlink targetCellHyperLink = targetCell.getHyperlink();
			targetCell.setHyperlink(sourceCell.getHyperlink());
			sourceCell.setHyperlink(targetCellHyperLink);
		}
		CellStyle targetCellStyle = targetCell.getCellStyle(); 
		targetCell.setCellStyle(sourceCell.getCellStyle());
		sourceCell.setCellStyle(targetCellStyle);
		
		Object targetCellValue = getCellValue(targetCell);
		Object sourceCellValue = getCellValue(sourceCell);
		int sourceCellType = sourceCell.getCellType();
		int targetCellType = targetCell.getCellType();
		setCellValue(targetCell, sourceCellType, sourceCellValue);
		setCellValue(sourceCell, targetCellType, targetCellValue);
	}

	public static Object getCellValue(Cell cell) {
		switch (cell.getCellType()) {
		case Cell.CELL_TYPE_BLANK:
			return null;
		case Cell.CELL_TYPE_STRING:
			return cell.getStringCellValue();
		case Cell.CELL_TYPE_BOOLEAN:
			return cell.getBooleanCellValue(); 
		case Cell.CELL_TYPE_ERROR:
			return cell.getErrorCellValue();
		case Cell.CELL_TYPE_FORMULA:
			return cell.getCellFormula();
		case Cell.CELL_TYPE_NUMERIC:
			return cell.getNumericCellValue();
		default:
			return null;
		}
	}
	
	public static void setCellValue(Cell cell, int cellType, Object value) {
		switch (cellType) {
		case Cell.CELL_TYPE_BLANK:
			cell.setCellValue("");
			cell.setCellType(cellType);
			break;
		case Cell.CELL_TYPE_STRING:
			String stringValue = (String)value;
			cell.setCellValue(stringValue);
			break;
		case Cell.CELL_TYPE_BOOLEAN:
			boolean booleanValue = (Boolean)value; 
			cell.setCellValue(booleanValue);
			break;
		case Cell.CELL_TYPE_ERROR:
			byte errorValue = (Byte)value;
			cell.setCellErrorValue(errorValue);
			break;
		case Cell.CELL_TYPE_FORMULA:
			String cellFormula = (String)value;
			cell.setCellFormula(cellFormula);
			break;
		case Cell.CELL_TYPE_NUMERIC:
			double numericValue = (Double)value;
			cell.setCellValue(numericValue);
		}

	}
	
	public static void setCellValue(Cell cell, DataField dataField) {
		
		if (dataField.getValue()==null) {
			cell.setCellValue(dataField.getMetadata().getNullValue());
			if ("".equals(dataField.getMetadata().getNullValue())) {
				cell.setCellType(Cell.CELL_TYPE_BLANK);
			}

			return;
		}
		
		try {
			switch (dataField.getType()) {
			case DataFieldMetadata.DATE_FIELD:
			case DataFieldMetadata.DATETIME_FIELD:
				cell.setCellValue(DateUtil.getExcelDate((Date)dataField.getValue()));
				break;
			case DataFieldMetadata.BYTE_FIELD:
			case DataFieldMetadata.STRING_FIELD:
				String stringValueToSet = dataField.toString();
				cell.setCellValue(stringValueToSet);
				break;
			case DataFieldMetadata.DECIMAL_FIELD:
				cell.setCellValue(((Decimal)dataField.getValue()).getDouble());
				break;
			case DataFieldMetadata.INTEGER_FIELD:
				cell.setCellValue((Integer)dataField.getValue());
				break;
			case DataFieldMetadata.LONG_FIELD:
				cell.setCellValue((Long)dataField.getValue());
				break;
			case DataFieldMetadata.NUMERIC_FIELD:
				cell.setCellValue((Double)dataField.getValue());
				break;
			case DataFieldMetadata.BOOLEAN_FIELD:
				cell.setCellValue(((Boolean) (dataField.getValue())).booleanValue());
				break;
			default:
				throw new IllegalStateException("Unknown data field type " + DataFieldMetadata.type2Str(dataField.getType()));
					
			}
		} catch (ClassCastException ex) {
			throw new BadDataFormatException("Problems while converting data record to cell data", ex);
		}
	}

	
	public static boolean cellIsEmpty(Cell cell) {
		switch (cell.getCellType()) {
		case Cell.CELL_TYPE_BLANK:
		case Cell.CELL_TYPE_STRING:
			return (cell.getStringCellValue().equals(""));
		case Cell.CELL_TYPE_BOOLEAN:
			return false;
		case Cell.CELL_TYPE_ERROR:
			return false;
		case Cell.CELL_TYPE_FORMULA:
			return false;
		case Cell.CELL_TYPE_NUMERIC:
			return false;
		default:
			return false;
		}
	}
	
	public static void setStringToCellGivenByRowAndColumn(SheetData sheetData, int rowIndex, int columnIndex, String stringValue) {
		Cell cell = sheetData.getCellByRowAndColumn(rowIndex, columnIndex);
		if (cell==null) {//this may happen if a record is curved and so it is not desired to create all rows of header before a first record is written 
			cell = sheetData.createCellAndRefreshLastColumnNumber(rowIndex, columnIndex);
		}
		cell.setCellValue(stringValue);
	}

	public static void setStyleToCellGivenByRowAndColumn(SheetData sheetData, int rowIndex, int columnIndex, CellStyle cellStyle) {
		Cell cell = sheetData.getCellByRowAndColumn(rowIndex, columnIndex);
		cell.setCellStyle(cellStyle);
	}
	

}