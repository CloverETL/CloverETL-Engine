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

import java.util.HashMap;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Workbook;
import org.jetel.data.formatter.SpreadsheetFormatter;
import org.jetel.metadata.DataFieldFormatType;
import org.jetel.metadata.DataFieldMetadata;

public class CellStyleLibrary {
		private Map<Integer, CellStyle> cloverFieldToCellStyle = new HashMap<Integer, CellStyle>();
		private DataFormat dataFormat;

		public void init(Workbook workbook) {
			clear();
			dataFormat = workbook.createDataFormat();
		}
		
		private class CellStyleFilter {
			private short alignment;
			private short verticalAlignment;
			private short borderBottom;
			private short borderLeft;
			private short borderRight;
			private short borderTop;
			private short bottomBorderColor;
			private short leftBorderColor;
			private short rightBorderColor;
			private short topBorderColor;
			private short dataFormat;
			private short fillBackgroundColor;
			private short fillForegroundColor;
			private short fillPattern;
			private short fontIndex;
			private boolean hidden;
			private short indention;
			private boolean locked;
			private short rotation;
			private boolean wrapText;
			
			CellStyleFilter(CellStyle cellStyle) {
				setAlignment(cellStyle.getAlignment());
				setVerticalAlignment(cellStyle.getVerticalAlignment());
				setBorderBottom(cellStyle.getBorderBottom());
				setBorderLeft(cellStyle.getBorderLeft());
				setBorderRight(cellStyle.getBorderRight());
				setBorderTop(cellStyle.getBorderTop());
				setBottomBorderColor(cellStyle.getBottomBorderColor());
				setLeftBorderColor(cellStyle.getLeftBorderColor());
				setRightBorderColor(cellStyle.getRightBorderColor());
				setTopBorderColor(cellStyle.getRightBorderColor());
				setDataFormat(cellStyle.getDataFormat());
				setFillBackgroundColor(cellStyle.getFillBackgroundColor());
				setFillForegroundColor(cellStyle.getFillForegroundColor());
				setFillPattern(cellStyle.getFillPattern());
				setFontIndex(cellStyle.getFontIndex());
				setHidden(cellStyle.getHidden());
				setIndention(cellStyle.getIndention());
				setLocked(cellStyle.getLocked());
				setRotation(cellStyle.getRotation());
				setWrapText(cellStyle.getWrapText());
			}

			boolean matches(CellStyle cellStyle) {
				return (getAlignment() == cellStyle.getAlignment() &&
						getVerticalAlignment() == cellStyle.getVerticalAlignment() &&
						getBorderBottom() == cellStyle.getBorderBottom() &&
						
				getBorderLeft() == cellStyle.getBorderLeft() &&
				getBorderRight() == cellStyle.getBorderRight() &&
				getBorderTop() == cellStyle.getBorderTop() &&
				getBottomBorderColor() == cellStyle.getBottomBorderColor() &&
				getLeftBorderColor() == cellStyle.getLeftBorderColor() &&
				getRightBorderColor() == cellStyle.getRightBorderColor() &&
				getTopBorderColor() == cellStyle.getRightBorderColor() &&
				getDataFormat() == cellStyle.getDataFormat() &&
				getFillBackgroundColor() == cellStyle.getFillBackgroundColor() &&
				getFillForegroundColor() == cellStyle.getFillForegroundColor() &&
				getFillPattern() == cellStyle.getFillPattern() &&
				getFontIndex() == cellStyle.getFontIndex() &&
				getHidden() == cellStyle.getHidden() &&
				getIndention() == cellStyle.getIndention() &&
				getLocked() == cellStyle.getLocked() &&
				getRotation() == cellStyle.getRotation() &&
				getWrapText() == cellStyle.getWrapText());
			}

			public short getAlignment() {
				return alignment;
			}

			public void setAlignment(short alignment) {
				this.alignment = alignment;
			}

			public short getVerticalAlignment() {
				return verticalAlignment;
			}

			public void setVerticalAlignment(short verticalAlignment) {
				this.verticalAlignment = verticalAlignment;
			}

			public short getBorderBottom() {
				return borderBottom;
			}

			public void setBorderBottom(short borderBottom) {
				this.borderBottom = borderBottom;
			}

			public short getBorderLeft() {
				return borderLeft;
			}

			public void setBorderLeft(short borderLeft) {
				this.borderLeft = borderLeft;
			}

			public short getBorderRight() {
				return borderRight;
			}

			public void setBorderRight(short borderRight) {
				this.borderRight = borderRight;
			}

			public short getBorderTop() {
				return borderTop;
			}

			public void setBorderTop(short borderTop) {
				this.borderTop = borderTop;
			}

			public short getBottomBorderColor() {
				return bottomBorderColor;
			}

			public void setBottomBorderColor(short bottomBorderColor) {
				this.bottomBorderColor = bottomBorderColor;
			}

			public short getLeftBorderColor() {
				return leftBorderColor;
			}

			public void setLeftBorderColor(short leftBorderColor) {
				this.leftBorderColor = leftBorderColor;
			}

			public short getRightBorderColor() {
				return rightBorderColor;
			}

			public void setRightBorderColor(short rightBorderColor) {
				this.rightBorderColor = rightBorderColor;
			}

			public short getTopBorderColor() {
				return topBorderColor;
			}

			public void setTopBorderColor(short topBorderColor) {
				this.topBorderColor = topBorderColor;
			}

			public short getDataFormat() {
				return dataFormat;
			}

			public void setDataFormat(short dataFormat) {
				this.dataFormat = dataFormat;
			}

			public short getFillBackgroundColor() {
				return fillBackgroundColor;
			}

			public void setFillBackgroundColor(short fillBackgroundColor) {
				this.fillBackgroundColor = fillBackgroundColor;
			}

			public short getFillPattern() {
				return fillPattern;
			}

			public void setFillPattern(short fillPattern) {
				this.fillPattern = fillPattern;
			}

			public short getFillForegroundColor() {
				return fillForegroundColor;
			}

			public void setFillForegroundColor(short fillForegroundColor) {
				this.fillForegroundColor = fillForegroundColor;
			}

			public short getFontIndex() {
				return fontIndex;
			}

			public void setFontIndex(short fontIndex) {
				this.fontIndex = fontIndex;
			}

			public boolean getHidden() {
				return hidden;
			}

			public void setHidden(boolean hidden) {
				this.hidden = hidden;
			}

			public short getIndention() {
				return indention;
			}

			public void setIndention(short indention) {
				this.indention = indention;
			}

			public boolean getLocked() {
				return locked;
			}

			public void setLocked(boolean locked) {
				this.locked = locked;
			}

			public short getRotation() {
				return rotation;
			}

			public void setRotation(short rotation) {
				this.rotation = rotation;
			}

			public boolean getWrapText() {
				return wrapText;
			}

			public void setWrapText(boolean wrapText) {
				this.wrapText = wrapText;
			}
			
			
		}

		
		public void addCellStyle(int cloverFieldIndex, CellStyle cellStyle) {
			cloverFieldToCellStyle.put(cloverFieldIndex, cellStyle);
		}
		
		public void removeCellStyle(int cloverFieldIndex) {
			cloverFieldToCellStyle.remove(cloverFieldIndex);
		}
		
		public CellStyle getCellStyle(int cloverFieldIndex) {
			return cloverFieldToCellStyle.get(cloverFieldIndex);
		}
		
		public void clear() {
			cloverFieldToCellStyle.clear();
		}
		
		public static CellStyle findCellStyle(Workbook workbook, CellStyleFilter cellStyleFilter) {
			for (short i=0; i<workbook.getNumCellStyles(); ++i) {
				CellStyle cellStyle = workbook.getCellStyleAt(i);
				if (cellStyleFilter.matches(cellStyle)) {
					return cellStyle;
				}
			}
			return null;
		}
		
		public short findOrCreateBoldStyle(Workbook workbook, SheetData referenceSheetData, int rowIndex, int columnIndex) {
			Cell cell = referenceSheetData.getCellByRowAndColumn(rowIndex, columnIndex);
			CellStyle origStyle = cell.getCellStyle();
			short fontIndex = origStyle.getFontIndex();
			Font font = workbook.getFontAt(fontIndex);
			Font correspondingBoldFont = workbook.findFont(Font.BOLDWEIGHT_BOLD, font.getColor(), font.getFontHeight(), font.getFontName(), font.getItalic(), font.getStrikeout(), font.getTypeOffset(), font.getUnderline());
			if (correspondingBoldFont==null) {
				correspondingBoldFont = workbook.createFont();
				correspondingBoldFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
				correspondingBoldFont.setColor(font.getColor());
				correspondingBoldFont.setFontHeight(font.getFontHeight());
				correspondingBoldFont.setFontName(font.getFontName());
				correspondingBoldFont.setItalic(font.getItalic());
				correspondingBoldFont.setStrikeout(font.getStrikeout());
				correspondingBoldFont.setTypeOffset(font.getTypeOffset());
				correspondingBoldFont.setUnderline(font.getUnderline());
			}
			
			CellStyleFilter cellStyleFilter = new CellStyleFilter(origStyle);
			cellStyleFilter.setFontIndex(correspondingBoldFont.getIndex());
			CellStyle correspondingCellStyle = CellStyleLibrary.findCellStyle(workbook, cellStyleFilter);
			
			if (correspondingCellStyle==null) {
				correspondingCellStyle = workbook.createCellStyle();
				correspondingCellStyle.cloneStyleFrom(origStyle);
				correspondingCellStyle.setFont(correspondingBoldFont);
			}
			
			return correspondingCellStyle.getIndex();
		}
		
		public CellStyle takeCellStyleOrPrepareCellStyle(Cell cell, String formatStringFromRecord, DataFieldMetadata fieldMetadata, Workbook workbook, XLSMappingStats mappingStats, SheetData referenceSheetData, SheetData templateSheetData, boolean insert) {
			CellStyle cellStyle = this.getCellStyle(fieldMetadata.getNumber());
			if (cellStyle==null || formatStringFromRecord!=null) {
				cellStyle = prepareCellStyle(cell, formatStringFromRecord, fieldMetadata, workbook, mappingStats, referenceSheetData, templateSheetData, insert);
				this.addCellStyle(fieldMetadata.getNumber(), cellStyle);
			}
			return cellStyle;
		}
		
		public CellStyle prepareCellStyle(Cell cell, String formatStringFromRecord, DataFieldMetadata fieldMetadata, Workbook workbook, XLSMappingStats mappingStats, SheetData referenceSheetData, SheetData templateSheetData, boolean insert) {
			Cell templateCell = null;
			if (insert) {
				XYRange firstRecordXYRange = mappingStats.getFirstRecordXYRange();
				int x = firstRecordXYRange.x1 + mappingStats.getXOffsetForCloverField(fieldMetadata.getNumber());
				int y = referenceSheetData.getTemplateCopiedRegionY2() + mappingStats.getYOffsetForCloverField(fieldMetadata.getNumber());
				if (templateSheetData!=null) {
					templateCell = templateSheetData.getCellByXY(x, y);
				}
			}

			CellStyle templateCellStyle;
			if (templateCell != null) {
				templateCellStyle = templateCell.getCellStyle();
			} else {
				if (workbook.getNumCellStyles() == 0) {
					templateCellStyle = workbook.createCellStyle();
				} else {
					templateCellStyle = cell.getCellStyle();
				}
			}

			String formatStr = SpreadsheetFormatter.GENERAL_FORMAT_STRING;
			if (formatStringFromRecord!=null && !"".equals(formatStringFromRecord)) {
				formatStr = formatStringFromRecord;
			} else if (fieldMetadata.hasFormat()) {
				formatStr = fieldMetadata.getFormat(DataFieldFormatType.EXCEL);
			}
			
			short modifiedDataFormat = dataFormat.getFormat(formatStr);

			CellStyleFilter cellStyleFilter = new CellStyleFilter(templateCellStyle);
			cellStyleFilter.setDataFormat(modifiedDataFormat);
			CellStyle cellStyle = CellStyleLibrary.findCellStyle(workbook, cellStyleFilter);
			if (cellStyle == null) {
				cellStyle = workbook.createCellStyle();
				cellStyle.cloneStyleFrom(templateCellStyle);
				cellStyle.setDataFormat(modifiedDataFormat);
			}
			
			return cellStyle;
		}

	}