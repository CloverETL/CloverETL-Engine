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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.jetel.data.parser.XLSMapping;
import org.jetel.data.parser.XLSMapping.HeaderGroup;
import org.jetel.data.parser.XLSMapping.HeaderRange;
import org.jetel.data.parser.XLSMapping.Stats;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

public class XLSMappingStats {
	private Map<CellPosition, Integer> headerRangePositionToCloverFieldMapping = new HashMap<CellPosition, Integer>();
	private Set<RelativeCellPosition> templateCellsToCopy = new LinkedHashSet<RelativeCellPosition>();
	private Map<Integer, Interval> xOffsetToMinimalYInterval = new HashMap<Integer,Interval>();
	private Map<Integer, Integer> cloverFieldToXOffsetMapping = new HashMap<Integer, Integer>();
	private Map<Integer, Integer> cloverFieldToYOffsetMapping = new HashMap<Integer, Integer>();
	private int minRecordFieldYOffset = 0;
	private XYRange headerXYRange;
	private XYRange firstRecordXYRange;
	private String templateSheetName;
	private int initialInsertionY;
	private int initialTemplateCopiedRegionY2;
	
	public XLSMappingStats() {
	}
	
	private DataFieldMetadata takeNextFieldInOrder(LinkedHashMap<String,DataFieldMetadata> cloverFields) {
		if (cloverFields.size()!=0) {
			String dataFieldMetaDataKey = cloverFields.keySet().iterator().next();
			 DataFieldMetadata dataFieldMetaData = cloverFields.remove(dataFieldMetaDataKey);
			return dataFieldMetaData;
		} else {
			return null;
		}

	}

	private String getCellStringValueByRowAndColumn(SheetData sheetData, int row, int column) {
		Cell cell = sheetData.getCellByRowAndColumn(row, column);
		if (cell==null) {
			return null;
		}
		String result = null;
		try {
			result = cell.getStringCellValue();
		} catch (Exception ex) {
			return null;
		}
		return result;

	}
	
	private DataFieldMetadata takeNextFieldByName(SheetData sheetData, LinkedHashMap<String,DataFieldMetadata> cloverFields, HeaderRange range) {
		if (cloverFields.size()!=0) {
			String dataFieldMetaDataKey = getCellStringValueByRowAndColumn(sheetData, range.getRowStart(), range.getColumnStart());
			if (dataFieldMetaDataKey!=null) {
				DataFieldMetadata dataFieldMetaData = cloverFields.remove(dataFieldMetaDataKey);
				return dataFieldMetaData;
			} else {
				return null;
			}
		} else {
			return null;
		}

	}

	private void computeHeaderAndRecordBounds(XLSMapping mappingInfo, CoordsTransformations transformations, boolean insert) {
		int maxX = 0;
		int maxY = 0;
		int maxYIncludingSkip = 0;
		int minYIncludingSkip = Integer.MAX_VALUE;
		int minX = Integer.MAX_VALUE;
		int minY = Integer.MAX_VALUE;
		
		for (HeaderGroup group : mappingInfo.getHeaderGroups()) {
			for (HeaderRange range : group.getRanges()) {
				maxX = transformations.getMaxX(maxX, range);
				maxYIncludingSkip = transformations.getMaxY(maxYIncludingSkip, range, group.getSkip());
				minYIncludingSkip = transformations.getMinY(minYIncludingSkip, range, group.getSkip());
				maxY = transformations.getMaxY(maxY, range, 0);
				minX = transformations.getMinX(minX, range);
				minY = transformations.getMinY(minY, range);
			}
		}
		
		if (minX==Integer.MAX_VALUE || minY==Integer.MAX_VALUE || minYIncludingSkip==Integer.MAX_VALUE) {
			this.setHeaderXYRange(null);
			this.setFirstRecordXYRange(null);
		} else {
			this.setHeaderXYRange(new XYRange(minX, minY, maxX, maxY));
			
			int firstRecordY1 = minYIncludingSkip;
			//When a step is really high (higher than a record itself), it is desired to define a
			//bottom of the first record so that empty lines complement the step.
			int firstRecordY2 = transformations.maximum(maxYIncludingSkip, minYIncludingSkip + mappingInfo.getStep() - 1);
			this.setFirstRecordXYRange(new XYRange(minX, firstRecordY1, maxX, firstRecordY2));
		}

		if (insert) {
			int firstRecordY2 = this.getFirstRecordXYRange().y2;
			this.setInitialInsertionY(firstRecordY2 - mappingInfo.getStep());
			this.setInitialTemplateCopiedRegionY2(firstRecordY2);
		}
	}
	
	public void init(XLSMapping mappingInfo, DataRecordMetadata metadata, SheetData sheetData, boolean insert, boolean hasTemplateWorkbook) {
		CoordsTransformations transformations = new CoordsTransformations(mappingInfo.getOrientation());
		Stats stats = mappingInfo.getStats();
		computeHeaderAndRecordBounds(mappingInfo, transformations, insert);
		
		this.removeAllCloverFieldsForHeaderPositions();
		this.removeAllXOffsetsForCloverFields();
		this.removeAllYOffsetsForCloverFields();
		this.removeAllYIntervalsForXOffsets();
		this.removeAllTemplateCellsToCopy();
		this.setMinRecordFieldYOffset(0);

		List<DataFieldMetadata> usedDataFields = new ArrayList<DataFieldMetadata>();
		Map<Integer,Integer> templateColumnsFieldCount = new HashMap<Integer,Integer>();
		
		LinkedHashMap<String, DataFieldMetadata> cloverFields = new LinkedHashMap<String, DataFieldMetadata>();
		for (DataFieldMetadata dataField : metadata.getFields()) {
			cloverFields.put(dataField.getName(), dataField);
		}
		
		for (HeaderGroup group : mappingInfo.getHeaderGroups()) {
			for (HeaderRange range : group.getRanges()) {
				
				int cloverField = XLSMapping.UNDEFINED;
				if (group.getCloverField() != XLSMapping.UNDEFINED) {
					cloverField = group.getCloverField();
				} else {
					switch (group.getMappingMode()) {
					case AUTO:
						if (stats.useAutoNameMapping()) {
							// name mapping
							DataFieldMetadata dataFieldMetadataByName = takeNextFieldByName(sheetData, cloverFields, range);
							if (dataFieldMetadataByName!=null) {
								cloverField = dataFieldMetadataByName.getNumber();
							}
						} else {
							// order mapping
							DataFieldMetadata dataFieldMetadata = takeNextFieldInOrder(cloverFields);
							if (dataFieldMetadata!=null) {
								cloverField = dataFieldMetadata.getNumber();
							}
						}
						break;
					case NAME:
						// order mapping
						DataFieldMetadata dataFieldMetadataByName = takeNextFieldByName(sheetData, cloverFields, range);
						if (dataFieldMetadataByName!=null) {
							cloverField = dataFieldMetadataByName.getNumber();
						}
						break;
					case ORDER:
						// order mapping
						DataFieldMetadata dataFieldMetadata = takeNextFieldInOrder(cloverFields);
						if (dataFieldMetadata!=null) {
							cloverField = dataFieldMetadata.getNumber();
						}
						break;
					}
				}
				
				if (cloverField != XLSMapping.UNDEFINED) {
					usedDataFields.add(metadata.getField(cloverField));
					
					XYRange firstRecordXYRange = this.getFirstRecordXYRange();
					this.addCloverFieldForHeaderPosition(transformations.getX1fromRange(range), transformations.getY1fromRange(range), cloverField);
					int recordFieldX = transformations.getX1fromRange(range);
					int recordFieldXOffset = recordFieldX - firstRecordXYRange.x1; //x coordinate minus x-indentation
					int headerBottomPlusSkip = transformations.getY2fromRange(range) + group.getSkip();
					int recordFieldYOffset = headerBottomPlusSkip - firstRecordXYRange.y2; //y coordinate of cell under header of this column minus y bound to the entire header
					this.addXYOffsetsForCloverField(cloverField, recordFieldXOffset, recordFieldYOffset);
					this.setMinRecordFieldYOffset(transformations.minimum(this.getMinRecordFieldYOffset(), recordFieldYOffset));
					this.addTemplateCellToCopy(new RelativeCellPosition(recordFieldXOffset, recordFieldYOffset));
					Integer templateColumnFieldCount = templateColumnsFieldCount.get(recordFieldX);
					if (templateColumnFieldCount==null) {
						templateColumnFieldCount = 0;
					}
					templateColumnsFieldCount.put(recordFieldX, templateColumnFieldCount+1);
				}
			}
		}
		
		for (Integer cloverFieldIndex : this.getRegisteredCloverFields()) {
			Integer yOffset = this.getYOffsetForCloverField(cloverFieldIndex);
			Integer xOffset = this.getXOffsetForCloverField(cloverFieldIndex);
			Interval currentYInterval = this.getMinimalYIntervalForXOffset(xOffset);
			if (currentYInterval==null) {
				this.addMinimalYIntervalForXOffset(xOffset, new Interval(yOffset, yOffset));
			} else if (currentYInterval.min > yOffset) {
				this.addMinimalYIntervalForXOffset(xOffset, new Interval(yOffset, currentYInterval.max));
			} else if (currentYInterval.max < yOffset) {
				this.addMinimalYIntervalForXOffset(xOffset, new Interval(currentYInterval.min, yOffset));
			}
		}
		
		if (hasTemplateWorkbook && insert) {
			//by default, template lines are expected to be at last "step" lines of the first record (fits in most realistic cases with straight-lined records)
			XYRange firstRecordXYRange = this.getFirstRecordXYRange();
			int defaultTemplateStartY = firstRecordXYRange.y2-mappingInfo.getStep()+1; 
			int defaultTemplateEndY   = defaultTemplateStartY + mappingInfo.getStep(); 
			for (int y=defaultTemplateStartY; y<defaultTemplateEndY; ++y) {
				int maximalX;
				if (mappingInfo.getOrientation() == XLSMapping.HEADER_ON_TOP) {
					Row row = sheetData.getRow(y);
					if (row==null) {
						maximalX=-1;
					} else {
						maximalX = sheetData.getLastCellNumber(row);
					}
				} else {
					maximalX = sheetData.getLastRowNumber();
				}
				for (int x=0; x<=maximalX; ++x) {
					Integer templateColumnFieldCount = templateColumnsFieldCount.get(x);
					if (templateColumnFieldCount==null) {
						templateColumnFieldCount=0;
					}
					RelativeCellPosition relativePosition = new RelativeCellPosition(x - firstRecordXYRange.x1, y - firstRecordXYRange.y2);
					if (templateColumnFieldCount<mappingInfo.getStep() && sheetData.getCellByXY(x, y)!=null && !this.getTemplateCellsToCopy().contains(relativePosition)) {
						this.addTemplateCellToCopy(relativePosition);
						templateColumnsFieldCount.put(x, templateColumnFieldCount+1);
					}
				}
			}
		}

		
		this.setTemplateSheetName(sheetData.getSheetName());
	}

	
	public Integer getCloverFieldForHeaderPosition(int x, int y) {
		return headerRangePositionToCloverFieldMapping.get(new CellPosition(x, y));
	}
	
	public void addCloverFieldForHeaderPosition(int x, int y, int cloverFieldIndex) {
		headerRangePositionToCloverFieldMapping.put(new CellPosition(x, y), cloverFieldIndex);
	}
	
	public void removeAllCloverFieldsForHeaderPositions() {
		headerRangePositionToCloverFieldMapping.clear();
	}
	
	public Set<RelativeCellPosition> getTemplateCellsToCopy() {
		return Collections.unmodifiableSet(templateCellsToCopy);
	}
	
	public boolean addTemplateCellToCopy(RelativeCellPosition templateCellPosition) {
		return templateCellsToCopy.add(templateCellPosition);
	}
	
	public void removeAllTemplateCellsToCopy() {
		templateCellsToCopy.clear();
	}
	
	public Interval getMinimalYIntervalForXOffset(int xOffset) {
		return xOffsetToMinimalYInterval.get(xOffset);
	}
	
	public Interval addMinimalYIntervalForXOffset(int xOffset, Interval yInterval) {
		return xOffsetToMinimalYInterval.put(xOffset, yInterval);
	}
	
	public Map<Integer, Interval> getXOffsetToMinimalYIntervalMap() {
		return Collections.unmodifiableMap(xOffsetToMinimalYInterval);
	}
	
	public void removeAllYIntervalsForXOffsets() {
		xOffsetToMinimalYInterval.clear();
	}
	
	public Set<Integer> getRegisteredCloverFields() {
		return Collections.unmodifiableSet(cloverFieldToYOffsetMapping.keySet());
	}
	
	public Integer getXOffsetForCloverField(int cloverFieldIndex) {
		return cloverFieldToXOffsetMapping.get(cloverFieldIndex);
	}
	
	public void addXYOffsetsForCloverField(int cloverFieldIndex, int xOffset, int yOffset) {
		cloverFieldToXOffsetMapping.put(cloverFieldIndex, xOffset);
		cloverFieldToYOffsetMapping.put(cloverFieldIndex, yOffset);
	}
	
	public void removeAllXOffsetsForCloverFields() {
		cloverFieldToXOffsetMapping.clear();
	}
	
	public Integer getYOffsetForCloverField(int cloverFieldIndex) {
		return cloverFieldToYOffsetMapping.get(cloverFieldIndex);
	}

	public void removeAllYOffsetsForCloverFields() {
		cloverFieldToYOffsetMapping.clear();
	}

	public int getMinRecordFieldYOffset() {
		return minRecordFieldYOffset;
	}

	public void setMinRecordFieldYOffset(int minRecordFieldYOffset) {
		this.minRecordFieldYOffset = minRecordFieldYOffset;
	}

	public XYRange getFirstRecordXYRange() {
		return firstRecordXYRange;
	}
	
	public void setFirstRecordXYRange(XYRange firstRecordXYRange) {
		this.firstRecordXYRange = firstRecordXYRange;
	}
	
	public XYRange getHeaderXYRange() {
		return headerXYRange;
	}

	public void setHeaderXYRange(XYRange headerXYRange) {
		this.headerXYRange = headerXYRange;
	}

	public String getTemplateSheetName() {
		return templateSheetName;
	}

	public void setTemplateSheetName(String templateSheetName) {
		this.templateSheetName = templateSheetName;
	}

	public int getInitialInsertionY() {
		return initialInsertionY;
	}

	public void setInitialInsertionY(int initialInsertionY) {
		this.initialInsertionY = initialInsertionY;
	}

	public int getInitialTemplateCopiedRegionY2() {
		return initialTemplateCopiedRegionY2;
	}

	public void setInitialTemplateCopiedRegionY2(int initialTemplateCopiedRegionY2) {
		this.initialTemplateCopiedRegionY2 = initialTemplateCopiedRegionY2;
	}
}