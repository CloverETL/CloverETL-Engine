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
package org.jetel.data.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SpreadsheetUtils;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18 Aug 2011
 */
public class XLSMapping {
	
	public static final String DEFAULT_VERSION = "1.0";
	public static final String DEFAULT_ENCODING = "UTF-8";
	
	public static final int DEFAULT_SKIP = 0;
	public static final SpreadsheetMappingMode DEFAULT_AUTO_MAPPING_MODE = SpreadsheetMappingMode.AUTO;
	
	public static final int UNDEFINED = -1;
	public static final char ESCAPE_START = '[';
	public static final char ESCAPE_END = ']';
	
	private static final String XPATH_GLOBAL_ATTRIBUTES = "/mapping/globalAttributes/";
	private static final String XPATH_STEP_ATTRIBUTE = XPATH_GLOBAL_ATTRIBUTES + "step/text()";
	private static final String XPATH_ORIENTATION_ATTRIBUTE = XPATH_GLOBAL_ATTRIBUTES + "orientation/text()";
	private static final String XPATH_WRITE_HEADER_ATTRIBUTE = XPATH_GLOBAL_ATTRIBUTES + "writeHeader/text()";

	private static final String XML_HEADER_GROUP = "headerGroup";
	private static final String XML_HEADER_GROUP_CLOVER_FIELD = "cloverField";
	private static final String XML_HEADER_GROUP_SKIP = "skip";
	private static final String XML_HEADER_GROUP_AUTO_MAPPING_TYPE = "autoMappingType";
	private static final String XML_HEADER_GROUP_FORMAT_FIELD = "formatField";
	private static final String XML_HEADER_RANGES = "headerRanges";
	private static final String XML_HEADER_RANGE_BEGIN = "begin";
	private static final String XML_HEADER_RANGE_END = "end";

	private static final Pattern HEADER_PATTERN = Pattern.compile("([A-Z]{1,3})([1-9][0-9]{0,6})");
	
	final public static SpreadsheetOrientation HEADER_ON_TOP = SpreadsheetOrientation.VERTICAL;
	
	private static final XLSMapping DEFAULT_MAPPING;
	static {
		List<HeaderGroup> groups = Collections.emptyList();
		DEFAULT_MAPPING = new XLSMapping(1, HEADER_ON_TOP, true, groups);
	}
	
	private final int step;
	private final SpreadsheetOrientation orientation;
	private final boolean writeHeader;
	
	private final List<HeaderGroup> headerGroups;
	private final Stats stats;
	
	public XLSMapping(DataRecordMetadata metaData) {
		this.step = 1;
		this.orientation = HEADER_ON_TOP;
		this.headerGroups = new ArrayList<HeaderGroup> ();
		DataFieldMetadata [] dataFields = metaData.getFields();
		for (int i = 0; i < dataFields.length; ++i) {
			DataFieldMetadata dataField = dataFields[i];
			headerGroups.add(new HeaderGroup(1, dataField.getNumber(), SpreadsheetMappingMode.AUTO, UNDEFINED, Collections.singletonList(new HeaderRange(0, 0, i, i))));
		}
		this.writeHeader = true;
		this.stats = resolveMappingStats();
	}
	
	private XLSMapping(int step, SpreadsheetOrientation orientation, boolean writeHeader, List<HeaderGroup> groups) {
		this.step = step;
		this.orientation = orientation;
		this.headerGroups = groups;
		this.writeHeader = writeHeader;
		this.stats = resolveMappingStats();
	}
	
	public int getStep() {
		return step;
	}

	public SpreadsheetOrientation getOrientation() {
		return orientation;
	}

	public boolean isWriteHeader() {
		return writeHeader;
	}

	public List<HeaderGroup> getHeaderGroups() {
		return headerGroups;
	}
	
	public Stats getStats() {
		return stats;
	}
	
	public static XLSMapping getDefault() {
		return DEFAULT_MAPPING;
	}
	
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		//TODO: implement this!
		return status;
	}
	
	public static XLSMapping parse(String inputString, DataRecordMetadata metadata) throws ComponentNotReadyException {
		InputSource is = new InputSource(new StringReader(inputString));
		
		try {
			return parse(getBuilder().parse(is), metadata);
		} catch (SAXException e) {
			 throw new ComponentNotReadyException(e);
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
	}
	
	public static XLSMapping parse(InputStream inputStream, DataRecordMetadata metadata) throws ComponentNotReadyException {
		
		try {
			return parse(getBuilder().parse(inputStream), metadata);
		} catch (SAXException e) {
			 throw new ComponentNotReadyException(e);
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
	}
	
	private static DocumentBuilder getBuilder() {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance(); //TODO: check schema checking
			factory.setAttribute("http://java.sun.com/xml/jaxp/properties/schemaLanguage", "http://www.w3.org/2001/XMLSchema");
			factory.setAttribute("http://java.sun.com/xml/jaxp/properties/schemaSource", "XLSMapping.xsd");

			return factory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new JetelRuntimeException(e);
		}
	}
	
	private static XLSMapping parse(Document document, DataRecordMetadata metadata) {
		try {
			XPathFactory xpathFactory = XPathFactory.newInstance();
			XPath xpath = xpathFactory.newXPath();

			Double intResult = (Double) xpath.evaluate(XPATH_STEP_ATTRIBUTE, document, XPathConstants.NUMBER);
			String orientationResult = (String) xpath.evaluate(XPATH_ORIENTATION_ATTRIBUTE, document, XPathConstants.STRING);
			Boolean writeHeaderResult = (Boolean) xpath.evaluate(XPATH_WRITE_HEADER_ATTRIBUTE, document, XPathConstants.BOOLEAN);

			return new XLSMapping(intResult.intValue(), SpreadsheetOrientation.valueOfIgnoreCase(orientationResult),
					writeHeaderResult, parseHeaderGroups(document.getElementsByTagName(XML_HEADER_GROUP), metadata));
		} catch (XPathExpressionException e) {
			throw new JetelRuntimeException(e);
		}
	}
	
	private static List<HeaderGroup> parseHeaderGroups(NodeList headerGroups, DataRecordMetadata metadata) {
		List<HeaderGroup> headerGroupList = new ArrayList<HeaderGroup>(headerGroups.getLength());
		
		Map<String, Integer> fieldMap = metadata.getFieldNamesMap();
		
		for (int i = 0; i < headerGroups.getLength(); i++) {
			Node group = headerGroups.item(i);
			NamedNodeMap attributes = group.getAttributes();
			
			int skip = DEFAULT_SKIP;
			Node skipAttribute = attributes.getNamedItem(XML_HEADER_GROUP_SKIP);
			if (skipAttribute != null) {
				skip = Integer.parseInt(skipAttribute.getNodeValue()); 
			}
			
			int cloverFieldIndex = UNDEFINED;
			SpreadsheetMappingMode mappingMode = DEFAULT_AUTO_MAPPING_MODE;
			List<HeaderRange> headerRanges = null;
			int formatFieldIndex = UNDEFINED;

			NodeList groupProperties = group.getChildNodes();
			for (int j = 0; j < groupProperties.getLength(); j++) {
				Node property = groupProperties.item(j);
				String propertyName = property.getNodeName(); 
			
				if (XML_HEADER_RANGES.equals(propertyName)) {
					headerRanges = parseHeaderRanges(property.getChildNodes());
				} else if (XML_HEADER_GROUP_CLOVER_FIELD.equals(propertyName) && property.getFirstChild() != null) {
					cloverFieldIndex = getCloverFieldIndex(fieldMap, property.getFirstChild().getNodeValue());
				} else if (XML_HEADER_GROUP_AUTO_MAPPING_TYPE.equals(propertyName) && property.getFirstChild() != null) {
					mappingMode = SpreadsheetMappingMode.valueOfIgnoreCase(property.getFirstChild().getNodeValue());
				} else if (XML_HEADER_GROUP_FORMAT_FIELD.equals(propertyName) && property.getFirstChild() != null) {
					formatFieldIndex = getCloverFieldIndex(fieldMap, property.getFirstChild().getNodeValue());
				}
			}
			
			headerGroupList.add(new HeaderGroup(skip, cloverFieldIndex, mappingMode, formatFieldIndex, headerRanges));
		}
		
		return headerGroupList;
	}

	private static int getCloverFieldIndex(Map<String, Integer> fieldMap, String field) {
		int isInteger = StringUtils.isInteger(field); 
		if (isInteger == 0 || isInteger == 1) {
			return Integer.parseInt(field);
		} else {
			Integer index = fieldMap.get(field);
			return index != null ? index : UNDEFINED; //TODO: fail instead?
		}
	}
	
	private static List<HeaderRange> parseHeaderRanges(NodeList headerRanges) {
		List<HeaderRange> ranges = new ArrayList<HeaderRange>(headerRanges.getLength());
		
		int rowStart;
		int rowEnd;
		int columnStart;
		int columnEnd;
		
		for (int rangeCounter = 0; rangeCounter < headerRanges.getLength(); rangeCounter++) {
			Node range = headerRanges.item(rangeCounter);
			if (range.getNodeType() != Node.ELEMENT_NODE) {
				continue;
			}
			NamedNodeMap rangeAttributes = range.getAttributes();
			
			Matcher matcher = HEADER_PATTERN.matcher(rangeAttributes.getNamedItem(XML_HEADER_RANGE_BEGIN).getNodeValue());
			matcher.matches();
			
			columnStart = columnEnd = SpreadsheetUtils.getColumnIndex(matcher.group(1));
			rowStart = rowEnd = Integer.parseInt(matcher.group(2)) - 1;
			
			Node end = rangeAttributes.getNamedItem(XML_HEADER_RANGE_END);
			if (end != null) {
				matcher = HEADER_PATTERN.matcher(end.getNodeValue());
				matcher.matches();
				columnEnd = SpreadsheetUtils.getColumnIndex(matcher.group(1));
				rowEnd = Integer.parseInt(matcher.group(2)) - 1;
			}
			
			ranges.add(new HeaderRange(rowStart, rowEnd, columnStart, columnEnd));
		}
		
		return ranges;
	}
	
	private Stats resolveMappingStats() {
		boolean nameMapping = false;
		boolean autoNameMapping = false;
		boolean formatMapping = false;
		
		int mappingMinRow = Integer.MAX_VALUE;
		int mappingMaxRow = 0;
		int mappingMinColumn = Integer.MAX_VALUE;
		int mappingMaxColumn = 0;
		
		int recordStartLine = Integer.MAX_VALUE;
		int recordEndLine = 0;
		
		int maxSkip = 0;
		
		if (headerGroups.isEmpty()) {
			mappingMinRow = 0;
			mappingMinColumn = 0;
		} else {
			for (HeaderGroup group : headerGroups) {
				if (!nameMapping && group.getMappingMode() == SpreadsheetMappingMode.NAME) {
					nameMapping = true;
				}
				if (!autoNameMapping && group.getMappingMode() == SpreadsheetMappingMode.AUTO) {
					autoNameMapping = true;
				}
				if (!formatMapping && group.getFormatField() != UNDEFINED) {
					formatMapping = true;
				}

				for (HeaderRange range : group.getRanges()) {
					if (range.getRowStart() < mappingMinRow) {
						mappingMinRow = range.getRowStart();
					}
					if (range.getRowEnd() > mappingMaxRow) {
						mappingMaxRow = range.getRowEnd();
					}
					if (range.getColumnStart() < mappingMinColumn) {
						mappingMinColumn = range.getColumnStart();
					}
					if (range.getColumnEnd() > mappingMaxColumn) {
						mappingMaxColumn = range.getColumnEnd();
					}

					int rangeStartLine;
					int rangeEndLine;
					if (getOrientation() == HEADER_ON_TOP) {
						rangeStartLine = range.getRowStart();
						rangeEndLine =  range.getRowEnd();
					} else {
						rangeStartLine = range.getColumnStart();
						rangeEndLine =  range.getColumnEnd();
					}
					
					rangeStartLine += group.getSkip();
					rangeEndLine += group.getSkip();
					
					if (rangeStartLine < recordStartLine) {
						recordStartLine = rangeStartLine;
					}
					if (rangeEndLine > recordEndLine) {
						recordEndLine = rangeEndLine;
					}
				}
				if (group.getSkip() > maxSkip) {
					maxSkip = group.getSkip();
				}
			}
		}
		
		if (maxSkip != 0) {
			autoNameMapping = false;
		}
		
		int rowCount;
		int columnCount;
		
		if (getOrientation() == HEADER_ON_TOP) {
			rowCount = recordEndLine - recordStartLine + 1;
			columnCount = mappingMaxColumn - mappingMinColumn + 1;
		} else {
			rowCount = mappingMaxRow - mappingMinRow + 1;
			columnCount = recordEndLine - recordStartLine + 1;
		}

		return new Stats(nameMapping, autoNameMapping, formatMapping, recordStartLine, rowCount, columnCount,
				mappingMinRow, mappingMaxRow, mappingMinColumn, mappingMaxColumn);
	}
	
	public static class HeaderGroup {
		private final int skip;
		private final int cloverField;
		private final SpreadsheetMappingMode mappingMode;
		private final int formatField;
		
		private final List<HeaderRange> ranges;
		
		public HeaderGroup(int skip, int cloverField, SpreadsheetMappingMode mappingMode, int formatField, List<HeaderRange> ranges) {
			this.skip = skip;
			this.cloverField = cloverField;
			this.mappingMode = mappingMode;
			this.formatField = formatField;
			this.ranges = Collections.unmodifiableList(ranges);
		}

		public int getSkip() {
			return skip;
		}

		public int getCloverField() {
			return cloverField;
		}

		public SpreadsheetMappingMode getMappingMode() {
			return mappingMode;
		}

		public int getFormatField() {
			return formatField;
		}
		
		public List<HeaderRange> getRanges() {
			return ranges;
		}
	}

	public static class HeaderRange {
		
		private final int rowStart;
		private final int rowEnd;
		private final int columnStart;
		private final int columnEnd;
		
		public HeaderRange(int rowStart, int rowEnd, int columnStart, int columnEnd) {
			this.rowStart = rowStart;
			this.rowEnd = rowEnd;
			this.columnStart = columnStart;
			this.columnEnd = columnEnd;
		}

		public int getRowStart() {
			return rowStart;
		}

		public int getRowEnd() {
			return rowEnd;
		}

		public int getColumnStart() {
			return columnStart;
		}

		public int getColumnEnd() {
			return columnEnd;
		}
	}

	public static class Stats {
		/** flag indicating that name mapping is set somewhere in the mapping */
		private final boolean nameMapping;
		/** flag indicating that auto mapping is resolved as name mapping */
		private final boolean autoNameMapping;
		/** flag indicating that format mapping is set somewhere in the mapping */
		private final boolean formatMapping;
		
		/** starting line of read/write. "line" means row or column for VERTICAL or HORIZONTAL orientation, resp. */
		private final int startLine;
		/** record height */
		private final int rowCount;
		/** record width */
		private final int columnCount;

		/** minimum row from all mapped ranges */
		private final int mappingMinRow;
		/** maximum row from all mapped ranges */
		private final int mappingMaxRow;
		/** minimum column from all mapped ranges */
		private final int mappingMinColumn;
		/** maximum column from all mapped ranges */
		private final int mappingMaxColumn;
		
		private Stats(boolean nameMapping, boolean autoNameMapping, boolean formatMapping, int startLine, int rowCount, int columnCount,
				int mappingMinRow, int mappingMaxRow, int mappingMinColumn, int mappingMaxColumn) {
			this.nameMapping = nameMapping;
			this.autoNameMapping = autoNameMapping;
			this.formatMapping = formatMapping;
			this.startLine = startLine;
			this.rowCount = rowCount;
			this.columnCount = columnCount;
			this.mappingMinRow = mappingMinRow;
			this.mappingMaxRow = mappingMaxRow;
			this.mappingMinColumn = mappingMinColumn;
			this.mappingMaxColumn = mappingMaxColumn;
		}

		public boolean useNameMapping() {
			return nameMapping;
		}

		public boolean useAutoNameMapping() {
			return autoNameMapping;
		}
		
		public boolean isFormatMapping() {
			return formatMapping;
		}
		
		public int getStartLine() {
			return startLine;
		}

		public int getRowCount() {
			return rowCount;
		}

		public int getColumnCount() {
			return columnCount;
		}

		public int getMappingMinRow() {
			return mappingMinRow;
		}

		public int getMappingMaxRow() {
			return mappingMaxRow;
		}

		public int getMappingMinColumn() {
			return mappingMinColumn;
		}

		public int getMappingMaxColumn() {
			return mappingMaxColumn;
		}
	}

	public static enum SpreadsheetOrientation {

		HORIZONTAL, VERTICAL;

		public static SpreadsheetOrientation valueOfIgnoreCase(String string) {
			for (SpreadsheetOrientation orientation : values()) {
				if (orientation.name().equalsIgnoreCase(string)) {
					return orientation;
				}
			}

			return VERTICAL;
		}

	}

	public static enum SpreadsheetMappingMode {

		ORDER, NAME, AUTO;

		public static SpreadsheetMappingMode valueOfIgnoreCase(String string) {
			for (SpreadsheetMappingMode orientation : values()) {
				if (orientation.name().equalsIgnoreCase(string)) {
					return orientation;
				}
			}

			return AUTO;
		}

	}
}


