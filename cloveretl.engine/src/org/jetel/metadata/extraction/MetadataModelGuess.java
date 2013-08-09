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
package org.jetel.metadata.extraction;

import java.io.IOException;
import java.io.InputStream;

import org.jetel.metadata.DataRecordParsingType;
import org.jetel.util.file.FileUtils;

/**
 * @author slamam (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.8.2013
 */
public class MetadataModelGuess {

	public static final int FIELD_COUNT_UNSPECIFIED = -1;
	
	public static final String FORMAT_PROPERTY = "format";

	private Integer[] fieldCounts;
	private FieldTypeGuess[] proposedTypes;

	private Character quoteChar;
	private String encoding;
	private String originName;
	private String fieldDelimiter;
	private String recordDelimiter;
	private String metadataName;
	private String metadataLabel;
	private DataRecordParsingType metadataType = DataRecordParsingType.DELIMITED;
	private boolean normalizeNames = true;
	private int skipSourceRows;
	private boolean quotedStrings;
	
	public MetadataModelGuess(String metadataName) {
		this.metadataName = metadataName;
	}
	
	public MetadataModelGuess(MetadataModelGuess modelGuess) {
		
		setEncoding(modelGuess.getEncoding());
		setFieldCounts(modelGuess.getFieldCounts());
		setFieldDelimiter(modelGuess.getFieldDelimiter());
		setMetadataName(modelGuess.getMetadataName());
		setMetadataType(modelGuess.getMetadataType());
		setNormalizeNames(modelGuess.isNormalizeNames());
		setOriginName(modelGuess.getOriginName());
		setProposedTypes(modelGuess.getProposedTypes());
		setQuoteChar(modelGuess.getQuoteChar());
		setQuotedStrings(modelGuess.isQuotedStrings());
		setRecordDelimiter(modelGuess.getRecordDelimiter());
		setSkipSourceRows(modelGuess.getSkipSourceRows());
	}
	
	public int getFieldCountSize() {
		if (fieldCounts == null) {
			return FIELD_COUNT_UNSPECIFIED;			
		} else {
			return fieldCounts.length;
		}
	}

	/**
	 * @return the delimiter
	 */
	public String getFieldDelimiter() {
		return fieldDelimiter;
	}

	/**
	 * @param delimiter the delimiter to set
	 */
	public void setFieldDelimiter(String delimiter) {
		this.fieldDelimiter = delimiter;
	}

	/**
	 * @return the fieldCounts
	 */
	public Integer[] getFieldCounts() {
		return fieldCounts;
	}

	/**
	 * @param fieldCounts the fieldCounts to set
	 */
	public void setFieldCounts(Integer[] fieldCounts) {
		this.fieldCounts = fieldCounts;
	}
	
	/**
	 * @return the proposedTypes
	 */
	public FieldTypeGuess[] getProposedTypes() {
		return proposedTypes;
	}

	/**
	 * @param proposedTypes the proposedTypes to set
	 */
	public void setProposedTypes(FieldTypeGuess[] proposedTypes) {
		this.proposedTypes = proposedTypes;
	}

	/**
	 * @return the skipSourceRows
	 */
	public int getSkipSourceRows() {
		return skipSourceRows;
	}

	/**
	 * @param skipSourceRows the skipSourceRows to set
	 */
	public void setSkipSourceRows(int skipSourceRows) {
		this.skipSourceRows = skipSourceRows;
	}

	/**
	 * @return the quotedStrings
	 */
	public boolean isQuotedStrings() {
		return quotedStrings;
	}

	/**
	 * @param quotedStrings the quotedStrings to set
	 */
	public void setQuotedStrings(boolean quotedStrings) {
		this.quotedStrings = quotedStrings;
	}

	/**
	 * @return the quoteChar
	 */
	public Character getQuoteChar() {
		return quoteChar;
	}

	/**
	 * @param quoteChar the quoteChar to set
	 */
	public void setQuoteChar(Character quoteChar) {
		this.quoteChar = quoteChar;
	}
	
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	/**
	 * @return the encoding
	 */
	public String getEncoding() {
		return encoding;
	}

	/**
	 * @return the inputStream
	 * @throws IOException 
	 */
	public InputStream getInputStream() throws IOException {
		return FileUtils.getInputStream(null, originName);
	}

	/**
	 * @return the metadataType
	 */
	public DataRecordParsingType getMetadataType() {
		return metadataType;
	}

	/**
	 * @param metadataType the metadataType to set
	 */
	public void setMetadataType(DataRecordParsingType metadataType) {
		this.metadataType = metadataType;
	}

	/**
	 * @return the normalizeNames
	 */
	public boolean isNormalizeNames() {
		return normalizeNames;
	}

	/**
	 * @param normalizeNames the normalizeNames to set
	 */
	public void setNormalizeNames(boolean normalizeNames) {
		this.normalizeNames = normalizeNames;
	}

	/**
	 * @return the originName
	 */
	public String getOriginName() {
		return originName;
	}

	/**
	 * @param originName the originName to set
	 */
	public void setOriginName(String originName) {
		this.originName = originName;
	}

	/**
	 * @return the recordDelimiter
	 */
	public String getRecordDelimiter() {
		return recordDelimiter;
	}

	/**
	 * @param recordDelimiter the recordDelimiter to set
	 */
	public void setRecordDelimiter(String recordDelimiter) {
		this.recordDelimiter = recordDelimiter;
	}

	/**
	 * @return the metadataName
	 */
	public String getMetadataName() {
		return metadataName;
	}

	/**
	 * @param metadataName the metadataName to set
	 */
	public void setMetadataName(String metadataName) {
		this.metadataName = metadataName;
	}

	/**
	 * @return the metadataLabel
	 */
	public String getMetadataLabel() {
		return metadataLabel;
	}

	/**
	 * @param metadataLabel the metadataLabel to set
	 */
	public void setMetadataLabel(String metadataLabel) {
		this.metadataLabel = metadataLabel;
	}
}
