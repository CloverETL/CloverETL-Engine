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

import java.nio.charset.Charset;

import org.jetel.data.Defaults;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author csochor (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Dec 6, 2010
 */
public class TextParserConfiguration {

	private DataRecordMetadata metadata = null;
	private String charset = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
	private boolean verbose = true;
	private boolean treatMultipleDelimitersAsOne = false;
	private boolean quotedStrings = false;
	private Boolean trim = null;
	private Boolean skipLeadingBlanks = null;
	private Boolean skipTrailingBlanks = null;
	private boolean skipRows = false;
	private PolicyType policyType = null;
	private IParserExceptionHandler exceptionHandler = null;

	public TextParserConfiguration() {
		super();
	}

	public TextParserConfiguration(DataRecordMetadata metadata) {
		super();
		this.metadata = metadata;
	}

	public TextParserConfiguration(DataRecordMetadata metadata, String charset) {
		super();
		this.metadata = metadata;
		if (charset != null) {
			this.charset = charset;
		}
	}

	public TextParserConfiguration(String charset, boolean verbose, DataRecordMetadata metadata,
			boolean treatMultipleDelimitersAsOne, boolean quotedStrings, Boolean skipLeadingBlanks,
			Boolean skipTrailingBlanks, Boolean trim, boolean incremental, 
			PolicyType policyType) {
		super();
		this.metadata = metadata;
		if (charset != null) {
			this.charset = charset;
		}
		this.verbose = verbose;
		this.treatMultipleDelimitersAsOne = treatMultipleDelimitersAsOne;
		this.quotedStrings = quotedStrings;
		this.skipLeadingBlanks = skipLeadingBlanks;
		this.skipTrailingBlanks = skipTrailingBlanks;
		this.trim = trim;
		this.skipRows = incremental;
		setPolicyType(policyType);
	}

	/**
	 * @return the charset
	 */
	public String getCharset() {
		return charset;
	}

	/**
	 * @param charset
	 *            the charset to set
	 */
	public void setCharset(String charset) {
		if (charset != null) {
			this.charset = charset;
		} else {
			this.charset = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
		}
	}

	/**
	 * @return the verbose
	 */
	public boolean isVerbose() {
		return verbose;
	}

	/**
	 * @param verbose
	 *            the verbose to set
	 */
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	/**
	 * @return the treatMultipleDelimitersAsOne
	 */
	public boolean isTreatMultipleDelimitersAsOne() {
		return treatMultipleDelimitersAsOne;
	}

	/**
	 * @param treatMultipleDelimitersAsOne
	 *            the treatMultipleDelimitersAsOne to set
	 */
	public void setTreatMultipleDelimitersAsOne(boolean treatMultipleDelimitersAsOne) {
		this.treatMultipleDelimitersAsOne = treatMultipleDelimitersAsOne;
	}

	/**
	 * @return the quotedStrings
	 */
	public boolean isQuotedStrings() {
		return quotedStrings;
	}

	/**
	 * @param quotedStrings
	 *            the quotedStrings to set
	 */
	public void setQuotedStrings(boolean quotedStrings) {
		this.quotedStrings = quotedStrings;
	}

	/**
	 * @return the skipLeadingBlanks
	 */
	public Boolean getSkipLeadingBlanks() {
		return skipLeadingBlanks;
	}

	/**
	 * @param skipLeadingBlanks
	 *            the skipLeadingBlanks to set
	 */
	public void setSkipLeadingBlanks(Boolean skipLeadingBlanks) {
		this.skipLeadingBlanks = skipLeadingBlanks;
	}

	/**
	 * @return the skipTrailingBlanks
	 */
	public Boolean getSkipTrailingBlanks() {
		return skipTrailingBlanks;
	}

	/**
	 * Specifies whether leading blanks at each field should be skipped
	 * 
	 * @param skippingLeadingBlanks
	 *            The skippingLeadingBlanks to set.
	 */
	public void setSkipTrailingBlanks(Boolean skipTrailingBlanks) {
		this.skipTrailingBlanks = skipTrailingBlanks;
	}

	/**
	 * @return the trim
	 */
	public Boolean getTrim() {
		return trim;
	}

	/**
	 * @param trim
	 *            the trim to set
	 */
	public void setTrim(Boolean trim) {
		this.trim = trim;
	}

	/**
	 * @return the metadata
	 */
	public DataRecordMetadata getMetadata() {
		return metadata;
	}

	/**
	 * @param metadata
	 *            the metadata to set
	 */
	public void setMetadata(DataRecordMetadata metadata) {
		this.metadata = metadata;
	}

	/**
	 * @return the skipRows
	 */
	public boolean isSkipRows() {
		return skipRows;
	}

	/**
	 * @param skipRows the skipRows to set
	 */
	public void setSkipRows(boolean skipRows) {
		this.skipRows = skipRows;
	}

	/**
	 * Sets data policy and the corresponding exception handler
	 * @param policyType
	 */
	public void setPolicyType(PolicyType policyType) {
		this.policyType = policyType;
		exceptionHandler = ParserExceptionHandlerFactory.getHandler(policyType);
	}

	/**
	 * Nomen omen
	 * @return policyType
	 */
	public PolicyType getPolicyType() {
		return policyType;
	}
	
	/**
	 * Nomen omen 
	 * @return
	 */
	public IParserExceptionHandler getExceptionHandler() {
		return exceptionHandler;
	}
	
	public boolean isSingleByteCharset() {
		return 1 == Math.round(Charset.forName(charset).newEncoder().maxBytesPerChar());
	}

	@Override
	public String toString() {
		return "ParserConfiguration [charset=" + charset + ", verbose=" + verbose + ", metadata=" + metadata + ", treatMultipleDelimitersAsOne=" + treatMultipleDelimitersAsOne + ", quotedStrings=" + quotedStrings + ", skipLeadingBlanks=" + skipLeadingBlanks + ", skipTrailingBlanks=" + skipTrailingBlanks + ", trim=" + trim + "]";
	}

}
