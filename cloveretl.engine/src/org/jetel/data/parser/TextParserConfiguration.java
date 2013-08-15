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
public class TextParserConfiguration implements Cloneable {

	private DataRecordMetadata metadata = null;
	private String charset = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
	private boolean verbose = true;
	private boolean treatMultipleDelimitersAsOne = false;
	private boolean quotedStringsOverride = false;
	private boolean quotedStrings = false;
	private Character quoteChar = null;
	private Boolean trim = null;
	private Boolean skipLeadingBlanks = null;
	private Boolean skipTrailingBlanks = null;
	private boolean skipRows = false;
	private PolicyType policyType = null;
	private IParserExceptionHandler exceptionHandler = null;
	/** Indicates, whether the parser should try to find longer delimiter when a match is found. This
	 *  applies for e.g. delimiter set \r | \r\n. When this flag is false and a \r is found, parser
	 *  should take \r as a delimiter. If the flag is true, parser should look if the next char is \n and 
	 *  if so, take \r\n as delimiter. 
	 */
	private boolean tryToMatchLongerDelimiter = false;
	
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

	public TextParserConfiguration(DataRecordMetadata metadata, String charset, boolean verbose) {
		super();
		this.metadata = metadata;
		if (charset != null) {
			this.charset = charset;
		}
		this.verbose = verbose;
	}

	public TextParserConfiguration(String charset, boolean verbose, DataRecordMetadata metadata,
			boolean treatMultipleDelimitersAsOne, boolean quotedStringsOverride, boolean quotedStrings, Character quoteChar, Boolean skipLeadingBlanks,
			Boolean skipTrailingBlanks, Boolean trim, boolean incremental, 
			PolicyType policyType) {
		super();
		this.metadata = metadata;
		if (charset != null) {
			this.charset = charset;
		}
		this.verbose = verbose;
		this.treatMultipleDelimitersAsOne = treatMultipleDelimitersAsOne;
		this.quotedStringsOverride = quotedStringsOverride;
		this.quotedStrings = quotedStrings;
		this.quoteChar = quoteChar;
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
	 * @return the quotedStringsOverride
	 */
	public boolean isQuotedStringsOverride() {
		return quotedStringsOverride;
	}
	
	/**
	 * @return the quotedStrings
	 */
	public boolean isQuotedStrings() {
		return quotedStrings;
	}
	
	/**
	 * @param quotedStringsOverride the quotedStringsOverride to set
	 */
	public void setQuotedStringsOverride(boolean quotedStringsOverride) {
		this.quotedStringsOverride = quotedStringsOverride;
	}

	/**
	 * @param quotedStrings
	 *            the quotedStrings to set
	 */
	public void setQuotedStrings(boolean quotedStrings) {
		this.quotedStrings = quotedStrings;
	}

	
	public Character getQuoteChar() {
		return this.quoteChar;
	}
	
	
	public void setQuoteChar(Character quoteChar) {
		this.quoteChar = quoteChar;
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
		if (this.policyType == null) {
			exceptionHandler = null;
		} else {
			exceptionHandler = ParserExceptionHandlerFactory.getHandler(policyType);
		}
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

	public static boolean isSingleByteCharset(Charset charset) {
		return 1 == Math.round(charset.newEncoder().maxBytesPerChar());
	}

	public static boolean isSingleByteCharset(String charset) {
		return isSingleByteCharset(Charset.forName(charset));
	}

	public boolean isSingleByteCharset() {
		return isSingleByteCharset(charset);
	}
	
	/**  Indicates, whether the parser should try to find longer delimiter when a match is found. This
	 *  applies for e.g. delimiter set \r | \r\n. When this flag is false and a \r is found, parser
	 *  should take \r as a delimiter. If the flag is true, parser should look if the next char is \n and 
	 *  if so, take \r\n as delimiter. 
	 * 
	 * @return the tryToMatchLongerDelimiter
	 */
	public boolean isTryToMatchLongerDelimiter() {
		return tryToMatchLongerDelimiter;
	}

	/** Sets the flag indicating, whether the parser should try to find longer delimiter when a match is found. This
	 *  applies for e.g. delimiter set \r | \r\n. When this flag is false and a \r is found, parser
	 *  should take \r as a delimiter. If the flag is true, parser should look if the next char is \n and 
	 *  if so, take \r\n as delimiter. 
	 * 
	 * @param tryToMatchLongerDelimiter the tryToMatchLongerDelimiter to set
	 */
	public void setTryToMatchLongerDelimiter(boolean tryToMatchLongerDelimiter) {
		this.tryToMatchLongerDelimiter = tryToMatchLongerDelimiter;
	}

	@Override
	public String toString() {
		return "ParserConfiguration [charset=" + charset + ", verbose=" + verbose + ", metadata=" + metadata + ", treatMultipleDelimitersAsOne=" + treatMultipleDelimitersAsOne + ", quotedStrings=" + quotedStrings + ", skipLeadingBlanks=" + skipLeadingBlanks + ", skipTrailingBlanks=" + skipTrailingBlanks + ", trim=" + trim + ", tryToMatchLongerDelimiter=" + tryToMatchLongerDelimiter + "]";
	}

	public TextParserConfiguration(TextParserConfiguration cfg) {
		super();
		if (cfg == null) {
			return;
		}
		if (cfg.metadata == null) {
			this.metadata = null;
		} else {
			this.metadata = cfg.metadata.duplicate();
		}
		
		this.charset = cfg.charset;
		this.verbose = cfg.verbose;
		this.treatMultipleDelimitersAsOne = cfg.treatMultipleDelimitersAsOne;
		this.quotedStrings = cfg.quotedStrings;
		this.quoteChar = cfg.quoteChar;
		this.trim = cfg.trim;
		this.skipLeadingBlanks = cfg.skipLeadingBlanks;
		this.skipTrailingBlanks = cfg.skipTrailingBlanks;
		this.skipRows = cfg.skipRows;
		this.setPolicyType(cfg.policyType);
	}

}
