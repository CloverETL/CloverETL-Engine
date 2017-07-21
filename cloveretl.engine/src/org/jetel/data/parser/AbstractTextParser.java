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
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;

/**
 * Abstract class for all TextParsers. It contains some useful common methods.
 * 
 * @author csochor (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Dec 10, 2010
 */
public abstract class AbstractTextParser extends AbstractParser implements TextParser {
	
	@Override
	public DataRecord getNext() throws JetelException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int skip(int nRec) throws JetelException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DataRecord getNext(DataRecord record) throws JetelException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setExceptionHandler(IParserExceptionHandler handler) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public IParserExceptionHandler getExceptionHandler() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PolicyType getPolicyType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() throws ComponentNotReadyException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object getPosition() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void movePosition(Object position) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void free() throws ComponentNotReadyException, IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean nextL3Source() {
		return false;
	}

	final protected TextParserConfiguration cfg;

	public AbstractTextParser(TextParserConfiguration cfg) {
		super();
		this.cfg = cfg;
	}
	
	@Override
	public TextParserConfiguration getConfiguration() {
		return cfg;
	}
	
	protected boolean isQuotedStrings() {
		return cfg.isQuotedStringsOverride() ? cfg.isQuotedStrings() : cfg.getMetadata().isQuotedStrings();
	}
	
	protected Character getQuoteChar() {
		return cfg.isQuotedStringsOverride() ? cfg.getQuoteChar() : cfg.getMetadata().getQuoteChar();
	}
	
	protected boolean isSkipFieldTrailingBlanks(int fieldIndex) {
		if (cfg.getSkipTrailingBlanks() != null) {
			return cfg.getSkipTrailingBlanks();
		} else if (cfg.getTrim() != null) {
			return cfg.getTrim();
		} else {
			return cfg.getMetadata().getField(fieldIndex).isSkipTrailingBlanks();
		}
	}

	protected boolean isSkipFieldLeadingBlanks(int fieldIndex) {
		if (cfg.getSkipLeadingBlanks() != null) {
			return cfg.getSkipLeadingBlanks();
		} else if (cfg.getTrim() != null) {
			return cfg.getTrim();
		} else {
			return cfg.getMetadata().getField(fieldIndex).isSkipLeadingBlanks();
		}
	}
	
	/**
	 * Creates charset decoder based on charset from parser configuration object.
	 * Moreover, parser policy is applied even on the charset decoder.
	 * @return
	 */
	protected CharsetDecoder createCharsetDecoder() {
		CharsetDecoder decoder = Charset.forName(cfg.getCharset()).newDecoder();

		applyDecoderPolicy(decoder, getPolicyType());
		
		return decoder;
	}
	
	/**
	 * Presets the given decoder according given policy. For lenient policy, malformed and
	 * unmappable characters are replaced. For controlled and strict policy, an exception
	 * is thrown on malformed and unmappable characters.
	 * @param decoder
	 * @param policyType
	 */
	public static void applyDecoderPolicy(CharsetDecoder decoder, PolicyType policyType) {
		if (policyType == null) {
			policyType = PolicyType.STRICT;
		}
		
		switch (policyType) {
		case LENIENT:
			setLenientDecoder(decoder);
			break;
		case CONTROLLED:
			setStrictDecoder(decoder);
			break;
		case STRICT:
			setStrictDecoder(decoder);
			break;
		}
	}
	
	/**
	 * Sets the given decoder to replace all malformed and unmappable characters.
	 * @param decoder
	 */
	protected static void setLenientDecoder(CharsetDecoder decoder) {
		decoder.onMalformedInput(CodingErrorAction.REPLACE);
		decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
	}

	/**
	 * Sets the given decoder to throw an exception on malformed and unmappable characters.
	 * @param decoder
	 */
	protected static void setStrictDecoder(CharsetDecoder decoder) {
		decoder.onMalformedInput(CodingErrorAction.REPORT);
		decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
	}

	/**
	 * Character decoding error occurred.
	 */
	public static class CharsetDecoderException extends IOException {
		private static final long serialVersionUID = 1L;
		
		public CharsetDecoderException(String message) {
			super(message);
		}

		public CharsetDecoderException(String message, Throwable cause) {
			super(message, cause);
		}
	}

}
