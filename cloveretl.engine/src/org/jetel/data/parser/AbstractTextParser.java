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
	
}
