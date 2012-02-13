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
import java.util.List;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26 Aug 2011
 */
interface SpreadsheetStreamHandler {
	
	public void init() throws ComponentNotReadyException;
	
	public void close() throws IOException;
		
	public List<String> getSheetNames() throws IOException;
	
	public int getCurrentRecordStartRow();
	
	public String[][] getHeader(int startRow, int startColumn, int endRow, int endColumn) throws ComponentNotReadyException;

	public void prepareInput(Object inputSource) throws IOException, ComponentNotReadyException;

	public boolean setCurrentSheet(int sheetNumber);

	public DataRecord parseNext(DataRecord record) throws JetelException;
	
	public int skip(int nRec) throws JetelException;

}
