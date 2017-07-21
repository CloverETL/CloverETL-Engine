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
package org.jetel.util;

import java.io.File;
import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.test.CloverTestCase;

/**
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jan 26, 2012
 */
public class MultiFileReaderTest extends CloverTestCase {
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}
	
	/** Tests whether parser indicating File as preferred source really gets File 
	 * (at least I guess there's no reason why it shouldn't get the File).
	 */
	public void testParserFileSource() throws Exception {
		MultiFileReader reader = new MultiFileReader(new FileExpectingDummyParser(), new File(".").toURI().toURL(), "data/data.dat");
		reader.init(null); // here parser checks if it got the File
	}
	
	private static class FileExpectingDummyParser implements Parser {

		@Override
		public DataRecord getNext() throws JetelException {
			return null;
		}

		@Override
		public int skip(int nRec) throws JetelException {
			return 0;
		}

		@Override
		public void init() throws ComponentNotReadyException {
		}
		
		@Override
		public void setDataSource(Object inputDataSource) throws IOException, ComponentNotReadyException {
			assertTrue("Input data source is not File", inputDataSource instanceof File);
		}

		@Override
		public void setReleaseDataSource(boolean releaseInputSource) {
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public DataRecord getNext(DataRecord record) throws JetelException {
			return null;
		}

		@Override
		public void setExceptionHandler(IParserExceptionHandler handler) {
		}

		@Override
		public IParserExceptionHandler getExceptionHandler() {
			return null;
		}

		@Override
		public PolicyType getPolicyType() {
			return null;
		}

		@Override
		public void reset() throws ComponentNotReadyException {
		}

		@Override
		public Object getPosition() {
			return null;
		}

		@Override
		public void movePosition(Object position) throws IOException {
		}

		@Override
		public void preExecute() throws ComponentNotReadyException {
		}

		@Override
		public void postExecute() throws ComponentNotReadyException {
		}

		@Override
		public void free() throws ComponentNotReadyException, IOException {
		}

		@Override
		public boolean nextL3Source() {
			return false;
		}

		@Override
		public DataSourceType getPreferredDataSourceType() {
			return DataSourceType.CHANNEL;
		}
		
	}
}
