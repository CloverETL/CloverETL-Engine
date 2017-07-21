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
import java.net.MalformedURLException;

import org.jetel.data.parser.Parser.DataSourceType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.test.CloverTestCase;
import org.jetel.util.file.FileUtils;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25 Jan 2012
 */
public class ReadableChannelIteratorTest extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		initEngine();
	}
	
	public void testFileSourcePreferred() throws JetelException, ComponentNotReadyException, MalformedURLException {
		ReadableChannelIterator sourceIterator = new ReadableChannelIterator(null, FileUtils.getFileURL("."), "neco/neco.txt");
		sourceIterator.setPreferredDataSourceType(DataSourceType.FILE);
		sourceIterator.init();
		assertTrue(sourceIterator.next() instanceof File);
	}

	public void testNextChannel() throws JetelException, ComponentNotReadyException, MalformedURLException {
		ReadableChannelIterator sourceIterator = new ReadableChannelIterator(null, FileUtils.getFileURL("."), "neco/neco.txt");
		sourceIterator.setPreferredDataSourceType(DataSourceType.FILE);
		sourceIterator.init();
		assertNull(sourceIterator.nextChannel());
	}

}
