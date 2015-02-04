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
package org.jetel.component.transform;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import org.jetel.data.DataRecord;
import org.jetel.data.formatter.AbstractFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

public class XSLTFormatter extends AbstractFormatter {
	
	private WritableByteChannel writableByteChannel;
	
	@Override
	public void close() throws IOException {
		writableByteChannel.close();
	}
	
	@Override
	public void finish() throws IOException {
		close();
	}
	
	@Override
	public void flush() throws IOException {
	}
	
	@Override
	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
	}
	
	@Override
	public void reset() {
	}
	
	@Override
	public void setDataTarget(Object outputDataTarget) {
		writableByteChannel = (WritableByteChannel)outputDataTarget;
	}
	
	@Override
	public int write(DataRecord record) throws IOException {
		return 0;
	}
	
	@Override
	public int writeFooter() throws IOException {
		return 0;
	}
	
	@Override
	public int writeHeader() throws IOException {
		return 0;
	}

	public WritableByteChannel getWritableByteChannel() {
		return writableByteChannel;
	}
}
