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
package org.jetel.component;

import java.io.IOException;
import java.util.Properties;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.Node;
import org.jetel.metadata.DataRecordMetadata;

/**
 * The only abstract implementation of {@link GenericTransform} interface.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20. 11. 2014
 */
public abstract class AbstractGenericTransform extends AbstractDataTransform implements GenericTransform {
	
	protected Properties additionalProperties;
	
	protected DataRecord[] inRecords;
	protected DataRecord[] outRecords;
	protected Node component;
	
	private void initRecords() {
		DataRecordMetadata[] inMeta = component.getInMetadataArray();
		inRecords = new DataRecord[inMeta.length];
		for (int i = 0; i < inRecords.length; i++) {
			inRecords[i] = DataRecordFactory.newRecord(inMeta[i]);
			inRecords[i].init();
		}
		
		DataRecordMetadata[] outMeta = component.getOutMetadataArray();
		outRecords = new DataRecord[outMeta.length];
		for (int i = 0; i < outRecords.length; i++) {
			outRecords[i] = DataRecordFactory.newRecord(outMeta[i]);
			outRecords[i].init();
		}
	}
	
	/**
	 * DataRecord objects returned by this method are re-used when this method is called repeatedly. If you need to hold data from input DataRecords
	 * between multiple calls, use {@link DataRecord#duplicate} on objects returned by this method or save the data elsewhere.
	 * 
	 * @param portIdx index of port to read from
	 * @return null if there are no more records
	 * @throws IOException
	 * @throws InterruptedException
	 */
	protected DataRecord readRecordFromPort(int portIdx) throws IOException, InterruptedException {
		return component.readRecord(portIdx, inRecords[portIdx]);
	}
	
	protected void writeRecordToPort(int portIdx, DataRecord record) throws IOException, InterruptedException {
		component.writeRecord(portIdx, record);
	}
	
	/**
	 * If you override this method, ALWAYS call super.init()
	 */
	@Override
	public void init(Properties properties) {
		additionalProperties = properties;
		component = getNode();
		initRecords();
	}

	@Override
	public abstract void execute();
	
	@Override
	public void executeOnError(Exception e) {
		throw new JetelRuntimeException("Transform failed!", e);
	}
	
	@Override
	public void free() {
		// do nothing by default
	}
	
}
