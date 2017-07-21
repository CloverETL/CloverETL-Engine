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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import org.apache.log4j.Logger;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.Node;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.graph.modelview.impl.MetadataPropagationResolver;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CloverPublicAPI;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;

/**
 * The only abstract implementation of {@link GenericTransform} interface.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 * @author salamonp (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20. 11. 2014
 */
@CloverPublicAPI
public abstract class AbstractGenericTransform extends AbstractDataTransform implements GenericTransform {
	
	protected DataRecord[] inRecords;
	protected DataRecord[] outRecords;
	
	private TypedProperties properties = null;
	
	private void initRecords() {
		DataRecordMetadata[] inMeta = getComponent().getInMetadataArray();
		inRecords = new DataRecord[inMeta.length];
		for (int i = 0; i < inRecords.length; i++) {
			if (inMeta[i] != null) {
				inRecords[i] = DataRecordFactory.newRecord(inMeta[i]);
			}
		}

		DataRecordMetadata[] outMeta = getComponent().getOutMetadataArray();
		outRecords = new DataRecord[outMeta.length];
		for (int i = 0; i < outRecords.length; i++) {
			if (outMeta[i] != null) {
				outRecords[i] = DataRecordFactory.newRecord(outMeta[i]);
			}
		}
	}
	
	/**
	 * @return Component object associated with the component.
	 */
	protected Node getComponent() {
		return getNode();
	}
	
	/**
	 * @return Configuration properties of the component (including custom properties).
	 */
	protected TypedProperties getProperties() {
		if (properties == null) {
			properties = new TypedProperties(getNode().getAttributes(), getGraph());
		}
		return properties;
	}
	
	/**
	 * @return Dedicated logger for this component.
	 */
	protected Logger getLogger() {
		return getNode().getLog();
	}
	
	/**
	 * Reads a record from specified input port.
	 * 
	 * DataRecord objects returned by this method are re-used when this method is called repeatedly.
	 * If you need to hold data from input DataRecords between multiple calls, use {@link DataRecord#duplicate()}
	 * on records returned by this method or save the data elsewhere.
	 * 
	 * @param portIdx Index of port to read from
	 * @return <code>null</code> if there are no more records; DataRecord otherwise
	 */
	protected DataRecord readRecordFromPort(int portIdx) {
		try {
			return getComponent().readRecord(portIdx, inRecords[portIdx]);
		} catch (IOException | InterruptedException e) {
			throw new JetelRuntimeException(e);
		}
	}
	
	/**
	 * Writes a record to specified output port.
	 * 
	 * @param portIdx Index of port to write to
	 * @param record The record to write
	 */
	protected void writeRecordToPort(int portIdx, DataRecord record) {
		try {
			getComponent().writeRecord(portIdx, record);
		} catch (IOException | InterruptedException e) {
			throw new JetelRuntimeException(e);
		}
	}
	
	/**
	 * Returns {@link File} for given FileURL.
	 * @param fileUrl e.g. "data-in/myInput.txt"
	 * @return {@link File} instance
	 */
	protected File getFile(String fileUrl) {
		URL contextURL = getComponent().getGraph().getRuntimeContext().getContextURL();
		File file = FileUtils.getJavaFile(contextURL, fileUrl);
		return file;
	}
	
	/**
	 * Returns {@link InputStream} for given FileURL. Caller is responsible for closing the stream.
	 * @param fileUrl e.g. "data-in/myInput.txt"
	 * @throws IOException
	 */
	protected InputStream getInputStream(String fileUrl) throws IOException {
		URL contextURL = getComponent().getGraph().getRuntimeContext().getContextURL();
		InputStream is = FileUtils.getInputStream(contextURL, fileUrl);
		return is;
	}
	
	/**
	 * Returns {@link OutputStream} for given FileURL. Caller is responsible for closing the stream.
	 * @param fileUrl e.g. "data-in/myInput.txt"
	 * @param append - If true, writing will append data to the end of the stream. This may not work for all protocols.
	 * @throws IOException
	 */
	protected OutputStream getOutputStream(String fileUrl, boolean append) throws IOException {
		URL contextURL = getComponent().getGraph().getRuntimeContext().getContextURL();
		OutputStream os = FileUtils.getOutputStream(contextURL, fileUrl, append, -1);
		return os;
	}
	
	/**
	 * {@inheritDoc}<br>
	 * If you override this method, ALWAYS call <code>super.init()</code>
	 */
	@Override
	public void init() {
		initRecords();
	}
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		return status;
	}

	@Override
	public abstract void execute() throws Exception;
	
	@Override
	public void executeOnError(Exception e) {
		throw new JetelRuntimeException("Execute failed!", e);
	}
	
	/**
     * @see org.jetel.graph.IGraphElement#free()
     */
	@Override
	public void free() {
		// do nothing by default
	}
	
}
