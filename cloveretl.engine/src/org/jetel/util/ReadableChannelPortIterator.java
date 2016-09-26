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

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.DirectoryStream;
import java.util.Collections;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.parser.Parser.DataSourceType;
import org.jetel.enums.ProcessingType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.InputPort;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.stream.Input;
import org.jetel.util.file.stream.Wildcards;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;

/***
 * Supports a port reading.
 * 
 * @author Jan Ausperger, Martin Slama (jan.ausperger@javlin.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */
public class ReadableChannelPortIterator implements Closeable {
	
    private static Log defaultLogger = LogFactory.getLog(ReadableChannelPortIterator.class);
	public static final String DEFAULT_CHARSET = "UTF-8"; //$NON-NLS-1$

	// source attributes
	private InputPort inputPort;
	private DataRecord record;

	// urls and file name resolver
	private String[] portFileURL;
	private URL contextURL;
	private PropertyRefResolver propertyRefResolve;

	// data wrapper and a charset for string->byte array
	private FieldDataWrapper fieldDataWrapper;
	private String charset;
	
	/**
	 * Constructor.
	 * @param inputPort
	 * @param fileURL
	 */
	public ReadableChannelPortIterator(InputPort inputPort, String[] portFileURL) {
		this.inputPort = inputPort;
		this.portFileURL = portFileURL;
	}
	
	/**
	 * Initialization.
	 * @throws ComponentNotReadyException 
	 */
	public void init() throws ComponentNotReadyException {
		if (inputPort == null) return;
		
		if (portFileURL == null || portFileURL.length == 0) {
			throw new ComponentNotReadyException(UtilMessages.
					getString("ReadableChannelPortIterator_no_reference_to_port_found")); //$NON-NLS-1$
		}
		if (portFileURL.length > 1) {
			throw new ComponentNotReadyException(UtilMessages.
					getString("ReadableChannelPortIterator_multiple_fields_not_allowed")); //$NON-NLS-1$
		}


		// set default charset
		if (charset == null) charset = DEFAULT_CHARSET;

		// data record
		record = DataRecordFactory.newRecord(inputPort.getMetadata());

		for (int i=0; i<portFileURL.length; i++) {
			// parse field url
			PortHandler portHandler = null;
			try {
				portHandler = new PortHandler(portFileURL[i], ProcessingType.DISCRETE);
			} catch (MalformedURLException e) {
				throw new ComponentNotReadyException(UtilMessages.getString("ReadableChannelPortIterator_source_string"), e); //$NON-NLS-1$
			}
			
			// prepare data field
			String fName = portHandler.getFieldName();
			if (!record.hasField(fName)) throw new ComponentNotReadyException(UtilMessages.
					getString("ReadableChannelPortIterator_field_not_found") + portFileURL[i] + "'"); //$NON-NLS-1$ //$NON-NLS-2$
			DataField field = record.getField(fName);
			if (field == null) throw new ComponentNotReadyException(UtilMessages.
					getString("ReadableChannelPortIterator_field_not_found") + portFileURL[i] + "'"); //$NON-NLS-1$ //$NON-NLS-2$
			
			// create a data wrapper
			if (portHandler.getProcessingType() == ProcessingType.DISCRETE) {
				fieldDataWrapper = new DiscreteFieldDataWrapper(field, charset, portHandler);
			} else if (portHandler.getProcessingType() == ProcessingType.SOURCE) {
				fieldDataWrapper = new SourceFieldDataWrapper(field, charset, portHandler);
				((SourceFieldDataWrapper)fieldDataWrapper).setContextURL(contextURL);
				((SourceFieldDataWrapper)fieldDataWrapper).setPropertyRefResolver(propertyRefResolve);
			} else if (portHandler.getProcessingType() == ProcessingType.STREAM) {
				fieldDataWrapper = new StreamFieldDataWrapper(field, charset, portHandler, inputPort);
			}
		}
	}

	/**
	 * Returns a channel if the input port has data otherwise null.
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws JetelException
	 */
	public Object getNextData(DataSourceType preferredDataSource) throws IOException, InterruptedException, JetelException {
		
		if ((fieldDataWrapper.getPortHandler().getProcessingType() == ProcessingType.STREAM)
				|| fieldDataWrapper.hasData()) {
			// if processing stream - do not read whole stream before processing starts but process data as they are coming
			// if processing sources, previous record resolved into more than one file
			Object nextData = fieldDataWrapper.getData(preferredDataSource);
			if (fieldDataWrapper instanceof StreamFieldDataWrapper) {
				// CLO-5524 ... when obtaining new stream, we need to save all fields from the first record of the stream
				record = ((StreamFieldDataWrapper) fieldDataWrapper).getFirstRecord();
			}
			return nextData;
		} else {
			//read next record
			record = inputPort.readRecord(record);
			// go through all data records
			while (record != null) {
				// return the first available data stream
				Object data;
				if ((data = fieldDataWrapper.getData(preferredDataSource)) != null) {
					return data;
				}

				// if there is no next available wrapper, read next record
				record = inputPort.readRecord(record);
			}
		}
		
		return null;
	}

	/**
	 * !!!returns!!! 
	 * 		true  - if the source contains data OR if the input port is NOT eof, 
	 * 				then it is necessary to call the method next() that returns null or an input stream. 
	 * 		false - no input data
	 * 
	 * TODO to make hasData method for the InputPort that waits for new data if the edge is empty. Is it good solution???
	 */
	public boolean hasNext() {
		return fieldDataWrapper.hasData() || !inputPort.isEOF();
	}
	
	/**
	 * If an input port is connected but not used.
	 */
	public void blankRead() {
		// empty read
		if (inputPort != null) {
			try {
				while ((record = inputPort.readRecord(record)) != null) {}
			} catch (Exception e) {
				throw new JetelRuntimeException("Blank data records reading failed.", e);
			}
		}
	}

	/**
	 * Sets context url.
	 * @param contextURL
	 */
	public void setContextURL(URL contextURL) {
		this.contextURL = contextURL;
	}
	
	/**
	 * Sets charset for StrindDataField.
	 * @param charset
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}

	/**
	 * Sets Property RefResolve.
	 * @param propertyRefResolve
	 */
	public void setPropertyRefResolver(PropertyRefResolver propertyRefResolve) {
		this.propertyRefResolve = propertyRefResolve;
	}

	/**
	 * Returns current field name.
	 */
	public String getCurrentFileName() {
		return fieldDataWrapper.getCurrentFileName();
	}
	
	/**
	 * @return the record
	 */
	public DataRecord getCurrentRecord() {
		return record;
	}
	
	/**
	 * Return last processed field name.
	 * @return
	 */
	public String getLastFieldName() {
		return fieldDataWrapper.getFieldName();
	}

	/**
	 * Abstract field data wrapper.
	 */
	private abstract static class FieldDataWrapper {
		// data field and charset for Strng data field
		protected DataField field;
		protected String charset;
		protected String currentFileName;
		protected PortHandler portHandler;

		/**
		 * Constructor.
		 * @param field
		 */
		public FieldDataWrapper(DataField field, String charset, PortHandler portHandler) {
			this.field = field;
			this.charset = charset;
			this.portHandler = portHandler;
		}
		
		/**
		 * Returns field name.
		 */
		public String getFieldName() {
			return field.getMetadata().getName();
		}

		/**
		 * Returns current field name.
		 */
		public String getCurrentFileName() {
			return currentFileName;
		}
		

		/**
		 * @return current port handler
		 */
		public PortHandler getPortHandler() {
			return portHandler;
		}
		
		/**
		 * Gets readable or null.
		 * @param preferredDataSource
		 * @return
		 * @throws JetelException
		 * @throws IOException 
		 */
		public abstract Object getData(DataSourceType preferredDataSource) throws IOException, JetelException;
		
		/**
		 * Returns <code>true</code> if the wrapper
		 * can provide another ReadableByteChannel
		 * without reading from the input port.
		 * 
		 * Applies in {@link SourceFieldDataWrapper}
		 * when the previous record contained a wildcard that
		 * has been resolved to more than one source.
		 * 
		 * @return
		 */
		public boolean hasData() {
			return false;
		}
	}
	
	/**
	 * Discrete field data wrapper.
	 */
	private static class DiscreteFieldDataWrapper extends FieldDataWrapper {

		/**
		 * Constructor.
		 * @param field
		 * @param charset
		 */
		public DiscreteFieldDataWrapper(DataField field, String charset, PortHandler portHandler) {
			super(field, charset, portHandler);
			currentFileName = UtilMessages.getString("ReadableChannelPortIterator_reading_field") + 
				field.getMetadata().getName() + "'"; //$NON-NLS-1$ //$NON-NLS-2$
		}

		@Override
		public ReadableByteChannel getData(DataSourceType preferredDataSource) throws IOException, JetelException {
			return createReadableByteChannel(field.getValue());
		}
		
		/**
		 * Creates readable channel from String value of oValue.toString().
		 * 
		 * @param oValue
		 * @return
		 * @throws UnsupportedEncodingException 
		 */
		private ReadableByteChannel createReadableByteChannel(Object oValue) throws UnsupportedEncodingException {
			if (oValue == null) throw new NullPointerException(UtilMessages.
					getString("ReadableChannelPortIterator_unsupported_null")); //$NON-NLS-1$
			ByteArrayInputStream str = oValue instanceof byte[] ? 
					new ByteArrayInputStream((byte[])oValue) : 
						new ByteArrayInputStream(oValue.toString().getBytes(charset));
			return Channels.newChannel(str);
		}
	}

	/**
	 * Source field data wrapper.
	 */
	private static class SourceFieldDataWrapper extends FieldDataWrapper implements Closeable {
		// property resolver
		private PropertyRefResolver propertyRefResolver;
		private URL contextURL;
		private Iterator<Input> iterator = Collections.emptyIterator();
		private DirectoryStream<Input> directoryStream;

		/**
		 * Constructor.
		 * @param field
		 * @param charset
		 */
		public SourceFieldDataWrapper(DataField field, String charset, PortHandler portHandler) {
			super(field, charset, portHandler);
		}

		/**
		 * Sets Property RefResolve.
		 * @param propertyRefResolve
		 */
		public void setPropertyRefResolver(PropertyRefResolver propertyRefResolve) {
			this.propertyRefResolver = propertyRefResolve;
		}
		
		/**
		 * Sets context url.
		 * @param contextURL
		 */
		public void setContextURL(URL contextURL) {
			this.contextURL = contextURL;
		}

		@Override
		public Object getData(DataSourceType preferredDataSource) throws UnsupportedEncodingException, JetelException {
			// urls processing
			if (!hasData()) {
				try {
					// close the previous directory stream
					FileUtils.close(directoryStream);
				} catch (IOException ex) {
					defaultLogger.error("Failed to release resources", ex);
				}
				String pattern = field.getValue().toString();
				if (propertyRefResolver != null) {
					pattern = propertyRefResolver.resolveRef(pattern, RefResFlag.SPEC_CHARACTERS_OFF);
				}
				
				this.directoryStream = Wildcards.newDirectoryStream(contextURL, pattern);
				Iterator<Input> it = directoryStream.iterator();
				if (!it.hasNext()) {
					return null;
				}
				iterator = it;
			}
			
			Input input = iterator.next();
			currentFileName = ReadableChannelIterator.unificateFileName(contextURL, input.getAbsolutePath());
			
			return getPreferredDataSource(input, preferredDataSource, currentFileName);
		}
		
		@Override
		public boolean hasData() {
			return iterator.hasNext();
		}
		
		/**
		 * Tries to return data source of the preferred type.
		 * Falls back to {@link ReadableByteChannel} if the attempt fails.
		 * 
		 * @param input
		 * @param preferredDataSource
		 * @param fileName
		 * 
		 * @return datasource of the preferred type or {@link ReadableByteChannel}
		 * @throws JetelException
		 */
		private Object getPreferredDataSource(Input input, DataSourceType preferredDataSource, String fileName) throws JetelException {
			defaultLogger.debug(UtilMessages.getString("ReadableChannelPortIterator_opening_input") + fileName); //$NON-NLS-1$
			try {
				Object dataSource = input.getPreferredInput(preferredDataSource);
				if (dataSource != null) {
					return dataSource;
				}
				dataSource = input.getPreferredInput(DataSourceType.CHANNEL);
				defaultLogger.debug(UtilMessages.getString("ReadableChannelPortIterator_reading_input") + fileName); //$NON-NLS-1$
				return dataSource;
			} catch (IOException e) {
				throw new JetelException(UtilMessages.
						getString("ReadableChannelPortIterator_file_unreachable") + fileName, e); //$NON-NLS-1$
			}
		}

		@Override
		public void close() throws IOException {
			FileUtils.close(directoryStream);
		}
		
	}

	/**
	 * Stream field data wrapper.
	 */
	private static class StreamFieldDataWrapper extends FieldDataWrapper {
		
		private InputPort inputPort;
		
		// first record of the stream
		private DataRecord firstRecord;
		
		public StreamFieldDataWrapper(DataField field, String charset, PortHandler portHandler, InputPort inputPort) {
			super(field, charset, portHandler);
			this.inputPort = inputPort;
			currentFileName = UtilMessages.getString("ReadableChannelPortIterator_reading_from_file") + 
				field.getMetadata().getName() + "'"; //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		@Override
		public ReadableByteChannel getData(DataSourceType preferredDataSource) throws IOException, JetelException {
			InputPortReadableChannel channel = new InputPortReadableChannel(inputPort, field.getMetadata().getName(), charset);
			if (!channel.isEOF()) {
				firstRecord = channel.getCurrentRecord();
				return channel;
			}
			//all records were read
			return null;
		}
		
		/**
		 * See {@link InputPortReadableChannel#getCurrentRecord()}
		 * @return
		 */
		public DataRecord getFirstRecord() {
			return firstRecord;
		}
		
	}

	/**
	 * Field url parser.
	 */
	public static class PortHandler {

		private static final String PARAM_DELIMITER = ":"; //$NON-NLS-1$
		private static final String PORT_DELIMITER = "\\."; //$NON-NLS-1$
		private static final String PORT = "port"; //$NON-NLS-1$
		
		private String portName;
		private String fieldName;
		private ProcessingType processingType;

		public PortHandler(String resource, ProcessingType defaultProcessingType) throws MalformedURLException {
			if (resource == null) {
				throw new MalformedURLException(resource);
			}
			String[] elements = resource.split(PARAM_DELIMITER);
			if (elements.length < 2) {
				throw new MalformedURLException(resource);
			}
			if (!elements[0].equals(PORT)) {
				throw new MalformedURLException(resource);
			}
			String[] fieldNamePort = elements[1].split(PORT_DELIMITER);
			if (fieldNamePort.length < 2) {
				throw new MalformedURLException(resource);
			}
			portName = fieldNamePort[0].replace("$", ""); //$NON-NLS-1$ //$NON-NLS-2$
			fieldName = fieldNamePort[1];
			processingType = ProcessingType.DISCRETE;
			if (elements.length > 2) {
				processingType = ProcessingType.fromString(elements[2], defaultProcessingType);
			}
		}
		
	    public String getFieldName() {
	    	return fieldName;
	    }

	    public String getPort() {
	    	return portName;
	    }

	    public ProcessingType getProcessingType() {
	    	return processingType;
	    }
	}

	@Override
	public void close() throws IOException {
		if (fieldDataWrapper instanceof Closeable) {
			FileUtils.close((Closeable) fieldDataWrapper);
		}
	}
}
