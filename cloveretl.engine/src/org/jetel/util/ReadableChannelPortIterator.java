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
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.primitive.ByteArray;
import org.jetel.enums.ProcessingType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.InputPort;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.PropertyRefResolver;

/***
 * Supports a port reading.
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */
public class ReadableChannelPortIterator {
	
    private static Log defaultLogger = LogFactory.getLog(ReadableChannelPortIterator.class);
	public static final String DEFAULT_CHARSET = "UTF-8";

	// source attributes
	private InputPort inputPort;
	private DataRecord record;

	// urls and file name resolver
	private String[] portFileURL;
	private URL contextURL;
	private PropertyRefResolver propertyRefResolve;

	// data wrapper and a charset for string->byte array
	private FieldDataWrapper[] fieldDataWrapper;
	private String charset;
	
	// pointer to current wrapper and current file name
	private int currentWrapper = Integer.MAX_VALUE;
	private String currentFileName;
	private String lastFieldName;
	
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
		
		// set default charset
		if (charset == null) charset = DEFAULT_CHARSET;

		// data record
		record = DataRecordFactory.newRecord(inputPort.getMetadata());
		record.init();

		// create field data wrappers - array of discrete, stream, source data wrappers
		fieldDataWrapper = new FieldDataWrapper[portFileURL.length];
		for (int i=0; i<portFileURL.length; i++) {
			// parse field url
			PortHandler portHandler = null;
			try {
				portHandler = new PortHandler(portFileURL[i], ProcessingType.DISCRETE);
			} catch (MalformedURLException e) {
				throw new ComponentNotReadyException("The source string '" + e.getMessage() + "' is not valid.");
			}
			
			// prepare data field
			String fName = portHandler.getFieldName();
			if (!record.hasField(fName)) throw new ComponentNotReadyException("The field not found for the statement: '" + portFileURL[i] + "'");
			DataField field = record.getField(fName);
			if (field == null) throw new ComponentNotReadyException("The field not found for the statement: '" + portFileURL[i] + "'");
			
			// create a data wrapper
			if (portHandler.getProcessingType() == ProcessingType.DISCRETE) {
				fieldDataWrapper[i] = new DiscreteFieldDataWrapper(field, charset);
			} else if (portHandler.getProcessingType() == ProcessingType.SOURCE) {
				fieldDataWrapper[i] = new SourceFieldDataWrapper(field, charset);
				((SourceFieldDataWrapper)fieldDataWrapper[i]).setContextURL(contextURL);
				((SourceFieldDataWrapper)fieldDataWrapper[i]).setPropertyRefResolver(propertyRefResolve);
			} else if (portHandler.getProcessingType() == ProcessingType.STREAM) {
				fieldDataWrapper[i] = new StreamFieldDataWrapper(field, charset);
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
	public ReadableByteChannel getNextData() throws IOException, InterruptedException, JetelException {
		// if there is no next available wrapper, read next record
		if (currentWrapper >= fieldDataWrapper.length) {
			record = inputPort.readRecord(record);
			currentWrapper = 0;
		}
		
		// go through all data records
		while (record != null) {
			// return the first available data stream
			ReadableByteChannel data;
			for (;currentWrapper<fieldDataWrapper.length; currentWrapper++) {
				if ((data = fieldDataWrapper[currentWrapper].getData()) != null) {
					currentFileName = fieldDataWrapper[currentWrapper].getCurrentFileName();
					lastFieldName = fieldDataWrapper[currentWrapper].getFieldName();
					currentWrapper++;
					return data;
				}
			}
			
			// if there is no next available wrapper, read next record
			record = inputPort.readRecord(record);
			currentWrapper = 0;
		}
		
		// the field of the last record doesn't need to have null value
		for (;currentWrapper<fieldDataWrapper.length; currentWrapper++) {
			if (fieldDataWrapper[currentWrapper] instanceof StreamFieldDataWrapper) {
				ReadableByteChannel data;
				if ((data = ((StreamFieldDataWrapper)fieldDataWrapper[currentWrapper]).getLastData()) != null) {
					currentFileName = fieldDataWrapper[currentWrapper].getCurrentFileName();
					lastFieldName = fieldDataWrapper[currentWrapper].getFieldName();
					currentWrapper++;
					return data;
				}
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
		return !inputPort.isEOF();
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
		return currentFileName;
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
		return lastFieldName;
	}

	/**
	 * Abstract field data wrapper.
	 */
	private abstract static class FieldDataWrapper {
		// data field and charset for Strng data field
		protected DataField field;
		protected String charset;
		protected String currentFileName;

		/**
		 * Constructor.
		 * @param field
		 */
		public FieldDataWrapper(DataField field, String charset) {
			this.field = field;
			this.charset = charset;
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
		 * Gets readable or null.
		 * @return
		 * @throws UnsupportedEncodingException
		 */
		public abstract ReadableByteChannel getData() throws UnsupportedEncodingException, JetelException;
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
		public DiscreteFieldDataWrapper(DataField field, String charset) {
			super(field, charset);
			currentFileName = "reading from field name '" + field.getMetadata().getName() + "'";
		}

		@Override
		public ReadableByteChannel getData() throws UnsupportedEncodingException, JetelException {
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
			if (oValue == null) throw new NullPointerException("The field contain unsupported null value.");
			ByteArrayInputStream str = oValue instanceof byte[] ? 
					new ByteArrayInputStream((byte[])oValue) : new ByteArrayInputStream(oValue.toString().getBytes(charset));
			return Channels.newChannel(str);
		}
	}

	/**
	 * Source field data wrapper.
	 */
	private static class SourceFieldDataWrapper extends FieldDataWrapper {
		// property resolver
		private PropertyRefResolver propertyRefResolve;
		private URL contextURL;

		/**
		 * Constructor.
		 * @param field
		 * @param charset
		 */
		public SourceFieldDataWrapper(DataField field, String charset) {
			super(field, charset);
		}

		/**
		 * Sets Property RefResolve.
		 * @param propertyRefResolve
		 */
		public void setPropertyRefResolver(PropertyRefResolver propertyRefResolve) {
			this.propertyRefResolve = propertyRefResolve;
		}
		
		/**
		 * Sets context url.
		 * @param contextURL
		 */
		public void setContextURL(URL contextURL) {
			this.contextURL = contextURL;
		}

		@Override
		public ReadableByteChannel getData() throws UnsupportedEncodingException, JetelException {
			// urls processing
			currentFileName = field.getValue().toString();
			if (propertyRefResolve != null)	currentFileName = propertyRefResolve.resolveRef(currentFileName, false);
			return createReadableByteChannel(currentFileName);
		}
		
		/**
		 * Creates readable channel for a file name.
		 * 
		 * @param fileName
		 * @return
		 * @throws JetelException
		 */
		private ReadableByteChannel createReadableByteChannel(String fileName) throws JetelException {
			defaultLogger.debug("Opening input file " + fileName);
			try {
				ReadableByteChannel channel = FileUtils.getReadableChannel(contextURL, fileName);
				defaultLogger.debug("Reading input file " + fileName);
				return channel;
			} catch (IOException e) {
				throw new JetelException("File is unreachable: " + fileName, e);
			}
		}
	}

	/**
	 * Source field data wrapper.
	 */
	private static class StreamFieldDataWrapper extends FieldDataWrapper {
		// byte array
		private ByteArray byteArray;

		// flush data at the end
		private boolean flushDataAtTheEnd;
		
		/**
		 * Constructor.
		 * @param field
		 * @param charset
		 */
		public StreamFieldDataWrapper(DataField field, String charset) {
			super(field, charset);
			byteArray = new ByteArray();
			currentFileName = "reading from field name '" + field.getMetadata().getName() + "'";
		}

		@Override
		public ReadableByteChannel getData() throws UnsupportedEncodingException, JetelException {
			Object value = field.getValue();
			if (value == null) {
				ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(byteArray.getValueDuplicate()));
				byteArray.reset();
				flushDataAtTheEnd = false;
				return ch;
			}
			byteArray.append(value instanceof byte[] ? (byte[])value : value.toString().getBytes(charset));
			flushDataAtTheEnd = true;
			return null;
		}
		
		/**
		 * Gets data if flushDataAtTheEnd = true.
		 * @return
		 */
		public ReadableByteChannel getLastData() {
			// return null
			if (!flushDataAtTheEnd) return null;
			flushDataAtTheEnd = false;
			
			// return last data
			ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(byteArray.getValueDuplicate()));
			byteArray.reset();
			return ch;
		}
	}

	/**
	 * Field url parser.
	 */
	public static class PortHandler {

		private static final String PARAM_DELIMITER = ":";
		private static final String PORT_DELIMITER = "\\.";
		private static final String PORT = "port";
		
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
			portName = fieldNamePort[0].replace("$", "");
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
}
