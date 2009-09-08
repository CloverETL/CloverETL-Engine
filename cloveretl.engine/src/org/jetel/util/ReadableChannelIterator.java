package org.jetel.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.primitive.ByteArray;
import org.jetel.enums.ProcessingType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.InputPort;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.WcardPattern;
import org.jetel.util.property.PropertyRefResolver;

/***
 * Supports a field reading and reading from urls.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) OpenSys (www.opensys.eu)
 */
public class ReadableChannelIterator {
	
    private static Log defaultLogger = LogFactory.getLog(ReadableChannelIterator.class);
	public static final String DEFAULT_CHARSET = "UTF-8";

	private static final String PARAM_DELIMITER = ":";
	private static final String PORT_DELIMITER = "\\.";
	private static final String DICT = "dict:";
	private static final String PORT = "port:";

	private String fileURL;
	private URL contextURL;
	
	private InputPort inputPort;
	
	private DataField[] fields;
	private ProcessingType[] processingType;
	
	private DataField[] fields4SourceAndDiscrete;
	private ProcessingType[] processing4SourceAndDiscreteType;
	private PropertyRefResolver propertyRefResolve;
	
	private DataRecord record;
	private int fieldIndex = Integer.MAX_VALUE;

	private Iterator<String> filenameItor;
	private List<String> files;

	private int firstPortProtocolPosition;
	private int firstDictProtocolPosition;
	private int currentPortProtocolPosition;
	private List<String> portProtocolFields;

	private String currentFileName;
	
	private boolean isGraphDependentSource;
	
	private boolean bInputPort;

	private ByteArray[] byteArrays;
	private DataField[] fields4Stream = null;
	private int streamFieldIndex;
	private boolean isStreamDataPrepared;

	private String charset;
	private Dictionary dictionary;
	private ObjectValueArray objectValueArray;
	
	/**
	 * Constructor.
	 * 
	 * @param inputPort
	 * @param contextURL
	 * @param fileURL
	 */
	public ReadableChannelIterator(InputPort inputPort, URL contextURL, String fileURL) {
		this.inputPort = inputPort;
		this.fileURL = fileURL;
		this.contextURL = contextURL;
	}

	/**
	 * Initializes this class for the first using.
	 */
	public void init() throws ComponentNotReadyException {
		streamFieldIndex = 0;
		initFileIterator();
		initPortFields();
		currentPortProtocolPosition = 0;
		fieldIndex = fields4SourceAndDiscrete != null ? fields4SourceAndDiscrete.length : 0;
		isGraphDependentSource = firstPortProtocolPosition == 0 || firstDictProtocolPosition == 0;
		bInputPort = inputPort != null && fields != null && fields.length > 0;
		if (charset == null) charset = DEFAULT_CHARSET;
		currentFileName = "unknown";
	}
	
	/**
	 * Resets this class for a next using.
	 */
	public void reset() {
		try {
			init();
		} catch (ComponentNotReadyException e) {
			defaultLogger.error(e); //never happened
		}
	}
	
	private void initFileIterator() throws ComponentNotReadyException {
		WcardPattern pat = new WcardPattern();
		pat.setParent(contextURL);
        pat.addPattern(fileURL, Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
        try {
			files = pat.filenames();
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		}
        firstPortProtocolPosition = getFirstProtocolPosition(files, PORT);
        firstDictProtocolPosition = getFirstProtocolPosition(files, DICT);
		if (firstPortProtocolPosition >= 0 && inputPort == null) throw new ComponentNotReadyException("Input port is not defined for '" + files.get(firstPortProtocolPosition) + "'.");
        portProtocolFields = getAndRemoveProtocol(files, PORT, firstPortProtocolPosition);
        this.filenameItor = files.iterator();
	}

	public void setDictionary(Dictionary dictionary) {
		this.dictionary = dictionary;
	}
	
	private void initPortFields() throws ComponentNotReadyException {
		if (inputPort == null) return;
		record = new DataRecord(inputPort.getMetadata());
		record.init();
		fields = new DataField[portProtocolFields.size()];
		processingType = new ProcessingType[fields.length];
		String portStatement;
		List<DataField> lProcessing4SourceAndDiscrete = new ArrayList<DataField>();
		List<ProcessingType> lProcessing4SourceAndDiscreteType = new ArrayList<ProcessingType>();
		for (int i=0; i<fields.length; i++) {
			portStatement = portProtocolFields.get(i);
			PortHandler portHandler = null;
			try {
				portHandler = new PortHandler(portStatement, ProcessingType.DISCRETE);
			} catch (MalformedURLException e) {
				throw new ComponentNotReadyException("The source string '" + e.getMessage() + "' is not valid.");
			}
			String fName = portHandler.getFieldName();
			if (!record.hasField(fName)) throw new ComponentNotReadyException("The field not found for the statement: '" + portStatement + "'");
			fields[i] = record.getField(fName);
			if (fields[i] == null) throw new ComponentNotReadyException("The field not found for the statement: '" + portStatement + "'");
			processingType[i] = portHandler.getProcessingType();
			if (processingType[i] != ProcessingType.STREAM) {
				lProcessing4SourceAndDiscrete.add(fields[i]);
				lProcessing4SourceAndDiscreteType.add(processingType[i]);
			}
		}
		fields4SourceAndDiscrete = new DataField[lProcessing4SourceAndDiscrete.size()];
		lProcessing4SourceAndDiscrete.toArray(fields4SourceAndDiscrete);
		processing4SourceAndDiscreteType = new ProcessingType[lProcessing4SourceAndDiscreteType.size()];
		lProcessing4SourceAndDiscreteType.toArray(processing4SourceAndDiscreteType);
		initStreamType();
		
	}

	private void initStreamType() {
		List<DataField> lFields = new ArrayList<DataField>(); 
		for (int i=0; i<fields.length; i++) {
			if (processingType[i] == ProcessingType.STREAM) {
				lFields.add(fields[i]);
			}
		}
		int size = lFields.size();
		if (size > 0) {
			fields4Stream = new DataField[size];
			lFields.toArray(fields4Stream);
			byteArrays = new ByteArray[size];
			for (int i=0; i<size; i++) {
				byteArrays[i] = new ByteArray();
			}
		}
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
		return hasNextWithoutStream() || hasNextWithStream();
	}

	private boolean hasNextWithoutStream() {
		return filenameItor.hasNext() || (bInputPort && !inputPort.isEOF());
	}
	
	private boolean hasNextWithStream() {
		return fields4Stream != null && streamFieldIndex < fields4Stream.length;
	}
	
	public Iterator<String> getFileIterator() {
		return files.iterator();
	}
	
	/**
	 * @throws JetelException 
	 * @see java.util.Iterator
	 */
	public ReadableByteChannel next() throws JetelException {
		// read next value from dictionary array or list
		if (objectValueArray != null && objectValueArray.hasNext()) return objectValueArray.next();
		
		// read from fields
		if (currentPortProtocolPosition == firstPortProtocolPosition) {
			if ((record=getNextRecord()) != null) {
				try {
					ReadableByteChannel channel = null;
					isStreamDataPrepared = true;
					if (processing4SourceAndDiscreteType[fieldIndex] == ProcessingType.DISCRETE) {
						// data processing
						currentFileName = fields4SourceAndDiscrete[fieldIndex].getMetadata().getName();
						channel = createReadableByteChannel(fields4SourceAndDiscrete[fieldIndex].getValue());
					} else {
						// urls processing
						currentFileName = fields4SourceAndDiscrete[fieldIndex].getValue().toString();
						if (propertyRefResolve != null)	currentFileName = propertyRefResolve.resolveRef(currentFileName, false);
						channel = createReadableByteChannel(currentFileName);
					}
					return channel;
				} catch (NullPointerException e) {
					throw new JetelException("The field '" + fields4SourceAndDiscrete[fieldIndex].getMetadata().getName() + "' contain unsupported null value.");
				} catch (UnsupportedEncodingException e) {
					throw new JetelException("The field '" + fields4SourceAndDiscrete[fieldIndex].getMetadata().getName() + "' contain an value that cannot be translated by " + charset + " charset." );
				} finally {
					fieldIndex++;
				}
			}
		}
		
		// read from urls
		if (filenameItor.hasNext()) {
			currentFileName = filenameItor.next();
			currentPortProtocolPosition++;
			
			if (currentFileName.indexOf(DICT) == 0) {
			//if (getMostInnerString(currentFileName).indexOf(DICT) == 0) {
				ReadableByteChannel rch = getChannelFromDictionary(currentFileName);
				return rch == null ? next() : rch;
			}
			return createReadableByteChannel(currentFileName);
		}

		// read from STREAM type
		if (!hasNextWithoutStream() && hasNextWithStream()) {
			if (isStreamDataPrepared) {
				return Channels.newChannel(new ByteArrayInputStream(byteArrays[streamFieldIndex++].getValueDuplicate()));
			}
		}

		return null;
	}

	private ReadableByteChannel getChannelFromDictionary(String source) throws JetelException {
		// parse source
		String[] aSource = currentFileName.substring(DICT.length()).split(PARAM_DELIMITER);
		//String[] aSource = getMostInnerString(source).substring(DICT.length()).split(PARAM_DELIMITER);
		String dictKey = aSource[0];
		ProcessingType dictProcesstingType = ProcessingType.fromString(aSource.length > 1 ? aSource[1] : null, ProcessingType.DISCRETE);
		if (dictionary == null) throw new RuntimeException("The component doesn't support dictionary reading.");
		Object value = dictionary.getValue(dictKey);
		if (value == null) throw new JetelException("Dictionary doesn't contain value for the key '" + dictKey + "'");
		
		ReadableByteChannel rch;
		try {
			if (value instanceof List || value instanceof Object[]) {
				objectValueArray = ObjectValueArray.getInstance(value, charset, dictProcesstingType);
				if (objectValueArray == null || !objectValueArray.hasNext()) {
					defaultLogger.warn("Dictionary contains empty list for the key '" + dictKey + "'.");
					return null;
				}
				rch = objectValueArray.next();
			} 
			else if (value instanceof InputStream) rch = Channels.newChannel((InputStream)value);
			else if (value instanceof ByteArrayOutputStream) rch = createReadableByteChannel(((ByteArrayOutputStream)value).toByteArray());
			else if (value instanceof ReadableByteChannel) rch = (ReadableByteChannel)value;
			else rch = createReadableByteChannel(value);
		} catch (UnsupportedEncodingException e) {
			throw new JetelException(e.getMessage(), e);
		}
		
		if (dictProcesstingType == ProcessingType.SOURCE) {
			rch = createChannelFromSource(rch, charset);
		}
		return rch;
	}
	
	private ReadableByteChannel createChannelFromSource(ReadableByteChannel readableByteChannel, String charset) throws JetelException {
		ByteBuffer dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		CharBuffer charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		CharsetDecoder decoder = Charset.forName(charset).newDecoder();
		dataBuffer.clear();
        dataBuffer.flip();
		charBuffer.clear();
		charBuffer.flip();
		try {
			dataBuffer.compact();	// ready for writing
			readableByteChannel.read(dataBuffer);	// write to buffer
			dataBuffer.flip();	// ready reading
		} catch (IOException e) {
			throw new JetelException(e.getMessage());
		}
		charBuffer.compact();	// ready for writing
        decoder.decode(dataBuffer, charBuffer, false);
        charBuffer.flip();
        return createReadableByteChannel(charBuffer.toString());
	}
	
	private DataRecord getNextRecord() {
		try {
			// for SOURCE or DISCRETE type (or STREAM type)
			if (fieldIndex >= fields4SourceAndDiscrete.length) {
				record = inputPort.readRecord(record);
				fieldIndex = 0;
				if (fields4Stream != null) read2ByteArrays();
			}
			// for STREAM type only
			if (fields4SourceAndDiscrete.length == 0) {
				if (fields4Stream != null) {
					isStreamDataPrepared = true;
					while ((record = inputPort.readRecord(record)) != null) {
						read2ByteArrays();
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return record;
	}
	
	private void read2ByteArrays() throws UnsupportedEncodingException {
		if (record != null) {
			Object value;
			for (int i=0; i<fields4Stream.length; i++) {
				value = fields4Stream[i].getValue();
				byteArrays[streamFieldIndex].append(value instanceof byte[] ? (byte[])value : value.toString().getBytes(charset));
			}
		}
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
	
	/**
	 * Returns current field name.
	 */
	public String getCurrentFileName() {
		return currentFileName;
	}
	
	private int getFirstProtocolPosition(List<String> files, String protocol) {
		for (int i=0; i<files.size(); i++) {
			if (files.get(i).indexOf(protocol) == 0) {
			//if (getMostInnerString(files.get(i)).indexOf(protocol) == 0) {
				return i;
			}			
		}
		return -1;
	}
	
	private List<String> getAndRemoveProtocol(List<String> files, String protocol, int start) {
		ArrayList<String> result = new ArrayList<String>();
		if (start < 0) return result;
		String file;
		for (int i=start; i<files.size(); i++) {
			file = files.get(i);
			if (file.indexOf(protocol) == 0) {
				files.remove(i);
				i--;
				result.add(file);
			}
		}
		return result;
	}
	
	public boolean isGraphDependentSource() {
		return isGraphDependentSource;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setPropertyRefResolver(PropertyRefResolver propertyRefResolve) {
		this.propertyRefResolve = propertyRefResolve;
	}
	
	private static abstract class ObjectValueArray implements Iterator<ReadableByteChannel> {
		protected int counter = 0;
		
		@SuppressWarnings("unchecked")
		public static ObjectValueArray getInstance(Object value, String charset, ProcessingType dictProcesstingType) {
			if (value instanceof CharSequence[]) {
				return new CharSeqValueArray((CharSequence[])value, charset);
			} else if (value instanceof List<?>) {
				List<?> list = (List<?>) value;
				if (list.size() == 0) return null;
				value = list.get(0);
				if (value instanceof CharSequence) {
					CharSequence[] aString = new CharSequence[list.size()];
					list.toArray(aString);
					return new CharSeqValueArray(aString, charset);
				} else if (value instanceof byte[]) {
					return new ByteValueArray((List<byte[]>)list);
				}
			}
			throw new RuntimeException("Cannot create input stream for class instance: '" + value.getClass() + "'.");
		}
	}
	
	private static class CharSeqValueArray extends ObjectValueArray {
		private CharSequence[] value;
		private String charset;

		public CharSeqValueArray(CharSequence[] value, String charset) {
			this.value = value;
			this.charset = charset;
		}
		
		public boolean hasNext() {
			return counter < value.length;
		}
		public ReadableByteChannel next() {
			try {
				return Channels.newChannel(new ByteArrayInputStream(value[counter++].toString().getBytes(charset)));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}
		public void remove() {
		}
	}
	
	private static class ByteValueArray extends ObjectValueArray {
		private List<byte[]> value;

		public ByteValueArray(List<byte[]> value) {
			this.value = value;
		}
		
		public boolean hasNext() {
			return counter < value.size();
		}
		public ReadableByteChannel next() {
			return Channels.newChannel(new ByteArrayInputStream(value.get(counter++)));
		}
		public void remove() {
		}
	}

	public static class PortHandler {

		public static final String PORT = "port";
		
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
