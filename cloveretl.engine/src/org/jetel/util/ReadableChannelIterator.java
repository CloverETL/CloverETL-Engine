package org.jetel.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
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
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.WcardPattern;

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

	private String fileURL;
	private URL contextURL;
	
	private InputPort inputPort;
	
	private DataField[] fields;
	private ProcessingType[] processingType;
	
	private DataField[] fields4SourceAndDiscrete;
	private ProcessingType[] processing4SourceAndDiscreteType;
	
	private DataRecord record;
	private int fieldIndex = Integer.MAX_VALUE;

	private Iterator<String> filenameItor;

	private int firstPortProtocolPosition;
	private int currentPortProtocolPosition;
	private List<String> portProtocolFields;

	private String currentFileName;
	
	private boolean isFirstFieldSource;
	
	private boolean bInputPort;

	private ByteArray[] byteArrays;
	private DataField[] fields4Stream = null;
	private int streamFieldIndex;
	private boolean isStreamDataPrepared;

	private String charset;

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
		initFileIterator();
		initPortFields();
		currentPortProtocolPosition = 0;
		fieldIndex = fields4SourceAndDiscrete != null ? fields4SourceAndDiscrete.length : 0;
		isFirstFieldSource = firstPortProtocolPosition == 0;
		bInputPort = inputPort != null && fields != null && fields.length > 0;
		if (charset == null) charset = DEFAULT_CHARSET;
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
	
	private void initFileIterator() {
		WcardPattern pat = new WcardPattern();
        pat.addPattern(fileURL, Defaults.DEFAULT_PATH_SEPARATOR_REGEX);
        List<String> files = pat.filenames();
        firstPortProtocolPosition = getFirstProtocolPosition(files, "port:");
        portProtocolFields = getAndRemoveProtocol(files, "port:", firstPortProtocolPosition);
        this.filenameItor = files.iterator();
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
			fields[i] = record.getField(getFieldName(portStatement));
			if (fields[i] == null) throw new ComponentNotReadyException("The field not found for the statement: '" + portStatement + "'");
			processingType[i] = getProcessingType(portStatement);
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

	private String getFieldName(String source) throws ComponentNotReadyException {
		String[] param = source.split(PARAM_DELIMITER); // port:$port.field[:processingType]
		if (param.length < 2) throw new ComponentNotReadyException("The source string '" + source + "' is not valid.");
		param = param[1].split(PORT_DELIMITER);
		if (param.length < 2) throw new ComponentNotReadyException("The source string '" + source + "' is not valid.");
		return param[1];
	}
	
	private ProcessingType getProcessingType(String source) {
		String[] param = source.split(PARAM_DELIMITER); // port:$port.field[:processingType]
		if (param.length < 3) return ProcessingType.DISCRETE;
		return ProcessingType.fromString(param[2], ProcessingType.DISCRETE);
	}
	
	/**
	 * @see java.util.Iterator
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
	
	/**
	 * @throws JetelException 
	 * @see java.util.Iterator
	 */
	public ReadableByteChannel next() throws JetelException {
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
			for (int i=0; i<fields4Stream.length; i++) {
				byteArrays[streamFieldIndex].append(fields4Stream[i].getValue().toString().getBytes(charset));
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
		ByteArrayInputStream str = new ByteArrayInputStream(oValue.toString().getBytes(charset));
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
	
	public boolean isFirstFieldSource() {
		return isFirstFieldSource;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}
}
