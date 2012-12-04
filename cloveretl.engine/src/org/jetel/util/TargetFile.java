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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.StringDataField;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.formatter.provider.FormatterProvider;
import org.jetel.enums.ProcessingType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.OutputPort;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.RestrictedByteArrayOutputStream;
import org.jetel.util.file.FileUtils;


/**
 * TargetFile is used for basic operation over output files or streams and formatter. It support methods
 * for multifile record.
 *
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class TargetFile {
    private static Log logger = LogFactory.getLog(TargetFile.class);

	private static final char NUM_CHAR='#';			// file markter that is replacet file tag.
	private static final String EMPTY_STRING="";
	private static final String PARAM_DELIMITER = ":";
	private static final String PORT_DELIMITER = "\\.";
	private static final String PORT_PROTOCOL = "port:";
	private static final String DICT_PROTOCOL = "dict:";
	private static final String DEFAULT_CHARSET = "UTF-8";

	private DecimalFormat format;					// it is used if the file tag is a number

	private String fileURL;							// output file url
	private URL contextURL;							// output context url
    private Iterator<WritableByteChannel> channels; // output channel
	private FormatterProvider formatterProvider;		// creates new formatter
	private DataRecordMetadata metadata;			// metadata

    private MultiOutFile fileNames;				// returns filename string
    private Formatter formatter;					// writes records into output

    private int records;							// count of record sent to formatter
    private int bytes;								// count of bytes sent to formatter
    private boolean appendData;						// appends data to output
	private boolean useChannel = true;				// if can be used a byteChannel

	private Object fileTag;							// string of marks '#' are replaced by this fileTag
    private String before;							// string of fileURL before last string of marks '#'
    private String after;							// string of fileURL after last string of marks '#'
	private String fileName;

    private WritableByteChannel byteChannel;

	private OutputPort outputPort;
	private DataRecord record;
	private DataField field;
	private boolean isStringDataField;
	private ProcessingType fieldProcesstingType;

	private String charset;

	private Dictionary dictionary;
	private ProcessingType dictProcesstingType;
	private WritableByteChannel dictOutChannel;
	private ArrayList<byte[]> dictOutArray;
	private Object dictObjectArray;
	private boolean fieldOrDictOutput;
	private ByteArrayOutputStream bbOutputStream;

	private int compressLevel = -1;

	private boolean storeRawData = true;
	private boolean objectDictionaryInitialized = false;

    /**
     * Constructors.
     */
    public TargetFile(String fileURL, URL contextURL, Formatter formatter, DataRecordMetadata metadata) {
    	this.fileURL = fileURL;
    	this.contextURL = contextURL;
    	this.formatter = formatter;
    	this.metadata = metadata;
    }

    public TargetFile(Iterator<WritableByteChannel> channels, Formatter formatter, DataRecordMetadata metadata) {
    	this.channels = channels;
    	this.formatter = formatter;
    	this.metadata = metadata;
    }

    public TargetFile(String fileURL, URL contextURL, FormatterProvider formatterProvider, DataRecordMetadata metadata) {
    	this.fileURL = fileURL;
    	this.contextURL = contextURL;
    	this.formatterProvider = formatterProvider;
    	this.metadata = metadata;
    }

    public TargetFile(Iterator<WritableByteChannel> channels, FormatterProvider formatterProvider, DataRecordMetadata metadata) {
    	this.channels = channels;
    	this.formatterProvider = formatterProvider;
    	this.metadata = metadata;
    }

    /**
     * Prepares file url string, initialize output and formatter.
     *
     * @throws IOException
     * @throws ComponentNotReadyException
     */
    public void init() throws IOException, ComponentNotReadyException {
    	if (charset == null) {
    		charset = DEFAULT_CHARSET;
    	}

    	if (fileURL != null && fileURL.startsWith(PORT_PROTOCOL)) {
        	initPortFields();
    	} else if (outputPort != null) {
    		throw new ComponentNotReadyException("File url must contains port or dict protocol.");
    	} else if (fileURL != null && fileURL.startsWith(DICT_PROTOCOL)) {
           	if (!initDictTarget()) {
           		//dictionary contains just regular fileURL (SOURCE type), let's process it
           		processRegularFileURL();
           	}
    	} else {
    		processRegularFileURL();
    	}
    	initOutput();
    }

    private void processRegularFileURL() throws ComponentNotReadyException, IOException {
    	initUrl();
    	if (fileURL != null && (after.indexOf(MultiOutFile.NUM_CHAR) != -1 && before.startsWith("zip:"))) {
    		throw new ComponentNotReadyException("File url must not contain wildcard in inzip filename");
    	}
    	if (fileTag == null) {
    		initFileNames(null);
    	}
    	else if (fileTag instanceof Number) {
    		initFileNames(format.format((Number)fileTag));
    	} else {
    		initFileNames(fileTag.toString());
    	}
    }

    public void reset() {
    	if (fileNames != null) {
			fileNames.reset();
		}
		formatter.reset();
	}

    /**
     * Output port if data should be write to an output field.
     *
     * @param outputPort
     */
    public void setOutputPort(OutputPort outputPort) {
    	this.outputPort = outputPort;
    }

    /**
     * FileURL can contains '#' mark, the string of marks is replaced the fileTag. If a file tag is Number,
     * there is used NumberFormat for definition of minimal lenght of the fileTag. Ie: ## and 5 is "05".
     *
     * @param fileTag
     */
    public void setFileTag(Object fileTag) {
    	this.fileTag = fileTag;
    }

    public void setFileName(String fileName) {
    	this.fileName = fileName;
    }

    /**
     * Replaces '#' string of marks for value and creates MultiOutFile.
     *
     * @param value
     * @throws IOException
     * @throws ComponentNotReadyException
     */
    private void initFileNames(String value) throws IOException, ComponentNotReadyException {
    	if (fileURL != null) {
    		fileNames = new MultiOutFile(value == null ? fileURL : before + value + after);
    	}
    }

    /**
     * Divides fileURL to two string. The first one is a string before '#' mark, the second one is
     * after mark. If no mark found, before string is fileURL.
     * Creates decimal format.
     */
    private void initUrl() {
    	if (fileURL == null) return;
    	int idxLast = fileURL.lastIndexOf(NUM_CHAR);
    	if (idxLast == -1) {
    		before = fileURL;
    		after = EMPTY_STRING;
    		format = new DecimalFormat();
    		return;
    	}
    	StringBuilder sb = new StringBuilder();
    	int idxFirst;
		for (idxFirst = idxLast; idxFirst > 0 && fileURL.charAt(idxFirst) == NUM_CHAR; idxFirst--) {
			sb.append(0);
		}
		before = fileURL.substring(0, idxFirst+1);
		after = fileURL.substring(idxLast+1, fileURL.length());
    	format = new DecimalFormat(sb.toString());
    }

	private boolean initDictTarget() throws ComponentNotReadyException {
		if (dictionary == null) {
			throw new RuntimeException("The component doesn't support dictionary writing.");
		}

		// parse target url
		String[] aDict = fileURL.substring(DICT_PROTOCOL.length()).split(PARAM_DELIMITER);
		Object dictValue = dictionary.getValue(aDict[0]);
		dictProcesstingType = ProcessingType.fromString(aDict.length > 1 ? aDict[1] : null, null);

		if (dictProcesstingType == null) {
			//a test (dictValue instanceof List) is a dirty fix for a case when a default value is set to an initial value (new ArrayList<byte[]>())
			if (dictValue != null && !(dictValue instanceof List)) {
				dictProcesstingType = ProcessingType.STREAM;
			} else {
				dictProcesstingType = ProcessingType.DISCRETE;
			}
		}

		// create target
		switch (dictProcesstingType) {
		case STREAM:
			if (dictValue instanceof WritableByteChannel) {
				dictOutChannel = (WritableByteChannel) dictValue;
			} else if (dictValue instanceof OutputStream) {
				dictOutChannel = Channels.newChannel((OutputStream) dictValue);
			} else {
				throw new IllegalStateException("Dictionary doesn't contain valid value for the key '" + aDict[0] + "' in stream processing mode.");
			}
			break;
		case DISCRETE:
			if (storeRawData) {
				dictOutArray = new ArrayList<byte[]>();
				dictionary.setValue(aDict[0], dictOutArray);
			} else {
                ArrayList<Object> list = new ArrayList<Object>();
                dictionary.setValue(aDict[0], list);
                if(dictValue!= null){ //If dictValue != null use this as a default root element
                    dictObjectArray = new Pair<ArrayList<Object>,Object>(list,dictValue);
                } else{
				    dictObjectArray = list;
                }
			}
			break;
		case SOURCE:
			if (dictValue instanceof CharSequence) {
				//fileURL refers to a dictionary entry which is SOURCE type
				//so dictionary entry content has to be a string which is real fileURL to be written
				//fileURL variable is updated and false value returned to inform
				//that writer has to write to file specified in fileURL variable
				fileURL = dictValue.toString();
				dictProcesstingType = null; //just to be sure that all other code will not be confused
				return false;
			} else {
				throw new IllegalStateException("Dictionary doesn't contain valid value for the key '" + aDict[0] + "' in source processing mode. " +
						"Only charsequence are supported.");
			}
		default:
			throw new ComponentNotReadyException("invalid dictionary processting type " + dictProcesstingType);
		}
		return true;
	}

	private void initPortFields() throws ComponentNotReadyException {
		if (outputPort == null) {
			throw new ComponentNotReadyException("Output port is not connected.");
		}

		// prepare output record
		record = DataRecordFactory.newRecord(outputPort.getMetadata());
		record.init();

		// parse target url
		String[] aField = fileURL.substring(PORT_PROTOCOL.length()).split(PARAM_DELIMITER);
		if (aField.length < 1) {
			throw new ComponentNotReadyException("The source string '" + fileURL + "' is not valid.");
		}

		String[] aFieldNamePort = aField[0].split(PORT_DELIMITER);
		fieldProcesstingType = ProcessingType.fromString(aField.length > 1 ? aField[1] : null, ProcessingType.DISCRETE);
		if (aFieldNamePort.length < 2) {
			throw new ComponentNotReadyException("The source string '" + fileURL + "' is not valid.");
		}

		// check setting
		String fName = aFieldNamePort[1];
		if (record.hasField(fName)) {
			field = record.getField(fName);
		} else if (field == null) {
			throw new ComponentNotReadyException("The field not found for the statement: '" + fileURL + "'");
		}

		if (field instanceof StringDataField) {
			isStringDataField = true;
		} else if (!(field instanceof ByteDataField)) {
			throw new ComponentNotReadyException("The field '" + field.getMetadata().getName() + "' must be String or (Compressed) Byte data field.");
		}
	}

    /**
     * The method writes footer and header and sets next output to the formatter.
     *
     * @throws IOException
     */
    public void setNextOutput() throws IOException {
    	if (field == null) {
        	checkOutput();
    	}

        //write footer to the previous destination if it is not first call of this method
        if(byteChannel != null || bbOutputStream != null || objectDictionaryInitialized) {
//        	formatter.writeFooter();	// issue 1503
        	formatter.finish();
        }
        setOutput();

        bytes = records = 0;

        formatter.writeHeader();
    }

    public void finish() throws IOException{
    	formatter.finish();
    	formatter.close();
    	write2FieldOrDict();
    }

    private void write2FieldOrDict() throws IOException {
    	if (fieldOrDictOutput) {
        	if (bbOutputStream != null) {
        		write2OutportOrDictionary(bbOutputStream.toByteArray());
        	}
    		try {
    			// there is only one target for port and dictionary protocol
				if (outputPort != null) {
					outputPort.eof();
				}
			} catch (InterruptedException e) {
				throw new IOException(e.getMessage());
			}
    	}
    }

    /**
     * Write data to the output port or to the dictionary.
     * @param aData
     * @throws IOException
     */
	private void write2OutportOrDictionary(byte[] aData) throws IOException {
		if (bbOutputStream != null) {
			if (dictProcesstingType != null) {
				if (dictOutArray != null) {
					dictOutArray.add(aData);
				}
			}
			if (field != null) {
				if (aData.length == 0) {
					return;
				}

				boolean streamType = fieldProcesstingType == ProcessingType.STREAM;
				if (isStringDataField) {
					write2StringField(aData, streamType);
				} else {
					write2ByteField(aData, streamType);
				}
			}
		}
	}

    /**
     * Write byte array to string field for n records
     * @param aData
     * @param streamType
     * @param repeat
     * @throws IOException
     */
    private void write2StringField(byte[] aData, boolean streamType) throws IOException {
    	// the writing to port string field is not often operation than to the byte field I guess
    	// the new String is quicker than ByteCharBuffer in all cases but bigger memory is needed
    	String sData = new String(aData, charset);

		// string field - stream mode
    	if (streamType) {
        	// repeat =0 is 1 record, =1 are 2 records, ...
        	int repeat = sData.length() / (streamType ? Defaults.PortReadingWriting.DATA_LENGTH : sData.length());
			int size = Defaults.PortReadingWriting.DATA_LENGTH;

			// send all data to records, last record is null
    		for (int i=0; i<=repeat; i++) {
    			if (i == repeat) {
    				size = sData.length() % Defaults.PortReadingWriting.DATA_LENGTH;
    			}
    			int start = Defaults.PortReadingWriting.DATA_LENGTH*i;
   				field.setValue(sData.substring(start, start+size));

    			//broadcast the record to the connected edge
    			writeRecord();
    		}

    		//null mark
			field.setNull(true);

   		// string field - byte mode
    	} else {
   			field.setValue(sData);
    	}

    	//broadcast the record to the connected edge
		writeRecord();
    }

    /**
     * Write byte array to byte/cbyte field for n records
     * @param aData
     * @param streamType
     * @param repeat
     */
    private void write2ByteField(byte[] aData, boolean streamType) {
		// byte field - stream mode
    	if (streamType) {
        	// repeat =0 is 1 record, =1 are 2 records, ...
        	int repeat = aData.length / (streamType ? Defaults.PortReadingWriting.DATA_LENGTH : aData.length);

			// is it necessary to copy the byte array?
			byte[] subArray = null;
			int size = Defaults.PortReadingWriting.DATA_LENGTH;
			if (repeat > 0) {
				subArray = new byte[size];
			}

			// send all data to records, last record is null
    		for (int i=0; i<=repeat; i++) {
    			if (i == repeat) {
    				size = aData.length % Defaults.PortReadingWriting.DATA_LENGTH;
    				subArray = new byte[size];
    			}
       	    	System.arraycopy(aData, Defaults.PortReadingWriting.DATA_LENGTH*i, subArray, 0, size);
   				field.setValue(subArray);

    			//broadcast the record to the connected edge
    			writeRecord();
    		}

    		//null mark
			field.setNull(true);

		// byte field - discrete mode
		} else {
			field.setValue(aData);
		}

		//broadcast the record to the connected edge
		writeRecord();
    }

    /**
     * Writes record to the output port.
     */
    private void writeRecord() {
		//broadcast the record to all connected Edges
		try {
			outputPort.writeRecord(record);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		SynchronizeUtils.cloverYield();
    }

    /**
     * Closes underlying formatter.
     * @throws IOException
     */
    public void close() throws IOException {
        formatter.close();
    }

    /**
     * Prepares output file.
     *
     * @throws IOException
     * @throws ComponentNotReadyException
     */
    private void initOutput() throws IOException, ComponentNotReadyException {
    	if (formatter == null) formatter = formatterProvider.getNewFormatter();
    	formatter.init(metadata);
    	setNextOutput();
    }

    private void checkOutput() {
    	if (fileNames != null && !fileNames.hasNext()) {
            logger.warn("Unable to open new output file. This may be caused by missing wildcard in filename specification. "
                    + "Size of output file will exceed specified limit.");
            return;
    	}
    	if (channels != null && !channels.hasNext()) {
            logger.warn("Unable to open new output stream. Size of last output stream will exceed specified limit.");
            return;
        }
    }

    /**
     * Prepares next output to the data formatter.
     *
     * @throws IOException
     */
    private void setOutput() throws IOException {
    	if (fieldOrDictOutput = (field != null || dictProcesstingType != null)) {
    		if (dictOutChannel != null) {
        		setDataTarget(dictOutChannel);
    		} else {
    			if (bbOutputStream != null) {
            		write2OutportOrDictionary(bbOutputStream.toByteArray());
                	bbOutputStream.reset();
            	} else {
                    if (fieldProcesstingType == ProcessingType.STREAM) {
                       	bbOutputStream = new ByteArrayOutputStream();
                    } else { // discrete mode
            			if (storeRawData) {
							RestrictedByteArrayOutputStream outStream = new RestrictedByteArrayOutputStream();
							if (field != null) {
								outStream.setMaxArrayLength(Defaults.Record.FIELD_LIMIT_SIZE);
							}
							bbOutputStream = outStream;
						} else {
							setDataTarget(dictObjectArray);
							objectDictionaryInitialized = true;
							return;
						}
            		}
            	}
        		setDataTarget(Channels.newChannel(bbOutputStream));
    		}

    	} else if (fileNames != null) {
            String fName = fileNames.next();
            if (fileName != null) {
            	fName = addUnassignedName(fName);
            }

        	if (isFileSourcePreferred()) {
        		//formatter request java.io.File as data target
        		try {
        			setDataTarget(FileUtils.getJavaFile(contextURL, fName));
        			return;
        		} catch (Exception e) {
					//DO NOTHING - just try to open a stream based on the fName in the next step
        		}
        	}
            OutputStream os = FileUtils.getOutputStream(contextURL, fName, appendData, compressLevel);
        	byteChannel = Channels.newChannel(os);

        	if (useChannel) {
        		setDataTarget(byteChannel);
        	} else {
           		setDataTarget(new Object[] {contextURL, fName, os});
        	}
        } else {
        	byteChannel = channels.next();
        	setDataTarget(byteChannel);
        }
    }

    private String addUnassignedName(String fName) throws IOException {
    	int hashIndex = fName.lastIndexOf('#');
    	if (hashIndex >= 0) {
    		String name = fName.substring(0, hashIndex) + fileName;
    		if (fName.length() >= hashIndex + 1) {
    			name += fName.substring(hashIndex + 1);
    		}
    		return name;
    	} else {
    		return fName + fileName;
    	}
    }

    /**
     * @return <code>true</code> if java.io.File source type is preferred instead of channel
     */
    private boolean isFileSourcePreferred() {
    	return formatter.isFileTargetPreferred();
    }

    /**
     * Sets logger.
     *
     * @param log
     */
    public static void setLogger(Log log) {
		logger = log;
	}

    /**
     * Sets a output to the data formatter.
     *
     * @param outputDataTarget
     * @throws IOException previous target cannot be closed
     */
    public void setDataTarget(Object outputDataTarget) throws IOException {
    	formatter.setDataTarget(outputDataTarget);
    }

    public Iterator<String> getFileNames() {
    	return fileNames;
    }

    public Formatter getFormatter() {
    	return formatter;
    }

    public int getRecords() {
    	return records;
    }

    public void setRecords(int records) {
    	this.records = records;
    }

    public int getBytes() {
    	return bytes;
    }

    public void setBytes(int bytes) {
    	this.bytes = bytes;
    }

    public void setAppendData(boolean appendData) {
        this.appendData = appendData;
    }

	public void setUseChannel(boolean useChannel) {
		this.useChannel = useChannel;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setDictionary(Dictionary dictionary) {
		this.dictionary = dictionary;
	}

	public int getCompressLevel() {
		return compressLevel;
	}

	public void setCompressLevel(int compressLevel) {
		this.compressLevel = compressLevel;
	}

	public void setStoreRawData(boolean storeRawData) {
		this.storeRawData  = storeRawData;
	}

}
