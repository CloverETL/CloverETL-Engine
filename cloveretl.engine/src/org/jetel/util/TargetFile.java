package org.jetel.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
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
import org.jetel.data.StringDataField;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.formatter.provider.FormatterProvider;
import org.jetel.data.primitive.ByteArray;
import org.jetel.enums.ProcessingType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.OutputPort;
import org.jetel.graph.dictionary.Dictionary;
import org.jetel.graph.dictionary.DictionaryValue;
import org.jetel.graph.dictionary.IDictionaryValue;
import org.jetel.metadata.DataRecordMetadata;
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
	
    private Iterator<String> fileNames;				// returns filename string
    private Formatter formatter;					// writes records into output
	
    private int records;							// count of record sent to formatter
    private int bytes;								// count of bytes sent to formatter
    private boolean appendData;						// appends data to output
	private boolean useChannel = true;				// if can be used a byteChannel
    
	private Object fileTag;							// string of marks '#' are replaced by this fileTag 
    private String before;							// string of fileURL before last string of marks '#'
    private String after;							// string of fileURL after last string of marks '#'

    private WritableByteChannel byteChannel;

	private OutputPort outputPort;
	private DataRecord record;
	private DataField field; 
    private PipedInputStream writeIn;
	private boolean isFinishing;
	private boolean isFinished;
	private boolean isStringDataField;

	private String charset;

	private Dictionary dictionary;
	private ProcessingType dictProcesstingType;
	private ByteArrayOutputStream dictOutStream;
	private ArrayList<byte[]> dictOutArray;
	private boolean wait4Finishing;
	private Object monitor;

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
    	if (charset == null) charset = DEFAULT_CHARSET;
    	monitor = new Object();
    	if (fileURL != null && fileURL.startsWith(PORT_PROTOCOL)) {
        	initPortFields();
    	} else if (outputPort != null) {
    		throw new ComponentNotReadyException("File url must contains port or dict protocol.");
    	} else if (fileURL != null && fileURL.startsWith(DICT_PROTOCOL)) {
           	initDictTarget();
    	} else {
        	initUrl();
        	if (fileTag == null) {
        		initFileNames(null);
        	} 
        	else if (fileTag instanceof Number) {
        		initFileNames(format.format((Number)fileTag));
        	} else {
        		initFileNames(fileTag.toString());
        	}
    	}
    	initOutput();
    }
    
    public void reset() {
    	if (fileNames != null) {
			((MultiOutFile) fileNames).reset();
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
    
    /**
     * Replaces '#' string of marks for value and creates MultiOutFile.
     * 
     * @param value
     * @throws IOException
     * @throws ComponentNotReadyException
     */
    private void initFileNames(String value) throws IOException, ComponentNotReadyException {
    	if (fileURL != null) fileNames = new MultiOutFile(value == null ? fileURL : before + value + after);
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

	private void initDictTarget() throws ComponentNotReadyException {
		// parse target url
		String[] aDict = fileURL.substring(DICT_PROTOCOL.length()).split(PARAM_DELIMITER);
		if (dictionary == null) throw new RuntimeException("The component doesn't support dictionary writing.");
		IDictionaryValue<?> dictValue = dictionary.get(aDict[0]);
		dictProcesstingType = ProcessingType.fromString(aDict.length > 1 ? aDict[1] : null, ProcessingType.STREAM);
		if (dictValue != null) logger.warn("Dictionary contains value for the key '" + aDict[0] + "'. The value will be replaced.");
		
		// create target
		if (dictProcesstingType == ProcessingType.STREAM) {
			dictOutStream = new ByteArrayOutputStream();
			dictionary.put(aDict[0], new DictionaryValue<ByteArrayOutputStream>(dictOutStream));
		}
		// create target
		else if (dictProcesstingType == ProcessingType.DISCRETE) {
			dictOutArray = new ArrayList<byte[]>();
			dictionary.put(aDict[0], new DictionaryValue<ArrayList<byte[]>>(dictOutArray));
		}
	}
    
	private void initPortFields() throws ComponentNotReadyException {
		if (outputPort == null) throw new ComponentNotReadyException("Output port is not connected.");
		record = new DataRecord(outputPort.getMetadata());
		record.init();
		String fName = getFieldName(fileURL);
		if (record.hasField(fName)) field = record.getField(fName);
		if (field == null) throw new ComponentNotReadyException("The field not found for the statement: '" + fileURL + "'");
		if (field instanceof StringDataField) isStringDataField = true;
		else if (!(field instanceof ByteDataField))	throw new ComponentNotReadyException("The field '" + field.getMetadata().getName() + "' must be String or (Compressed) Byte data field.");
	}
    
	private String getFieldName(String source) throws ComponentNotReadyException {
		String[] param = source.split(PARAM_DELIMITER); // port:$port.field[:processingType]
		if (param.length < 2) throw new ComponentNotReadyException("The source string '" + source + "' is not valid.");
		param = param[1].split(PORT_DELIMITER);
		if (param.length < 2) throw new ComponentNotReadyException("The source string '" + source + "' is not valid.");
		return param[1];
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
        if(byteChannel != null || writeIn != null) {
        	formatter.writeFooter();
        	formatter.finish();
        }
        setOutput();

        bytes = records = 0;
        
        formatter.writeHeader();
    }

    public void finish() throws IOException{
    	isFinishing = true;
    	formatter.finish();
    	formatter.close();
    	wait4Finishing();
    }
    
    private void wait4Finishing() throws IOException {
    	if (wait4Finishing) {
    		synchronized (monitor) {
       			try {
       				if (!isFinished) monitor.wait();
       			} catch (InterruptedException e) {
       				throw new RuntimeException(e);
      			}
			}
    		try {
    			// there is only one target for port and dictionary protocol
				if (outputPort != null) outputPort.eof();
			} catch (InterruptedException e) {
				throw new IOException(e.getMessage());
			}
    	}
    }
    
	public boolean isFinished() {
		return isFinished;
	}
    
    private void write2OutportOrDictionary(ByteArray aBytes) {
    	if (writeIn != null) {
            if (dictProcesstingType != null) {
           		write2Dictionary(aBytes);
            }
   	    	if (isFinishing) isFinished = true;
            if (field != null) {
       			field.setValue(isStringDataField ? aBytes.toString(charset) : aBytes.getValueDuplicate());
       	        //broadcast the record to all connected Edges
       	        try {
       	        	outputPort.writeRecord(record.duplicate());
       			} catch (Exception e) {
       				throw new RuntimeException(e);
       			}
       	        SynchronizeUtils.cloverYield();
            }
    	}
    }
    
    private void write2Dictionary(ByteArray aBytes) {
    	if (dictOutStream != null) {
    		try {
    			dictOutStream.write(aBytes.getValueDuplicate());
    			dictOutStream.flush();
    			dictOutStream.close();
    		} catch (IOException e) {
    			throw new RuntimeException(e);
    		}
    	} else if (dictOutArray != null) {
    		dictOutArray.add(aBytes.getValueDuplicate());
    	}
    }
    
    /**
     * Closes underlying formatter.
     */
    public void close() {
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
    	if (wait4Finishing = (field != null || dictProcesstingType != null)) {
            writeIn = new PipedInputStream();
            PipedOutputStream readOut = new PipedOutputStream(writeIn);
            final ReadThread readThread = new ReadThread(writeIn);
        	readThread.addDataPreparedListener(new DataPreparedListener() {
				@Override
				public void dataPrepared() {
		    		synchronized (monitor) {
						write2OutportOrDictionary(readThread.getBytes());
						readThread.interrupt();
	    				monitor.notifyAll();
					}
				}
			});
        	readThread.start();
    		setDataTarget(Channels.newChannel(readOut));
    	} else if (fileNames != null) {
            String fName = fileNames.next();
        	byteChannel = FileUtils.getWritableChannel(contextURL, fName, appendData);
        	if (useChannel) {
        		setDataTarget(byteChannel);
        	} else {
        		setDataTarget(new File(FileUtils.getFileURL(contextURL, fName).getFile()));
        	}
        } else {
        	byteChannel = channels.next();
        	setDataTarget(byteChannel);
        }
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
     */
    public void setDataTarget(Object outputDataTarget) {
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

	private static class ReadThread extends Thread {
		  private InputStream pi = null;
		  private ByteArray bytes;
		  private byte[] buffer;
		  private int len;
		  private List<DataPreparedListener> listListener;
		  
		  public ReadThread(PipedInputStream pi) {
			  buffer = new byte[1024];
			  setName("ReadThread");
			  listListener = new ArrayList<DataPreparedListener>();
			  bytes = new ByteArray();
			  this.pi = pi;
		  }
		  
		  public ByteArray getBytes() {
			  return bytes;
		  }
		  
		  public synchronized void run() {
			  try {
				  while ((len = pi.read(buffer)) != -1) {
					  bytes.append(buffer, 0, len);
				  }
				  for (DataPreparedListener listener: listListener) {
					  listener.dataPrepared();
				  }
			  } catch (Exception e) {
				  logger.error(e);
			  }
		  }

		  public void addDataPreparedListener(DataPreparedListener recordPreparedListener) {
			  listListener.add(recordPreparedListener);
		  }
	}

	public static abstract class DataPreparedListener {
		public DataPreparedListener() {
		}
		public abstract void dataPrepared();
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setDictionary(Dictionary dictionary) {
		this.dictionary = dictionary;
	}

}
