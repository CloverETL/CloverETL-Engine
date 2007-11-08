package org.jetel.util;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.WritableByteChannel;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.formatter.provider.FormatterProvider;
import org.jetel.exception.ComponentNotReadyException;
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
    	initUrl();
    	if (fileTag == null) {
    		initFileNames(null);
    	} 
    	else if (fileTag instanceof Number) {
    		initFileNames(format.format((Number)fileTag));
    	} else {
    		initFileNames(fileTag.toString());
    	}
    	initOutput();
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
     * Devides fileURL to two string. The first one is a string before '#' mark, the second one is
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

    /**
     * The method writes footer and header and sets next output to the formatter.
     * 
     * @throws IOException
     */
    public void setNextOutput() throws IOException {
    	checkOutput();
    	
        //write footer to the previous destination if it is not first call of this method
        if(byteChannel != null) {
        	formatter.writeFooter();
        }
        setOutput();

        //write header
        formatter.writeHeader();
        bytes = records = 0;
    }

    /**
     * Closes underlying formatter.
     */
    public void close() {
    	try {
			formatter.writeFooter();
		} catch (IOException e) {
			logger.error(e);
		}
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
        if (fileNames != null) {
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

}
