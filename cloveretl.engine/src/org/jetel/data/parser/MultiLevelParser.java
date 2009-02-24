package org.jetel.data.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;

public class MultiLevelParser implements Parser {

	static Log logger = LogFactory.getLog(MultiLevelParser.class);

	private String charset;
	private MultiLevelSelector selector;
	private int selectorLookAheadChars;
	private int selectorSkipChars;
	private DataRecordMetadata[] metadataPool;

	private DataRecordMetadata lastRecordMetadata;
	private int lastRecordMetadataIndex;
	private Parser lastRecordParser;
	
	private Parser[] parserPool;
	private Properties properties;
	private boolean releaseDataSource = true;
	
	private ReadableByteChannel reader;
	private CharsetDecoder decoder;
	private boolean isEof = false;
	private long readCharsTotal = 0;
	
	private ByteBuffer dataBuffer;
	private CharBuffer charBuffer;

	private IParserExceptionHandler exceptionHandler;
	
	public MultiLevelParser(String charset, MultiLevelSelector selector, DataRecordMetadata[] metadata, Properties properties) {
		this.selector = selector;
		this.metadataPool = metadata;
		this.properties = properties;
		setCharset(charset);
	}
	
	void setCharset(String charset) {
		this.charset = charset;
	}
	
	public void close() {
		releaseDataSource();
	}

	public DataRecord getNext() throws JetelException {
		DataRecord result = getNext(null);
		if(exceptionHandler != null ) {  //use handler only if configured
			while(exceptionHandler.isExceptionThrowed()) {
                exceptionHandler.handleException();
				result = getNext(null);
			}
		}
		return result; 
	}

	/**
	 * This method does a few things:
	 * - takes a look into the file and passes the sample to a TypeSelector
	 * - the selector tries its best to pick proper metadata (returns an index to this.metadataPool
	 * - having the right metadata, we can either ensure the whole record in buffer if fixed length, or compact a reload the buffer otherwise
	 * - then we can parse the data in it according to 
	 */
	public DataRecord getNext(DataRecord record) throws JetelException {
		
		try {
			int readChars; // helper var
			
			selectorLookAheadChars = selector.lookAheadCharacters();
			
			// sneak peek
			// read as many bytes as our typeselector requests
			if (charBuffer.remaining() < selectorLookAheadChars) {
				readChars = readChars();
				if (readChars <= 0) {
					return null;
				}
			}

			int startPosition = charBuffer.position();

			boolean successfulChoice = false;
			selector.reset();
			do {

				try {
					selector.choose(charBuffer);
					successfulChoice = true;
				} catch (BufferUnderflowException e1) {
					// selector claims to have insufficient data
					// let's read some and try re-choose
					// note: if we data is re-read more than once there is no chance how to get back to them
					// we can only operate on single charbuffer!
					// but this shouldn't be an issue
					charBuffer.position(startPosition);
					readChars = readChars();
					if (readChars <= 0) {
						return null;
					}
					startPosition = charBuffer.position();
				}

			} while (!successfulChoice);

			lastRecordMetadataIndex = selector.nextRecordMetadataIndex();
			
			if (lastRecordMetadataIndex < 0) {
				// selector claims that it is unable to determine the record type
				PolicyType policy = getPolicyType();
				if (policy.equals(PolicyType.STRICT)) {
					throw new RuntimeException("MultiLevelParser: Unable to parse input at character #" + readCharsTotal);
				} else {
					// lets try to recover
					successfulChoice = false; // abuse
					boolean recovered = false;
					do {
						try {
							recovered = selector.recoverToNextRecord(charBuffer);
							if (! recovered) {
								throw new RuntimeException("MultiLevelParser: Unable to recover from bad input at charackter #" + readCharsTotal);
							}
							successfulChoice = true;
						} catch (BufferUnderflowException e1) {
							charBuffer.position(startPosition);
							readChars = readChars();
							if (readChars <= 0) {
								return null;
							}
							startPosition = charBuffer.position();
						}

					} while (!successfulChoice);
				}
				throw new BadDataFormatException("Unable to determine data record type");
			}

			lastRecordMetadata = this.metadataPool[lastRecordMetadataIndex];
			lastRecordParser = this.parserPool[lastRecordMetadataIndex];
			selectorSkipChars = selector.nextRecordOffset();

			// rewind to starting character of the record (and skip some if required)
			charBuffer.position(startPosition + selectorSkipChars);

			if (record == null) {
				record = new DataRecord(lastRecordMetadata);
				record.init();
			} else if (! record.getMetadata().equals(lastRecordMetadata)) {
				record.setMetadata(lastRecordMetadata);
				record.init();
			}

			// now this is the tricky part... 
			// use appropriate Parser to parse this data part

			try {
				return lastRecordParser.getNext(record);
			} catch (BadDataFormatException e) {
				// this can mean that a record data is on buffer boundary
				// we can try to load more data and retry.. if no more data is available or parsing fails anyways, we've got ourselves a problem
				charBuffer.position(startPosition);
				readChars = readChars();
				if (readChars <= 0) {
					// no more characters so the error is justified
					throw e;
				}
				// retry and don't bother catching the exception again because there's nothing more we can do 
				return lastRecordParser.getNext(record);
			}

		} catch (IOException e) {
			throw new JetelException(e.getMessage(), e);
		}

	}

	int readChars() throws IOException {
        CoderResult result;
        
        dataBuffer.compact();
        charBuffer.compact();
        
        if (reader.read(dataBuffer) == -1) {
            isEof = true;
        }        
        dataBuffer.flip();

        if (dataBuffer.remaining() == 0) {
        	return 0;
        }
        
        result = decoder.decode(dataBuffer, charBuffer, isEof);
        if (result == CoderResult.UNDERFLOW) {
            // try to load additional data
            dataBuffer.compact();

            if (reader.read(dataBuffer) == -1) {
                isEof = true;
            }
            dataBuffer.flip();
            decoder.decode(dataBuffer, charBuffer, isEof);
        } else if (result.isError()) {
            throw new IOException(result.toString()+" when converting from "+decoder.charset());
        }
        if (isEof) {
            result = decoder.flush(charBuffer);
            if (result.isError()) {
                throw new IOException(result.toString()+" when converting from "+decoder.charset());
            }
        }
        charBuffer.flip();
        readCharsTotal += charBuffer.remaining();
        return charBuffer.remaining();
	}
	
	

	public Object getPosition() {
		// TODO Auto-generated method stub
		return null;
	}

	public void init() throws ComponentNotReadyException {
		init(null);
	}
	
	/**
	 * A standard init, but we don't support passing in any metadata
	 * since metadata is kept in this.metadataPool and picked dynamically by TypeSelector
	 */
	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
		if (_metadata != null) { 
			throw new IllegalArgumentException("This parser doesn't allow metadata specification");
		}
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		dataBuffer.flip(); // we must do this, first operation on the buffer is a read attempt
		charBuffer.flip(); // we must do this, first operation on the buffer is a read attempt
		decoder = Charset.forName(charset != null ? charset : Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
		selector.init(this.metadataPool, this.properties);
	}

	public void movePosition(Object position) throws IOException {
		// TODO Auto-generated method stub
	}

	public void reset() throws ComponentNotReadyException {
		if (releaseDataSource) {
			releaseDataSource();
		}
		dataBuffer.reset();
		charBuffer.reset();
	}

	public void setDataSource(Object inputDataSource) throws ComponentNotReadyException {
		if (this.releaseDataSource) {
			releaseDataSource();
		}
		
		if (inputDataSource == null) {
			isEof = true;
		} else {
			isEof = false;
			if (inputDataSource instanceof ReadableByteChannel) {
				reader = ((ReadableByteChannel)inputDataSource);
			} else if (inputDataSource instanceof InputStream){
				reader = Channels.newChannel((InputStream)inputDataSource);
			} else if (inputDataSource instanceof CharBuffer) {
				reader = null;
				charBuffer = (CharBuffer) inputDataSource;
			}
			
			initParsers();
			
		}
		
	}

	/**
	 * This method assumes the metadataPool to be set correctly
	 * and that a charBuffer is available
	 * For each metadata in the pool it constructs a parser appropriately and puts it to parserPool
	 */
	void initParsers() throws ComponentNotReadyException {
		
		if (this.metadataPool == null || this.metadataPool.length == 0) {
			this.parserPool = null;
			return;
		}
		
		this.parserPool = new Parser[this.metadataPool.length];
		
		DataRecordMetadata metadata;
		
		for(int i = 0; i < this.metadataPool.length; i++) {
			metadata = this.metadataPool[i];
			if (metadata.getRecType() == DataRecordMetadata.DELIMITED_RECORD || metadata.getRecType() == DataRecordMetadata.FIXEDLEN_RECORD) {
				this.parserPool[i] = new DataParser(this.charset != null ? this.charset : Defaults.DataParser.DEFAULT_CHARSET_DECODER);
				this.parserPool[i].init(metadata);
				this.parserPool[i].setDataSource(this.charBuffer);
			} else {
				// we cannot work with this kind of metadata
				throw new ComponentNotReadyException("Metadata type '" + metadata.getRecType() + "' is not supported.");
			}
		}
		
	}
	
	
	void releaseDataSource() {
		if (this.reader != null) {
			try {
				this.reader.close();
			} catch (IOException e) {
				logger.debug("Error releasing data source: " +e.getMessage());
			}
		}
	}

	public PolicyType getPolicyType() {
		if (this.exceptionHandler != null) {
			return this.exceptionHandler.getType();
		} else {
			return null;
		}
	}
	

    public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
    }


    public IParserExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

	public void setReleaseDataSource(boolean releaseInputSource) {
		this.releaseDataSource = releaseInputSource;
	}

	public int skip(int nRec) throws JetelException {
		int skipped;
		DataRecord rec = new DataRecord(null);
		for (skipped = 0; skipped < nRec; skipped++) {
			if (getNext(rec) == null) {	// end of file reached
				break;
			}
		}
		return skipped;
	}

	
	public int getTypeIdx() {
		return lastRecordMetadataIndex;
	}
	
}
