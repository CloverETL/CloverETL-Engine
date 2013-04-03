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
package org.jetel.data.parser;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;
import org.jetel.util.string.QuotingDecoder;
import org.jetel.util.string.StringUtils;

/**
 *  Parsing delimited text data. Supports delimiters with the length of up to 32
 *  characters. Delimiter for each individual field must be specified - through
 *  metadata definition. Parser handles quoted strings (single or double).
 *  This class is using the new IO (NIO) features
 *  introduced in Java 1.4 - directly mapped byte buffers & character
 *  encoders/decoders
 *
 *@author     D.Pavlis
 *@since      March 27, 2002
 *@see        Parser
 *@see 	      org.jetel.data.Defaults
 */
public class DelimitedDataParser extends AbstractParser {

	static Log logger = LogFactory.getLog(DelimitedDataParser.class);
	
	private String charSet = null;
	private IParserExceptionHandler exceptionHandler;
	private ByteBuffer dataBuffer;
	private CharBuffer charBuffer;
	private StringBuilder fieldStringBuffer;
	private char[] delimiterCandidateBuffer;
	private DataRecordMetadata metadata;
	private ReadableByteChannel reader;
	private CharsetDecoder decoder;
	private int recordCounter;
	private char[][] delimiters;
	private boolean[] eofAsDelimiters;
	private DataFieldType[] fieldTypes;
	private boolean[] isAutoFilling;
	private boolean isEof;
	private int bytesProcessed;
	
	private Boolean trim = null; 
	private Boolean skipLeadingBlanks = null;
	private Boolean skipTrailingBlanks = null;
	private boolean[] isSkipLeadingBlanks;
	private boolean[] isSkipTrailingBlanks;

	// Attributes
	// maximum length of delimiter
	private final static int DELIMITER_CANDIDATE_BUFFER_LENGTH = 32;

	//private static final String EOF_DELIMITER = "EOF";
	
	private QuotingDecoder qdecoder;
	
	public DelimitedDataParser(DataRecordMetadata metadata) {
		this(metadata,Defaults.DataParser.DEFAULT_CHARSET_DECODER, new QuotingDecoder());		
	}
	// Associations

	// Operations
	/**
	 *  Constructor for the DelimitedDataParser object. With default size and
	 *  default character encoding.
	 *
	 *@since    March 28, 2002
	 */
	public DelimitedDataParser(DataRecordMetadata metadata, QuotingDecoder qdecoder) {
		this(metadata, Defaults.DataParser.DEFAULT_CHARSET_DECODER, qdecoder);
	}


	public DelimitedDataParser(DataRecordMetadata metadata, String charsetDecoder) {
		this(metadata, charsetDecoder, new QuotingDecoder());		
	}

	/**
	 *  Constructor for the DelimitedDataParser object
	 *
	 *@param  charsetDecoder  Charset Decoder used for converting input data into
	 *      UNICODE chars
	 *@since                  March 28, 2002
	 */
	public DelimitedDataParser(DataRecordMetadata metadata, String charsetDecoder, QuotingDecoder qdecoder) {
		this.metadata = metadata;
		this.charSet = charsetDecoder;
		this.qdecoder = qdecoder;
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
        charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		fieldStringBuffer = new StringBuilder(Defaults.Record.FIELD_INITIAL_SIZE);
		delimiterCandidateBuffer = new char [DELIMITER_CANDIDATE_BUFFER_LENGTH];
		decoder = Charset.forName(charsetDecoder).newDecoder();
	}


	/**
	 *  Returs next data record parsed from input stream or NULL if no more data
	 *  available
	 *
	 *@return                  The Next value
	 *@exception  IOException  Description of Exception
	 *@since                   May 2, 2002
	 */
	@Override
	public DataRecord getNext() throws JetelException {
		// create a new data record
		DataRecord record = DataRecordFactory.newRecord(metadata);

		record.init();

		boolean success = parseNext(record);
		if(exceptionHandler != null ) {  //use handler only if configured
			while(exceptionHandler.isExceptionThrowed()) {
                exceptionHandler.handleException();
				success = parseNext(record);
			}
		}
		return success ? record : null;
	}


	/**
	 *  Returs next data record parsed from input stream or NULL if no more data
	 *  available The specified DataRecord's fields are altered to contain new
	 *  values.
	 *
	 *@param  record           Description of Parameter
	 *@return                  The Next value
	 *@exception  IOException  Description of Exception
	 *@since                   May 2, 2002
	 */
	@Override
	public DataRecord getNext(DataRecord record) throws JetelException {
		return getNext0(record) ? record : null; 
	}

	private boolean getNext0(DataRecord record) throws JetelException {
		boolean retval = parseNext(record);
		if(exceptionHandler != null ) {  //use handler only if configured
			while(exceptionHandler.isExceptionThrowed()) {
                exceptionHandler.handleException();
				//record.init();   //redundant
				retval = parseNext(record);
			}
		}
		return retval;
	}


	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#init(org.jetel.metadata.DataRecordMetadata)
	 */
	@Override
	public void init()
	throws ComponentNotReadyException {
		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata are null");
		}
		if (metadata.getParsingType() != DataRecordParsingType.DELIMITED) {
			throw new ComponentNotReadyException("Delimited data expected but not encountered");
		}        
        DataFieldMetadata fieldMetadata;

		// create array of delimiters & initialize them
		delimiters = new char[metadata.getNumFields()][];
		eofAsDelimiters = new boolean[metadata.getNumFields()];
		fieldTypes = new DataFieldType[metadata.getNumFields()];
		isAutoFilling = new boolean[metadata.getNumFields()];
		isSkipLeadingBlanks = new boolean[metadata.getNumFields()];
		isSkipTrailingBlanks = new boolean[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			fieldMetadata = metadata.getField(i);
			String[] tempDelimiters = fieldMetadata.getDelimiters();
			if (tempDelimiters != null && tempDelimiters.length > 0) { //that could happen in case 'EOF as delimiter' setting
				delimiters[i] = tempDelimiters[0].toCharArray();
			}
			eofAsDelimiters[i] = fieldMetadata.isEofAsDelimiter();
			fieldTypes[i] = fieldMetadata.getDataType();
			isAutoFilling[i] = fieldMetadata.getAutoFilling() != null;
			// we handle only one character delimiters
			isSkipLeadingBlanks[i] = skipLeadingBlanks != null ? skipLeadingBlanks : trim != null ? trim : metadata.getField(i).isTrim();
			isSkipTrailingBlanks[i] = skipTrailingBlanks != null ? skipTrailingBlanks : trim != null ? trim : metadata.getField(i).isTrim();
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#setDataSource(java.lang.Object)
	 */
	@Override
	public void setDataSource(Object inputDataSource) {
		if (releaseDataSource)	releaseDataSource();

		decoder.reset();// reset CharsetDecoder
		dataBuffer.clear();
        dataBuffer.flip();
		charBuffer.clear();
		charBuffer.flip();
		recordCounter = 1;// reset record counter
		bytesProcessed = 0;

		if (inputDataSource == null) {
			isEof = true;
		} else {
			isEof = false;
			if (inputDataSource instanceof ReadableByteChannel) {
				reader = ((ReadableByteChannel)inputDataSource);
			} else {
				reader = Channels.newChannel((InputStream)inputDataSource);
			}
		}
	}

	/**
	 * Discard bytes for incremental reading.
	 * 
	 * @param bytes
	 * @throws IOException 
	 */
	private void discardBytes(int bytes) throws IOException {
		while (bytes > 0) {
			if (reader instanceof FileChannel) {
				((FileChannel)reader).position(bytes);
				return;
			}
			dataBuffer.clear();
			if (bytes < Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE) dataBuffer.limit(bytes);
			try {
				reader.read(dataBuffer);
			} catch (IOException e) {
				break;
			}
			bytes =- Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE;
		}
		dataBuffer.clear();
        dataBuffer.flip();
	}
	
	/**
	 * Release data source
	 *
	 */
	@Override
	protected void releaseDataSource() {
		if (reader == null || !reader.isOpen()) {
			return;
		}
		try {
			reader.close();
		} catch (IOException e) {
			logger.error(e.getStackTrace());
		}
		reader = null;		
	}

	/**
	 *  Release resources
	 *
	 *@since    May 2, 2002
	 */
	@Override
	public void close() {
		releaseDataSource();
	}


	/**
	 *  Assembles error message when exception occures during parsing
	 *
	 *@param  exceptionMessage  message from exception getMessage() call
	 *@param  recNo             recordNumber
	 *@param  fieldNo           fieldNumber
	 *@return                   error message
	 *@since                    September 19, 2002
	 */
	private String getErrorMessage(CharSequence value, int recNo, int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append("Error when parsing record #");
		message.append(recordCounter);
		message.append(" field ");
		message.append(metadata.getField(fieldNo).getName());
		if (value!=null){
			message.append(" value \"").append(value).append("\"");
		}
		return message.toString();
	}


	/**
     * Description of the Method
     * 
     * @return Description of the Returned Value
     * @exception IOException
     *                Description of Exception
     * @since May 13, 2002
     */
    private int readChar() throws IOException {
        CoderResult result;

        if (charBuffer.hasRemaining()) {
            return charBuffer.get();
        }

        if (isEof)
            return -1;

        charBuffer.clear();
        if (dataBuffer.hasRemaining())
            dataBuffer.compact();
        else
            dataBuffer.clear();

        int i;
        if ((i = reader.read(dataBuffer)) == -1) {
            isEof = true;
        } else {
        	bytesProcessed += i;
        }        
        dataBuffer.flip();

        result = decoder.decode(dataBuffer, charBuffer, isEof);
        if (result == CoderResult.UNDERFLOW) {
            // try to load additional data
            dataBuffer.compact();

            if ((i = reader.read(dataBuffer)) == -1) {
                isEof = true;
            } else {
            	bytesProcessed += i;
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
        return charBuffer.hasRemaining() ? charBuffer.get() : -1;

    }
	


	/**
	 *  An operation that does ...
	 *
	 *@param  record           Description of Parameter
	 *@return                  Next DataRecord (parsed from input data) or null if
	 *      no more records available
	 *@exception  IOException  Description of Exception
	 *@since                   March 27, 2002
	 */
	public boolean parseNext(DataRecord record) throws JetelException {
		int result;
		int fieldCounter = 0;
		int character;
		int totalCharCounter = 0;
		int delimiterPosition;
		int charCounter;
		boolean isWithinQuotes;

		// populate all data fields

		while (fieldCounter < metadata.getNumFields()) {
			// skip all fields that are internally filled 
			if (isAutoFilling[fieldCounter]) {
				fieldCounter++;
				continue;
			}
			
			// we clear our buffer
			fieldStringBuffer.setLength(0);
			character = 0;
			isWithinQuotes=false;
			boolean eofDelimiter = false;
			// read data till we reach delimiter, end of file or exceed buffer size
			// exceeded buffer is indicated by BufferOverflowException
			charCounter = 0;
			delimiterPosition = 0;
			try {
				while ((character = readChar()) != -1) {
//					causes problem when composed delimiter "\r\n" is used
//					if(character=='\r')  //fix for new line being \r\n
//						continue;
					totalCharCounter++;
					// handle quoted strings
					// TODO ignore quotes in case they are preceded by escape character
					if (isWithinQuotes) {
						/* TODO complain in case that the closing  quote isn't at the end of the field
						or consider unescaped closing quote to be end of the field */
						if (qdecoder.isEndQuote((char)character)) {
							isWithinQuotes = false;
						}
					} else {
						if (qdecoder.isStartQuote((char)character) && charCounter == 0) {
					        isWithinQuotes=true;
				        }
					}
					if ((result = is_delimiter((char) character, fieldCounter, delimiterPosition,isWithinQuotes)) == 1) {
						/*
						 *  DELIMITER
						 */
						break;
					} else if (result == 0) {
						/*
						 *  NOT A DELIMITER
						 */
						if (delimiterPosition > 0) {
							fieldStringBuffer.append(delimiterCandidateBuffer,0,delimiterPosition);
						} else {
                            try{
                                fieldStringBuffer.append((char) character);
                            }catch(BufferOverflowException ex){
                                throw new IOException(
										"Field too long or can not find delimiter ["
                                		+ (delimiters[fieldCounter] != null 
                                				? 
                                				StringUtils.specCharToString(String.valueOf(delimiters[fieldCounter]))
                                				:
                                				"EOF as delimiter")
                                		+ "]");
                            }
						}
						delimiterPosition = 0;
					} else {
						/*
						 *  CAN'T DECIDE DELIMITER
						 */
						delimiterCandidateBuffer[delimiterPosition]=((char) character);
						delimiterPosition++;
					}
					charCounter++;
				}// while
				if ((character == -1) && (totalCharCounter > 1)) {
					// EOF
					// is EOF delimiter specified for this field?
					if (eofAsDelimiters[fieldCounter]){
						eofDelimiter = true;
					} else {
						//- incomplete record - do something
						BadDataFormatException exception = new BadDataFormatException("Incomplete record");
						exception.setRecordNumber(recordCounter);
						exception.setFieldNumber(fieldCounter);
						throw exception;
					}
				}// if
			} catch (BadDataFormatException ex) {
                throw ex;
			} catch (Exception ex) {
				throw new RuntimeException(getErrorMessage(null, recordCounter, fieldCounter), ex);
			}

			// did we have EOF situation ?
			if (character == -1) {
				try {
					reader.close();
				} catch (IOException e) {
					throw new JetelException(e);
				}
				// if not eofDelimiter, then skip this record 
				if (!eofDelimiter)
					return false;
			}

			// set field's value
			// are we skipping this row/field ?
			if (record != null){
			    if (isSkipLeadingBlanks[fieldCounter]) {
			    	StringUtils.trimLeading(fieldStringBuffer);
			    }
			    if (isSkipTrailingBlanks[fieldCounter]){
			    	StringUtils.trimTrailing(fieldStringBuffer);
			    }
			    populateField(record, fieldCounter, fieldStringBuffer);
			}
			fieldCounter++;
		}
		recordCounter++;
		return true;
	}


	/**
	 *  Description of the Method
	 *
	 *@param  record    Description of Parameter
	 *@param  fieldNum  Description of Parameter
	 *@param  data      Description of Parameter
	 *@since            March 28, 2002
	 */
	private void populateField(DataRecord record, int fieldNum, StringBuilder data) {
        CharSequence strData = buffer2String(data, fieldNum);
        try {
			record.getField(fieldNum).fromString(strData);
		} catch (BadDataFormatException bdfe) {
			if(exceptionHandler != null ) {  //use handler only if configured
				bdfe.setRecordNumber(recordCounter);
                bdfe.setFieldNumber(fieldNum);
                bdfe.setOffendingValue(strData);
                exceptionHandler.populateHandler(getErrorMessage(data, recordCounter, fieldNum), record,
						-1, fieldNum, strData.toString(), bdfe);
			} else {
                bdfe.setRecordNumber(recordCounter);
                bdfe.setFieldNumber(fieldNum);
                bdfe.setOffendingValue(strData);
                throw bdfe;
			}
		} catch (Exception ex) {
			throw new RuntimeException(getErrorMessage(null,recordCounter, fieldNum),ex);
		}
	}


	/**
	 *  Transfers CharBuffer into string and handles quoting of strings (removes quotes)
	 *
	 *@param  buffer        Character buffer to work on
	 *@return               String with quotes removed if specified
	 */
	private CharSequence buffer2String(StringBuilder buffer, int fieldNum) {
		if (fieldTypes[fieldNum] != DataFieldType.BYTE &&
			fieldTypes[fieldNum] != DataFieldType.CBYTE) {
			return qdecoder.decode(buffer);
		}
	    return buffer;	    
	}


	/**
	 *  Decides whether delimiter was encountered 
	 *
	 *@param  character          character to compare with delimiter
	 *@param  fieldCounter       delimiter for which field
	 *@param  delimiterPosition  current position within delimiter string
	 *@return                    1 if delimiter matched; -1 if can't decide yet; 0 if not part of delimiter
	 */
	private int is_delimiter(char character, int fieldCounter, int delimiterPosition, boolean isWithinQuotes) {
		if (isWithinQuotes){
		    return 0;
		}
	    if (delimiters[fieldCounter] != null //'EOF as delimiter' 
	    		&& character == delimiters[fieldCounter][delimiterPosition]) {
			if (delimiterPosition == delimiters[fieldCounter].length - 1) {
				return 1;
				// whole delimiter matched
			} else {
				return -1;
				// can't decide
			}
		} else {
			return 0;
			// not a match
		}
	}

	/**
	 * Returns charset name of this parser
	 * @return Returns name of the charset used to construct or null if none was specified
	 */
	public String getCharsetName() {
		return(this.charSet);
	}
	
	/**
	 * Returns data policy type for this parser
	 * @return Data policy type or null if none was specified
	 */
	@Override
	public PolicyType getPolicyType() {
		if (this.exceptionHandler != null) {
			return this.exceptionHandler.getType();
		} else {
			return null;
		}
	}

    @Override
	public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
    }


    @Override
	public IParserExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }


	/**
	 * Skip records.
	 * @param nRec Number of records to be skipped
	 * @return Number of successfully skipped records.
	 * @throws JetelException
	 */
	@Override
	public int skip(int nRec) throws JetelException {
		int skipped;
		for (skipped = 0; skipped < nRec; skipped++) {
			if (!getNext0(null)) {	// end of file reached
				break;
			}
		}
		return skipped;
	}
	
	public Boolean getTrim() {
		return trim;
	}

	public void setTrim(Boolean trim) {
		this.trim = trim;
	}

	public void setSkipLeadingBlanks(Boolean skipLeadingBlanks) {
		this.skipLeadingBlanks = skipLeadingBlanks;
	}

	public void setSkipTrailingBlanks(Boolean skipTrailingBlanks) {
		this.skipTrailingBlanks = skipTrailingBlanks;
	}

    /**
	 * Reset parser for next graph execution. 
     */
	@Override
	public void reset() {
		if (releaseDataSource)	
			releaseDataSource();

		decoder.reset();// reset CharsetDecoder
		dataBuffer.clear();
        dataBuffer.flip();
		charBuffer.clear();
		charBuffer.flip();
		recordCounter = 0;// reset record counter
		bytesProcessed = 0;
	}

	@Override
	public Object getPosition() {
		return bytesProcessed;
	}

	@Override
	public void movePosition(Object position) throws IOException {
		int pos = 0;
		if (position instanceof Integer) {
			pos = ((Integer) position).intValue();
		} else if (position != null) {
			pos = Integer.parseInt(position.toString());
		}
		if (pos > 0) {
			discardBytes(pos);
			bytesProcessed = pos;
		}
	}
	
	@Override
    public void preExecute() throws ComponentNotReadyException {
    }
    
	@Override
    public void postExecute() throws ComponentNotReadyException {    	
    	reset();
    }
    
	@Override
    public void free() {
    	close();
    }

	@Override
	public boolean nextL3Source() {
		return false;
	}
	
}	
/*
 *  end class DelimitedDataParser
 */

