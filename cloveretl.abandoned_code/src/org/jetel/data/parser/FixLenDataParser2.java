/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
// FILE: c:/projects/jetel/org/jetel/data/FixLenDataParser2.java

package org.jetel.data.parser;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.InvalidMarkException;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Parsing fix length data. It should be used in cases where new lines 
 *  separate records.  Supports fields up to <tt><b>FIELD_BUFFER_LENGTH</b></tt> long.
 *  Size of each individual field must be specified - through metadata definition.
 *
 *  This class is using the new IO (NIO) features introduced in Java 1.4 - directly mapped
 *  byte buffers & character encoders/decoders.  
 *
 * @author     David Pavlis,Wes Maciorowski     
 * @since    August 21, 2002
 * @see        Parser
 * @see      Defaults
 * @revision    $Revision$
 */
public class FixLenDataParser2 implements Parser {
	private boolean oneRecordPerLinePolicy = false;
	private boolean skipLeadingBlanks = true;
	private int lineSeparatorSize;
	private IParserExceptionHandler exceptionHandler;
	private ByteBuffer dataBuffer;
//	private ByteBuffer fieldBuffer;
	private CharBuffer charBuffer;
	private CharBuffer fieldStringBuffer;
	private DataRecordMetadata metadata;
	private int recordCounter;
	private int fieldLengths[];
	private int recordLength;
	private boolean skipRows=false;
	private ReadableByteChannel reader = null;
	private CharsetDecoder decoder;
	private String charSet = null;
	static Log logger = LogFactory.getLog(FixLenDataParser2.class);
	private boolean isEOF;
	
	/**
	 *Constructor for the FixLenDataParser object
	 *
	 * @since    August 21, 2002
	 */
	public FixLenDataParser2() {
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer.flip(); // initially empty 
		fieldStringBuffer = CharBuffer.allocate(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
//		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		decoder = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER).newDecoder();
		lineSeparatorSize=System.getProperty("line.separator","\n").length();
		skipRows=false;
	}
	/**
	 *Constructor for the FixLenDataParser object
	 *
	 * @param  charsetDecoder  Description of Parameter
	 * @since    August 21, 2002
	 */
	public FixLenDataParser2(String charsetDecoder) {
		charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		charBuffer.flip(); // initially empty 
		fieldStringBuffer = CharBuffer.allocate(Defaults.DataParser.FIELD_BUFFER_LENGTH);

		dataBuffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
//		fieldBuffer = ByteBuffer.allocateDirect(Defaults.DataParser.FIELD_BUFFER_LENGTH);
		this.charSet = charsetDecoder;
		decoder = Charset.forName(charsetDecoder).newDecoder();
		lineSeparatorSize=System.getProperty("line.separator","\n").length();
		skipRows=false;
	}
	/**
	 *  An operation that opens/initializes parser.
	 *
	 *@param  in         InputStream of delimited text data
	 *@param  _metadata  Metadata describing the structure of data
	 *@since             March 27, 2002
	 */
	public void open(Object in, DataRecordMetadata _metadata) {
		CoderResult result;
		this.metadata = _metadata;
		
		if (_metadata.getRecType()!=DataRecordMetadata.FIXEDLEN_RECORD){
			throw new RuntimeException("Invalid record format - is not FIXLEN !");
		}
		
		reader = ((FileInputStream) in).getChannel();
		recordLength=0;
		// create array of field sizes & initialize them
		fieldLengths = new int[metadata.getNumFields()];
		for (int i = 0; i < metadata.getNumFields(); i++) {
			fieldLengths[i] = metadata.getField(i).getSize();
			recordLength += fieldLengths[i];
		}
		decoder.reset();
		recordCounter = 1;
		if (oneRecordPerLinePolicy){
			recordLength+=lineSeparatorSize;
		}
		isEOF=false;

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
	private DataRecord parseNext(DataRecord record) throws JetelException {
		int fieldCounter = 0;
		int posCounter = 0;
		int remaining=charBuffer.remaining();

		if (remaining < recordLength){
			// need to get some data
			if (isEOF) return null;
			try {
				readRecord();
				remaining=charBuffer.remaining();
				if (remaining < recordLength){
				    if (remaining > 0 && remaining < recordLength - lineSeparatorSize){
						//- incomplete record - do something
				        StringBuffer buffer = new StringBuffer(250);
				        buffer.append("Incomplete record at the end of the stream. Expected length: ");
				        buffer.append(recordLength).append(" read data size: ").append(remaining);
				        logger.debug(buffer);
				        logger.debug("Record content:"+charBuffer.toString());
						throw new RuntimeException(buffer.toString());
					}
				}
			} catch (IOException e) {
				throw new JetelException(e.getMessage());
			}
		}

		// process the line 
		// populate all data fields
		try {
			while (fieldCounter < metadata.getNumFields()) {
				fieldStringBuffer.clear();
				char c;
				boolean skipBlanks=skipLeadingBlanks;
				for(int i=0; i < fieldLengths[fieldCounter] ; i++) {
					
					c = charBuffer.get();
					//	skip leading blanks
					if(skipBlanks && c == ' ') {
						continue;
					} 
					
					if(skipBlanks && c != ' ') {
						skipBlanks  = false;
					}
					//keep track of trailing blanks
					fieldStringBuffer.put( c );
					if( c != ' ') {
						fieldStringBuffer.mark();
					} 
				}
				try {
					fieldStringBuffer.reset();
					//fieldStringBuffer.limit(fieldStringBuffer.position()-1);
				} catch (InvalidMarkException e) {
					// not a real problem, would need new logic to get rid of 
				}
				// prepare for reading
				fieldStringBuffer.flip();
				
				// are we skipping this row/field ?
				if (!skipRows){
				    populateField(record, fieldCounter, fieldStringBuffer);
				}
				
				posCounter += fieldLengths[fieldCounter];
				fieldCounter++;
			}
			// handle EOL chars ? we just read as many chars as specified to
			// constitute line delimiter
			try{
			    if (oneRecordPerLinePolicy){
			        for(int i=0;i<lineSeparatorSize;i++) charBuffer.get();
			    }
			}catch(BufferUnderflowException ignore){
			    // just ignore it - probably EOF file reached
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex.getClass().getName()+":"+ex.getMessage());
		}

		recordCounter++;
		return record;
	}


	/**
	 * Fills the charBuffer with at least records worth of chars.
	 * 
	 * @return false if there is nothing left to read; true at least one record's worth is available
	 */
	private boolean readRecord() throws IOException {
		CoderResult decodingResult;
		int size;
		int remaining=charBuffer.remaining();
		
		dataBuffer.clear();
		if(remaining>0) {
			charBuffer.compact();
			dataBuffer.limit(dataBuffer.capacity()-remaining);
		} else {
			charBuffer.clear();
		}
		
		remaining=dataBuffer.remaining();
		size = reader.read(dataBuffer);
//		if (logger.isDebugEnabled()) {
//			logger.debug("Read: " + size);
//		}
		dataBuffer.flip();

		// if no more data - set EOF status 
		if ( size < remaining ) {
		    isEOF=true;
		}

		decodingResult=decoder.decode(dataBuffer,charBuffer,true);
		charBuffer.flip();
		
// CR, LF handled elsewhere		
//		// check if \r or \n ; if yes discard   
//		charBuffer.mark();
//		char c = charBuffer.get();
//		while (c=='\r' || c=='\n') {
//			charBuffer.mark();
//			c = charBuffer.get();
//		}
//		charBuffer.reset();
		return true;
	}

	/**
	 *  Assembles error message when exception occures during parsing
	 *
	 * @param  exceptionMessage  message from exception getMessage() call
	 * @param  recNo             recordNumber
	 * @param  fieldNo           fieldNumber
	 * @return                   error message
	 * @since                    September 19, 2002
	 */
	private String getErrorMessage(String exceptionMessage, int recNo, int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append(exceptionMessage);
		message.append(" when parsing record #");
		message.append(recordCounter);
		message.append(" field ");
		message.append(metadata.getField(fieldNo).getName());
		return message.toString();
	}

	/**
	 *  Assembles error message when exception occures during parsing
	 *
	 * @param  exceptionMessage  message from exception getMessage() call
	 * @param  recNo             recordNumber
	 * @param  fieldNo           fieldNumber
	 * @return                   error message
	 * @since                    September 19, 2002
	 */
	private String getErrorMessage(String exceptionMessage,CharSequence  value, int recNo, int fieldNo) {
		StringBuffer message = new StringBuffer();
		message.append(exceptionMessage);
		message.append(" when parsing record #").append(recordCounter);
		message.append(" field ").append(metadata.getField(fieldNo).getName());
		message.append(" value \"").append(value.toString()).append("\"");
		return message.toString();
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
	public DataRecord getNext(DataRecord record) throws JetelException {
		record = parseNext(record);
		if(exceptionHandler != null ) {  //use handler only if configured
			while(record!=null && exceptionHandler.isExceptionThrowed()) {
                exceptionHandler.handleException();
				record = parseNext(record);
			}
		}
		return record;
	}
	/**
	 *  Description of the Method
	 *
	 *@param  record    Description of Parameter
	 *@param  fieldNum  Description of Parameter
	 *@param  data      Description of Parameter
	 *@since            March 28, 2002
	 */
	private void populateField(DataRecord record, int fieldNum, CharBuffer data) {
		try {
			record.getField(fieldNum).fromString(buffer2String(data, fieldNum, false));
		} catch (BadDataFormatException bdfe) {
			if(exceptionHandler != null ) {  //use handler only if configured
                exceptionHandler.populateHandler(getErrorMessage(bdfe.getMessage(),data,recordCounter, fieldNum), record, -1, fieldNum, data.toString(), bdfe);
			} else {
				throw new RuntimeException(getErrorMessage(bdfe.getMessage(),data,recordCounter, fieldNum));
			}
		} catch (Exception ex) {
			throw new RuntimeException(getErrorMessage(ex.getMessage(), recordCounter, fieldNum));
		}
	}

	/**
	 *  Transfers CharBuffer into string and handles quoting of strings (removes quotes)
	 *
	 *@param  buffer        Character buffer to work on
	 *@param  removeQuotes  true/false remove quotation characters
	 *@return               String with quotes removed if specified
	 */
	private String buffer2String(CharBuffer buffer,int fieldNum, boolean removeQuotes) {
		if (removeQuotes && buffer.hasRemaining() &&
			metadata.getField(fieldNum).getType()== DataFieldMetadata.STRING_FIELD) {
			/* if first & last characters are quotes (and quoted is at least one character, remove quotes */
			if (buffer.charAt(0) == '\'') { 
				if (buffer.charAt(buffer.limit()-1) == '\'') {
					if (buffer.remaining()>2){
						return buffer.subSequence(1, buffer.limit() - 1).toString();
					}else{
						return ""; //empty string after quotes removed
					}
				}
			} else if (buffer.charAt(0) == '"' ) {
				if (buffer.charAt(buffer.limit()-1) == '"') {
					if ( buffer.remaining()>2){
						return buffer.subSequence(1, buffer.limit() - 1).toString();
					}else{
						return ""; //empty string after quotes removed
					}
				}
			}
		}
		return buffer.toString();
	}
	/**
	 *  Gets the Next attribute of the FixLenDataParser object
	 *
	 * @return                  The Next value
	 * @exception  IOException  Description of Exception
	 * @since                   August 21, 2002
	 */
	public DataRecord getNext() throws JetelException {
		// create a new data record
		DataRecord record = new DataRecord(metadata);
		record.init();
		return parseNext(record);
	}

	/**
	 *  Description of the Method
	 *
	 * @since    August 21, 2002
	 */
	public void close() {
		if (reader != null) {
			try {
				reader.close();
			}
			catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	/**
	 *  Sets OneRecordPerLinePolicy - if set to true, then the parser assumes that
	 * each record is on separate line - i.e. at the end of each record, the newline
	 * characeter(s) is present. This character gets automatically removed.
	 * @see org.jetel.data.formatter.Formatter#setOneRecordPerLinePolicy(boolean)
	 */
	public void setOneRecordPerLinePolicy(boolean b) {
		oneRecordPerLinePolicy = b;
	}

	/**
	 * Specifies whether leading blanks at each field should be skipped
	 * @param skippingLeadingBlanks The skippingLeadingBlanks to set.
	 */
	public void setSkipLeadingBlanks(boolean skipLeadingBlanks) {
		this.skipLeadingBlanks = skipLeadingBlanks;
	}
	/**
	 * Sets the size/length of line delimiter. It is 1 for "\n" - UNIX style
	 * and 2 for "\n\r" - DOS/Windows style. Can be set to any value and is added
	 * to total record length.<br>
	 * It is automatically determined from system properties. This method overrides
	 * the default value. 
	 * @param lineDelimiterSize The lineDelimiterSize to set.
	 */
	public void setLineSeparatorSize(int lineDelimiterSize) {
		this.lineSeparatorSize = lineDelimiterSize;
	}
	
	/**
	 * 
	 * @return Charset name or null if none was specified
	 */
	public String getCharsetName() {
		return(this.charSet);
	}
	
	/**
	 * Returns data policy type for this parser
	 * @return Data policy type or null if none was specified
	 */
	public PolicyType getPolicyType() {
		if (this.exceptionHandler != null) {
			return this.exceptionHandler.getType();
		} else {
			return null;
		}		
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean getOneRecordPerLinePolicy() {
		return(this.oneRecordPerLinePolicy);
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean getSkipLeadingBlanks() {
		return(this.skipLeadingBlanks);
	}
	
	/**
	 * 
	 * @return
	 */
	public int getLineSeparatorSize() {
		return(this.lineSeparatorSize);
	}
	
    /**
     * @return Returns the skipRows.
     */
    public boolean getSkipRows() {
        return skipRows;
    }
    /**
     * @param skipRows The skipRows to set.
     */
    public void setSkipRows(boolean skipRows) {
        this.skipRows = skipRows;
    }
    public void setExceptionHandler(IParserExceptionHandler handler) {
        this.exceptionHandler = handler;
    }
    public IParserExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }
    
	public int skip(int nRec) throws JetelException {
		throw new UnsupportedOperationException("Not yet implemented");
	}

	public void setDataSource(Object inputDataSource) {
		throw new UnsupportedOperationException();
	}

}
