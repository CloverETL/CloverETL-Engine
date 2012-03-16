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
package org.jetel.database.dbf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Calendar;

import org.jetel.data.BooleanDataField;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DateDataField;
import org.jetel.data.formatter.AbstractFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.formatter.DateFormatter;
import org.jetel.util.formatter.DateFormatterFactory;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Feb 7, 2012
 */
public class DBFDataFormatter extends AbstractFormatter {

	private static final int CONTINGENCY = 32;
	private FileChannel writer;
	private CharsetEncoder encoder;
	private ByteBuffer dataBuffer;
	private ByteBuffer fillerBuffer;
	private int recordCounter;
	private DataRecordMetadata metadata;
	private int[] fieldSizes;
	private byte[] fieldTypesDBF;
	private DateFormatter dateFormatter;
	
	public DBFDataFormatter(String charset){
		this.encoder=Charset.forName(charset).newEncoder();
	}
	
	@Override
	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
		if (_metadata.getRecType()!=DataRecordMetadata.FIXEDLEN_RECORD){
			throw new ComponentNotReadyException("Only fixed-lenght metadata allowed !");
		}
		if (_metadata.getNumFields()>DBFAnalyzer.DBF_MAX_NUMBER_OF_FIELDS){
			throw new ComponentNotReadyException("Exceeded maximum number of fields in DBase file (128) !");
			
		}
		fieldSizes=new int[_metadata.getNumFields()];
		fieldTypesDBF=new byte[_metadata.getNumFields()];

		try {
			for (int i = 0; i < _metadata.getNumFields(); i++) {
				fieldSizes[i] = DBFTypes.cloverSize2dbf(_metadata.getField(i));
				fieldTypesDBF[i] = DBFTypes.cloverType2dbf(_metadata.getField(i).getType());
				if (_metadata.getField(i).getSize() > DBFAnalyzer.DBF_FIELD_MAX_LENGTH) {
					throw new ComponentNotReadyException(String.format("Field '%s', size %i exceeds the maximum allowed size/length for DBase field (%i)", _metadata.getField(i).getName(), _metadata.getField(i).getSize(), DBFAnalyzer.DBF_FIELD_MAX_LENGTH));
				}
			}
		} catch (Exception ex) {
			throw new ComponentNotReadyException(ex.getMessage(), ex);
		}
		
		this.metadata=_metadata;
		int rec_size=_metadata.getRecordSize();
		int dbf_header_size= DBFAnalyzer.DBF_HEADER_SIZE_BASIC + 1 + metadata.getNumFields()*DBFAnalyzer.DBF_FIELD_DEF_SIZE;
		dataBuffer = ByteBuffer.allocate((rec_size > dbf_header_size ? rec_size : dbf_header_size) + CONTINGENCY );
		dataBuffer.order(ByteOrder.LITTLE_ENDIAN);
		fillerBuffer = ByteBuffer.allocate(DBFAnalyzer.DBF_FIELD_MAX_LENGTH);
		org.jetel.util.bytes.ByteBufferUtils.fill(fillerBuffer, (byte)0x20); // fill with spaces
		fillerBuffer.flip();
		
		dateFormatter= DateFormatterFactory.getFormatter(DBFTypes.DATE_FORMAT_MASK);
		recordCounter=0;
		
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		recordCounter=0;
		dataBuffer.reset();
	}

	@Override
	public void setDataTarget(Object outputDataTarget) throws IOException {
		 // close previous output
        close();
        
        // create buffered input stream reader
        if (outputDataTarget == null) {
            writer = null;
        } else if (outputDataTarget instanceof FileChannel) {
            writer = (FileChannel) outputDataTarget;
		}else {
            throw new IOException("Unsupported output data stream type: "+outputDataTarget.getClass()+". (need seekable stream).");
        }

    }


	@Override
	public void close() throws IOException {
		if (writer!=null) writer.close();
	}

	@Override
	public int write(DataRecord record) throws IOException {
		int i=0;
		int saveLimit;
		dataBuffer.clear();
		dataBuffer.put((byte)0x20); // delete flag - non deleted
		for(DataField field:record){
			saveLimit=dataBuffer.limit();
			dataBuffer.limit(dataBuffer.position()+fieldSizes[i]);
			switch (this.fieldTypesDBF[i]) {
			case DBFTypes.DBF_TYPE_DATE:
				 DateDataField d_field=(DateDataField)field;
				 encoder.encode(CharBuffer.wrap(dateFormatter.format(d_field.getDate())), dataBuffer, true);
			break;
			case DBFTypes.DBF_TYPE_CHARACTER:
				encoder.encode(CharBuffer.wrap(field.toString()), dataBuffer, true);
				break;
			case DBFTypes.DBF_TYPE_NUMBER:
				// must be right-justified
				CharBuffer cbuf=CharBuffer.wrap(field.toString());
				if (cbuf.limit() > fieldSizes[i]){ 
					cbuf.limit(fieldSizes[i]);
				}else{
					// fill in some spaces
					org.jetel.util.bytes.ByteBufferUtils.fill(dataBuffer, (byte)0x20, fieldSizes[i] - cbuf.limit(), true); // space
				}
				encoder.encode(cbuf, dataBuffer, true);
				break;
			case DBFTypes.DBF_TYPE_LOGICAL:
				BooleanDataField b_field = (BooleanDataField) field;
				if (b_field.isNull())
					dataBuffer.put((byte) '?'); // 0x3f '?"
				else if (b_field.getBoolean())
					dataBuffer.put((byte) 'T'); // 0x54 'T'
				else
					dataBuffer.put((byte) 'F'); // 0x46 'F'
				break;
			}
			
			// pad with blanks if needed
			if (dataBuffer.hasRemaining()){
				fillerBuffer.limit(dataBuffer.remaining());
				dataBuffer.put(fillerBuffer);
				fillerBuffer.limit(fillerBuffer.capacity());
				fillerBuffer.rewind();
			}
			dataBuffer.limit(saveLimit);
			i++;
		}
		dataBuffer.flip();
		recordCounter++;
		return writer.write(dataBuffer);
		
	}
	
	private void fillDBFHeader(ByteBuffer buffer){
		Calendar cal=Calendar.getInstance();
		org.jetel.util.bytes.ByteBufferUtils.fill(buffer, (byte)0x0); // fill with zeros
		// DBF TYPE  - 0x0
		buffer.position(0);
		buffer.put(DBFTypes.KNOWN_TYPES[0]); // FoxPro
		// LAST UPDATE - 0x01
		buffer.put((byte) (cal.get(Calendar.YEAR) - 1900)); //year
		buffer.put((byte) cal.get(Calendar.MONTH)); //month
		buffer.put((byte)cal.get(Calendar.DAY_OF_MONTH)); //day
		// NUM RECORDS - 0x04
		buffer.putInt(0); //Integer.MAX_VALUE); // don't know yet
		// POSITION OF FIRST DATA RECORD - 0x08
		//buffer.position(8);
		buffer.putShort((short)( DBFAnalyzer.DBF_HEADER_SIZE_BASIC + (metadata.getNumFields() * DBFAnalyzer.DBF_FIELD_DEF_SIZE) +1));
		// LENGTH OF EACH RECORD - 0x0A
		buffer.putShort((short) (getRecordSizeConverted(metadata)+1)); // cater for delete flag
		// CODEPAGE
		buffer.position(0x1D);
		buffer.put(DBFTypes.javaCodepage2dbf(this.encoder.charset().name()));
		// move to field meta position - 0x20h
		buffer.position(0x20);
		
		// field metadata
		fillDBFFieldMetadata(buffer);
		
		// HEADER TERMINATOR
		buffer.put(DBFAnalyzer.DBF_FIELD_HEADER_TERMINATOR);
				
	}

	private void fillDBFFieldMetadata(ByteBuffer buffer){
		int counter=0;
		for(DataFieldMetadata field : metadata){
			// FIELD NAME
			ByteBuffer name= encoder.charset().encode(field.getName());
			if (name.remaining() > 10) name.limit(10);
			int post=buffer.position();
			buffer.put(name);
			buffer.position(post + 10);
			buffer.put((byte)0x00);
			// FIELD TYPE - 0x0b
			buffer.put((byte)this.fieldTypesDBF[counter]);
			// FIELD ADDRESS - 0x0c
			buffer.putInt(0x0);
			// FIELD LENGTH - 0x10
			buffer.put((byte)DBFTypes.cloverSize2dbf(field));
			// FIELD DECIMAL COUNT - 0x11
			if (field.getType()==DataFieldMetadata.DECIMAL_FIELD)
				buffer.put((byte)Integer.parseInt(field.getProperty(DataFieldMetadata.SCALE_ATTR)));
			else
				buffer.put((byte)0x0);

			// VARIOUS INDICATORS (not used/populated here) just fill with zeros
			for(int i=18;i<=31;i++)
				buffer.put((byte)0x0);
			
			counter++;
			
		}
		
	}
	
	@Override
	public int writeHeader() throws IOException {
		dataBuffer.clear();
		fillDBFHeader(dataBuffer);
		dataBuffer.flip();
		int size=dataBuffer.remaining();
		writer.write(dataBuffer);
		return size;
	}

	@Override
	public int writeFooter() throws IOException {
		// put EOF mark
		dataBuffer.clear();
		dataBuffer.put(DBFAnalyzer.DBF_FILE_EOF_INDICATOR);
		dataBuffer.flip();
		writer.write(dataBuffer);
		
		// update the record count in header
		dataBuffer.clear();
		dataBuffer.putInt(recordCounter);
		dataBuffer.flip();
		// seek to rec num position
		writer.position(DBFAnalyzer.DBF_HEADER_NUM_REC_OFFSET);
		writer.write(dataBuffer);
		return Integer.SIZE;
	}

	@Override
	public void flush() throws IOException {
		writer.force(true);
	}

	@Override
	public void finish() throws IOException {
		// TODO Auto-generated method stub

	}
	
	private static int getRecordSizeConverted(DataRecordMetadata record){
		int size=0;
		for(DataFieldMetadata field : record){
			size+= DBFTypes.cloverSize2dbf(field);
		}
		return size;
	}
}
