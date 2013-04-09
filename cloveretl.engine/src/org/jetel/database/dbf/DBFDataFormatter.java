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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.jetel.data.BooleanDataField;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DateDataField;
import org.jetel.data.formatter.AbstractFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordParsingType;
import org.jetel.util.bytes.ByteBufferUtils;
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
	private static final String FILE_ACCESS_MODE = "rw";
	
	private final byte dbfType;
	
	private FileChannel writer;
	private CharsetEncoder encoder;
	private ByteBuffer dataBuffer;
	private ByteBuffer fillerBuffer;
	private int recordCounter;
	private DataRecordMetadata metadata;
	private int[] fieldSizes;
	private byte[] fieldTypesDBF;
	private Set<String> excludedFieldNames = Collections.emptySet();
	private DateFormatter dateFormatter;
	
	/**
	 * Constructor.
	 * 
	 * @param charset Char set of the formatter.
	 */
	public DBFDataFormatter(String charset, byte dbfType){
		this.encoder=Charset.forName(charset).newEncoder();
		this.dbfType = dbfType;
	}
	
	@Override
	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
		int countOfNotExcludedFields = getCountOfNotExcludedFields(_metadata);
		if (_metadata.getParsingType() != DataRecordParsingType.FIXEDLEN){
			throw new ComponentNotReadyException("Component DBFWriter supports only fixed-length metadata on input port.");
		}
		if (countOfNotExcludedFields > DBFAnalyzer.DBF_MAX_NUMBER_OF_FIELDS){
			throw new ComponentNotReadyException("Exceeded maximum number of fields in DBase file (128) !");
			
		}
		fieldSizes=new int[_metadata.getNumFields()];
		fieldTypesDBF=new byte[_metadata.getNumFields()];

		try {
			for (int i = 0; i < _metadata.getNumFields(); i++) {
				fieldSizes[i] = DBFTypes.cloverSize2dbf(_metadata.getField(i));
				fieldTypesDBF[i] = DBFTypes.cloverType2dbf(_metadata.getField(i).getDataType());
				if (_metadata.getField(i).getSize() > DBFAnalyzer.DBF_FIELD_MAX_LENGTH) {
					throw new ComponentNotReadyException(String.format("Field '%s', size %d exceeds the maximum allowed size/length for DBase field (%d)", _metadata.getField(i).getName(), _metadata.getField(i).getSize(), DBFAnalyzer.DBF_FIELD_MAX_LENGTH));
				}
			}
		} catch (Exception ex) {
			throw new ComponentNotReadyException(ex);
		}
		
		this.metadata = _metadata;
		int rec_size = getRecordSize(metadata);
		int dbf_header_size= DBFAnalyzer.DBF_HEADER_SIZE_BASIC + 1 + countOfNotExcludedFields * DBFAnalyzer.DBF_FIELD_DEF_SIZE;
		dataBuffer = ByteBuffer.allocate((rec_size > dbf_header_size ? rec_size : dbf_header_size) + CONTINGENCY );
		dataBuffer.order(ByteOrder.LITTLE_ENDIAN);
		fillerBuffer = ByteBuffer.allocate(DBFAnalyzer.DBF_FIELD_MAX_LENGTH);
		org.jetel.util.bytes.ByteBufferUtils.fill(fillerBuffer, (byte)0x20); // fill with spaces
		fillerBuffer.flip();
		
		dateFormatter= DateFormatterFactory.getFormatter(DBFTypes.DATE_FORMAT_MASK);
		resetRecordCounter();
		
	}

	@Override
	public void reset() {
		resetRecordCounter();
		dataBuffer.mark();
		dataBuffer.reset();
	}

	@Override
	public void setDataTarget(Object outputDataTarget) throws IOException {
		 // close previous output
        close();
        
        // create buffered input stream reader
        if (outputDataTarget == null) {
            writer = null;
        } else if (outputDataTarget instanceof File) {
			writer = new RandomAccessFile(((File) outputDataTarget), FILE_ACCESS_MODE).getChannel();
		} else if (outputDataTarget instanceof FileChannel) {
			writer = (FileChannel) outputDataTarget;
		}
        else {
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
			if (!excludedFieldNames.contains(field.getMetadata().getName())) {
				saveLimit = dataBuffer.limit();
				dataBuffer.limit(dataBuffer.position() + fieldSizes[i]);
				switch (this.fieldTypesDBF[i]) {
				case DBFTypes.DBF_TYPE_DATE:
					DateDataField d_field = (DateDataField) field;
					if (d_field != null && !d_field.isNull()) {
						encoder.encode(CharBuffer.wrap(dateFormatter.format(d_field.getDate())), dataBuffer, true);
					}
					break;
				case DBFTypes.DBF_TYPE_CHARACTER:
					if (field != null && !field.isNull()) {
						encoder.encode(CharBuffer.wrap(field.toString()), dataBuffer, true);
					}
					break;
				case DBFTypes.DBF_TYPE_NUMBER:
					// must be right-justified
					CharBuffer cbuf = CharBuffer.wrap(field.toString());
					if (cbuf.limit() > fieldSizes[i]) {
						cbuf.limit(fieldSizes[i]);
					} else {
						// fill in some spaces
						ByteBufferUtils.fill(dataBuffer, (byte) 0x20, fieldSizes[i] - cbuf.limit(), true); // space
					}
					encoder.encode(cbuf, dataBuffer, true);
					break;
				case DBFTypes.DBF_TYPE_LOGICAL:
					BooleanDataField b_field = (BooleanDataField) field;
					if (b_field.isNull())
						dataBuffer.put((byte) 0x20); // 0x20 space
					else if (b_field.getBoolean())
						dataBuffer.put((byte) 'T'); // 0x54 'T'
					else
						dataBuffer.put((byte) 'F'); // 0x46 'F'
					break;
				}
				// pad with blanks if needed
				if (dataBuffer.hasRemaining()) {
					fillerBuffer.limit(dataBuffer.remaining());
					dataBuffer.put(fillerBuffer);
					fillerBuffer.limit(fillerBuffer.capacity());
					fillerBuffer.rewind();
				}
				dataBuffer.limit(saveLimit);
			}
			i++;
		}
		dataBuffer.flip();
		recordCounter++;
		return writer.write(dataBuffer);
		
	}
	
	private void fillDBFHeader(ByteBuffer buffer){
		org.jetel.util.bytes.ByteBufferUtils.fill(buffer, (byte)0x0); // fill with zeros
		// DBF TYPE  - 0x0
		buffer.position(0);
		buffer.put(dbfType);
		// LAST UPDATE - 0x01  (updated when writing footer)
		buffer.put((byte)0); //year
		buffer.put((byte)0); //month
		buffer.put((byte)0); //day
		// NUM RECORDS - 0x04 (updated when writing footer)
		buffer.putInt(0); // 0
		// POSITION OF FIRST DATA RECORD - 0x08
		buffer.putShort((short)( DBFAnalyzer.DBF_HEADER_SIZE_BASIC + (getCountOfNotExcludedFields(metadata) * DBFAnalyzer.DBF_FIELD_DEF_SIZE) +1));
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
			if (!excludedFieldNames.contains(field.getName())) {
				// FIELD NAME
				ByteBuffer name = encoder.charset().encode(field.getName());
				if (name.remaining() > 10)
					name.limit(10);
				int post = buffer.position();
				buffer.put(name);
				buffer.position(post + 10);
				buffer.put((byte) 0x00);
				// FIELD TYPE - 0x0b
				buffer.put((byte) this.fieldTypesDBF[counter]);
				// FIELD ADDRESS - 0x0c
				buffer.putInt(0x0);
				// FIELD LENGTH - 0x10
				buffer.put((byte) DBFTypes.cloverSize2dbf(field));
				// FIELD DECIMAL COUNT - 0x11
				if (field.getDataType() == DataFieldType.DECIMAL)
					buffer.put((byte) Integer.parseInt(field.getProperty(DataFieldMetadata.SCALE_ATTR)));
				else
					buffer.put((byte) 0x0);
				// VARIOUS INDICATORS (not used/populated here) just fill with zeros
				for (int i = 18; i <= 31; i++)
					buffer.put((byte) 0x0);
			}
			counter++;
			
		}
		
	}
	
	@Override
	public int writeHeader() throws IOException {
		if (append && writer.size() > 0) {
			long curPosInFile;
			try {
				curPosInFile = writer.size();
				// basic sanity check during append - at least the same number of fields:
				writer.position(DBFAnalyzer.DBF_HEADER_NUM_REC_OFFSET);
				dataBuffer.clear();
				writer.read(dataBuffer);
			} catch (IOException ex) {
				throw new IOException("Can't append to DBase/DBF file - file does not seem to contain valid DBF header.", ex);
			}
			dataBuffer.flip();
			final int recCount = dataBuffer.getInt();
			final int headerSize = dataBuffer.getShort();
			final int recSize = dataBuffer.getShort();
			final int expectedHeaderSize = (DBFAnalyzer.DBF_HEADER_SIZE_BASIC + (getCountOfNotExcludedFields(metadata) * DBFAnalyzer.DBF_FIELD_DEF_SIZE) + 1);
			final int expectedRecSize = (getRecordSizeConverted(metadata) + 1);
			if ((headerSize != expectedHeaderSize) || (recSize != expectedRecSize)) {
				throw new IOException(String.format("Existing target DBase/DBF file does not correspond to provided CloverETL metadata [rec.size DBF/CloverETL  %d/%d bytes].", recSize, expectedRecSize));
			}
			recordCounter = recCount;
			writer.position(curPosInFile - 1); // removing EOF flag in file, so following write starts after the current
												// last record
			return 0;
		} else {
			writer.position(0); // creating new or overwriting
			dataBuffer.clear();
			fillDBFHeader(dataBuffer);
			dataBuffer.flip();
			int size = dataBuffer.remaining();
			writer.write(dataBuffer);
			return size;
		}
	}

	@Override
	public int writeFooter() throws IOException {
		// put EOF mark
		dataBuffer.clear();
		dataBuffer.put(DBFAnalyzer.DBF_FILE_EOF_INDICATOR);
		dataBuffer.flip();
		writer.write(dataBuffer);

		// truncate the file if necessary
		if (writer.position() < writer.size())
			writer.truncate(writer.position());
				
		// update header - last update + rec counter
		Calendar cal=Calendar.getInstance();
		dataBuffer.clear();
		dataBuffer.put((byte) (cal.get(Calendar.YEAR) - 1900)); //year
		dataBuffer.put((byte) cal.get(Calendar.MONTH)); //month
		dataBuffer.put((byte)cal.get(Calendar.DAY_OF_MONTH)); //day
		
		dataBuffer.putInt(recordCounter);
		dataBuffer.flip();
		
		writer.position(DBFAnalyzer.DBF_HEADER_LAST_UPDATED_OFFSET);
		writer.write(dataBuffer);

		return 0;
	}

	@Override
	public void flush() throws IOException {
		writer.force(true);
	}

	@Override
	public void finish() throws IOException {
		writeFooter();
		flush();

	}

	@Override
	public DataTargetType getPreferredDataTargetType() {
		return DataTargetType.FILE;
	}
	
	public void setExcludedFieldNames(String[] excludedFieldNames) {
		this.excludedFieldNames = new HashSet<String>(Arrays.asList(excludedFieldNames != null ? excludedFieldNames : new String[0]));
	}
	
	public void resetRecordCounter() {
		recordCounter = 0;
	}
	
	/**
	 * @param metadata
	 * @return count of fields which are not excluded.
	 */
	private int getCountOfNotExcludedFields(DataRecordMetadata metadata) {
		int countOfNotExcludedField = 0;
		for (DataFieldMetadata fieldMetadata: metadata.getFields()) {
			if (!excludedFieldNames.contains(fieldMetadata.getName())) {
				countOfNotExcludedField++;
			}
		}
		return countOfNotExcludedField;
	}
	
	/**
	 * @param record
	 * @return Size of the record (excluded fields are skipped).
	 */
	private int getRecordSize(DataRecordMetadata record) {
		int size=0;
		for(DataFieldMetadata field : record){
			if (!excludedFieldNames.contains(field.getName())) {
				size += field.getSize();
			}
		}
		return size;
	}
	
	/**
	 * @param record
	 * @return Size of converted record (excluded fields are skipped).
	 */
	private int getRecordSizeConverted(DataRecordMetadata record) {
		int size=0;
		for(DataFieldMetadata field : record){
			if (!excludedFieldNames.contains(field.getName())) {
				size += DBFTypes.cloverSize2dbf(field);
			}
		}
		return size;
	}
	
}
