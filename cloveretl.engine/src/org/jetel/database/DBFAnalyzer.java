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

package org.jetel.database.dbf;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Properties;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;

/**
 * @author DPavlis
 * @since  29.6.2004
 *
 * Class which analyzes dBase/FoxBase/FoxPRO table/file and extracts metadata about defined fields.
 * Used by DBFDataReader to which it provides neccessary info for reading DBF records.
 */


public class DBFAnalyzer {
	
	private static final int DBF_HEADER_SIZE=32;
	private static final int DBF_FIELD_DEF_SIZE=32;
	private static final String HEADER_CHARACTER_ENCODING="ISO-8859-1";
	private final static String VERSION = "1.0";
	private final static String LAST_UPDATED = "2004/06/29";  
	
	private ByteBuffer buffer;
	private int dbfNumRows;
	private int dbfNumFields;
	private int dbfRecSize;
	private DBFFieldMetadata[] dbfFields;
	private int dbfType;
	private int dbfDataOffset;
	private int dbfRowNum;
	private byte dbfCodePage;
	private Charset charset;
	private String dbfTableName;
	
	public DBFAnalyzer(){
		
	}

	
	
	void analyze(String dbfFileName) throws IOException,DBFErrorException{
		FileChannel dbfFile=new FileInputStream(dbfFileName).getChannel();
		analyze(dbfFile,new File(dbfFileName).getName());
		dbfFile.close();
	}

	void analyze(FileChannel dbfFile,String dbfTableName)throws IOException,DBFErrorException{
	    
	    buffer=ByteBuffer.allocate(DBF_HEADER_SIZE);
	    buffer.order(ByteOrder.LITTLE_ENDIAN);
		int count=dbfFile.read(buffer);
		if (count!=32){
			throw new DBFErrorException("Problem reading DBF header - too short !");
		}
		this.dbfTableName=dbfTableName;
		
		buffer.flip();
		// read-in basic table definition
		buffer.position(0);
		dbfType=buffer.get();
		buffer.position(4);
		dbfNumRows=buffer.getInt();
		buffer.position(8);
		dbfDataOffset=buffer.getShort();
		dbfNumFields=(dbfDataOffset-32-1)/32;
		buffer.position(29);
		dbfCodePage=buffer.get();
		buffer.position(10);
		dbfRecSize=buffer.getShort();
		
		buffer=ByteBuffer.allocate(dbfNumFields*DBF_FIELD_DEF_SIZE);
	    buffer.order(ByteOrder.LITTLE_ENDIAN);
		dbfFile.read(buffer);
		buffer.flip();
	    
		// read-in definition of individual fields
		int offset=0;
		dbfFields=new DBFFieldMetadata[dbfNumFields];
		charset=Charset.forName(HEADER_CHARACTER_ENCODING);
		for(int i=0;i<dbfNumFields;i++){
			dbfFields[i]=new DBFFieldMetadata();
			buffer.limit(11+offset);
			dbfFields[i].name=charset.decode(buffer).toString().trim();
			buffer.limit(12+offset);
			dbfFields[i].type=charset.decode(buffer).get();
			buffer.limit(32+offset);
			dbfFields[i].offset=buffer.getInt();
			dbfFields[i].length=buffer.get();
			dbfFields[i].decPlaces=buffer.get();
			offset+=DBF_FIELD_DEF_SIZE;
		}
		
		
		// let's construct the valid character decoder based on info found in table header
		try{
			charset=Charset.forName(DBFTypes.dbfCodepage2Java(dbfCodePage));
		}catch (Exception ex){
			throw new DBFErrorException("Unsupported DBF codepage ID: "+dbfCodePage);
		}
	}

	/**
	 * Main method which allows analyzing dBase file/table. It extracts Clover-style
	 * metadata from information specified in dBase table header.
	 * 
	 * @param args
	 */
	public static void main(String[] args){
		
		if (args.length==0){
			System.out.println("*** Jetel DBFAnalyzer (" + VERSION + ") created on "+LAST_UPDATED+" (c) 2002-04 D.Pavlis, released under GNU Lesser General Public license ***\n");
			System.out.println("Usage: DBFAnalyzer <DBF filename> [<metadata output filename>]");
			System.exit(-1);
		}
		
		DBFAnalyzer dbf = new DBFAnalyzer();
		try{
			dbf.analyze(args[0]);
		}catch(Exception ex){
			System.err.println(ex.getMessage());
			System.exit(-1);
		}
		DataRecordMetadataXMLReaderWriter writer=new DataRecordMetadataXMLReaderWriter();
		try{
			OutputStream outstream;
			if (args.length<2){
				outstream=System.out;
			}else{
				outstream=new BufferedOutputStream(new FileOutputStream(args[1]));
			}
			writer.write(dbf.getCloverMetadata(), outstream);
		}catch(IOException ex){
			System.err.println(ex.getMessage());
			System.exit(-1);
		}
		
	}


	/**
	 * @return Returns the dbfFields array - contains description if individual
	 * fields in dBase table header
	 */
	public DBFFieldMetadata[] getFields() {
		return dbfFields;
	}
	/**
	 * @return Returns number of fields defined in table
	 */
	public int getNumFields() {
		return dbfNumFields;
	}
	/**
	 * @return Returns number of rows present in dBase datafile.
	 */
	public int getNumRows() {
		return dbfNumRows;
	}
	/**
	 * @return Returns the dbfType - can be used to distinguis which dBase
	 * version (dBase, FoxBase, FoxPRO) created the datafile
	 */
	public int getDBFType() {
		return dbfType;
	}
	
	/**
	 * Method which returns name (String) of the dBase variant which
	 * created analyzed DBF file.
	 * 
	 * @return
	 */
	public String getDBFTypeName(){
	    int type=this.dbfType;
	    for (int i=0;i<DBFTypes.KNOWN_TYPES.length;i++){
	        if (type==DBFTypes.KNOWN_TYPES[i]){
	            return DBFTypes.KNOWN_TYPES_NAMES[i];
	        }
	    }
	    return null;
	}
	
	/**
	 * @return Returns the dbfRecSize.
	 */
	public int getRecSize() {
		return dbfRecSize;
	}
	
	/**
	 * @param dBase field type
	 * @return CloverETL field type
	 */
	public static char dbfFieldType2Clover(char type){
		char cloverType;	
		switch(type){
			case 'C': return DataFieldMetadata.STRING_FIELD;
			case 'N': return DataFieldMetadata.NUMERIC_FIELD;
			case 'D': return DataFieldMetadata.DATE_FIELD;
			case 'L': return DataFieldMetadata.STRING_FIELD;
			case 'M': return DataFieldMetadata.BYTE_FIELD;
			default: throw new DBFErrorException("Unsupported DBF field type: "+type);
		}
	}
	
	public DataRecordMetadata getCloverMetadata(){
		DataRecordMetadata record=new DataRecordMetadata(dbfTableName.replace('.','_'),
							DataRecordMetadata.FIXEDLEN_RECORD);
	
		// set record properties - additional info for DBF-type of data 
		Properties recProp=new Properties();
		recProp.setProperty(DBFTypes.DATA_OFFSET_XML_ATTRIB_NAME,String.valueOf(dbfDataOffset));
		recProp.setProperty(DBFTypes.DATA_ENCODING_XML_ATTRIB_NAME,DBFTypes.dbfCodepage2Java(dbfCodePage));
		recProp.setProperty(DBFTypes.RECORD_SIZE_XML_ATTRIB_NAME,String.valueOf(dbfRecSize));
		record.setRecordProperties(recProp);
		
		// add "hidden" field indicatind deletion status
		record.addField(new DataFieldMetadata("_IS_DELETED_",DataFieldMetadata.STRING_FIELD,(short)1));
		for (int i=0;i<dbfNumFields;i++){
			// create field definition based on what we read from DBF file header
			DataFieldMetadata field=new DataFieldMetadata(dbfFields[i].name,
					dbfFieldType2Clover(dbfFields[i].type),
					dbfFields[i].length);

			// if DATE DBF type, then set format/date mask
			if (dbfFields[i].type=='D'){
				field.setFormatStr(DBFTypes.DATE_FORMAT_MASK);			
			}
			record.addField(field);
		}
		return record;
	}
	/**
	 * @return Returns the dbfCodePage.
	 */
	public byte getDBFCodePage() {
		return dbfCodePage;
	}
	/**
	 * @return Returns the dbfDataOffset.
	 */
	public int getDBFDataOffset() {
		return dbfDataOffset;
	}
}
