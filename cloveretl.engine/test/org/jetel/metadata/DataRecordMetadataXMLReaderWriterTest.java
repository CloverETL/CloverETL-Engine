package org.jetel.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.jetel.test.CloverTestCase;

/*  
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on May 24, 2003
 *  Copyright (C) 2003, 2002  David Pavlis, Wes Maciorowski
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

/**
 * Provides unit tests for DataRecordMetadataXMLReaderWriter.
 * 
 * @author Wes Maciorowski
 *
 */
public class DataRecordMetadataXMLReaderWriterTest extends CloverTestCase {
	private DataRecordMetadata aDelimitedDataRecordMetadata;
	private DataRecordMetadataXMLReaderWriter aDataRecordMetadataXMLReaderWriter;
	private String testFile1 = null;

	@Override
	protected void setUp() throws Exception { 
		super.setUp();
		
		DataFieldMetadata aDataFieldMetadata = null;

		aDataRecordMetadataXMLReaderWriter= new DataRecordMetadataXMLReaderWriter();

		aDelimitedDataRecordMetadata = new DataRecordMetadata("record2",DataRecordMetadata.DELIMITED_RECORD);

		aDataFieldMetadata  = new DataFieldMetadata("Field_4",DataFieldMetadata.INTEGER_FIELD,";");
		aDelimitedDataRecordMetadata.addField(aDataFieldMetadata);

		aDataFieldMetadata = new DataFieldMetadata("Field_0",DataFieldMetadata.INTEGER_FIELD,";");
// aDataFieldMetadata.setCodeStr("return Math.abs(-5);");
		aDelimitedDataRecordMetadata.addField(aDataFieldMetadata);
		aDataFieldMetadata = new DataFieldMetadata("Field_1",DataFieldMetadata.STRING_FIELD,":");
		aDelimitedDataRecordMetadata.addField(aDataFieldMetadata);
		aDataFieldMetadata = new DataFieldMetadata("Field_2",DataFieldMetadata.INTEGER_FIELD,",");
// aDataFieldMetadata.setCodeStr("return 7;");
		aDelimitedDataRecordMetadata.addField(aDataFieldMetadata);
		aDataFieldMetadata = new DataFieldMetadata("Field_3",DataFieldMetadata.NUMERIC_FIELD,"\n");
// aDataFieldMetadata.setCodeStr("return [record1].[field1]/[record2].[field2];");
		aDelimitedDataRecordMetadata.addField(aDataFieldMetadata);

		testFile1 = "data/test1.txt";	
		File aFile=new File(testFile1);
		if (!aFile.exists()) {
			if (!aFile.getParentFile().isDirectory()) {
				final boolean created = aFile.getParentFile().mkdirs();
				assertTrue("can't create directory " + aFile.getParentFile().getAbsolutePath(), created);
			}

			try {
				aFile.createNewFile();
			} catch (IOException e3) {
				e3.printStackTrace();
			}
		}
	}

	@Override
	protected void tearDown() {
		aDelimitedDataRecordMetadata = null;
		aDataRecordMetadataXMLReaderWriter = null;
		//remove testFile if any
		File aFile=new File(testFile1);
		 if(aFile.exists()) {
			 final boolean deleted = aFile.delete();
			 assertTrue("can't delete "+ aFile.getAbsolutePath(), deleted );
		 }
	}





	/**
	 *  Test for @link org.jetel.metadata.DataRecordMetadataXMLReaderWriter.read(InputStream in)
	 *           @link org.jetel.metadata.DataRecordMetadataXMLReaderWriter.write(DataRecordMetadata record, OutputStream outStream)
	 */

	public void test_DataRecordMetadataXMLReaderWriter() {
		try {
			OutputStream outputStream = new FileOutputStream(testFile1);
			DataRecordMetadataXMLReaderWriter.write(aDelimitedDataRecordMetadata, outputStream);
			outputStream.close();

			aDelimitedDataRecordMetadata = null;
			
			InputStream inputStream = new FileInputStream(testFile1);
			aDelimitedDataRecordMetadata = aDataRecordMetadataXMLReaderWriter.read(inputStream);
			inputStream.close();

			assertEquals("record2",aDelimitedDataRecordMetadata.getName());
			assertEquals(DataRecordMetadata.DELIMITED_RECORD,aDelimitedDataRecordMetadata.getRecType());
			assertEquals(5,aDelimitedDataRecordMetadata.getNumFields());

			DataFieldMetadata aDataFieldMetadata = null;
			aDataFieldMetadata = aDelimitedDataRecordMetadata.getField("Field_4");
			assertEquals(";",aDataFieldMetadata.getDelimiters()[0]);
			assertEquals(null,aDataFieldMetadata.getDefaultValue());
			assertEquals(null,aDataFieldMetadata.getFormatStr());
			assertEquals(DataFieldMetadata.INTEGER_FIELD,aDataFieldMetadata.getType());
//			assertEquals(null,aDataFieldMetadata.getCodeStr());
			
			aDataFieldMetadata = aDelimitedDataRecordMetadata.getField("Field_0");
			assertEquals(";",aDataFieldMetadata.getDelimiters()[0]);
			assertEquals(null,aDataFieldMetadata.getDefaultValue());
			assertEquals(null,aDataFieldMetadata.getFormatStr());
			assertEquals(DataFieldMetadata.INTEGER_FIELD,aDataFieldMetadata.getType());
//			assertEquals("return Math.abs(-5);",aDataFieldMetadata.getCodeStr().replace('\n',' ').replace('\t',' ').trim());

			aDataFieldMetadata = aDelimitedDataRecordMetadata.getField("Field_1");
			assertEquals(":",aDataFieldMetadata.getDelimiters()[0]);
			assertEquals(null,aDataFieldMetadata.getDefaultValue());
			assertEquals(null,aDataFieldMetadata.getFormatStr());
			assertEquals(DataFieldMetadata.STRING_FIELD,aDataFieldMetadata.getType());
//			assertEquals(null,aDataFieldMetadata.getCodeStr());

			aDataFieldMetadata = aDelimitedDataRecordMetadata.getField("Field_2");
			assertEquals(",",aDataFieldMetadata.getDelimiters()[0]);
			assertEquals(null,aDataFieldMetadata.getDefaultValue());
			assertEquals(null,aDataFieldMetadata.getFormatStr());
			assertEquals(DataFieldMetadata.INTEGER_FIELD,aDataFieldMetadata.getType());
//			assertEquals("return 7;",aDataFieldMetadata.getCodeStr().replace('\n',' ').replace('\t',' ').trim());

			aDataFieldMetadata = aDelimitedDataRecordMetadata.getField("Field_3");
			assertEquals("\n",aDataFieldMetadata.getDelimiters()[0]);
			assertEquals(null,aDataFieldMetadata.getDefaultValue());
			assertEquals(null,aDataFieldMetadata.getFormatStr());
			assertEquals(DataFieldMetadata.NUMERIC_FIELD,aDataFieldMetadata.getType());
//			assertEquals("return [record1].[field1]/[record2].[field2];",aDataFieldMetadata.getCodeStr().replace('\n',' ').replace('\t',' ').trim());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
