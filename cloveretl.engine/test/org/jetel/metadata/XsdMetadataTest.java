/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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
package org.jetel.metadata;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.jetel.test.CloverTestCase;

public class XsdMetadataTest extends CloverTestCase  { 
	private String xmlMetadata =
		"<Record name=\"rec\" type=\"fixed\" recordDelimiter=\"\n\" recordSize=\"40\">"
		+ "<Field name=\"f00\" size=\"1\" shift=\"0\" type=\"decimal\"/>"
		+ "<Field name=\"f01\" size=\"2\" shift=\"0\" type=\"numeric\"/>"
		+ "<Field name=\"f02\" size=\"3\" shift=\"0\" type=\"byte\"/>"
		+ "<Field name=\"f03\" size=\"4\" shift=\"0\" type=\"cbyte\"/>"
		+ "<Field name=\"f04\" size=\"3\" shift=\"0\" type=\"date\"/>"
		+ "<Field name=\"f05\" size=\"1\" shift=\"0\" type=\"datetime\"/>"
		+ "<Field name=\"f06\" size=\"1\" shift=\"0\" type=\"integer\"/>"
		+ "<Field name=\"f07\" size=\"1\" shift=\"0\" type=\"long\"/>"
		+ "<Field name=\"f08\" size=\"1\" shift=\"0\" type=\"decimal\"/>"
		+ "<Field name=\"f09\" size=\"2\" shift=\"0\" type=\"decimal\"/>"
		+ "</Record>";

	private DataRecordMetadata metadata;
	
	private final static String TEST_FILE = "XsdTest.xsd";
	
    @Override
	protected void setUp() throws Exception {
    	super.setUp();
	    
        DataRecordMetadataXMLReaderWriter xmlReader = new DataRecordMetadataXMLReaderWriter();
        metadata = xmlReader.read(new ByteArrayInputStream(xmlMetadata.getBytes()));
    }

    public void test_1(){
    	try {
			(new XsdMetadata(metadata)).write(TEST_FILE);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
    }
    
    @Override
	protected void tearDown(){
    	boolean deleted = (new File(TEST_FILE)).delete();
		 assertTrue("can't delete "+ TEST_FILE, deleted );
    }
    
} 
