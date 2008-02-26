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
import java.io.FileNotFoundException;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import junit.framework.TestCase;

import org.jetel.graph.runtime.EngineInitializer;

public class XsdMetadataTest extends TestCase  { 
	private String xmlMetadata =
		"<Record name=\"rec\" type=\"fixed\" recordDelimiter=\"\n\" recordSize=\"40\">"
		+ "<Field name=\"00\" size=\"1\" shift=\"0\" type=\"decimal\"/>"
		+ "<Field name=\"01\" size=\"2\" shift=\"0\" type=\"numeric\"/>"
		+ "<Field name=\"02\" size=\"3\" shift=\"0\" type=\"byte\"/>"
		+ "<Field name=\"03\" size=\"4\" shift=\"0\" type=\"cbyte\"/>"
		+ "<Field name=\"04\" size=\"3\" shift=\"0\" type=\"date\"/>"
		+ "<Field name=\"05\" size=\"1\" shift=\"0\" type=\"datetime\"/>"
		+ "<Field name=\"06\" size=\"1\" shift=\"0\" type=\"integer\"/>"
		+ "<Field name=\"07\" size=\"1\" shift=\"0\" type=\"long\"/>"
		+ "<Field name=\"08\" size=\"1\" shift=\"0\" type=\"decimal\"/>"
		+ "<Field name=\"09\" size=\"2\" shift=\"0\" type=\"decimal\"/>"
		+ "</Record>";

	private DataRecordMetadata metadata;
	
    protected void setUp() throws Exception {
        super.setUp();
    	EngineInitializer.initEngine(null, null, null);
        DataRecordMetadataXMLReaderWriter xmlReader = new DataRecordMetadataXMLReaderWriter();
        metadata = xmlReader.read(new ByteArrayInputStream(xmlMetadata.getBytes()));
    }

    public void test_1(){
    	try {
			(new XsdMetadata(metadata)).write("XsdTest.xsd");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		}
    }
    
    protected void tearDown(){
    }
    
} 
