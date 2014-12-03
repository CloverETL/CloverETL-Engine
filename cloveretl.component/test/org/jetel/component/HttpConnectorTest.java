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
package org.jetel.component;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.input.NullReader;
import org.jetel.component.HttpConnector.PartWithName;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.NullRecord;
import org.jetel.data.NullRecordTest;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.graph.InputPortDirect;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.CTLMapping;

/**
 * @author sedlacek (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Nov 20, 2014
 */
public class HttpConnectorTest  extends CloverTestCase {

	@Override
	protected void setUp() throws Exception {
		super.setUp();
	}


	@Override
	protected void tearDown() throws Exception {
	}

	public void testPrepareMultipartEntitiesEmpty() {
		HttpConnector httpConnector = createHttpConnector();
		httpConnector.setMultipartEntities(null);
		Map<String, HttpConnectorMutlipartEntity> result = httpConnector.prepareMultipartEntities();
		assertNotNull(result);
	}
	
	public void testPrepareMutlipartMappedByFieldNamesOnly() throws ComponentNotReadyException {
		HttpConnector httpConnector = createHttpConnector();
		httpConnector.setMultipartEntities("entity2");
		
		HashMap<String, String> inputRecord = new HashMap<String, String>();
		inputRecord.put("entity1", "ValueOfEntity1");
		inputRecord.put("entity2", "ValueOfEntity2");
		httpConnector.inputRecord = this.createConstDataRecord(inputRecord);

		httpConnector.tryToInit(false);
		Map<String, String> mappingFieldValues = null;
		
//		this.createInputTransformation(httpConnector,mappingFieldValues);

		Map<String, HttpConnectorMutlipartEntity> result = null;
		result = httpConnector.prepareMultipartEntities();
		assertEquals("ValueOfEntity2", result.get("entity2").content);
		
	}

	public void testPrepareMutlipartMappedByFieldEnitityContentMapped() throws ComponentNotReadyException {
		HttpConnector httpConnector = createHttpConnector();
		httpConnector.setMultipartEntities("entity2");
		
		HashMap<String, String> inputRecord = new HashMap<String, String>();
		inputRecord.put("entity1", "ValueOfEntity1");
		inputRecord.put("entity2", "ValueOfEntity2");
		httpConnector.inputRecord = this.createConstDataRecord(inputRecord);

		httpConnector.tryToInit(false);
		Map<String, String> mappingFieldValues = new HashMap<String, String>();
		mappingFieldValues.put("entity2_Content", "ValueOfContent");
		
		this.createInputTransformation(httpConnector,mappingFieldValues);

		Map<String, HttpConnectorMutlipartEntity> result = null;
		result = httpConnector.prepareMultipartEntities();
		assertEquals("ValueOfContent", result.get("entity2").content);
		
	}
	
	public void testPrepareMutlipartMappedByFieldEnitityFileMapped() throws ComponentNotReadyException {
		HttpConnector httpConnector = createHttpConnector();
		httpConnector.setMultipartEntities("entity2");
		
		HashMap<String, String> inputRecord = new HashMap<String, String>();
		inputRecord.put("entity1", "ValueOfEntity1");
		inputRecord.put("entity2", "ValueOfEntity2");
		httpConnector.inputRecord = this.createConstDataRecord(inputRecord);

		httpConnector.tryToInit(false);
		Map<String, String> mappingFieldValues = new HashMap<String, String>();
		mappingFieldValues.put("entity2_File", "ValueOfContent");
		
		this.createInputTransformation(httpConnector,mappingFieldValues);

		Map<String, HttpConnectorMutlipartEntity> result = null;
		result = httpConnector.prepareMultipartEntities();
		assertNull(result.get("entity2").content);
		assertEquals("ValueOfContent",result.get("entity2").file);
		
	}

	public void testPrepareMutlipartMappedByFieldEnitityFileAndContentMapped() throws ComponentNotReadyException {
		HttpConnector httpConnector = createHttpConnector();
		httpConnector.setMultipartEntities("entity2");
		
		HashMap<String, String> inputRecord = new HashMap<String, String>();
		inputRecord.put("entity1", "ValueOfEntity1");
		inputRecord.put("entity2", "ValueOfEntity2");
		httpConnector.inputRecord = this.createConstDataRecord(inputRecord);

		httpConnector.tryToInit(false);
		Map<String, String> mappingFieldValues = new HashMap<String, String>();
		mappingFieldValues.put("entity2_File", "filename");
		mappingFieldValues.put("entity2_Content", "ValueOfContent");
		
		this.createInputTransformation(httpConnector,mappingFieldValues);

		Map<String, HttpConnectorMutlipartEntity> result = null;
		result = httpConnector.prepareMultipartEntities();
		assertEquals("ValueOfContent",result.get("entity2").content);
		assertEquals("filename",result.get("entity2").file);
		
	}
	
	public void testPrepareMultipartEntitiesNotMapped() throws ComponentNotReadyException {
		HttpConnector httpConnector = null;
		
		httpConnector = createHttpConnector();
		httpConnector.setMultipartEntities("test1");
		httpConnector.tryToInit(false);
		Map<String, HttpConnectorMutlipartEntity> result = null;
		result = httpConnector.prepareMultipartEntities();
		assertNotNull(result);
		assertEquals(1, result.size());

		httpConnector = createHttpConnector();
		httpConnector.setMultipartEntities("test1;test2");
		httpConnector.tryToInit(false);
		result = httpConnector.prepareMultipartEntities();
		assertNotNull(result);
		assertEquals(2, result.size());
		
		assertNotNull(result.get("test1"));
		assertEquals("test1",result.get("test1").name);
		assertEquals("",result.get("test1").content);
		assertNull(result.get("test1").file);
		assertNull(result.get("test1").charset);
		assertNull(result.get("test1").conentType);

		assertNotNull(result.get("test2"));
		assertEquals("test2",result.get("test2").name);
		assertEquals("",result.get("test2").content);
		assertNull(result.get("test2").file);
		assertNull(result.get("test2").charset);
		assertNull(result.get("test2").conentType);
	}
	
	public void testMultipartEntitiesContentMapped() throws ComponentNotReadyException {
		HttpConnector httpConnector = createHttpConnector();
		httpConnector.setMultipartEntities("entity");
		httpConnector.tryToInit(false);
		Map<String, String> mappingFieldValues = null;
		mappingFieldValues = new HashMap<String, String>();
		mappingFieldValues.put("entity_Content", "contentValue");
		this.createInputTransformation(httpConnector,mappingFieldValues);

		Map<String, HttpConnectorMutlipartEntity> result = null;
		result = httpConnector.prepareMultipartEntities();
		assertEquals("contentValue", result.get("entity").content);
	}
	
	public void testMultipartEntitiesCharsetMapped() throws ComponentNotReadyException {
		HttpConnector httpConnector = createHttpConnector();
		httpConnector.setMultipartEntities("entity");
		httpConnector.tryToInit(false);
		Map<String, String> mappingFieldValues = null;
		mappingFieldValues = new HashMap<String, String>();
		mappingFieldValues.put("entity_Charset", "customCharset");
		this.createInputTransformation(httpConnector,mappingFieldValues);

		Map<String, HttpConnectorMutlipartEntity> result = null;
		result = httpConnector.prepareMultipartEntities();
		assertEquals("customCharset", result.get("entity").charset);
	}

	public void testMultipartEntitiesContentTypeMapped() throws ComponentNotReadyException {
		HttpConnector httpConnector = createHttpConnector();
		httpConnector.setMultipartEntities("entity");
		httpConnector.tryToInit(false);
		Map<String, String> mappingFieldValues = null;
		mappingFieldValues = new HashMap<String, String>();
		mappingFieldValues.put("entity_ContentType", "customContentType");
		this.createInputTransformation(httpConnector,mappingFieldValues);

		Map<String, HttpConnectorMutlipartEntity> result = null;
		result = httpConnector.prepareMultipartEntities();
		assertEquals("customContentType", result.get("entity").conentType);
	}
	
	public void testCreateMultipartRecord() {
		DataRecordMetadata record = HttpConnector.createMultipartMetadataFromString("field1;otherField", "nameOfMetadata", ";");
		
		assertEquals(8, record.getFields().length);
		assertEquals("field1_Content", record.getField(0).getLabelOrName());
		assertEquals("otherField_ContentType", record.getField(6).getLabelOrName());
		
		record = HttpConnector.createMultipartMetadataFromString(null, "nameOfMetadata", ";");
		assertNull(record);

		record = HttpConnector.createMultipartMetadataFromString("", "nameOfMetadata", ";");
		assertNull(record);

	}

	public void testMultipartEntitiesMultipleValues() throws ComponentNotReadyException {
		HttpConnector httpConnector = createHttpConnector();
		httpConnector.setMultipartEntities("entity;second;third");
		httpConnector.tryToInit(false);
		Map<String, String> mappingFieldValues = null;
		mappingFieldValues = new HashMap<String, String>();
		mappingFieldValues.put("entity_Charset", "customCharset");
		mappingFieldValues.put("second_Charset", "customCharset2");
		mappingFieldValues.put("second_Content", "secContent");
		mappingFieldValues.put("third_File", "file");
		mappingFieldValues.put("third_ContentType", "myContentType");
		
		this.createInputTransformation(httpConnector,mappingFieldValues);

		Map<String, HttpConnectorMutlipartEntity> result = null;
		result = httpConnector.prepareMultipartEntities();
		assertEquals("customCharset", result.get("entity").charset);
		assertEquals("customCharset2", result.get("second").charset);
		assertEquals("secContent", result.get("second").content);
		assertEquals("file", result.get("third").file);
		assertEquals("myContentType", result.get("third").conentType);
	}
	
	
	
	private HttpConnector createHttpConnector() {
		HttpConnector httpConnector = new HttpConnector("HTTP_CONNECTOR1");
//		DataRecord record = DataRecordFactory.newRecord(new DataRecordMetadata("TEST1"));
		DataRecord record = NullRecord.NULL_RECORD;
		httpConnector.inputRecord = record;
		
		return httpConnector;
	}
	
	private void createInputTransformation(HttpConnector httpConnector, Map<String,String> fieldValues) {
//		CTLMapping mapping = new CTLMapping("name", httpConnector);
		httpConnector.inputMappingTransformation.addInputMetadata("input", new CustomMetadata());
		httpConnector.inputMappingTransformation.setTransformation("//#CTL2\nfunction integer transform() {return ALL;}");
		DataRecord multipartRecord = null;
		if(fieldValues.keySet()!=null) {
			DataRecordMetadata[] metadata = httpConnector.inputMappingTransformation.getOutputRecordsMetadata();
			for (int i=0; i<metadata.length; i++) {
				if(metadata[i]!=null && metadata[i].getName()!=null && metadata[i].getName().equals("MultipartEntities")) {
					multipartRecord = httpConnector.inputMappingTransformation.getOutputRecord(i);
				}
			}
			if (multipartRecord != null) {
				for (String key : fieldValues.keySet()) {
					multipartRecord.getField(key).setValue(fieldValues.get(key));
				}
			}
		}
	}
	
	private DataRecord createConstDataRecord(Map<String,String> fieldValues) {
		DataRecordMetadata metadata = new DataRecordMetadata("Test");
		for(String key : fieldValues.keySet()) {
			metadata.addField(new DataFieldMetadata(key, "|"));
		}
		DataRecord record = DataRecordFactory.newRecord(metadata );
		record.init();
		for(String key : fieldValues.keySet()) {
			record.getField(key).setValue(fieldValues.get(key));
		}
		
		return record;
		
	}
	
	
	public void testBuildMultiPart() throws IOException {
		HashMap<String, HttpConnectorMutlipartEntity> multipartEntities = new HashMap<String, HttpConnectorMutlipartEntity>();
		HttpConnectorMutlipartEntity entity;
		entity = new HttpConnectorMutlipartEntity();
		entity.name = "test1";
		entity.content = "contenetOfValue";
		multipartEntities.put(entity.name, entity);
		
		PartWithName[] result = this.createHttpConnector().buildMultiPart(multipartEntities);
		assertEquals(1, result.length);
		assertEquals("test1", result[0].name);
		assertEquals("contenetOfValue".length(), result[0].value.getContentLength());
	}
	
	public void testBuildMultiPartFileName() throws IOException {
		HashMap<String, HttpConnectorMutlipartEntity> multipartEntities = new HashMap<String, HttpConnectorMutlipartEntity>();
		HttpConnectorMutlipartEntity entity;
		entity = new HttpConnectorMutlipartEntity();
		entity.name = "test1";
		entity.file = this.getClass().getResource("./HttpConnectorTest.class").getFile();
		multipartEntities.put(entity.name, entity);
		
		PartWithName[] result = this.createHttpConnector().buildMultiPart(multipartEntities);
		assertEquals(1, result.length);
		assertEquals("HttpConnectorTest.class", result[0].value.getFilename());
		assertEquals("test1", result[0].name);
	}

	public void testBuildMultiPartFileNameNameOverriden() throws IOException {
		HashMap<String, HttpConnectorMutlipartEntity> multipartEntities = new HashMap<String, HttpConnectorMutlipartEntity>();
		HttpConnectorMutlipartEntity entity;
		entity = new HttpConnectorMutlipartEntity();
		entity.name = "test1";
		entity.content = "ContentOfFile";
		entity.file = "someFileName";
		multipartEntities.put(entity.name, entity);
		
		PartWithName[] result = this.createHttpConnector().buildMultiPart(multipartEntities);
		assertEquals(1, result.length);
		assertEquals("someFileName", result[0].value.getFilename());
		assertEquals("ContentOfFile".length(), result[0].value.getContentLength());
		assertEquals("test1", result[0].name);
	}

	public void testBuildMultiPartFileNameMultipleSettings() throws IOException {
		HashMap<String, HttpConnectorMutlipartEntity> multipartEntities = new HashMap<String, HttpConnectorMutlipartEntity>();
		HttpConnectorMutlipartEntity entity;
		entity = new HttpConnectorMutlipartEntity();
		entity.name = "test1";
		entity.content = "Filename1";
		entity.file = this.getClass().getResource("./HttpConnectorTest.class").getFile();
		multipartEntities.put(entity.name, entity);
		
		entity = new HttpConnectorMutlipartEntity();
		entity.name = "test2";
		entity.conentType = "application/octetstream";
		entity.file = this.getClass().getResource("./HttpConnectorTest.class").getFile();
		multipartEntities.put(entity.name, entity);

		entity = new HttpConnectorMutlipartEntity();
		entity.name = "test3";
		entity.content = "Filename2";
		entity.charset = "UTF-16";
		entity.conentType = "application/octetstream";
		entity.file = this.getClass().getResource("./HttpConnectorTest.class").getFile();
		multipartEntities.put(entity.name, entity);
		
		entity = new HttpConnectorMutlipartEntity();
		entity.name = "test4";
		entity.content = "Filename2";
		entity.charset = "UTF-16";
		entity.conentType = "abcd";
		multipartEntities.put(entity.name, entity);

		PartWithName[] result = this.createHttpConnector().buildMultiPart(multipartEntities);
		assertEquals(4, result.length);
		
	
	}
	
	public void testBuildMultiPartFileNameFileWithDetails() throws IOException {
		HashMap<String, HttpConnectorMutlipartEntity> multipartEntities = new HashMap<String, HttpConnectorMutlipartEntity>();
		HttpConnectorMutlipartEntity entity;
		entity = new HttpConnectorMutlipartEntity();
		entity.name = "test3";
		entity.content = "FileContent";
		entity.charset = "UTF-16";
		entity.conentType = "application/aaa";
		entity.file = "Filename2";
		multipartEntities.put(entity.name, entity);
	
		PartWithName[] result = this.createHttpConnector().buildMultiPart(multipartEntities);
		assertEquals(1, result.length);

		assertEquals("test3", result[0].name);
		assertEquals("Filename2", result[0].value.getFilename());
		assertEquals("application/aaa", result[0].value.getMimeType());
		assertEquals("UTF-16",result[0].value.getCharset());
	}
	
	
	public void testBuildMultiPartContentType() throws IOException {
		HashMap<String, HttpConnectorMutlipartEntity> multipartEntities = new HashMap<String, HttpConnectorMutlipartEntity>();
		HttpConnectorMutlipartEntity entity;
		entity = new HttpConnectorMutlipartEntity();
		entity.name = "test1";
		entity.conentType = "abcdef";
		multipartEntities.put(entity.name, entity);
		
		PartWithName[] result = this.createHttpConnector().buildMultiPart(multipartEntities);
		assertEquals(1, result.length);
		assertEquals("abcdef", result[0].value.getMimeType());
	}

	public void testBuildMultiPartCharset() throws IOException {
		HashMap<String, HttpConnectorMutlipartEntity> multipartEntities = new HashMap<String, HttpConnectorMutlipartEntity>();
		HttpConnectorMutlipartEntity entity;
		entity = new HttpConnectorMutlipartEntity();
		entity.name = "test1";
		entity.charset = "UTF-16";
		multipartEntities.put(entity.name, entity);
		
		
		PartWithName[] result = this.createHttpConnector().buildMultiPart(multipartEntities);
		assertEquals(1, result.length);
		assertEquals("UTF-16", result[0].value.getCharset());
	}

	public void testBuildMultiPartEmpty() throws IOException {
		assertNull(this.createHttpConnector().buildMultiPart(null));

		assertNotNull(this.createHttpConnector().buildMultiPart(new HashMap<String, HttpConnectorMutlipartEntity>()));
		assertEquals(0, this.createHttpConnector().buildMultiPart(new HashMap<String, HttpConnectorMutlipartEntity>()).length);

	}
	

}

class CustomMetadata extends DataRecordMetadata {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * @param name
	 */
	public CustomMetadata() {
		super("TestMetadata");
	}
	
}


