/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
 */

package org.jetel.data.tree.bean.schema;

import java.io.StringWriter;
import java.util.HashMap;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.jetel.data.tree.bean.ComplexTestType;
import org.jetel.data.tree.bean.ComplexTestTypeWithArray;
import org.jetel.data.tree.bean.ComplexTestTypeWithCollection;
import org.jetel.data.tree.bean.ListHolder;
import org.jetel.data.tree.bean.ListOfMapsOfMaps;
import org.jetel.data.tree.bean.SimpleTestType;
import org.jetel.data.tree.bean.schema.generator.BeanParser;
import org.jetel.data.tree.bean.schema.model.SchemaCollection;
import org.jetel.data.tree.bean.schema.model.SchemaMap;
import org.jetel.data.tree.bean.schema.model.SchemaObject;
import org.jetel.data.tree.bean.schema.model.TypedObject;
import org.jetel.data.tree.bean.schema.model.TypedObjectRef;
import org.jetel.data.tree.bean.util.SchemaObjectWriter;
import org.junit.Test;
import org.w3c.dom.Document;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25.10.2011
 */
public class TestSchemaObjectSerialization {

	@Test
	public void testSimpleObject() throws Exception {
		
		SchemaObject typeStructure = BeanParser.parse(SimpleTestType.class);
		
		Document document = SchemaObjectWriter.writeAsDocument(typeStructure);
		printDocument(document);
	}
	
	@Test
	public void testComplexObject() throws Exception {
		
		SchemaObject typeStructure = BeanParser.parse(ComplexTestType.class);
		
		Document document = SchemaObjectWriter.writeAsDocument(typeStructure);
		printDocument(document);
	}
	
	@Test
	public void testComplexObjectWithCollection() throws Exception {
		
		SchemaObject typeStructure = BeanParser.parse(ComplexTestTypeWithCollection.class);
		
		Document document = SchemaObjectWriter.writeAsDocument(typeStructure);
		printDocument(document);
	}
	
	@Test
	public void testComplexObjectWithArray() throws Exception {
		
		SchemaObject typeStructure = BeanParser.parse(ComplexTestTypeWithArray.class);
		
		Document document = SchemaObjectWriter.writeAsDocument(typeStructure);
		printDocument(document);
	}
	
	@Test
	public void testMap() throws Exception {
		/*
		 * Map<String, SimpleTestType>
		 */
		SchemaMap map = new SchemaMap(null);
		TypedObject typedObject = new TypedObject(String.class.getName());
		TypedObjectRef ref = new TypedObjectRef(map, typedObject);
		map.setKey(ref);
		typedObject = new TypedObject(SimpleTestType.class.getName());
		ref = new TypedObjectRef(map, typedObject);
		map.setValue(ref);
		map = (SchemaMap)BeanParser.addReferencedTypes(map, getClass().getClassLoader());
		
		Document document = SchemaObjectWriter.writeAsDocument(map);
		printDocument(document);
	}
	
	@Test
	public void testMapImplementationInStructure() throws Exception {
		
		SchemaCollection collection = new SchemaCollection(null);
		SchemaMap map = new SchemaMap(collection);
		collection.setItem(map);
		map.setKey(new TypedObjectRef(map, new TypedObject(String.class.getName())));
		map.setValue(new TypedObjectRef(map, new TypedObject(HashMap.class.getName())));
		
		collection = BeanParser.addReferencedTypes(collection, getClass().getClassLoader());
		
		Document document = SchemaObjectWriter.writeAsDocument(map);
		printDocument(document);
	}

	@Test
	public void testListOfMapsOfMaps() throws Exception {
		
		SchemaObject typeStructure = BeanParser.parse(ListOfMapsOfMaps.class);
		
		Document document = SchemaObjectWriter.writeAsDocument(typeStructure);
		printDocument(document);
	}
	
	@Test
	public void testListHolder() throws Exception {
		
		SchemaObject typeStructure = BeanParser.parse(ListHolder.class);
		
		Document document = SchemaObjectWriter.writeAsDocument(typeStructure);
		printDocument(document);
	}
	
	private void printDocument(Document document) throws Exception {
		//set up a transformer
        TransformerFactory transfac = TransformerFactory.newInstance();
        Transformer trans = transfac.newTransformer();
        //trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        trans.setOutputProperty(OutputKeys.INDENT, "yes");

        //create string from xml tree
        StringWriter sw = new StringWriter();
        StreamResult result = new StreamResult(sw);
        DOMSource source = new DOMSource(document);
        trans.transform(source, result);
        System.out.println(sw.toString());
	}
}
