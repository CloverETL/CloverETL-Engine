/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
 */

package org.jetel.data.tree.bean.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.jetel.data.tree.bean.schema.model.SchemaCollection;
import org.jetel.data.tree.bean.schema.model.SchemaMap;
import org.jetel.data.tree.bean.schema.model.SchemaObject;
import org.jetel.data.tree.bean.schema.model.TypedObject;
import org.jetel.data.tree.bean.schema.model.TypedObjectRef;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


/**
 * Utility to read <code>SchemaObject</code> representing the bean structure
 * from XML DOM. 
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26.10.2011
 */
public class SchemaObjectReader {

	protected Map<String, TypedObject> parsedTypes;
	protected Document document; 
	
	protected SchemaObjectReader() {
		
	}
	
	public static SchemaObject readFromString(String string) throws SAXException {
		DocumentBuilder builder;
		try {
			builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new RuntimeException(e);
		}
		
		InputSource source = new InputSource(new StringReader(string));
		Document document;
		try {
			document = builder.parse(source);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		return readFromDocument(document);
	}
	
	public static SchemaObject readFromDocument(Document document) {
			SchemaObjectReader reader = new SchemaObjectReader();
			return reader.parseDocument(document);
	}
	
	protected SchemaObject parseDocument(Document document) {
		
		parsedTypes = new HashMap<String, TypedObject>();
		this.document = document;
		
		List<SchemaObject> parsedTopLevel = new ArrayList<SchemaObject>();
		for (Node node : iterable(document.getDocumentElement())) {
			if (node.getNodeType() == Node.ELEMENT_NODE) {
				Element element = (Element)node;
				parsedTopLevel.add(parseElement(element, null));
			}
		}
		/*
		 * find any structure except for type structure
		 */
		for (SchemaObject schemaObject : parsedTopLevel) {
			if (schemaObject != null && schemaObject instanceof TypedObject == false) {
				return schemaObject;
			}
		}
		throw new IllegalArgumentException("Malformed document: no top level structure element found.");
	}
	
	protected SchemaObject parseElement(Element element, SchemaObject container) {
		
		if ("typed-object".equals(element.getNodeName())) {
			return parseTypedObject(element);
		}
		if ("schema-collection".equals(element.getNodeName())) {
			return parseCollectionElement(element, container);
		}
		if ("schema-map".equals(element.getNodeName())) {
			return parseMapElement(element, container);
		}
		if ("typed-object-ref".equals(element.getNodeName())) {
			return parseObjectRef(element, container);
		}
		throw new IllegalArgumentException("Unknown element: " + element.getNodeName());
	}
	
	protected TypedObjectRef parseObjectRef(Element element, SchemaObject container) {
		
		TypedObjectRef objectRef = new TypedObjectRef(container, null);
		if (element.hasAttribute("name")) {
			objectRef.setName(element.getAttribute("name"));
		}
		String typeName = element.getAttribute("type-ref");
		TypedObject typedObject = parsedTypes.get(typeName);
		if (typedObject == null) {
			typedObject = new TypedObject(typeName);
			parsedTypes.put(typeName, typedObject);
		}
		objectRef.setTypedObject(typedObject);
		objectRef.setType(typeName);
		return objectRef;
	}
	
	protected TypedObject parseTypedObject(Element element) {
		
		String typeName = element.getAttribute("type");
		TypedObject typedObject = parsedTypes.get(typeName);
		if (typedObject == null) {
			typedObject = new TypedObject(typeName);
			parsedTypes.put(typeName, typedObject);
		}
		for (Node childNode : iterable(element)) {
			if (childNode.getNodeType() == Node.ELEMENT_NODE) {
				Element childElement = (Element)childNode;
				typedObject.addChild(parseElement(childElement, typedObject));
			}
		}
		return typedObject;
	}
	
	protected SchemaCollection parseCollectionElement(Element element, SchemaObject container) {
		
		SchemaCollection collection = new SchemaCollection(container);
		if (element.hasAttribute("name")) {
			collection.setName(element.getAttribute("name"));
		}
		if (element.hasAttribute("type")) {
			collection.setType(element.getAttribute("type"));
		}
		for (Node childNode : iterable(element)) {
			if (childNode.getNodeType() == Node.ELEMENT_NODE) {
				Element childElement = (Element)childNode;
				SchemaObject itemObject = parseElement(childElement, collection);
				collection.setItem(itemObject);
				break;
			}
		}
		return collection;
	}
	
	protected SchemaMap parseMapElement(Element element, SchemaObject container) {
		
		SchemaMap map = new SchemaMap(container);
		if (element.hasAttribute("name")) {
			map.setName(element.getAttribute("name"));
		}
		if (element.hasAttribute("type")) {
			map.setType(element.getAttribute("type"));
		}
		for (Node childNode : iterable(element)) {
			if (childNode.getNodeType() == Node.ELEMENT_NODE) {
				Element childElement = (Element)childNode;
				SchemaObject entryObject = parseElement(childElement, map);
				if (map.getKey() == null) {
					map.setKey(entryObject);
				} else if (map.getValue() == null) {
					map.setValue(entryObject);
				} else {
					break;
				}
			}
		}
		return map;
	}
	
	protected Iterable<Node> iterable(final Node node) {
		
		return new Iterable<Node>() {
			
			@Override
			public Iterator<Node> iterator() {
				return new NodeChildIterator(node);
			}
		};
	}
	
	protected static class NodeChildIterator implements Iterator<Node> {

		NodeList nodeList;
		int index = 0;
		
		public NodeChildIterator(Node node) {
			nodeList = node.getChildNodes();
		}
		
		@Override
		public boolean hasNext() {
			return index < nodeList.getLength();
		}

		@Override
		public Node next() {
			Node node = nodeList.item(index++);
			if (node == null) {
				throw new NoSuchElementException();
			}
			return node;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("remove()");
		}
	}
}
