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

package org.jetel.data.tree.bean.util;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.jetel.data.tree.bean.schema.model.BaseSchemaObjectVisitor;
import org.jetel.data.tree.bean.schema.model.SchemaCollection;
import org.jetel.data.tree.bean.schema.model.SchemaMap;
import org.jetel.data.tree.bean.schema.model.SchemaObject;
import org.jetel.data.tree.bean.schema.model.TypedObject;
import org.jetel.data.tree.bean.schema.model.TypedObjectRef;
import org.w3c.dom.Document;
import org.w3c.dom.Element;


/**
 * Utility to serialize <code>SchemaObject</code> bean structure into XML.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25.10.2011
 */
public class SchemaObjectWriter {

	protected Document document;
	protected Map<String, Element> typeElements;
	protected Stack<Element> elementStack;
	
	public static Document writeAsDocument(SchemaObject schemaObject) {
		try {
			SchemaObjectWriter serializer = new SchemaObjectWriter();
			return serializer.writeObject(schemaObject);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public static String writeAsString(SchemaObject schemaObject) throws TransformerException {
		Document doc = writeAsDocument(schemaObject);
		DOMSource source = new DOMSource(doc);
		StringWriter writer = new StringWriter();
		Result result = new StreamResult(writer);

		Transformer transformer;
		try {
			transformer = TransformerFactory.newInstance().newTransformer();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		transformer.transform(source, result);
		
		try {
			writer.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return writer.toString();
	}
	
	public Document writeObject(SchemaObject schemaObject) throws ParserConfigurationException {
		
		document = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
		document.setXmlStandalone(true);
		typeElements = new HashMap<String, Element>();
		elementStack = new Stack<Element>();
		SchemaObjectWritingVisitor visitor = new SchemaObjectWritingVisitor();
		schemaObject.acceptVisitor(visitor);
		
		Element root = document.createElement("schema-object");
		root.appendChild(elementStack.pop());
		for (Element typeElement : typeElements.values()) {
			root.appendChild(typeElement);
		}
		document.appendChild(root);
		return document;
	}
	
	protected class SchemaObjectWritingVisitor extends BaseSchemaObjectVisitor {

		@Override
		public void visit(TypedObjectRef typedObjectRef) {
			
			String typeName = typedObjectRef.getTypedObject().getType();
			if (!typeElements.containsKey(typeName)) {
				typeElements.put(typeName, null);
				typedObjectRef.getTypedObject().acceptVisitor(this);
			}
			Element objectRefElement = document.createElement("typed-object-ref");
			if (typedObjectRef.getName() != null) {
				objectRefElement.setAttribute("name", typedObjectRef.getName());
			}
			objectRefElement.setAttribute("type-ref", typeName);
			elementStack.push(objectRefElement);
		}
		
		@Override
		public void visit(TypedObject object) {
			
			Element objectElement = document.createElement("typed-object");
			objectElement.setAttribute("type", object.getType());
			if (object.hasChildren()) {
				Element lastChildInserted = null;
				for (int i = 0; i < object.getChildCount(); ++i) {
					/*
					 * order is significant - first child popped is the last element
					 */
					Element childElement = elementStack.pop();
					objectElement.insertBefore(childElement, lastChildInserted);
					lastChildInserted = childElement;
				}
			}
			typeElements.put(object.getType(), objectElement);
		}
		
		@Override
		public void visit(SchemaCollection collection) {
			
			Element collectionElement = document.createElement("schema-collection");
			if (collection.getType() != null) {
				collectionElement.setAttribute("type", collection.getType());
			}
			if (collection.getName() != null) {
				collectionElement.setAttribute("name", collection.getName());
			}
			collectionElement.appendChild(elementStack.pop());
			elementStack.push(collectionElement);
		};
		
		@Override
		public void visit(SchemaMap map) {
			
			Element mapElement = document.createElement("schema-map");
			if (map.getType() != null) {
				mapElement.setAttribute("type", map.getType());
			}
			if (map.getName() != null) {
				mapElement.setAttribute("name", map.getName());
			}
			/*
			 * key is visited first, so it is on the bottom of stack
			 */
			Element valueElement = elementStack.pop();
			Element keyElement = elementStack.pop();
			mapElement.appendChild(keyElement);
			mapElement.appendChild(valueElement);
			elementStack.push(mapElement);
		}
	}
}
