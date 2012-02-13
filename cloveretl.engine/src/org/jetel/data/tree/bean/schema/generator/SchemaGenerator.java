/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
 */

package org.jetel.data.tree.bean.schema.generator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Stack;

import javax.xml.namespace.QName;

import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaAnyAttribute;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaComplexType;
import org.apache.ws.commons.schema.XmlSchemaContentProcessing;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaForm;
import org.apache.ws.commons.schema.XmlSchemaSequence;
import org.apache.ws.commons.schema.XmlSchemaSimpleContent;
import org.apache.ws.commons.schema.XmlSchemaSimpleContentExtension;
import org.apache.ws.commons.schema.XmlSchemaType;
import org.apache.ws.commons.schema.constants.Constants;
import org.jetel.data.tree.bean.BeanConstants;
import org.jetel.data.tree.bean.SimpleTypes;
import org.jetel.data.tree.bean.schema.model.BaseSchemaObjectVisitor;
import org.jetel.data.tree.bean.schema.model.SchemaCollection;
import org.jetel.data.tree.bean.schema.model.SchemaMap;
import org.jetel.data.tree.bean.schema.model.SchemaObject;
import org.jetel.data.tree.bean.schema.model.TypedObject;
import org.jetel.data.tree.bean.schema.model.TypedObjectRef;
import org.jetel.util.string.StringUtils;

/**
 * Utility to create XML schema from Object's structure, the schema created
 * is not restrictive - it allows attributes from different namespaces.
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.10.2011
 */
public class SchemaGenerator  {
	
	private static final String NS_URI = "schema:object.structure.generated";
	private XmlSchema xmlSchema;
	private Map<String, XmlSchemaType> schemaTypes = new HashMap<String, XmlSchemaType>();
	private Stack<XmlSchemaElement> elementStack = new Stack<XmlSchemaElement>();
	
	public static XmlSchema generateSchema(SchemaObject schemaObject) {
		return new SchemaGenerator().generateObjectSchema(schemaObject);
	}
	
	protected XmlSchema generateObjectSchema(SchemaObject object) {
		
		xmlSchema = new XmlSchema(NS_URI, new XmlSchemaCollection());
		xmlSchema.setAttributeFormDefault(new XmlSchemaForm(XmlSchemaForm.QUALIFIED));
		xmlSchema.setElementFormDefault(new XmlSchemaForm(XmlSchemaForm.QUALIFIED));
		/*
		 * wrapping TypeObject in reference makes algorithm more uniform
		 * and top-level element is created automatically
		 */
		if (object instanceof TypedObject) {
			object = new TypedObjectRef(null, (TypedObject)object);
		}
		Visitor visitor = new Visitor();
		object.acceptVisitor(visitor);
		XmlSchemaElement top = elementStack.pop();
		/*
		 * remove cardinality
		 */
		top.setMinOccurs(1);
		top.setMaxOccurs(1);
		xmlSchema.getItems().add(top);
		xmlSchema.getElements().add(getQName(top.getName()), top);
		for (XmlSchemaType type : schemaTypes.values()) {
			xmlSchema.getItems().add(type);
		}
		return xmlSchema;
	}
	
	protected class Visitor extends BaseSchemaObjectVisitor {
		
		@Override
		public void visit(TypedObjectRef typedObjectRef) {
			
			TypedObject typedObject = typedObjectRef.getTypedObject();
			XmlSchemaElement element = new XmlSchemaElement();
			element.setName(getName(typedObjectRef));
			if (typedObject != null) {
				if (!schemaTypes.containsKey(typedObject.getType())) {
					/*
					 * prevent loop
					 */
					schemaTypes.put(typedObject.getType(), null);
					/*
					 * drill down into unknown type
					 */
					typedObjectRef.getTypedObject().acceptVisitor(this);
				}
				element.setSchemaTypeName(getQName(typedObject.getType()));
				if (!SimpleTypes.isPrimitiveType(typedObject.getType())) {
					element.setMinOccurs(0);
				}
			} else {
				throw new IllegalArgumentException(typedObjectRef + " does not reference type");
			}
			/*
			 * leave for parent
			 */
			elementStack.push(element);
		}

		@Override
		public void visit(TypedObject object) {
			
			XmlSchemaComplexType complexType = null;
			if (isSimpleType(object.getType())) {
				complexType = newUnstructuredType(getNCName(object.getType()));
			} else {
				complexType = newComplexType(getNCName(object.getType()));
				XmlSchemaSequence propertiesSequence = new XmlSchemaSequence();
				complexType.setParticle(propertiesSequence);
				/*
				 *  collect child elements
				 */
				if (object.hasChildren()) {
					List<XmlSchemaElement> childElements = new ArrayList<XmlSchemaElement>(object.getChildCount());
					for (int i = 0; i < object.getChildCount(); ++i) {
						XmlSchemaElement element = elementStack.pop();
						/*
						 * prepend to reverse order
						 */
						childElements.add(0, element);
					}
					for (XmlSchemaElement childElement : childElements) {
						propertiesSequence.getItems().add(childElement);
					}
				}
				
			}
			schemaTypes.put(object.getType(), complexType);
		}
		
		@Override
		public void visit(SchemaCollection collection) {
			
			/*
			 * repeated element
			 */
			XmlSchemaElement collectionItem = new XmlSchemaElement();
			collectionItem.setName(getName(collection));
			collectionItem.setMinOccurs(0);
			collectionItem.setMaxOccurs(Long.MAX_VALUE);
			
			/*
			 * item content
			 */
			XmlSchemaElement nestedElement = elementStack.pop();
			if (collection.getItem() instanceof TypedObjectRef) {
				TypedObjectRef typedObjectRef = (TypedObjectRef)collection.getItem();
				collectionItem.setSchemaTypeName(getQName(typedObjectRef.getTypedObject().getType()));
			} else {
				XmlSchemaComplexType complexType = newComplexType();
				XmlSchemaSequence itemValueSequence = new XmlSchemaSequence();
				complexType.setParticle(itemValueSequence);
				itemValueSequence.getItems().add(nestedElement);
				collectionItem.setSchemaType(complexType);
			}
			/*
			 * leave for parent
			 */
			elementStack.push(collectionItem);
		}
		
		@Override
		public void visit(SchemaMap map) {
			
			/*
			 * repeated element
			 */
			XmlSchemaElement resultElement = null;
			XmlSchemaElement entryElement = new XmlSchemaElement();
			entryElement.setName(BeanConstants.MAP_ENTRY_ELEMENT_NAME);
			entryElement.setMinOccurs(0);
			entryElement.setMaxOccurs(Long.MAX_VALUE);
			XmlSchemaComplexType entryType = newComplexType();
			
			XmlSchemaSequence keyValueSequence = new XmlSchemaSequence();
			entryType.setParticle(keyValueSequence);
			entryElement.setSchemaType(entryType);
			resultElement = entryElement;
			/*
			 * container (only if inside type or top-level)
			 */
			if (map.getParent() == null || map.getParent() instanceof TypedObject) {
				XmlSchemaElement mapElement = new XmlSchemaElement();
				mapElement.setName(getName(map));
				XmlSchemaComplexType mapType = newComplexType();
				XmlSchemaSequence entrySequence = new XmlSchemaSequence();
				mapType.setParticle(entrySequence);
				mapElement.setSchemaType(mapType);
				entrySequence.getItems().add(entryElement);
				resultElement = mapElement;
			}
			// first value then key because value is visited at last
			/*
			 * map value
			 */
			XmlSchemaElement valueElement = new XmlSchemaElement();
			valueElement.setName(BeanConstants.MAP_VALUE_ELEMENT_NAME);
			XmlSchemaElement nestedElement = elementStack.pop();
			if (map.getValue() instanceof TypedObjectRef) {
				TypedObjectRef typedObjectRef = (TypedObjectRef)map.getValue();
				valueElement.setSchemaTypeName(getQName(typedObjectRef.getTypedObject().getType()));
			} else {
				XmlSchemaComplexType valueType = newComplexType();
				XmlSchemaSequence valueSequence = new XmlSchemaSequence();
				valueType.setParticle(valueSequence);
				valueSequence.getItems().add(nestedElement);
				valueElement.setSchemaType(valueType);
			}
			/*
			 * map key
			 */
			XmlSchemaElement keyElement = new XmlSchemaElement();
			keyElement.setName(BeanConstants.MAP_KEY_ELEMENT_NAME);
			nestedElement = elementStack.pop();
			if (map.getKey() instanceof TypedObjectRef) {
				TypedObjectRef typedObjectRef = (TypedObjectRef)map.getKey();
				keyElement.setSchemaTypeName(getQName(typedObjectRef.getTypedObject().getType()));
			} else {
				XmlSchemaComplexType keyType = newComplexType();
				XmlSchemaSequence keySequence = new XmlSchemaSequence();
				keyType.setParticle(keySequence);
				keySequence.getItems().add(nestedElement);
				keyElement.setSchemaType(keyType);
			}
			keyValueSequence.getItems().add(keyElement);
			keyValueSequence.getItems().add(valueElement);
			/*
			 * leave element for parent
			 */
			elementStack.push(resultElement);
		}

	}
	
	protected static String getName(SchemaObject object) {
		return object.getName() != null ? object.getName() : object.getDefaultName();
	}
	
	private QName getQName(String localPart) {
		return new QName(NS_URI, getNCName(localPart));
	}
	
	private XmlSchemaComplexType newUnstructuredType(String typeName) {
		
		XmlSchemaComplexType complexType = new XmlSchemaComplexType(xmlSchema);
		complexType.setName(typeName);
		
		XmlSchemaSimpleContent simpleContent = new XmlSchemaSimpleContent();
		complexType.setContentModel(simpleContent);
		
		XmlSchemaSimpleContentExtension contentExtension = new XmlSchemaSimpleContentExtension();
		simpleContent.setContent(contentExtension);
		contentExtension.setBaseTypeName(getBaseTypeName(typeName));
		
		/*
		 * allow attributes from other namespaces
		 */
		contentExtension.setAnyAttribute(anyAttributeOfOtherNamespace());
		
		xmlSchema.addType(complexType);
		return complexType;
	}
	
	private XmlSchemaComplexType newComplexType() {
		return newComplexType(null);
	}
	
	private String getNCName(String name) {
		return name.replace('$', '-');
	}
	
	private XmlSchemaComplexType newComplexType(String name) {
		
		XmlSchemaComplexType complexType = new XmlSchemaComplexType(xmlSchema);
		if (name != null) {
			complexType.setName(name);
			xmlSchema.addType(complexType);
		}
		/*
		 * allow attributes from other namespaces
		 */
		complexType.setAnyAttribute(anyAttributeOfOtherNamespace());
		return complexType;
	}
	
	private XmlSchemaAnyAttribute anyAttributeOfOtherNamespace() {
		
		XmlSchemaAnyAttribute anyAttribute = new XmlSchemaAnyAttribute();
		anyAttribute.setNamespace("##other");
		anyAttribute.setProcessContent(new XmlSchemaContentProcessing(Constants.BlockConstants.LAX));
		return anyAttribute;
	}
	
	private boolean isSimpleType(String typeName) {
		return SimpleTypes.isSimpleType(typeName);
	}
	
	public static SchemaObject findSchemaObject(String path, SchemaObject context) {
		return new Finder(context).findSchemaObject(path);
	}
	
	protected static class Finder {
		
		protected SchemaObject context;
		
		public Finder(SchemaObject object) {
			this.context = object;
		}
		
		public SchemaObject findSchemaObject(String path) {
			return findSchemaObject(new LinkedList<String>(Arrays.asList(StringUtils.split(path.substring(1), "/"))), context);
		}
		
		protected SchemaObject findSchemaObject(Queue<String> path, SchemaObject object) {
			
			if (path.isEmpty() || object == null) {
				return null;
			}
			
			String objectName = getName(object);
			String pathName = path.peek();
			if (objectName.equals(pathName)) {
				if (path.size() == 1) {
					return object;
				} else {
					path.poll();
				}
			}
			if (object instanceof SchemaCollection) {
				SchemaCollection collection = (SchemaCollection) object;
				if (path.isEmpty()) {
					return collection.getItem();
				}
				return findSchemaObject(path, collection.getItem());
			}
			if (object instanceof SchemaMap) {
				SchemaMap map = (SchemaMap)object;
				if (BeanConstants.MAP_ENTRY_ELEMENT_NAME.equals(path.peek())) {
					path.poll();
					if (path.isEmpty()) {
						return map;
					} else {
						pathName = path.poll();
						if (BeanConstants.MAP_KEY_ELEMENT_NAME.equals(pathName)) {
							if (path.isEmpty()) {
								return map.getKey();
							}
							return findSchemaObject(path, map.getKey());
						}
						if (BeanConstants.MAP_VALUE_ELEMENT_NAME.equals(pathName)) {
							if (path.isEmpty()) {
								return map.getValue();
							}
							return findSchemaObject(path, map.getValue());
						}
					}
				}
				return null;
			}
			if (object instanceof TypedObjectRef) {
				TypedObjectRef objectRef = (TypedObjectRef)object;
				TypedObject typedObject = objectRef.getTypedObject();
				if (typedObject.hasChildren()) {
					SchemaObject matchingChild = null;
					for (SchemaObject child : typedObject.getChildren()) {
						matchingChild = findSchemaObject(path, child);
						if (matchingChild != null) {
							return matchingChild;
						}
					}
				}
			}
			return null;
		}
	}
	
	private static final Map<String, QName> TYPE_CONVERSION_TABLE = new HashMap<String, QName>();
	static {
		// primitive types
		TYPE_CONVERSION_TABLE.put(boolean.class.getName(), Constants.XSD_BOOLEAN);
		TYPE_CONVERSION_TABLE.put(byte.class.getName(), Constants.XSD_BYTE);
		TYPE_CONVERSION_TABLE.put(char.class.getName(), Constants.XSD_STRING);
		TYPE_CONVERSION_TABLE.put(double.class.getName(), Constants.XSD_DOUBLE);
		TYPE_CONVERSION_TABLE.put(float.class.getName(), Constants.XSD_FLOAT);
		TYPE_CONVERSION_TABLE.put(int.class.getName(), Constants.XSD_INT);
		TYPE_CONVERSION_TABLE.put(long.class.getName(), Constants.XSD_LONG);
		TYPE_CONVERSION_TABLE.put(short.class.getName(), Constants.XSD_SHORT);
		
		TYPE_CONVERSION_TABLE.put(byte[].class.getName(), Constants.XSD_BASE64);

		// Standard types
		TYPE_CONVERSION_TABLE.put(BigDecimal.class.getName(), Constants.XSD_DECIMAL);
		TYPE_CONVERSION_TABLE.put(BigInteger.class.getName(), Constants.XSD_POSITIVEINTEGER);
		TYPE_CONVERSION_TABLE.put(Boolean.class.getName(), Constants.XSD_BOOLEAN);
		TYPE_CONVERSION_TABLE.put(Byte.class.getName(), Constants.XSD_BYTE);
		TYPE_CONVERSION_TABLE.put(Character.class.getName(), Constants.XSD_STRING);
		TYPE_CONVERSION_TABLE.put(Double.class.getName(), Constants.XSD_DOUBLE);
		TYPE_CONVERSION_TABLE.put(Float.class.getName(), Constants.XSD_FLOAT);
		TYPE_CONVERSION_TABLE.put(Integer.class.getName(), Constants.XSD_INT);
		TYPE_CONVERSION_TABLE.put(Long.class.getName(), Constants.XSD_LONG);
		TYPE_CONVERSION_TABLE.put(Short.class.getName(), Constants.XSD_SHORT);
		TYPE_CONVERSION_TABLE.put(String.class.getName(), Constants.XSD_STRING);

		// Other types
		TYPE_CONVERSION_TABLE.put(java.util.Date.class.getName(), Constants.XSD_DATETIME);
		TYPE_CONVERSION_TABLE.put(java.sql.Date.class.getName(), Constants.XSD_DATE);
		TYPE_CONVERSION_TABLE.put(Calendar.class.getName(), Constants.XSD_DATETIME);
		TYPE_CONVERSION_TABLE.put(Timestamp.class.getName(), Constants.XSD_TIME);
		TYPE_CONVERSION_TABLE.put(URL.class.getName(), Constants.XSD_ANYURI);
	}
	
	private static QName getBaseTypeName(String typeName) {
		QName toReturn = TYPE_CONVERSION_TABLE.get(typeName);
		if (toReturn == null) {
			return Constants.XSD_STRING;
		}
		
		return toReturn;
	}
}
