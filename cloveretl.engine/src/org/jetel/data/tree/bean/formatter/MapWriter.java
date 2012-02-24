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
package org.jetel.data.tree.bean.formatter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.jetel.data.DataField;
import org.jetel.data.ListDataField;
import org.jetel.data.tree.formatter.CollectionWriter;
import org.jetel.data.tree.formatter.TreeWriter;
import org.jetel.exception.JetelException;

/**
 * @author krejcil (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 1.2.2012
 */
public class MapWriter implements TreeWriter, CollectionWriter {

	private static enum ObjectType {
		LIST, MAP, SIMPLE_PROPERTY
	}

	private Object result;

	private String[] nameStack = new String[11];
	private ObjectType[] typeStack = new ObjectType[11];
	private int depth;
	private int writtenDepth;

	private Stack<List<Object>> listStack = new Stack<List<Object>>();
	private Stack<Map<String, Object>> mapStack = new Stack<Map<String, Object>>();

	public void push(ObjectType objectType, char[] name) {
		if (depth == nameStack.length) {
			String[] newNameStack = new String[nameStack.length * 2 + 1];
			System.arraycopy(nameStack, 0, newNameStack, 0, depth);
			nameStack = newNameStack;

			ObjectType[] newTypeStack = new ObjectType[nameStack.length];
			System.arraycopy(typeStack, 0, newTypeStack, 0, depth);
			typeStack = newTypeStack;
		}
		if (typeStack[depth] == ObjectType.SIMPLE_PROPERTY) {
			typeStack[depth] = ObjectType.MAP;
		}
		depth++;
		typeStack[depth] = objectType;
		if (name != null) {
			nameStack[depth] = new String(name);
		}
	}

	public void pop() {
		depth--;
		if (writtenDepth > depth) {
			writtenDepth = depth;
			switch (typeStack[depth + 1]) {
			case LIST:
				listStack.pop();
				break;
			case MAP:
				mapStack.pop();
				break;

			default:
				break;
			}
		}
	}

	@Override
	public void writeStartTree() throws JetelException {
		// Do nothing
	}

	@Override
	public void writeStartCollection(char[] collectionName) throws JetelException {
		push(ObjectType.LIST, collectionName);
	}

	@Override
	public void writeStartNode(char[] name) throws JetelException {
		push(ObjectType.SIMPLE_PROPERTY, name);
	}

	@Override
	public void writeLeaf(Object value) throws JetelException {
		for (int i = writtenDepth; i < depth; i++) {
			if (i == 0) {
				result = createObjectForType(typeStack[i + 1], null);
			} else if (typeStack[i] == ObjectType.LIST) {
				List<Object> parent = listStack.peek();
				parent.add(createObjectForType(typeStack[i + 1], value));
			} else {
				Map<String, Object> parent = mapStack.peek();
				Object child = createObjectForType(typeStack[i + 1], value);
				parent.put(nameStack[i + 1], child);
			}
		}
		writtenDepth = depth;
	}
	
	private Object createObjectForType(ObjectType type, Object value) {
		switch (type) {
		case LIST:
			listStack.push(new ArrayList<Object>());
			return listStack.peek();
		case MAP:
			mapStack.push(new HashMap<String, Object>());
			return mapStack.peek();
		case SIMPLE_PROPERTY:
			if (value != null) {
				return getActualValue(value);
			}
		}
		
		throw new IllegalStateException("Simple property is allowed only as map entry");
	}

	private Object getActualValue(Object content) {
		if (content instanceof ListDataField) {
			ListDataField list = (ListDataField) content;
			List<Object> actualValue = new ArrayList<Object>();
			for (DataField field : list) {
				actualValue.add(getDataFieldValue(field));
			}
			return actualValue;
		} else if (content instanceof DataField) {
			return getDataFieldValue((DataField) content);
		} else if (content instanceof String) {
			return content;
		} else {
			throw new IllegalArgumentException("Unknown type of property content");
		}
	}

	private Object getDataFieldValue(DataField field) {
		switch (field.getMetadata().getDataType()) {
		case STRING:
			return field.toString();
		default:
			return field.getValue();
		}
	}

	@Override
	public void writeEndNode(char[] name) throws JetelException {
		pop();
	}

	@Override
	public void writeEndCollection(char[] collectionName) throws JetelException {
		pop();
	}

	@Override
	public void writeEndTree() throws JetelException {
		// Do nothing
	}

	public Object flushResult() {
		Object toReturn = result;
		result = null;
		return toReturn;
	}

}
