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
package org.jetel.data.tree.json.formatter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.jetel.data.DataField;
import org.jetel.data.DecimalDataField;
import org.jetel.data.ListDataField;
import org.jetel.data.tree.formatter.CollectionWriter;
import org.jetel.data.tree.formatter.TreeWriter;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 3.1.2012
 */
public class JsonWriterAdapter implements TreeWriter, CollectionWriter {

	private static enum NodeType {
		LIST, OBJECT, PROPERTY
	}

	private JsonGenerator jsonGenerator;

	private String[] nameStack = new String[11];
	private NodeType[] typeStack = new NodeType[11];
	private int depth;
	private int writtenDepth;
	private NodeType requestedObjectType;

	public JsonWriterAdapter(OutputStream outStream, String encoding, boolean omitNewLines) {
		JsonFactory jsonFactory = new JsonFactory();
		try {
			JsonEncoding jsonEncoding = jsonEncodingFromJavaType(encoding);
			jsonGenerator = jsonFactory.createJsonGenerator(outStream, jsonEncoding);
			if (!omitNewLines) {
				jsonGenerator.useDefaultPrettyPrinter();
			}
		} catch (IOException e) {
			throw new JetelRuntimeException(e);
		}
	}

	private JsonEncoding jsonEncodingFromJavaType(String encoding) {
		for (JsonEncoding jsonEncoding : JsonEncoding.values()) {
			if (encoding.equals(jsonEncoding.getJavaName())) {
				return jsonEncoding;
			}
		}

		throw new UnsupportedCharsetException(encoding);
	}

	public void push(NodeType objectType, boolean enforceType, char[] name) {
		if (depth == nameStack.length) {
			int newLength = nameStack.length * 2 + 1;

			nameStack = Arrays.copyOf(nameStack, newLength);
			typeStack = Arrays.copyOf(typeStack, newLength);
		}

		if (depth > 0 && typeStack[depth - 1] == NodeType.PROPERTY) {
			typeStack[depth - 1] = requestedObjectType;
		}
		if (enforceType) {
			typeStack[depth] = objectType;
		} else {
			typeStack[depth] = NodeType.PROPERTY;
		}

		this.requestedObjectType = objectType;
		if (name != null) {
			nameStack[depth] = new String(name);
		}

		depth++;
	}

	public void pop() {
		depth--;
		if (writtenDepth > depth) {
			writtenDepth = depth;
		}
	}

	@Override
	public void writeStartTree() throws JetelException {
		// Do nothing
	}

	private boolean inAnnonymousContext() {
		return jsonGenerator.getOutputContext().inRoot() || jsonGenerator.getOutputContext().inArray();
	}

	@Override
	public void writeStartNode(char[] name) throws JetelException {
		push(NodeType.OBJECT, false, name);
	}

	private void performDeferredWrite() throws JsonGenerationException, IOException {
		for (int i = writtenDepth; i < depth; i++) {
			if (!inAnnonymousContext()) {
				jsonGenerator.writeFieldName(nameStack[i]);
			}

			switch (typeStack[i]) {
			case LIST:
				jsonGenerator.writeStartArray();
				break;
			case OBJECT:
				jsonGenerator.writeStartObject();
			default:
				break;
			}
		}
		writtenDepth = depth;
	}

	@Override
	public void writeLeaf(Object value) throws JetelException {
		try {
			performDeferredWrite();
			writeObjectInternal(value);
		} catch (JsonGenerationException e) {
			throw new JetelException(e.getMessage(), e);
		} catch (IOException e) {
			throw new JetelException(e.getMessage(), e);
		}
	}

	@Override
	public void writeEndNode(char[] name) throws JetelException {
		try {
			performDeferredWrite();
			pop();

			if (typeStack[depth] == NodeType.OBJECT) {
				jsonGenerator.writeEndObject();
			}
		} catch (JsonGenerationException e) {
			throw new JetelException(e.getMessage(), e);
		} catch (IOException e) {
			throw new JetelException(e.getMessage(), e);
		}
	}

	@Override
	public void writeStartCollection(char[] collectionName) throws JetelException {
		push(NodeType.LIST, true, collectionName);
	}

	@Override
	public void writeEndCollection(char[] collectionName) throws JetelException {
		try {
			performDeferredWrite();
			jsonGenerator.writeEndArray();
		} catch (JsonGenerationException e) {
			throw new JetelException(e.getMessage(), e);
		} catch (IOException e) {
			throw new JetelException(e.getMessage(), e);
		}

		pop();
	}

	private void writeObjectInternal(Object content) throws JsonProcessingException, IOException {
		if (content instanceof ListDataField) {
			ListDataField list = (ListDataField) content;
			for (DataField dataField : list) {
				writeDataFieldValue(dataField);
			}
		} else if (content instanceof DataField) {
			writeDataFieldValue((DataField) content);
		} else if (content instanceof String) {
			jsonGenerator.writeString((String) content);
		} else {
			throw new IllegalArgumentException("Unknown type of property content");
		}
	}

	private void writeDataFieldValue(DataField field) throws JsonGenerationException, IOException {
		switch (field.getMetadata().getDataType()) {
		case BYTE:
		case CBYTE:
		case DATE:
		case STRING:
			jsonGenerator.writeString(field.toString());
			break;
		case DECIMAL:
			jsonGenerator.writeNumber(((DecimalDataField) field).getValue().getBigDecimalOutput());
			break;
		default:
			jsonGenerator.writeObject(field.getValue());
		}
	}

	public void flush() throws IOException {
		jsonGenerator.flush();
	}

	public void close() throws IOException {
		jsonGenerator.close();
	}

	@Override
	public void writeEndTree() throws JetelException {
		// Do nothing
	}

}