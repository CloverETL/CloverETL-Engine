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

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.jetel.data.DataField;
import org.jetel.data.tree.formatter.CollectionWriter;
import org.jetel.data.tree.formatter.TreeWriter;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataFieldMetadata;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 3.1.2012
 */
public class JsonWriterAdapter implements TreeWriter, CollectionWriter {

	private JsonGenerator jsonGenerator;
	private StateStack stateStack = new StateStack();

	/**
	 * @param outStream
	 * @param encoding
	 */
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

	@Override
	public void writeStartTree() throws JetelException {
		// Do nothing
	}
	
	private boolean inAnnonymousContext() {
		return jsonGenerator.getOutputContext().inRoot() || jsonGenerator.getOutputContext().inArray();
	}

	@Override
	public void writeStartNode(char[] name) throws JetelException {
		try {
			boolean writeFieldName = !inAnnonymousContext();
			if (!stateStack.isObject()) {
				jsonGenerator.writeStartObject();
			}
			if (writeFieldName) {
				jsonGenerator.writeFieldName(new String(name));
				stateStack.push(false);
			} else {
				stateStack.push(true);
			}
		} catch (JsonGenerationException e) {
			throw new JetelException(e.getMessage(), e);
		} catch (IOException e) {
			throw new JetelException(e.getMessage(), e);
		}
	}

	@Override
	public void writeLeaf(Object value) throws JetelException {
		try {
			stateStack.setHasValue();
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
			if (stateStack.isObject()) { 
				jsonGenerator.writeEndObject();
			} else if (!stateStack.hasValue()) {
				jsonGenerator.writeStartObject();
				jsonGenerator.writeEndObject();
			}
			stateStack.pop();
		} catch (JsonGenerationException e) {
			throw new JetelException(e.getMessage(), e);
		} catch (IOException e) {
			throw new JetelException(e.getMessage(), e);
		}
	}

	@Override
	public void writeStartCollection(char[] collectionName) throws JetelException {
		try {
			if (!inAnnonymousContext()) {
				jsonGenerator.writeFieldName(new String(collectionName));
			}
			jsonGenerator.writeStartArray();
			
			stateStack.push(false);
		} catch (JsonGenerationException e) {
			throw new JetelException(e.getMessage(), e);
		} catch (IOException e) {
			throw new JetelException(e.getMessage(), e);
		}
	}

	@Override
	public void writeEndCollection(char[] collectionName) throws JetelException {
		try {
			jsonGenerator.writeEndArray();
			stateStack.pop();
		} catch (JsonGenerationException e) {
			throw new JetelException(e.getMessage(), e);
		} catch (IOException e) {
			throw new JetelException(e.getMessage(), e);
		}
	}

	private void writeObjectInternal(Object content) throws JsonProcessingException, IOException {
		Object actualContent;
		if (content instanceof DataField) {
			actualContent = getDataFieldValue((DataField) content);
		} else if (content instanceof String) {
			actualContent = (String) content;
		} else {
			throw new IllegalArgumentException("Unknown type of property content");
		}

		jsonGenerator.writeObject(actualContent);
	}

	private Object getDataFieldValue(DataField field) {
		switch (field.getType()) {
		case DataFieldMetadata.STRING_FIELD:
			return field.toString();
		default:
			return field.getValue();
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

	private static class StateStack {
		private boolean[] isObject = new boolean[11];
		private boolean[] hasValue = new boolean[11];
		private int depth;

		public void push(boolean state) {
			if (depth == hasValue.length) {
				boolean[] newHasChild = new boolean[hasValue.length * 2 + 1];
				System.arraycopy(hasValue, 0, newHasChild, 0, depth);
				hasValue = newHasChild;

				boolean[] newShouldIndent = new boolean[hasValue.length];
				System.arraycopy(isObject, 0, newShouldIndent, 0, depth);
				isObject = newShouldIndent;

			}
			hasValue[depth] = true;
			depth++;
			isObject[depth] = state;
		}

		public void pop() {
			depth--;
		}

		public boolean hasValue() {
			return hasValue[depth];
		}

		public void setHasValue() {
			hasValue[depth] = true;
		}

		public boolean isObject() {
			return isObject[depth];
		}
	}

}
