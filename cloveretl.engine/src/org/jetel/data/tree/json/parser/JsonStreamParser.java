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
package org.jetel.data.tree.json.parser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.jetel.data.tree.parser.TreeContentHandler;
import org.jetel.data.tree.parser.TreeStreamParser;
import org.jetel.exception.JetelRuntimeException;
import org.xml.sax.InputSource;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 19 Jan 2012
 */
public class JsonStreamParser implements TreeStreamParser {

	private static final JsonFactory JSON_FACTORY = new JsonFactory();

	private JsonParser jsonParser;
	private TreeContentHandler contentHandler;

	@Override
	public void parse(Object input) {
		InputStream inputStream;
		if (input instanceof ReadableByteChannel) {
			inputStream = Channels.newInputStream((ReadableByteChannel) input);
		} else if (input instanceof InputSource) {
			inputStream = ((InputSource) input).getByteStream();
		} else {
			throw new JetelRuntimeException("Unsupported type of inpu " + input);
		}
		
		try {
			jsonParser = JSON_FACTORY.createJsonParser(inputStream);
		} catch (JsonParseException e) {
			throw new JetelRuntimeException(e);
		} catch (IOException e) {
			throw new JetelRuntimeException(e);
		}

		try {
			jsonParser.nextToken();
			contentHandler.startTree();
			
			String rootNodeName;
			JsonToken currentToken = jsonParser.getCurrentToken();
			if (JsonToken.START_ARRAY.equals(currentToken)) {
				rootNodeName = "array";
			} else {
				rootNodeName = "object";
			}
			parseElement(rootNodeName, false);
			
			contentHandler.endTree();
		} catch (IOException e) {
			throw new JetelRuntimeException("Parsing error: " + e.getMessage(), e);
		} catch (Exception e) {
			throw new JetelRuntimeException("Parsing error: " + e.getMessage(), e);
		}
	}

	private void parseObject() throws IOException, JsonParseException, Exception {
		while (jsonParser.nextToken() != null && jsonParser.getCurrentToken() != JsonToken.END_OBJECT) {
			if (JsonToken.FIELD_NAME.equals(jsonParser.getCurrentToken())) {
				String elementName = jsonParser.getCurrentName();
				// jump to element value
				jsonParser.nextToken();
				parseElement(elementName, false);
			} else {
				throw new JetelRuntimeException("Error when parsing. Expected field name got " + jsonParser.getCurrentToken());
			}
		}
	}

	private void parseElement(String elementName, boolean parentIsArray) throws Exception {
		JsonToken currentToken = jsonParser.getCurrentToken();
		if (!JsonToken.START_ARRAY.equals(currentToken) || parentIsArray) { 
			contentHandler.startNode(elementName);
		}

		if (JsonToken.START_OBJECT.equals(currentToken)) {
			parseObject();
		} else if (JsonToken.START_ARRAY.equals(currentToken)) {
			parseArray(elementName);
		} else if (currentToken.isScalarValue()) {
			parseValue();
		}

		if (!JsonToken.START_ARRAY.equals(currentToken) || parentIsArray) { 
			contentHandler.endNode(elementName);
		}
	}

	private void parseArray(String elementName) throws Exception {
		while (jsonParser.nextToken() != JsonToken.END_ARRAY && jsonParser.getCurrentToken() != null) {
			parseElement(elementName, true);
		}
	}

	private void parseValue() throws Exception {
		if (JsonToken.VALUE_NULL != jsonParser.getCurrentToken()) {
			contentHandler.leaf(jsonParser.getText());
		}
	}

	@Override
	public TreeContentHandler getContentHandler() {
		return contentHandler;
	}

	@Override
	public void setTreeContentHandler(TreeContentHandler contentHandler) {
		this.contentHandler = contentHandler;
	}

}
