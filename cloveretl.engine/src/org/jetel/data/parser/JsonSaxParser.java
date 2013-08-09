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
package org.jetel.data.parser;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayDeque;
import java.util.Deque;

import javax.xml.parsers.SAXParser;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.jetel.data.parser.XmlSaxParser.SAXHandler;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Aug 8, 2013
 */
public class JsonSaxParser extends SAXParser {
	
	private static final JsonFactory JSON_FACTORY = new JsonFactory();
	private static final String NAMESPACE_URI = "";//"{}";
	private static final Attributes ATTRIBUTES = new InternalAttributes();
	
	
	SAXHandler handler;
	
	public JsonSaxParser(){
	}
	
	
	@Override
	public org.xml.sax.Parser getParser() throws SAXException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XMLReader getXMLReader() throws SAXException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isNamespaceAware() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isValidating() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
		// TODO Auto-generated method stub

	}

	@Override
	public Object getProperty(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}

	public void parse(InputSource is,SAXHandler handler) throws IOException, SAXException{
		this.handler=handler;
		doParse(is.getCharacterStream());
	}

	@Override
	public void parse(InputSource is,DefaultHandler handler) throws IOException, SAXException{
		parse(is,(SAXHandler)handler);
	}

	
	protected void doParse(Reader reader)throws IOException, SAXException{
		JsonParser parser;
		try {
			parser = JSON_FACTORY.createJsonParser(reader);
		} catch (JsonParseException e) {
			throw new IOException(e);
		}
		Deque<JsonToken> tokens = new ArrayDeque<JsonToken>();
		Deque<String> names = new ArrayDeque<String>();
		JsonToken currentToken = null;
		
		handler.startDocument();
		
		while ((currentToken = parser.nextToken()) != null) {
			//ensureRootStart();
			processToken(currentToken, parser, tokens, names);
		}
		
		handler.endDocument();
		
		
	}
	
	protected void processToken(final JsonToken token, JsonParser parser, Deque<JsonToken> tokens, Deque<String> names) 
		throws JsonParseException, IOException, SAXException {
		
		if (token == null) {
			return;
		}
		switch (token) {
		case FIELD_NAME: {
			names.add(parser.getText());
			tokens.add(token);
			break;
		}
		case START_ARRAY: {
			if (names.isEmpty()) {
				// top level array
				names.add("array");
			} else if (tokens.peekLast() == JsonToken.FIELD_NAME) {
				// named array - remove field token
				tokens.removeLast();
			} else if (tokens.peekLast() == JsonToken.START_ARRAY) {
				// add nested element
				
				String name = names.getLast();
				handler.startElement(NAMESPACE_URI, name, name, ATTRIBUTES);
				//DEBUG
				//System.out.println("<"+name+">");
			}
			tokens.add(token);
			break;
		}
		case START_OBJECT: {
			if (names.isEmpty()) {
				names.add("object");
			} else if (tokens.peekLast() == JsonToken.FIELD_NAME) {
				// named object - remove field token
				tokens.removeLast();
			}
			tokens.add(token);
			String name = names.getLast();
			handler.startElement(NAMESPACE_URI, name,name, ATTRIBUTES);
			//DEBUG
			//System.out.println("<"+name+">");
			break;
		}
		case END_ARRAY: {
			// remove corresponding start
			tokens.removeLast();
			if (!tokens.isEmpty() && tokens.peekLast() == JsonToken.START_ARRAY) {
				// end nested array
				String name = names.getLast();
				handler.endElement(NAMESPACE_URI,name, name);
				//DEBUG
				//System.out.println("</"+name+">");
			} else {
				// remove name if not inside array
				names.removeLast();
			}
			break;
		}
		case END_OBJECT: {
			// remove corresponding start
			tokens.removeLast();
			// end current object
			String name = names.getLast();
			handler.endElement(NAMESPACE_URI, name, name);
			//DEBUG
			//System.out.println("</"+name+">");
			if (tokens.isEmpty() || tokens.peekLast() != JsonToken.START_ARRAY) {
				// remove name if not inside array
				names.removeLast();
			}
			break;
		}
		}
		if (token.isScalarValue()) {
			String valueName = names.getLast();
			switch (tokens.getLast()) {
			case FIELD_NAME: {
				// simple property
				handler.startElement(NAMESPACE_URI, valueName, valueName, ATTRIBUTES);
				//DEBUG
				//System.out.print("<"+valueName+">");
				processScalarValue(parser);
				handler.endElement(NAMESPACE_URI, valueName, valueName);
				//DEBUG
				//System.out.println("</"+valueName+">");
				tokens.removeLast();
				names.removeLast();
				break;
			}
			case START_ARRAY: {
				// array item
				handler.startElement(NAMESPACE_URI, valueName, valueName, ATTRIBUTES);
				//DEBUG
				//System.out.print("<"+valueName+">");
				processScalarValue(parser);
				handler.endElement(NAMESPACE_URI, valueName, valueName);
				//DEBUG
				//System.out.println("</"+valueName+">");
			}
			}
		}
	}
	
	protected void processScalarValue(JsonParser parser) throws JsonParseException, IOException, SAXException {
		
		if (parser.getCurrentToken() != JsonToken.VALUE_NULL) {
			char[] chars= parser.getText().toCharArray();
			handler.characters(chars, 0, chars.length);
			//DEBUG
			//System.out.print(str.toString());
		}
	}


	/**
	 * JsonParser.getText() decodes escape sequences, such as "\b".
	 * This results in invalid XML characters, which need to be
	 * replaced with &#nnnn; entities.
	 * 
	 * See <a href="http://www.w3.org/TR/REC-xml/#NT-Char">http://www.w3.org/TR/REC-xml/#NT-Char</a>.
	 * 
	 * @param text
	 * @return
	 */
	/* This is not needed for direct JSON -> Clover
	private static StringBuilder xmlEscape(String text) {
		StringBuilder sb = new StringBuilder(text.length());
		for (int i = 0; i < text.length(); ) {
			int c = text.codePointAt(i);
			switch (c) {
			case 0x9:
			case 0xA:
			case 0xD:
				sb.appendCodePoint(c);
				break;
			default:
				if (c == '&') {
					sb.append("&amp;");
				} else if ((c >= 0x20 && c <= 0xD7FF) || (c >= 0xE000 && c <= 0xFFFD) || (c >= 0x10000 && c <= 0x10FFFF)) {
					sb.appendCodePoint(c);
				} else {
					sb.append("&#x").append(Integer.toHexString(c)).append(';');
				}
			}
			i += Character.charCount(c);
		}
		return sb;
	}
	*/
	private static class InternalAttributes implements Attributes{

		@Override
		public int getLength() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public String getURI(int index) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getLocalName(int index) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getQName(int index) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getType(int index) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getValue(int index) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int getIndex(String uri, String localName) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int getIndex(String qName) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public String getType(String uri, String localName) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getType(String qName) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getValue(String uri, String localName) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getValue(String qName) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	
}
