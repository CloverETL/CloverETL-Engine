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
import java.io.Writer;
import java.net.URLEncoder;
import java.util.ArrayDeque;
import java.util.Deque;

import javax.xml.parsers.SAXParser;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
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
	
	
	private static final String NAMESPACE_URI = "";
	
	private static final String XML_NAME_OBJECT = "json_object";
	private static final String XML_NAME_ARRAY = "json_array";
	
	private static final Attributes ATTRIBUTES = new InternalAttributes();
	private static final JsonFactory JSON_FACTORY = new JsonFactory();
	
	DefaultHandler handler;
	boolean xmlEscapeChars=false;
	
	private boolean jsonStarted = false;
	
	public JsonSaxParser(){
	}
	
	
	@Override
	public org.xml.sax.Parser getParser() throws SAXException {
		return null;
	}

	@Override
	public XMLReader getXMLReader() throws SAXException {
		return null;
	}

	@Override
	public boolean isNamespaceAware() {
		return false;
	}

	@Override
	public boolean isValidating() {
		return false;
	}

	@Override
	public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
		//do nothing - no properties supported
	}

	@Override
	public Object getProperty(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
		return null;
	}

	@Override
	public void parse(InputSource is,DefaultHandler handler) throws IOException, SAXException{
		this.handler=handler;
		xmlEscapeChars=false;
		doParse(is.getCharacterStream());
	}

	
	public void convertJSON2XML(Reader in,Writer out,boolean firstObjectOnly,boolean suppresNodeValues)throws IOException, SAXException{
		this.handler=new JSON2XMLHandler(out,suppresNodeValues);
		JsonParser parser;
		try {
			parser = JSON_FACTORY.createJsonParser(in);
		} catch (JsonParseException e) {
			throw new IOException(e);
		}
		Deque<JsonToken> tokens = new ArrayDeque<JsonToken>();
		Deque<String> names = new ArrayDeque<String>();
		Deque<Integer> depthCounter = new ArrayDeque<Integer>();
		depthCounter.add(0);
		JsonToken currentToken = null;
		
		xmlEscapeChars=true;
		
		handler.startDocument();
		boolean go = true;
		jsonStarted = false;
		while (go && (currentToken = parser.nextToken()) != null) {
			if (firstObjectOnly && jsonStarted && depthCounter.size() == 1 && depthCounter.peek() == 0){
				go = false;
			} else {
				processToken(currentToken, parser, tokens, names, depthCounter);
			}
		}
		
		handler.endDocument();
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
		Deque<Integer> depthCounter = new ArrayDeque<Integer>();
		depthCounter.add(0);
		JsonToken currentToken = null;
		
		handler.startDocument();
		
		while ((currentToken = parser.nextToken()) != null) {
			processToken(currentToken, parser, tokens, names, depthCounter);
		}
		
		handler.endDocument();
		
		
	}
	
	protected void processToken(final JsonToken token, JsonParser parser, Deque<JsonToken> tokens, Deque<String> names, Deque<Integer> depthCounter) 
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
				names.add(XML_NAME_ARRAY);
			} else if (tokens.peekLast() == JsonToken.FIELD_NAME) {
				// named array - remove field token
				tokens.removeLast();
			} else if (tokens.peekLast() == JsonToken.START_ARRAY) {
				// add nested element
				
				String name = names.getLast();
				int top = depthCounter.pollLast();
				if (top > 0) {
					name = name + "_" + top;
				}
				top++;
				depthCounter.add(top);
				jsonStarted = true;
				handler.startElement(NAMESPACE_URI, name, name, ATTRIBUTES);
			}
			tokens.add(token);
			break;
		}
		case START_OBJECT: {
			if (names.isEmpty()) {
				names.add(XML_NAME_OBJECT);
			} else if (tokens.peekLast() == JsonToken.FIELD_NAME) {
				// named object - remove field token
				tokens.removeLast();
			}
			tokens.add(token);
			String name = names.getLast();
			if (!depthCounter.isEmpty()) {
				int top = depthCounter.peekLast();
				if (top > 0) {
					name = name + "_" + top;
				}
			}
			jsonStarted = true;
			depthCounter.add(Integer.valueOf(0));
			handler.startElement(NAMESPACE_URI, name,name, ATTRIBUTES);
			break;
		}
		case END_ARRAY: {
			// remove corresponding start
			tokens.removeLast();
			
			String name = names.getLast();
			int top = depthCounter.pollLast();
			if (top > 0) {
				top--;
			}
			if (top > 0) {
				name = name + "_" + top;
			}
			depthCounter.add(top);
			
			if (!tokens.isEmpty() && tokens.peekLast() == JsonToken.START_ARRAY) {
				// end nested array
				handler.endElement(NAMESPACE_URI,name, name);
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
			depthCounter.pollLast();
			if (!depthCounter.isEmpty()) {
				int top = depthCounter.pollLast();
				if (top > 0) {
					name = name + "_" + top;
				}
				depthCounter.add(top);
			}
			handler.endElement(NAMESPACE_URI, name, name);
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
				processScalarValue(parser);
				handler.endElement(NAMESPACE_URI, valueName, valueName);
				tokens.removeLast();
				names.removeLast();
				break;
			}
			case START_ARRAY: {
				// array item
				String name = names.getLast();
				int top = depthCounter.peekLast();
				if (top > 0) {
					name = name + "_" + top;
				}
				
				handler.startElement(NAMESPACE_URI, name, name, ATTRIBUTES);
				processScalarValue(parser);
				handler.endElement(NAMESPACE_URI, name, name);
			}
			}
		}
	}
	
	protected void processScalarValue(JsonParser parser) throws JsonParseException, IOException, SAXException {
		
		if (parser.getCurrentToken() != JsonToken.VALUE_NULL) {
			char[] chars;
			if (xmlEscapeChars){
				chars=URLEncoder.encode(parser.getText()).toCharArray(); //may be too conservative
			}else{
				chars= parser.getText().toCharArray();
			}
			handler.characters(chars, 0, chars.length);
		}
	}

	protected static class JSON2XMLHandler extends DefaultHandler{
		Writer out;
		boolean suppressNodeValues; 
		
		JSON2XMLHandler(Writer out, boolean suppresNodeValues){
			this.out=out;
			this.suppressNodeValues=suppresNodeValues;
		}
		

		/*	
		@Override
		public void startDocument ()
				throws SAXException{
			try{
				out.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
			} catch (IOException e) {
				new SAXException(e);
			}
		}
		*/
		
		@Override
		public void startElement (String uri, String localName,
			      String qName, Attributes attributes)	throws SAXException	{
			try {
				out.append("<").append(localName).append(">");
			} catch (IOException e) {
				new SAXException(e);
			}
			
		}
		
		@Override
		 public void endElement (String uri, String localName, String qName)
					throws SAXException{
			try {
				out.append("</").append(localName).append(">");
			} catch (IOException e) {
				new SAXException(e);
			}
			
		}
		
		@Override
		public void characters (char ch[], int start, int length)
				throws SAXException{
			if (suppressNodeValues) return;
			try {
				out.write(ch,start,length);
			} catch (IOException e) {
				new SAXException(e);
			}
		}
	}
	
	
	
	/**
	 * Dummy class - a placeholder.
	 * 
	 * @author dpavlis (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Aug 12, 2013
	 */
	private static class InternalAttributes implements Attributes{

		@Override
		public int getLength() {
			return 0;
		}

		@Override
		public String getURI(int index) {
			return null;
		}

		@Override
		public String getLocalName(int index) {
			return null;
		}

		@Override
		public String getQName(int index) {
			return null;
		}

		@Override
		public String getType(int index) {
			return null;
		}

		@Override
		public String getValue(int index) {
			return null;
		}

		@Override
		public int getIndex(String uri, String localName) {
			return 0;
		}

		@Override
		public int getIndex(String qName) {
			return 0;
		}

		@Override
		public String getType(String uri, String localName) {
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
