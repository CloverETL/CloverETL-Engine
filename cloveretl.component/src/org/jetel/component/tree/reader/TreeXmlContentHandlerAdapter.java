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
package org.jetel.component.tree.reader;

import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.string.TagName;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 19 Jan 2012
 */
public class TreeXmlContentHandlerAdapter implements TreeContentHandler {
	
	public static final String DUMMY_ROOT_ELEMENT_NAME = "root";
	
	private static final AttributesImpl EMPTY_ATTRIBUTES = new AttributesImpl();
	private ContentHandler contentHandler;

	@Override
	public void startTree() {
		try {
			contentHandler.startDocument();
		} catch (SAXException e) {
			throw new JetelRuntimeException("transformation to XML failed", e);
		}
	}

	@Override
	public void startNode(String name) {
		try {
			/*
			 * name has to be valid NCName
			 */
			name = TagName.encode(name);
			contentHandler.startElement("", name, name, EMPTY_ATTRIBUTES);
		} catch (SAXException e) {
			throw new JetelRuntimeException("transformation to XML failed", e);
		}
	}

	@Override
	public void leaf(Object value) {
		if (value instanceof String) {
			String escaped = xmlEscape((String)value);
			char[] stringCharArray = escaped.toCharArray();
			try {
				contentHandler.characters(stringCharArray, 0, stringCharArray.length);
			} catch (SAXException e) {
				throw new JetelRuntimeException("transformation to XML failed", e);
			}
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
	private String xmlEscape(String text) {
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
		return sb.toString();
	}

	@Override
	public void endNode(String name) {
		try {
			/*
			 * name has to be valid NCName
			 */
			name = TagName.encode(name);
			contentHandler.endElement("", name, name);
		} catch (SAXException e) {
			throw new JetelRuntimeException("transformation to XML failed", e);
		}
	}

	@Override
	public void endTree() {
		try {
			contentHandler.endDocument();
		} catch (SAXException e) {
			throw new JetelRuntimeException("transformation to XML failed", e);
		}
	}

	public ContentHandler getContentHander() {
		return contentHandler;
	}

	public void setContentHander(ContentHandler contentHander) {
		this.contentHandler = contentHander;
	}

}
