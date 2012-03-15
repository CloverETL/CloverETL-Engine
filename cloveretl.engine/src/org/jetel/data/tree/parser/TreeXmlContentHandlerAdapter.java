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
package org.jetel.data.tree.parser;

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
	private ContentHandler contentHander;

	@Override
	public void startTree() {
		try {
			contentHander.startDocument();
			contentHander.startElement("", DUMMY_ROOT_ELEMENT_NAME, DUMMY_ROOT_ELEMENT_NAME, EMPTY_ATTRIBUTES);
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void startNode(String name) {
		try {
			contentHander.startElement("", name, name, EMPTY_ATTRIBUTES);
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void leaf(Object value) {
		char[] stringCharArray = value.toString().toCharArray();
		try {
			contentHander.characters(stringCharArray, 0, stringCharArray.length);
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void endNode(String name) {
		try {
			contentHander.endElement("", name, name);
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void endTree() {
		try {
			contentHander.endElement("", DUMMY_ROOT_ELEMENT_NAME, DUMMY_ROOT_ELEMENT_NAME);
			contentHander.endDocument();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public ContentHandler getContentHander() {
		return contentHander;
	}

	public void setContentHander(ContentHandler contentHander) {
		this.contentHander = contentHander;
	}

}
