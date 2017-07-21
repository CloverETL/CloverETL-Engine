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

import java.io.IOException;

import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 19 Jan 2012
 */
public class TreeXMLReaderAdaptor implements XMLReader {

	private TreeStreamParser treeParser;

	public TreeXMLReaderAdaptor(TreeStreamParser treeParser) {
		this.treeParser = treeParser;
	}

	@Override
	public void parse(InputSource input) throws IOException, SAXException {
		treeParser.parse(input);
	}

	@Override
	public ContentHandler getContentHandler() {
		TreeXmlContentHandlerAdapter treeContentHandler = (TreeXmlContentHandlerAdapter) treeParser.getContentHandler();
		return treeContentHandler.getContentHander();
	}
	
	@Override
	public void setContentHandler(ContentHandler contentHandler) {
		TreeXmlContentHandlerAdapter treeContentHandler = (TreeXmlContentHandlerAdapter) treeParser.getContentHandler();
		treeContentHandler.setContentHander(contentHandler);
	}

	@Override
	public void parse(String systemId) throws IOException, SAXException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean getFeature(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setFeature(String name, boolean value) throws SAXNotRecognizedException, SAXNotSupportedException {
		// Do nothing
	}

	@Override
	public Object getProperty(String name) throws SAXNotRecognizedException, SAXNotSupportedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setProperty(String name, Object value) throws SAXNotRecognizedException, SAXNotSupportedException {
		// Do nothing
	}

	@Override
	public void setEntityResolver(EntityResolver resolver) {
		// Do nothing
	}

	@Override
	public EntityResolver getEntityResolver() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setDTDHandler(DTDHandler handler) {
		// Do nothing
	}

	@Override
	public DTDHandler getDTDHandler() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setErrorHandler(ErrorHandler handler) {
		// Do nothing
	}

	@Override
	public ErrorHandler getErrorHandler() {
		throw new UnsupportedOperationException();
	}

}
