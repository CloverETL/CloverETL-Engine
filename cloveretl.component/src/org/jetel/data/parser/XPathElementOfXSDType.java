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

import javax.xml.transform.TransformerException;

import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.trans.XPathException;

import org.jetel.data.DataField;
import org.jetel.data.xsd.ConvertorRegistry;
import org.jetel.data.xsd.IGenericConvertor;
import org.jetel.exception.DataConversionException;

/**
 * 
 * @author Pavel Pospichal
 */
public class XPathElementOfXSDType extends XPathElement {

	/**
	 * Constructor for xpath expression.
	 * 
	 * @param xpathExpression
	 * @param cloverField
	 * @throws XPathException
	 */
	public XPathElementOfXSDType(XPathExpression xpathExpression, String cloverField) throws XPathException {
		super(xpathExpression, cloverField);
	}

	/**
	 * Constructor for getting value from child node.
	 * 
	 * @param childNodeName
	 * @param cloverField
	 */
	public XPathElementOfXSDType(String childNodeName, String cloverField) {
		super(childNodeName, cloverField);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.XPathElement#assignValue(org.jetel.data.DataField, java.lang.String)
	 */
	@Override
	protected void assignValue(DataField currentField, String value)
			throws TransformerException {
		IGenericConvertor convertor = ConvertorRegistry
				.getConvertor(currentField.getMetadata().getTypeAsString());

		if (convertor == null) {
			throw new TransformerException(
					"Unable to find appropriate convertor for data type ["
							+ currentField.getMetadata().getTypeAsString()
							+ "].");
		}
		
		try {
			currentField.setValue(convertor.parse(value));
		} catch (DataConversionException e) {
			throw new TransformerException(e);
		}
	}
	
	
}
