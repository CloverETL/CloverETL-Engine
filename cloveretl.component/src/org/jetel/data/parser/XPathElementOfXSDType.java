package org.jetel.data.parser;

import javax.xml.transform.TransformerException;

import org.jetel.data.DataField;
import org.jetel.data.xsd.ConvertorRegistry;
import org.jetel.data.xsd.IGenericConvertor;
import org.jetel.exception.DataConversionException;

import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.trans.XPathException;

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
			throw new TransformerException(e.getMessage());
		}
	}
	
	
}
