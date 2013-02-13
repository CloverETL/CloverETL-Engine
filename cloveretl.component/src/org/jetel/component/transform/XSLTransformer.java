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
package org.jetel.component.transform;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.xml.sax.InputSource;

/**
 * XSL transformer does XSL transformation on the input XML data.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz), Magdalena Krygielova (krygielovam@javlin.eu) (c) Javlin
 *         Consulting (www.javlinconsulting.cz)
 * 
 * @created 24.10.2006
 * @version 18.1.2013
 */
public class XSLTransformer {

	public static final String DEFAULT_CHARSET = "UTF-8";

	private Transformer transformerJAXP;
	
	private String charset;

	/**
	 * Constructor. Sets default charset to UTF-8
	 */
	public XSLTransformer() {
		this.charset = DEFAULT_CHARSET;
	}

	/**
	 * Sets charset.
	 * 
	 * @param xslTransform
	 */
	public void setCharset(String charset) {
		 this.charset = charset;
	}

	/**
	 * Initialization method.
	 * 
	 * @throws Exception
	 */
	public void init(InputStream xslTransform) throws Exception {
		
		// because of XSLT2 support we need to use saxon
		TransformerFactory factory = TransformerFactory.newInstance("net.sf.saxon.TransformerFactoryImpl", null);
		
		transformerJAXP = factory.newTransformer(new StreamSource(xslTransform));
        transformerJAXP.setOutputProperty(OutputKeys.INDENT, "yes");
        transformerJAXP.setOutputProperty(OutputKeys.ENCODING, charset);
	}

	/**
	 * Transforms given input data and writes them using the given writer.
	 * @param source - input data
	 * @param writer - writer for writing output
	 * @throws Exception
	 */
	public void transform(InputStream source, Writer writer) throws Exception {
		transform(new StreamSource(source), new StreamResult(writer));
	}

	/**
	 * Transforms given input data and writes them using the given writer.
	 * @param source - input data
	 * @param writer - writer for writing output
	 * @throws Exception
	 */
	public void transform(InputSource source, Writer writer) throws Exception {
		transform(new SAXSource(source), new StreamResult(writer));
	}

	/**
	 * Transforms given input data and sends them to the given output stream.
	 * @param source - input data
	 * @param outXML - output stream
	 * @throws Exception
	 */
	public void transform(InputSource source, OutputStream outXML) throws Exception {
		transform(new SAXSource(source), new StreamResult(outXML));
	}

	/**
	 * Transforms given input data and sends them to the given output stream.
	 * @param source - input data
	 * @param outXML - output stream
	 * @throws Exception
	 */
	public void transform(InputStream source, OutputStream outXML) throws Exception {
		transform(new StreamSource(source), new StreamResult(outXML));
	}

	/**
	 * Transforms given input data and writes them using the given writer.
	 * @param source - input data
	 * @param writer - writer for writing output
	 * @throws Exception
	 */
	public void transform(Source source, Writer writer) throws Exception {
		transform(source, new StreamResult(writer));
	}

	/**
	 * Transforms given input data and sends them to the given output stream.
	 * @param source - input data
	 * @param outXML - output stream
	 * @throws Exception
	 */
	public void transform(Source source, OutputStream outXML) throws Exception {
		transform(source, new StreamResult(outXML));
	}

	/**
	 * Transforms given input data and sends them to the given destination.
	 * This function does the actual transformation.
	 * @param source
	 * @param result
	 * @throws Exception
	 */
	private void transform(Source source, StreamResult result) throws Exception {
		try {
			transformerJAXP.transform(source, result);
		} catch (TransformerException e) {
			throw processTransformException(e);
		}
	}
	
	private Exception processTransformException(Exception e) {
    	if (e instanceof TransformerConfigurationException) {
    		TransformerConfigurationException transConEx = (TransformerConfigurationException) e;
            String errString = "Transformer Config Error: " + transConEx.getMessage();

            Throwable ex = transConEx;
            if(transConEx.getException() != null) {
                ex = transConEx.getException();
                errString += "\n" + ex.getMessage();
            }

            return new Exception(errString, transConEx);
    	}
    	
    	if (e instanceof TransformerException) {
    		TransformerException transEx = (TransformerException) e;
            String errString = "Transformation error: " + transEx.getMessage();

            Throwable ex = transEx;
            if(transEx.getException() != null) {
                ex = transEx.getException();
                errString += "\n" + ex.getMessage();
            }

            return new Exception(errString, transEx);
    	}
    	return e;
    }

	public static void main(String[] args) {
		XSLTransformer transformer = new XSLTransformer();

		String inXML = new String("<DBConnection a=\"b\">abc</DBConnection>");
		ByteArrayInputStream bais = new ByteArrayInputStream(inXML.getBytes());

		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		String s = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> " + "<xsl:stylesheet xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\" version=\"1.0\">" + "</xsl:stylesheet>";

		try {
			transformer.init(new ByteArrayInputStream(s.getBytes()));
			transformer.transform(bais, baos);
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}

		System.out.println(new String(baos.toByteArray()));
	}

}
