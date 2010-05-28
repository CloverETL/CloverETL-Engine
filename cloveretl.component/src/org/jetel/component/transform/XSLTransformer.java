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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.sun.org.apache.xml.internal.serialize.OutputFormat.Defaults;

/**
 * XSL transformer class can be used for transform input stream, where xml data are expected
 * and transforms them by xsl transformation and the result is sended in output stream.
 *  
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 24.10.2006
 */
public class XSLTransformer {

    private InputStream xslTransform;
    
    private Transformer transformer;
    
    private DocumentBuilder parser;
    
    private String errString;

	private String charset;

	private Document document;

	/**
	 * Constructor.
	 * Sets default charset to Defaults.Encoding (UTF-8)
	 */
    public XSLTransformer() {
    	charset = Defaults.Encoding;
    }
    
    /**
     * Sets xslt statement.
     * 
     * @param xslTransform
     */
    public void setXSLT(InputStream xslTransform) {
        this.xslTransform = xslTransform;
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
    public void init() throws Exception {
        //Get a TransformerFactory object
	    TransformerFactory xformFactory = TransformerFactory.newInstance();
	    
        //Get an XSL Transformer object
        if(xslTransform == null) {
            transformer = xformFactory.newTransformer(); //if xslTransform is not adjusted, it is used identity transformation
        } else {
            transformer = xformFactory.newTransformer(new StreamSource(xslTransform));
        }
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(OutputKeys.ENCODING, charset);
        
        //Get a factory object for DocumentBuilder
        // objects with default configuration.
        DocumentBuilderFactory docBuildFactory = DocumentBuilderFactory.newInstance();

        //Get a DocumentBuilder (parser) object
        parser = docBuildFactory.newDocumentBuilder();
    }

    /**
     * 
     * 
     * @param stream
     * @param writer
     * @throws Exception
     */
	public void transform(InputStream stream, Writer writer) throws Exception {
    	//Parse the XML input stream to create a
    	// Document object that represents the
    	// input XML file.
		try {
			document = parser.parse(stream);
		} 
		catch(Exception e){
			throw proccessParseException(e);
		}

    	//Get a DOMSource object that represents the Document object
        // and transform
    	transform(new DOMSource(document), writer);
	}

	public void transform(InputSource source, Writer writer) throws Exception {
    	//Parse the XML input stream to create a
    	// Document object that represents the
    	// input XML file.
		try {
			document = parser.parse(source);
		} 
		catch(Exception e){
			throw proccessParseException(e);
		}

    	//Get a DOMSource object that represents the Document object
        // and transform
    	transform(new DOMSource(document), writer);
	}
    
	public void transform(InputSource inXML, OutputStream outXML) throws Exception {
    	//Parse the XML input stream to create a
    	// Document object that represents the
    	// input XML file.
		try {
			document = parser.parse(inXML);
		} 
		catch(Exception e){
			throw proccessParseException(e);
		}

    	//Get a DOMSource object that represents the Document object
        // and transform
    	transform(new DOMSource(document), outXML);
    }

    public void transform(InputStream inXML, OutputStream outXML) throws Exception {
    	//Parse the XML input stream to create a
    	// Document object that represents the
    	// input XML file.
		try {
			document = parser.parse(inXML);
		} 
		catch(Exception e){
        	throw proccessParseException(e);
        }

    	//Get a DOMSource object that represents the Document object
        // and transform
    	transform(new DOMSource(document), outXML);
    }

    public void transform(DOMSource source, Writer writer) throws Exception {
        try {
            //Get a StreamResult object that points to
            // the screen.  Then transform the DOM
            // sending XML to the screen.
   	        transformer.transform(source, new StreamResult(writer));
        }//end try block
        catch(Exception e){
            throw proccessTransformException(e);
        }//end catch
	}

    public void transform(DOMSource domSource, OutputStream outXML) throws Exception {
        try {
            //Get a StreamResult object that points to
            // the screen.  Then transform the DOM
            // sending XML to the screen.
   	        transformer.transform(domSource, new StreamResult(outXML));
        }//end try block
        catch(Exception e){
            throw proccessTransformException(e);
        }//end catch
    }
    
    private Exception proccessTransformException(Exception e) {
    	if (e instanceof TransformerConfigurationException) {
    		TransformerConfigurationException transConEx = (TransformerConfigurationException) e;
            errString = "Transformer Config Error: " + transConEx.getMessage();

            Throwable ex = transConEx;
            if(transConEx.getException() != null) {
                ex = transConEx.getException();
                errString += "\n" + ex.getMessage();
            }

            return new Exception(errString, transConEx);
    	}
    	
    	if (e instanceof TransformerException) {
    		TransformerException transEx = (TransformerException) e;
            errString = "Transformation error: " + transEx.getMessage();

            Throwable ex = transEx;
            if(transEx.getException() != null) {
                ex = transEx.getException();
                errString += "\n" + ex.getMessage();
            }

            return new Exception(errString, transEx);
    	}
    	return e;
    }
    
    private Exception proccessParseException(Exception e) {
    	if (e instanceof SAXParseException) {
    		SAXParseException saxEx = (SAXParseException) e;
            errString = "SAXParseException\n" +
            			"Public ID: " + saxEx.getPublicId() + "\n" +
            			"System ID: " + saxEx.getSystemId() + "\n" +
            			"Line: " + saxEx.getLineNumber() + "\n" +
            			"Column:" + saxEx.getColumnNumber() + "\n" +
            			saxEx.getMessage();

            Exception ex = saxEx;
            if(saxEx.getException() != null) {
                ex = saxEx.getException();
                errString += "\n" + ex.getMessage();
            }
            
            return new Exception(errString, saxEx);
    	}
    	if (e instanceof SAXException) {
    		SAXException saxEx = (SAXException) e;
            //This catch block may not be reachable.
            errString = "Parser Error: " + saxEx.getMessage();

            Exception ex = saxEx;
            if(saxEx.getException() != null) {
                ex = saxEx.getException();
                errString += "\n" + ex.getMessage();
            }

            return new Exception(errString, saxEx);
    	}
    	return e;
    }
    
    
    
    public static void main(String[] args) {
        XSLTransformer transformer = new XSLTransformer();
        
        String inXML = new String("<DBConnection a=\"b\">abc</DBConnection>");
        ByteArrayInputStream bais = new ByteArrayInputStream(inXML.getBytes());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
    
        String s = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> " +
        		   "<xsl:stylesheet xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\" version=\"1.0\">" +
        		   "</xsl:stylesheet>";
        
       	transformer.setXSLT(new ByteArrayInputStream(s.getBytes()));
        
        try {
        	transformer.init();
            transformer.transform(bais, baos);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        
        System.out.println(new String(baos.toByteArray()));
    }

}
