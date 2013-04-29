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
package org.jetel.component.validator.utils;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.jetel.component.validator.ValidationGroup;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.sun.xml.internal.bind.marshaller.CharacterEscapeHandler;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 17.12.2012
 */
public class ValidationRulesPersister {
	
	private static class CDATAEscaperHandler implements CharacterEscapeHandler {
		@Override
		public void escape(char[] ch, int start, int length, boolean isAttVal, Writer writer)
				throws IOException {
			writer.write(ch, start, length);						
		}
	}

	public static String serialize(ValidationGroup group) {
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(ValidationGroup.class);
			Marshaller marshaller = jaxbContext.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			
			// Workaround to wrap element item (code of custom rule) into CDATA section
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
			Document document = docBuilderFactory.newDocumentBuilder().newDocument();
			
			marshaller.marshal(group, document);
			
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
			transformer.setOutputProperty(OutputKeys.CDATA_SECTION_ELEMENTS, "item");
			StringWriter sw = new StringWriter();
			transformer.transform(new DOMSource(document), new StreamResult(sw));
			return sw.getBuffer().toString();
		} catch(Exception ex) {
			ex.printStackTrace();
			throw new ValidationRulesPersisterException("Could not serialize.", ex);
		}
	}
	
	public static void validate(String input) throws ValidationRulesPersisterException {
		try {
			SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			Schema schema = schemaFactory.newSchema(new StreamSource(new StringReader(ValidationRulesPersister.generateSchema())));
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	    	//factory.setValidating(true);
	    	factory.setNamespaceAware(true);
	    	factory.setSchema(schema);
	    	DocumentBuilder builder = factory.newDocumentBuilder();
	    	Document document = builder.parse(new InputSource(new StringReader(input)));
		} catch(Exception e) {
			throw new ValidationRulesPersisterException("Can't validate input string, probably corupted.", e);
		}
	}
	
	public static ValidationGroup deserialize(String input) throws ValidationRulesPersisterException {
		//validate(input);
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(ValidationGroup.class);
			Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
			unmarshaller.setSchema(getSchema());
		    ValidationGroup group = (ValidationGroup) unmarshaller.unmarshal(new StringReader(input));
		    return group;
		} catch(JAXBException ex) {
			throw new ValidationRulesPersisterException("Can't deserialize validation rules.", ex);
		}
	}
	
	public static String generateSchema() {
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(ValidationGroup.class);
			final StringWriter sw = new StringWriter();
			jaxbContext.generateSchema(new SchemaOutputResolver() {
				
				@Override
				public Result createOutput(String namespaceUri, String suggestedFileName) throws IOException {
					StreamResult result = new StreamResult(sw);
					result.setSystemId(suggestedFileName);
					return result;
				}
			});
			return sw.toString();
		} catch (Exception ex) {
			throw new ValidationRulesPersisterException("Can't generate validation rules schema.", ex);
		}
	}
	
	private static Schema getSchema() throws ValidationRulesPersisterException {
		try {
			return SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(new StreamSource(new StringReader(generateSchema())));
		} catch (SAXException ex) {
			throw new ValidationRulesPersisterException("Can't parse validation rules schema.", ex);
		}
	}
	
}
