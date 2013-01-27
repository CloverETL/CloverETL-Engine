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

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.jetel.component.validator.ValidationGroup;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 17.12.2012
 */
public class ValidationRulesPersister {

	public static String serialize(ValidationGroup group) {
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(ValidationGroup.class);
			Marshaller marshaller = jaxbContext.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			StringWriter sw = new StringWriter();
			marshaller.marshal(group, sw);
			return sw.toString();
		} catch(JAXBException ex) {
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
