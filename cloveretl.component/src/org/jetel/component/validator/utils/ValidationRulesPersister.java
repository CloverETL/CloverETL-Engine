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
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.jetel.component.Validator;
import org.jetel.component.validator.ValidationGroup;
import org.jetel.component.validator.ValidatorMessages;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Class to take care about serialization/validation/deserialization of validation tree.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 17.12.2012
 * @see ValidationGroup
 * @see Validator
 */
public class ValidationRulesPersister {

	/**
	 * Serialize given validation group (and its childrens) into XML
	 * @param group Validation group to serialize
	 * @return Serialized text
	 * @throws ValidationRulesPersisterException when serialization failed (new rule added wrong, wrong JAXB annotation).
	 */
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
			transformer.setOutputProperty(OutputKeys.INDENT, "yes"); //$NON-NLS-1$
			transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4"); //$NON-NLS-1$ //$NON-NLS-2$
			transformer.setOutputProperty(OutputKeys.CDATA_SECTION_ELEMENTS, "item"); //$NON-NLS-1$
			StringWriter sw = new StringWriter();
			transformer.transform(new DOMSource(document), new StreamResult(sw));
			return sw.getBuffer().toString();
		} catch(Exception ex) {
			throw new ValidationRulesPersisterException(ValidatorMessages.getString("ValidationRulesPersister.SerializationError"), ex); //$NON-NLS-1$
		}
	}
	
	/**
	 * Validates given string against validation tree hierarchy (generated from currently known ValidationGroup)
	 * 
	 * @param input Input XML to validate
	 * @throws ValidationRulesPersisterException when code is invalid
	 */
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
			throw new ValidationRulesPersisterException(ValidatorMessages.getString("ValidationRulesPersister.ValidationError"), e); //$NON-NLS-1$
		}
	}
	
	/**
	 * Deserialize given input into validation tree hierarchy.
	 * 
	 * @param input Input XML to deserialize
	 * @return Deserialized validation group
	 * @throws ValidationRulesPersisterException when given input is not valid
	 */
	public static ValidationGroup deserialize(String input) throws ValidationRulesPersisterException {
		//validate(input);
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(ValidationGroup.class);
			Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
			unmarshaller.setSchema(getSchema());
		    ValidationGroup group = (ValidationGroup) unmarshaller.unmarshal(new StringReader(input));
		    return group;
		} catch(JAXBException ex) {
			throw new ValidationRulesPersisterException(ValidatorMessages.getString("ValidationRulesPersister.DeserializationError"), ex); //$NON-NLS-1$
		}
	}
	
	/**
	 * Generate schema from currently known validation tree hierarchy.
	 * @return XML Schema in string
	 * @throws ValidationRulesPersisterException when JAXB annotation are messed up
	 */
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
			throw new ValidationRulesPersisterException(ValidatorMessages.getString("ValidationRulesPersister.SchemaGenerateError"), ex); //$NON-NLS-1$
		}
	}
	
	/**
	 * Generate schema from currently known validation tree hierarchy
	 * @return XML Schema
	 * @throws ValidationRulesPersisterException when JAXB annotation are messed up
	 */
	private static Schema getSchema() throws ValidationRulesPersisterException {
		try {
			return SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(new StreamSource(new StringReader(generateSchema())));
		} catch (SAXException ex) {
			throw new ValidationRulesPersisterException(ValidatorMessages.getString("ValidationRulesPersister.SchemaParseError"), ex); //$NON-NLS-1$
		}
	}
	
}
