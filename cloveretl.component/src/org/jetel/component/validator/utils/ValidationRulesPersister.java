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

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import org.jetel.component.validator.ValidationGroup;

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
	
	public static ValidationGroup deserialize(String input) {
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance(ValidationGroup.class);
			Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
			//u.setEventHandler(new javax.xml.bind.helpers.DefaultValidationEventHandler());
		    ValidationGroup group = (ValidationGroup) unmarshaller.unmarshal(new StringReader(input));
		    return group;
		} catch(JAXBException ex) {
			throw new ValidationRulesPersisterException("Could not deserialize, probable corupted validation rules configuration", ex);
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
			throw new ValidationRulesPersisterException("Could not generate serialize.", ex);
		}
	}
	
}
