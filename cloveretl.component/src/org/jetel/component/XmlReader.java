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
package org.jetel.component;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.jetel.component.tree.reader.TreeReaderParserProvider;
import org.jetel.component.tree.reader.xml.XmlReaderParserProvider;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;

/**
 * @author krejcil (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 22.2.2012
 */
public class XmlReader extends TreeReader {

	public static final String COMPONENT_TYPE = "XML_READER";

	private static final String XML_XML_FEATURES_ATTRIBUTE = "xmlFeatures";
	
	private static final String FEATURES_DELIMETER = ";";
	private static final String FEATURES_ASSIGN = ":=";

	public static XmlReader fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		XmlReader reader = new XmlReader(xattribs.getString(XML_ID_ATTRIBUTE));
		readCommonAttributes(reader, xattribs);

		if (xattribs.exists(XML_XML_FEATURES_ATTRIBUTE)) {
			reader.setXmlFeatures(xattribs.getString(XML_XML_FEATURES_ATTRIBUTE));
		}

		return reader;
	}

	private String xmlFeatures;

	public XmlReader(String id) {
		super(id);
	}

	@Override
	protected TreeReaderParserProvider getTreeReaderParserProvider() {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		XMLReader xmlReader = null;
		try {
			initXmlFeatures(factory);
			xmlReader = factory.newSAXParser().getXMLReader();
		} catch (SAXException e) {
			throw new JetelRuntimeException(e);
		} catch (ParserConfigurationException e) {
			throw new JetelRuntimeException(e);
		}
	
	
		return new XmlReaderParserProvider(charset, xmlReader);
	}
	
	private void initXmlFeatures(SAXParserFactory factory) throws ParserConfigurationException {
		if (xmlFeatures == null) {
			return;
		}

		String[] aXmlFeatures = xmlFeatures.split(FEATURES_DELIMETER);
		String[] aOneFeature;
		for (String oneFeature : aXmlFeatures) {
			aOneFeature = oneFeature.split(FEATURES_ASSIGN);
			if (aOneFeature.length != 2) {
				throw new JetelRuntimeException("The xml feature '" + oneFeature + "' has wrong format");
			}

			try {
				factory.setFeature(aOneFeature[0], Boolean.parseBoolean(aOneFeature[1]));
			} catch (SAXNotRecognizedException e) {
				throw new JetelRuntimeException(e);
			} catch (SAXNotSupportedException e) {
				throw new JetelRuntimeException(e);
			}
		}
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		ConfigurationStatus configStatus = super.checkConfig(status);

		disallowEmptyCharsetOnDictionaryAndPort(configStatus);

		return configStatus;
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	private void setXmlFeatures(String xmlFeatures) {
		this.xmlFeatures = xmlFeatures;
	}

}
