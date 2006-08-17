/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/

package org.jetel.component;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Read component description from xml file. Used by ComponentFactory class.
 * @author Martin Zatopek
 *
 */
public class ComponentDescriptionReader {
	
	private static final String FILE_NAME = "components.xml";

	private static final String ETL_COMPONENT_LIST_ELEMENT = "ETLComponentList";
	
	private static final String ETL_COMPONENT_ELEMENT = "ETLComponent";

	private static final String CLASS_NAME_ATTR = "className";

	private static final String TYPE_ATTR = "type";

    private String fileName;
    
    private InputStream getDefaultInputStream() {
        return ComponentDescriptionReader.class.getResourceAsStream(FILE_NAME);
    }

    private InputStream getInputStreamFromFile(String fileName) {
        try {
            return new FileInputStream(fileName);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException(this.getClass().getName() + ": Component description file (" + fileName + ") does not exist or is corrupted.");
        }
    }
    
	private Document getSourceDocument(InputStream inputStream) {
		try {

			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setNamespaceAware(true);
		
			Document doc;
			doc = dbf.newDocumentBuilder().parse(inputStream);
			
			return doc;
		} catch (Exception e) {
			// TODO Auto-generated catch block
            throw new RuntimeException(this.getClass().getName() + ": Component description file (" + fileName + ") does not exist or is corrupted.");
		}
	}
    
	public ComponentDescription[] getComponentDescriptions() {
		return getComponentDescriptions(getDefaultInputStream());
	}

    public ComponentDescription[] getComponentDescriptions(String fileName) {
        this.fileName = fileName;
        return getComponentDescriptions(getInputStreamFromFile(fileName));
    }
    
    private ComponentDescription[] getComponentDescriptions(InputStream inputStream) {
        Document doc = getSourceDocument(inputStream);
        
        return buildComponentDescriptions(doc);
    }

    private ComponentDescription[] buildComponentDescriptions(Document doc) {
		return buildETLComponentList((Element) doc.getElementsByTagName(ETL_COMPONENT_LIST_ELEMENT).item(0));
	}

	private ComponentDescription[] buildETLComponentList(Element etlComponentList) {
		if(etlComponentList == null) return new ComponentDescription[0];
		
		NodeList nl = etlComponentList.getElementsByTagName(ETL_COMPONENT_ELEMENT);
		ComponentDescription[] ret = new ComponentDescription[nl.getLength()];
		
		
		for(int i = 0; i < nl.getLength(); i++) {
			ret[i] = buildETLComponent((Element) nl.item(i));
		}

		return ret;
	}

	private ComponentDescription buildETLComponent(Element etlComponent) {
		String type = etlComponent.getAttribute(TYPE_ATTR);

		String className = etlComponent.getAttribute(CLASS_NAME_ATTR);
		
		ComponentDescription component = new ComponentDescription(type, className, null);
		
		return component;
	}

}
