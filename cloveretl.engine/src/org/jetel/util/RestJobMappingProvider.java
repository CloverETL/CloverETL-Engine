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
package org.jetel.util;

import java.util.Map;
import java.util.Map.Entry;

import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.metadata.DataFieldMetadata;

/**
 * @author jedlickad (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 10. 11. 2017
 */
public class RestJobMappingProvider {

	public static String createMapping(Node outputComponent, RestJobResponseFormat outputFormat) {		
		if (outputComponent == null || outputFormat == null || outputComponent.getInputPorts().isEmpty() || 
				(outputFormat != RestJobResponseFormat.JSON && outputFormat != RestJobResponseFormat.XML)) {
			return ""; //$NON-NLS-1$
		}
		
		Map<Integer, InputPort> inputPorts = outputComponent.getInputPorts();

		boolean metadataName = outputFormat != RestJobResponseFormat.JSON || Boolean.parseBoolean(outputComponent.getAttributes().getProperty("metadataName"));
		boolean topLevelArray = outputFormat == RestJobResponseFormat.JSON && (Boolean.parseBoolean(outputComponent.getAttributes().getProperty("topLevelArray")) || metadataName);
		
		StringBuilder result = new StringBuilder();
		result.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>"); //$NON-NLS-1$
		
		if (outputFormat == RestJobResponseFormat.XML) {
			result.append("<root xmlns:clover=\"http://www.cloveretl.com/ns/xmlmapping\">"); //$NON-NLS-1$
			for (Entry<Integer, InputPort> entrySet : inputPorts.entrySet()) {
				int portNumber = entrySet.getKey();
					
				result.append("<clover:element clover:inPort=\"").append(portNumber).append("\" clover:name=\"") //$NON-NLS-1$ //$NON-NLS-2$
					.append(entrySet.getValue().getEdge().getMetadata().getName()).append("\">"); //$NON-NLS-1$
				result.append("<clover:elements clover:include=\"$").append(portNumber).append(".*\"/>"); //$NON-NLS-1$ //$NON-NLS-2$
				result.append("</clover:element>"); //$NON-NLS-1$
			}			
			result.append("</root>"); //$NON-NLS-1$
			
		} else if (metadataName) {
			result.append("<root xmlns:clover=\"http://www.cloveretl.com/ns/xmlmapping\">"); //$NON-NLS-1$
			for (Entry<Integer, InputPort> entrySet : inputPorts.entrySet()) {
				int portNumber = entrySet.getKey();
				
				if (entrySet.getValue().getEdge().getMetadata() == null) {
					continue;
				}
					
				result.append("<clover:collection clover:inPort=\"").append(portNumber).append("\" clover:name=\"") //$NON-NLS-1$ //$NON-NLS-2$
					.append(entrySet.getValue().getEdge().getMetadata().getName()).append("\">"); //$NON-NLS-1$
				result.append("<element").append(portNumber).append("><clover:elements clover:include=\"$").append(portNumber).append(".*\"/></element") //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
					.append(portNumber).append(">");
				result.append("</clover:collection>"); //$NON-NLS-1$
			}
			result.append("</root>"); //$NON-NLS-1$
			
		} else if (topLevelArray) {
			result.append("<clover:collection xmlns:clover=\"http://www.cloveretl.com/ns/xmlmapping\" clover:inPort=\"0\" clover:name=\"root\">"); //$NON-NLS-1$
			result.append("<element0><clover:elements clover:include=\"$0.*\"/></element0>"); //$NON-NLS-1$
			result.append("</clover:collection>"); //$NON-NLS-1$
			
		} else {
			result.append("<root xmlns:clover=\"http://www.cloveretl.com/ns/xmlmapping\" clover:inPort=\"0\">"); //$NON-NLS-1$
			for (DataFieldMetadata field : inputPorts.values().iterator().next().getMetadata().getFields()) {
				result.append("<").append(field.getName()).append(">"); //$NON-NLS-1$ //$NON-NLS-2$
				result.append("$0.").append(field.getName()); //$NON-NLS-1$
				result.append("</").append(field.getName()).append(">"); //$NON-NLS-1$ //$NON-NLS-2$
			}
			result.append("</root>"); //$NON-NLS-1$
		}

		return result.toString();
	}
}
