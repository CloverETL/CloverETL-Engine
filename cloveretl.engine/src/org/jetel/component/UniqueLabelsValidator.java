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

import java.util.HashSet;
import java.util.Set;

import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.GraphElement;
import org.jetel.metadata.DataRecordMetadata;

/**
 * This class adds warnings to the configuration status
 * when the labels of the validated metadata are not unique.
 * 
 * The validation is performed because non-unique labels
 * may cause problems with field mapping.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 3.11.2011
 */
public class UniqueLabelsValidator {
	
	protected ConfigurationStatus status;
	protected GraphElement graphElement;

	/**
	 * 
	 */
	public UniqueLabelsValidator() {
	}

	/**
	 * @param status
	 * @param graphElement
	 */
	public UniqueLabelsValidator(ConfigurationStatus status, GraphElement graphElement) {
		this.status = status;
		this.graphElement = graphElement;
	}
	
	/**
	 * @return the status
	 */
	public ConfigurationStatus getStatus() {
		return status;
	}

	/**
	 * @param status the status to set
	 */
	public void setStatus(ConfigurationStatus status) {
		this.status = status;
	}

	/**
	 * @return the graphElement
	 */
	public GraphElement getGraphElement() {
		return graphElement;
	}

	/**
	 * @param graphElement the graphElement to set
	 */
	public void setGraphElement(GraphElement graphElement) {
		this.graphElement = graphElement;
	}
	
	/**
	 * To be overridden by subclasses.
	 * 
	 * @param label
	 */
	protected void validateLabel(String label) {
		// empty implementation
	}

	public boolean validateMetadata(DataRecordMetadata metadata) {
		return validateMetadata(status, graphElement, metadata);
	}

	public boolean validateMetadata(ConfigurationStatus status, GraphElement graphElement, DataRecordMetadata metadata) {
		if (metadata == null) {
			return false;
		}
		Set<String> labels = new HashSet<String>();
		for (int i = 0; i < metadata.getNumFields(); i++) {
			String label = metadata.getField(i).getLabelOrName();
			validateLabel(label);
			if (labels.contains(label)) {
				status.add("Field label \"" + label + "\" is not unique", Severity.WARNING, graphElement, Priority.LOW);
			}
			labels.add(label);
		}
		return true;
	}

}
