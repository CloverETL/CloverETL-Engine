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
package org.jetel.graph.rest.jaxb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.jetel.graph.Node;

/**
 * Mapping error from execution of Component to HttpResponse.
 * 
 * @author kocik (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Mar 21, 2017
 */
@XmlRootElement(name = "RestErrorMapping")
@XmlAccessorType(XmlAccessType.NONE)
public class ErrorMapping implements Cloneable {

	/**
	 * Key for default error Mesasge
	 */
	public static final String ANY_JOB_ERROR_KEY = "*";
	public static final String VALIDATION_ERROR_KEY = "REQUEST_VALIDATION_ERROR";

	private static final ErrorMessage GENERIC_ERROR = new ErrorMessage(null, 500, "Job failed");
	
	private List<ErrorMessage> errors;
	private Map<String, ErrorMessage> errorsByComponentId;
	
	public ErrorMapping() {
		super();
	}
	
	@XmlElement(name = "Error")
	public List<ErrorMessage> getErrors() {
		if (errors == null) {
			errors = new ArrayList<>();
		}
		return errors;
	}
	
	public void setErrors(List<ErrorMessage> errors) {
		this.errors = errors;
	}
	
	public ErrorMessage getErrorMessage(Node node) {
		return getErrorMessage(node, null);
	}

	/**
	 * Return error message for exception.
	 * 
	 * @param key
	 * @return ErrorMessages
	 */
	public ErrorMessage getErrorMessage(Node node, Throwable throwable) {
		if (errors == null) {
			return GENERIC_ERROR;
		}
		if (node != null) {
			ErrorMessage msg = getErrorsByComponentId().get(node.getId());
			if (msg == null && node.isPartOfRestInput()) {
				msg = getErrorsByComponentId().get(VALIDATION_ERROR_KEY);
			}
			if (msg == null) {
				msg = getErrorsByComponentId().get(ANY_JOB_ERROR_KEY);
			}
			return msg != null ? msg : GENERIC_ERROR;
		}
		return GENERIC_ERROR;
	}
	
	public ErrorMapping createCopy() {
		try {
			ErrorMapping copy = (ErrorMapping)super.clone();
			if (errors != null) {
				copy.errors = new ArrayList<>(errors.size());
				for (ErrorMessage msg : errors) {
					copy.errors.add(msg.createCopy());
				}
			}
			return copy;
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static ErrorMessage genericMessage() {
		return new ErrorMessage(GENERIC_ERROR.getComponentId(), GENERIC_ERROR.getStatusCode(), GENERIC_ERROR.getReasonPhrase());
	}
	
	private Map<String, ErrorMessage> getErrorsByComponentId() {
		if (errorsByComponentId == null) {
			errorsByComponentId = new HashMap<>();
			if (errors != null) {
				for (ErrorMessage em : errors) {
					errorsByComponentId.put(em.getComponentId(), em);
				}
			}
		}
		return errorsByComponentId;
	}
}
