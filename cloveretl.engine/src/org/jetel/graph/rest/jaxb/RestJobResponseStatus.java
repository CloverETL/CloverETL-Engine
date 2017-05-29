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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.jetel.graph.Node;

import javax.xml.bind.annotation.XmlElement;

/**
 * Response status mapping
 * 
 * @author Roland (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 24. 5. 2017
 */
@XmlRootElement(name = "RestJobResponseStatus")
@XmlAccessorType(XmlAccessType.NONE)
public class RestJobResponseStatus implements Cloneable, Serializable {

	private static final long serialVersionUID = 56313319828721615L;

	private RestJobSuccessStatus success;
	private List<RestJobComponentErrorStatus> componentError;
	private RestJobErrorStatus validationError;
	private RestJobErrorStatus jobError;
	
	private static final RestJobErrorStatus GENERIC_ERROR = new RestJobErrorStatus(500, "Job failed", false);
	
	private Map<String, RestJobComponentErrorStatus> errorsByComponentId;
	
	public RestJobResponseStatus() {
		super();
	}

	@XmlElement(name = "Success")
	public RestJobSuccessStatus getSuccess() {
		return success;
	}

	public void setSuccess(RestJobSuccessStatus success) {
		this.success = success;
	}

	@XmlElement(name = "ComponentError")
	public List<RestJobComponentErrorStatus> getComponentErrors() {
		if (componentError == null) {
			componentError = new ArrayList<>();
		}
		return componentError;
	}

	public void setComponentError(List<RestJobComponentErrorStatus> componentError) {
		this.componentError = componentError;
	}

	@XmlElement(name = "ValidationError")
	public RestJobErrorStatus getValidationError() {
		return validationError;
	}

	public void setValidationError(RestJobErrorStatus validationError) {
		this.validationError = validationError;
	}

	@XmlElement(name = "JobError")
	public RestJobErrorStatus getJobError() {
		return jobError;
	}

	public void setJobError(RestJobErrorStatus jobError) {
		this.jobError = jobError;
	}
	
	public ResponseStatusInterface getErrorMessage(Node node, Throwable throwable) {
		ResponseStatusInterface responseStatus = null;
		if ((success != null || componentError != null || validationError != null || jobError != null) && node != null) {
			if (node.isPartOfRestInput()) {
				responseStatus = validationError;
			} else {
				if (responseStatus == null) {
					responseStatus = getErrorsByComponentId().get(node.getId());
				}
				if (responseStatus == null) {
					responseStatus = jobError;
				}
			}
		}
		return responseStatus != null ? responseStatus : GENERIC_ERROR;
	}
	
	private Map<String, RestJobComponentErrorStatus> getErrorsByComponentId() {
		if (errorsByComponentId == null) {
			errorsByComponentId = new HashMap<>();
			if (componentError != null) {
				for (RestJobComponentErrorStatus component : componentError) {
					errorsByComponentId.put(component.getComponentId(), component);
				}
			}
		}
		return errorsByComponentId;
	}
	
	public RestJobResponseStatus createCopy() {
		try {
			RestJobResponseStatus copy = (RestJobResponseStatus)super.clone();
			if (success != null) {
				copy.success = success.createCopy();
			} 
			if (componentError != null) {
				copy.componentError = new ArrayList<>(componentError.size());
				for (RestJobComponentErrorStatus component : componentError) {
					copy.componentError.add(component.createCopy());
				}
			}
			if (validationError != null) {
				copy.validationError = validationError.createCopy();
			}
			if (jobError != null) {
				copy.jobError = jobError.createCopy();
			}
			return copy;
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((componentError == null) ? 0 : componentError.hashCode());
		result = prime * result + ((jobError == null) ? 0 : jobError.hashCode());
		result = prime * result + ((success == null) ? 0 : success.hashCode());
		result = prime * result + ((validationError == null) ? 0 : validationError.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RestJobResponseStatus other = (RestJobResponseStatus) obj;
		if (componentError == null) {
			if (other.componentError != null)
				return false;
		} else if (!componentError.equals(other.componentError))
			return false;
		if (jobError == null) {
			if (other.jobError != null)
				return false;
		} else if (!jobError.equals(other.jobError))
			return false;
		if (success == null) {
			if (other.success != null)
				return false;
		} else if (!success.equals(other.success))
			return false;
		if (validationError == null) {
			if (other.validationError != null)
				return false;
		} else if (!validationError.equals(other.validationError))
			return false;
		return true;
	}
}
