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

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

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

	private SuccessStatus success;
	private ErrorStatus validationError;
	private ErrorStatus jobError;
		
	private static final ErrorStatus GENERIC_ERROR = new ErrorStatus(500, "Job failed", false);
	
	public RestJobResponseStatus() {
		super();
	}

	@XmlElement(name = "Success")
	public SuccessStatus getSuccess() {
		return success;
	}

	public void setSuccess(SuccessStatus success) {
		this.success = success;
	}

	@XmlElement(name = "ValidationError")
	public ErrorStatus getValidationError() {
		return validationError;
	}

	public void setValidationError(ErrorStatus validationError) {
		this.validationError = validationError;
	}

	@XmlElement(name = "JobError")
	public ErrorStatus getJobError() {
		return jobError;
	}

	public void setJobError(ErrorStatus jobError) {
		this.jobError = jobError;
	}

	public ResponseStatus getDefaultErrorStatus() {
		return GENERIC_ERROR;
	}
	
	public RestJobResponseStatus createCopy() {
		try {
			RestJobResponseStatus copy = (RestJobResponseStatus)super.clone();
			if (success != null) {
				copy.success = success.createCopy();
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
