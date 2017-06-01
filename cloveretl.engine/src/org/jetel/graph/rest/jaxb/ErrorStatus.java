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
 * ValidationError,JobError elements in RestJobResponsesStatus
 * 
 * @author Roland (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 24. 5. 2017
 */
@XmlRootElement(name = "Errors")
@XmlAccessorType(XmlAccessType.NONE)
public class ErrorStatus extends ResponseStatus implements Cloneable, Serializable {

	private static final long serialVersionUID = -5536323975040035396L;
	
	private Integer statusCode;
	private String reasonPhrase;
	private boolean validationError;
	
	public ErrorStatus() {
	}
	
	public ErrorStatus(int statusCode, String reasonPhrase, boolean validationError) {
		this.statusCode = statusCode;
		this.reasonPhrase = reasonPhrase;
		this.validationError = validationError;
	}
	
	@Override
	@XmlElement(name = "StatusCode")
	public Integer getStatusCode() {
		return statusCode;
	}
	
	@Override
	public void setStatusCode(Integer statusCode) {
		this.statusCode = statusCode;
	}

	@Override
	@XmlElement(name = "ReasonPhrase")
	public String getReasonPhrase() {
		return reasonPhrase;
	}
	
	@Override
	public void setReasonPhrase(String reasonPhrase) {
		this.reasonPhrase = reasonPhrase;
	}

	@Override
	public String getComponentId() {
		return null;
	}
	
	public boolean isValidationError() {
		return validationError;
	}

	public void setValidationError(boolean validationError) {
		this.validationError = validationError;
	}
	
	public ErrorStatus createCopy() {
		try {
			return (ErrorStatus)super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((reasonPhrase == null) ? 0 : reasonPhrase.hashCode());
		result = prime * result + ((statusCode == null) ? 0 : statusCode.hashCode());
		result = prime * result + (validationError ? 1231 : 1237);
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
		ErrorStatus other = (ErrorStatus) obj;
		if (reasonPhrase == null) {
			if (other.reasonPhrase != null)
				return false;
		} else if (!reasonPhrase.equals(other.reasonPhrase))
			return false;
		if (statusCode == null) {
			if (other.statusCode != null)
				return false;
		} else if (!statusCode.equals(other.statusCode))
			return false;
		if (validationError != other.validationError)
			return false;
		return true;
	}
}
