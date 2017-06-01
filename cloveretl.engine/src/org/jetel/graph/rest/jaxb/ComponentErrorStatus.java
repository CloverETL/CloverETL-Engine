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
 * ComponentError element in RestJobResponsesStatus
 * 
 * @author Roland (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 24. 5. 2017
 */
@XmlRootElement(name = "ComponentError")
@XmlAccessorType(XmlAccessType.NONE)
public class ComponentErrorStatus extends ResponseStatus implements Cloneable, Serializable {

	private static final long serialVersionUID = -1620260144974715802L;
	
	private String componentId;
	private Integer statusCode;
	private String reasonPhrase;
	
	public ComponentErrorStatus() {
	}
	
	public ComponentErrorStatus(String componentId, int statusCode, String reasonPhrase) {
		this.componentId = componentId;
		this.statusCode = statusCode;
		this.reasonPhrase = reasonPhrase;
	}
	
	@Override
	@XmlElement(name = "ComponentId")
	public String getComponentId() {
		return componentId;
	}

	public void setComponentId(String componentId) {
		this.componentId = componentId;
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
	
	public ComponentErrorStatus createCopy() {
		try {
			return (ComponentErrorStatus)super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((componentId == null) ? 0 : componentId.hashCode());
		result = prime * result + ((reasonPhrase == null) ? 0 : reasonPhrase.hashCode());
		result = prime * result + ((statusCode == null) ? 0 : statusCode.hashCode());
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
		ComponentErrorStatus other = (ComponentErrorStatus) obj;
		if (componentId == null) {
			if (other.componentId != null)
				return false;
		} else if (!componentId.equals(other.componentId))
			return false;
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
		return true;
	}
}
