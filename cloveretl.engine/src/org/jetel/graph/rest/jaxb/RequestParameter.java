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
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12.4.2017
 */
@XmlAccessorType(XmlAccessType.NONE)
public class RequestParameter implements Serializable {
	
	private static final long serialVersionUID = 1;

	@XmlAttribute
	private String name;
	
	@XmlAttribute
	private String id;
	
	@XmlAttribute
	private Boolean required;
	
	@XmlAttribute
	private String description;
	
	@XmlAttribute
	private String type;
	
	@XmlAttribute
	private Location location;
	
	@XmlAttribute
	private Boolean sensitive;
	
	@XmlType(name = "location", namespace = "http://cloveretl.com/server/data") // https://java.net/jira/browse/JAXB-933
	public enum Location {
		@XmlEnumValue("query") query,
		@XmlEnumValue("url_path") path,
		@XmlEnumValue("form_data") formData;
		
		public String value() {
			return name();
		}
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	

	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public Boolean getRequired() {
		return required;
	}
	
	public boolean isRequired() {
		return Boolean.TRUE.equals(required);
	}
	
	public void setRequired(Boolean required) {
		this.required = required;
	}
	
	public String getDescription() {
		return description;
	}
	
	public void setDescription(String description) {
		this.description = description;
	}
	
	public String getType() {
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
	}
	
	public Location getLocation() {
		return location;
	}
	
	public void setLocation(Location location) {
		this.location = location;
	}

	public Boolean getSensitive() {
		return sensitive;
	}
	
	public boolean isSensitive() {
		return Boolean.TRUE.equals(sensitive);
	}

	public void setSensitive(Boolean sensitive) {
		this.sensitive = sensitive;
	}
}
