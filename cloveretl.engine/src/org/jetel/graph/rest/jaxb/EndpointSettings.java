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
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 12.4.2017
 */
@XmlRootElement(name = "EndpointSettings")
@XmlAccessorType(XmlAccessType.NONE)
public class EndpointSettings implements Serializable {

	private static final long serialVersionUID = -2691513925619239673L;
	
	private String urlPath;
	private String description;
	private String endpointName;
	private String exampleOutput;
	private List<RequestMethod> requestMethods;
	private List<RequestParameter> requestParameters;
	
	@XmlElement(name = "UrlPath")
	public String getUrlPath() {
		return urlPath;
	}
	public void setUrlPath(String urlPath) {
		this.urlPath = urlPath;
	}
	
	@XmlElement(name = "Description")
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	
	@XmlElement(name = "EndpointName")
	public String getEndpointName() {
		return endpointName;
	}
	public void setEndpointName(String endpointName) {
		this.endpointName = endpointName;
	}
	
	@XmlElement(name = "ExampleOutput")
	public String getExampleOutput() {
		return exampleOutput;
	}
	public void setExampleOutput(String exampleOutput) {
		this.exampleOutput = exampleOutput;
	}
	
	@XmlElement(name = "RequestMethod")
	public List<RequestMethod> getRequestMethods() {
		if (requestMethods == null) {
			requestMethods = new LinkedList<>();
		}
		return requestMethods;
	}
	
	public void setRequestMethods(List<RequestMethod> requestMethods) {
		this.requestMethods = requestMethods;
	}

	public List<String> getRequestMethodNames() {
		List<RequestMethod> methods = getRequestMethods();
		List<String> result = new ArrayList<>(methods.size());
		for (RequestMethod method : methods) {
			result.add(method.getName());
		}
		return result;
	}
	
	@XmlElement(name = "RequestParameter")
	public List<RequestParameter> getRequestParameters() {
		if (requestParameters == null) {
			requestParameters = new LinkedList<>();
		}
		return requestParameters;
	}
	
	public void setRequestParameters(List<RequestParameter> requestParameters) {
		this.requestParameters = requestParameters;
	}
}
