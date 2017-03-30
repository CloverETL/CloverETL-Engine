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
package org.jetel.graph.runtime;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Error message for http response. Actualy describes status line of https response. 
 * @author kocik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Mar 21, 2017
 */
@XmlRootElement(name = "error-message")
@XmlAccessorType(XmlAccessType.FIELD)
public class ErrorMessage {
	
	/**
	 * Numeric http status code 
	 */
	@XmlElement(name="http-code")
	private int httpCode;
	
	/**
	 * Textual phrase of status line
	 */
	@XmlElement(name="message")
	private String message;
	
	public ErrorMessage() {
	}
	
	public ErrorMessage(int httpCode, String message) {
		this.httpCode = httpCode;
		this.message = message;
	}

	public int getHttpCode() {
		return httpCode;
	}

	public String getMessage() {
		return message;
	}
}
