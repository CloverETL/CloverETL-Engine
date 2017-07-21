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
/**
 * 
 */
package org.jetel.exception;

import java.text.MessageFormat;

import org.jetel.graph.IGraphElement;
import org.jetel.util.string.StringUtils;

/**
 * @author avackova
 *
 */
public class NotInitializedException extends IllegalStateException {
	
	String notInitializeMessage = "Element {0} is not initialized!!!";
    IGraphElement graphElement;

	/**
	 * 
	 */
	public NotInitializedException() {
		super();
	}

	public NotInitializedException(IGraphElement graphElement){
		super();
		this.graphElement = graphElement;
	}
	/**
	 * @param s
	 */
	public NotInitializedException(String s) {
		super(s);
	}

	public NotInitializedException(String s, IGraphElement graphElement) {
		super(s);
		this.graphElement = graphElement;
	}
	
	public NotInitializedException(Throwable cause) {
		super(cause);
	}

	/**
	 * @param cause
	 */
	public NotInitializedException(Throwable cause, IGraphElement graphElement) {
		super(cause);
		this.graphElement = graphElement;
	}

	/**
	 * @param message
	 * @param cause
	 */
	public NotInitializedException(String message, Throwable cause) {
		super(message, cause);
	}

	public NotInitializedException(String message, Throwable cause, IGraphElement graphElement) {
		super(message, cause);
		this.graphElement = graphElement;
	}
	
	@Override
	public String getMessage() {
		StringBuilder message = new StringBuilder(MessageFormat.format(notInitializeMessage, 
				graphElement != null ? graphElement.getId() : "unknown"));
		if (!StringUtils.isEmpty(super.getMessage())) {
			message.append(" Message: ");
			message.append(super.getMessage());
		}
		return message.toString();
	}
}
