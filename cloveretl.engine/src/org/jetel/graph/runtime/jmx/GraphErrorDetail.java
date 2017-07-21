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
package org.jetel.graph.runtime.jmx;

import org.jetel.exception.SerializableException;
import org.jetel.graph.IGraphElement;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.runtime.WatchDog;

/**
 * Implementation of {@link GraphError} interface used on engine side of JMX interface.
 *  
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19. 6. 2017
 */
public class GraphErrorDetail implements GraphError {

	private static final long serialVersionUID = -5912895797292615832L;

	private String errorMessage;
	
	private Throwable causeException;
	
	private String causeGraphElementId;
	
	private String causeComponentType;

	/**
	 * @return {@link GraphError} representation of result stored in the given watchDog
	 */
	public static GraphErrorDetail createInstance(WatchDog watchDog) {
		if (watchDog.getStatus() == Result.ERROR) {
			GraphErrorDetail graphError = new GraphErrorDetail();
			graphError.setErrorMessage(watchDog.getErrorMessage());
			if (watchDog.getCauseException() != null) {
				graphError.setCauseException(new SerializableException(watchDog.getCauseException()));
			}
			IGraphElement causeGraphElement = watchDog.getCauseGraphElement();
			if (causeGraphElement != null) {
				graphError.setCauseGraphElementId(causeGraphElement.getId());
				if (causeGraphElement instanceof Node) { 
					graphError.setCauseComponentType(((Node) causeGraphElement).getType());
				}
			}
			return graphError;
		} else {
			return null;
		}
	}
	
	@Override
	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	@Override
	public Throwable getCauseException() {
		return causeException;
	}

	public void setCauseException(Throwable causeException) {
		this.causeException = causeException;
	}

	@Override
	public String getCauseGraphElementId() {
		return causeGraphElementId;
	}

	public void setCauseGraphElementId(String causeGraphElementId) {
		this.causeGraphElementId = causeGraphElementId;
	}

	@Override
	public String getCauseComponentType() {
		return causeComponentType;
	}

	public void setCauseComponentType(String causeComponentType) {
		this.causeComponentType = causeComponentType;
	}

	public void setCauseGraphElement(IGraphElement graphElement) {
		if (graphElement != null) {
			this.causeGraphElementId = graphElement.getId();
			if (graphElement instanceof Node) {
				this.causeComponentType = ((Node) graphElement).getType();
			}
		}
	}

}
