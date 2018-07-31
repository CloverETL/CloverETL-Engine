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

	private final String errorMessage;
	
	private final Throwable causeException;
	
	private final String causeGraphElementId;
	
	private final String causeComponentType;

	/**
	 * @return {@link GraphError} representation of result stored in the given watchDog
	 */
	public static GraphErrorDetail createInstance(WatchDog watchDog) {
		if (watchDog.getStatus() == Result.ERROR) {
			
			String errorMessage = watchDog.getErrorMessage();

			Throwable causeException = null;
			if (watchDog.getCauseException() != null) {
				causeException = SerializableException.wrap(watchDog.getCauseException());
			}
			
			String causeGraphElementId = null;
			String causeComponentType = null;
			IGraphElement causeGraphElement = watchDog.getCauseGraphElement();
			if (causeGraphElement != null) {
				causeGraphElementId = causeGraphElement.getId();
				if (causeGraphElement instanceof Node) { 
					causeComponentType = ((Node) causeGraphElement).getType();
				}
			}
			
			return new GraphErrorDetail(errorMessage, causeException, causeGraphElementId, causeComponentType);
		} else {
			return null;
		}
	}
	
	public GraphErrorDetail(Throwable causeException, String causeGraphElementId, String causeComponentType) {
		this(null, causeException, causeGraphElementId, causeComponentType);
	}
	
	public GraphErrorDetail(String errorMessage, Throwable causeException, String causeGraphElementId,
			String causeComponentType) {
		super();
		this.errorMessage = errorMessage;
		this.causeException = causeException;
		this.causeGraphElementId = causeGraphElementId;
		this.causeComponentType = causeComponentType;
	}

	@Override
	public String getErrorMessage() {
		return errorMessage;
	}

	@Override
	public Throwable getCauseException() {
		return causeException;
	}

	@Override
	public String getCauseGraphElementId() {
		return causeGraphElementId;
	}

	@Override
	public String getCauseComponentType() {
		return causeComponentType;
	}
}
