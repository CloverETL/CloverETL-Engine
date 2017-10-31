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
package org.jetel.util;

/**
 * @author Jiri Musil (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Mar 28, 2017
 */
public class RestJobUtils {
	
	public static final String REST_JOB_INPUT_TYPE = "RESTJOB_INPUT";
	public static final String REST_JOB_OUTPUT_TYPE = "RESTJOB_OUTPUT";
	
	/**
	 * Answers whether given component type is the visible Rest Job Input component
	 * 
	 * @param componentType
	 * @return
	 */
	public static boolean isRestJobInputComponent(String componentType) {
		return REST_JOB_INPUT_TYPE.equals(componentType);
	}
	
	/**
	 * Answers whether given component type is the visible Rest Job output component
	 * @param componentType
	 * @return
	 */
	public static boolean isRestJobOutputComponent(String componentType) {
		return REST_JOB_OUTPUT_TYPE.equals(componentType);
	}
	
	/**
	 * Answers whether given component type is the visible RestJobInput or RestJobOutput components
	 * @param componentType
	 * @return
	 */
	public static boolean isRestJobInputOutputComponent(String componentType) {
		return isRestJobInputComponent(componentType) || isRestJobOutputComponent(componentType);
	}
	
}
