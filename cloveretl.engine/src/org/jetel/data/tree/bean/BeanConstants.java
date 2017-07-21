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
package org.jetel.data.tree.bean;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 7.11.2011
 */
public class BeanConstants {
	
	public static final String PATH_SEPARATOR = "/";
	
	public static final String OBJECT_ELEMENT_NAME = "object";
	public static final String LIST_ELEMENT_NAME = "list";
	public static final String LIST_ITEM_ELEMENT_NAME = "item";
	public static final String MAP_ELEMENT_NAME = "map";
	public static final String MAP_ENTRY_ELEMENT_NAME = "entry";
	public static final String MAP_KEY_ELEMENT_NAME = "key";
	public static final String MAP_VALUE_ELEMENT_NAME = "value";
	
	private BeanConstants() {
		// This class is not meant to be instantiated
	}

}
