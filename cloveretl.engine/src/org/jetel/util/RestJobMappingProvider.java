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

import org.jetel.graph.Node;

/**
 * @author jedlickad (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 10. 11. 2017
 */
public class RestJobMappingProvider {

	public static String createMapping(Node outputComponent, RestJobOutputType outputType) {
		//TODO implement com.cloveretl.gui.dataservice.utils.MappingProvider.createMapping
		return "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><root xmlns:clover=\"http://www.cloveretl.com/ns/xmlmapping\"><clover:collection clover:inPort=\"0\" clover:name=\"requestEcho\"><element0><clover:elements clover:include=\"$0.*\"/></element0></clover:collection></root>";
	}
}
