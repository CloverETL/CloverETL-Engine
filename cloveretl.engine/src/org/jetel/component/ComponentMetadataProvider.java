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
package org.jetel.component;

import org.jetel.graph.Node;

/**
 * This metadata provider extends regular {@link MetadataProvider},
 * so engine component is available for provider.
 * This provider can be referenced by plugin.xml in 'component'
 * extension point, see for example SubGraphInput component description
 * (SubGraphInput$PortToPortMetadataProvider).
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 3. 10. 2013
 */
public interface ComponentMetadataProvider extends MetadataProvider {

	/**
	 * Engine calls this method automatically to provide correct
	 * component instance.
	 * @param component
	 */
	public void setComponent(Node component);
	
}
