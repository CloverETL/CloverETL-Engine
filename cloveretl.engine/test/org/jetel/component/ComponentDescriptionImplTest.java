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

import org.jetel.exception.JetelException;
import org.jetel.plugin.Extension;
import org.jetel.test.CloverTestCase;
import org.jetel.util.XmlUtils;
import org.w3c.dom.Document;

/**
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 10. 7. 2015
 */
public class ComponentDescriptionImplTest extends CloverTestCase {

	public void testPrimitiveComponent() throws JetelException {
		String extensionPointString = "<extension point-id=\"component\">"
				+ "<ETLComponent type=\"TRASHIFIER\" className=\"org.jetel.component.Trashifier\">"
				+ "</ETLComponent>"
				+ "</extension>";
		
		Document doc = XmlUtils.createDocumentFromString(extensionPointString);
		
		Extension componentExtension = new Extension("component", doc.getDocumentElement(), null);
		
		ComponentDescription componentDescription = new ComponentDescriptionImpl(componentExtension);
		componentDescription.getDefaultInputMetadataId(0);
		componentDescription.getDefaultOutputMetadataId(0);
	}
	
}
