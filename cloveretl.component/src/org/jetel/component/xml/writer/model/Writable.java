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
package org.jetel.component.xml.writer.model;

import java.io.IOException;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

import org.jetel.component.xml.writer.XmlFormatter;
import org.jetel.data.DataRecord;

/**
 * Interface for xml writer engine model.
 * 
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20 Dec 2010
 */
public interface Writable {
	
	public void write(XmlFormatter formatter, Map<Integer, DataRecord> availableData) throws XMLStreamException, IOException;
	
	public boolean isEmpty(Map<Integer, DataRecord> availableData);
	
	public String getText(Map<Integer, DataRecord> availableData);

}
