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
package org.jetel.component.xml.writer;

import org.jetel.component.xml.writer.mapping.ObjectAggregate;
import org.jetel.component.xml.writer.mapping.ObjectAttribute;
import org.jetel.component.xml.writer.mapping.ObjectComment;
import org.jetel.component.xml.writer.mapping.ObjectElement;
import org.jetel.component.xml.writer.mapping.ObjectNamespace;
import org.jetel.component.xml.writer.mapping.ObjectTemplateEntry;
import org.jetel.component.xml.writer.mapping.ObjectValue;
import org.jetel.component.xml.writer.mapping.RecurringElementInfo;

/**
 * @author LKREJCI (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 15 Dec 2010
 */
public interface MappingVisitor {
	void visit(ObjectAggregate element) throws Exception;
	void visit(ObjectAttribute element) throws Exception;
	void visit(ObjectElement element) throws Exception;
	void visit(ObjectNamespace element) throws Exception;
	void visit(ObjectValue element) throws Exception;
	void visit(RecurringElementInfo element) throws Exception;
	void visit(ObjectTemplateEntry objectTemplateEntry) throws Exception;
	void visit(ObjectComment objectComment) throws Exception;
}
