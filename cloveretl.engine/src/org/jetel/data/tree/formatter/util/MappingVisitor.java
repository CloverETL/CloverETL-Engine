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
package org.jetel.data.tree.formatter.util;

import org.jetel.data.tree.formatter.designmodel.Attribute;
import org.jetel.data.tree.formatter.designmodel.CollectionNode;
import org.jetel.data.tree.formatter.designmodel.Comment;
import org.jetel.data.tree.formatter.designmodel.Namespace;
import org.jetel.data.tree.formatter.designmodel.ObjectNode;
import org.jetel.data.tree.formatter.designmodel.Relation;
import org.jetel.data.tree.formatter.designmodel.TemplateEntry;
import org.jetel.data.tree.formatter.designmodel.Value;
import org.jetel.data.tree.formatter.designmodel.WildcardAttribute;
import org.jetel.data.tree.formatter.designmodel.WildcardNode;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 15 Dec 2010
 */
public interface MappingVisitor {
	void visit(WildcardNode element) throws Exception;
	void visit(WildcardAttribute wildcardAttribute) throws Exception;
	void visit(Attribute element) throws Exception;
	void visit(ObjectNode element) throws Exception;
	void visit(CollectionNode element) throws Exception;
	void visit(Namespace element) throws Exception;
	void visit(Value element) throws Exception;
	void visit(Relation element) throws Exception;
	void visit(TemplateEntry objectTemplateEntry) throws Exception;
	void visit(Comment objectComment) throws Exception;
}
