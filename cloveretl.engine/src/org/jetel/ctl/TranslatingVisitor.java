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
package org.jetel.ctl;

import org.jetel.ctl.ASTnode.Node;
import org.jetel.ctl.ASTnode.SimpleNode;

/**
 * Visitor with support for AST rewriting. 
 * Descendants should rewritten AST node instance from their visit() methods. 
 * 
 * @author mtomcanyi
 *
 */
public class TranslatingVisitor extends NavigatingVisitor {
	
	@Override
	protected Object visitNode(SimpleNode node, Object data) {
		if (node != null && node.jjtHasChildren()) {
			Node[] newChildren = new Node[node.jjtGetNumChildren()];
			for (int i=0; i<node.jjtGetNumChildren(); i++) {
				Node child = node.jjtGetChild(i);
				newChildren[i] = (Node)child.jjtAccept(this, data);
			}
			
			// swap children in case some of them were modified
			node.setChildren(newChildren);
		}
		
		return node;
	}

}
