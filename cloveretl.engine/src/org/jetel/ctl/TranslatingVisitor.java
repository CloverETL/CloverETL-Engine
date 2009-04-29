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
