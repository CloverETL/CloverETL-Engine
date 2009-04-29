package org.jetel.ctl;

import org.jetel.ctl.ASTnode.CastNode;

/**
 * Interface defines visit methods for synthetic AST nodes.
 * I.e. AST nodes that are not generated directly by the parser but 
 * by additional compiler phases
 * 
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 *
 */
public interface SyntheticNodeVisitor {

	/** Unqiue node identifier */
	public static final int CAST_NODE_ID = 10000; 

	
	public Object visit(CastNode node, Object data);
}
