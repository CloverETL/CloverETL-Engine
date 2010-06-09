package org.jetel.ctl.ASTnode;

import org.jetel.ctl.TransformLangParser;
import org.jetel.ctl.TransformLangParserVisitor;

public class CLVFUnaryNonStatement extends SimpleNode {
	
	// One of ++, --, -, not
	private int operator;
	
	public CLVFUnaryNonStatement(int id) {
		super(id);
	}

	public CLVFUnaryNonStatement(TransformLangParser p, int id) {
		super(p, id);
	}

	public CLVFUnaryNonStatement(CLVFUnaryNonStatement node) {
		super(node);
		this.operator = node.operator;
	}

	@Override
	public SimpleNode duplicate() {
		return new CLVFUnaryNonStatement(this);
	}
	
	public void setOperator(int operator) {
		this.operator = operator;
	}
	
	public int getOperator() {
		return operator;
	}

	/** Accept the visitor. **/
	public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
		return visitor.visit(this, data);
	}
}
