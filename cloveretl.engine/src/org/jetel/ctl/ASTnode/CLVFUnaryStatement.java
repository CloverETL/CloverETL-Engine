package org.jetel.ctl.ASTnode;

import org.jetel.ctl.TransformLangParser;
import org.jetel.ctl.TransformLangParserVisitor;

public class CLVFUnaryStatement extends SimpleNode {
	
	// One of ++, --, -, not
	private int operator;
	
	public CLVFUnaryStatement(int id) {
		super(id);
	}

	public CLVFUnaryStatement(TransformLangParser p, int id) {
		super(p, id);
	}

	public CLVFUnaryStatement(CLVFUnaryStatement node) {
		super(node);
		this.operator = node.operator;
	}

	@Override
	public SimpleNode duplicate() {
		return new CLVFUnaryStatement(this);
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
