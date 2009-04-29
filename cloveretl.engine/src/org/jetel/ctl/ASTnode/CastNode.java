package org.jetel.ctl.ASTnode;

import org.jetel.ctl.TransformLangParserVisitor;
import org.jetel.ctl.data.TLType;

/**
 * Synthetic node representing a type cast.
 * 
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 *
 */
public class CastNode extends SimpleNode  {

	/**
	 * Type to cast from
	 */
	private final TLType fromType;
	
	private CastNode(CastNode c) {
		super(c);
		fromType = c.fromType;
	}
	
	/**
	 * Allocates new type-cast node.
	 * Source type is stored within {@link #fromType} field.
	 * Target type is stored accessible via {@link #getType()}
	 * 
	 * @param id	unique node id
	 * @param fromType	type to cast from
	 */
	public CastNode(int id, TLType fromType, TLType toType) {
		super(id);
		this.fromType = fromType;
		setType(toType);
	}
	
	public TLType getFromType() {
		return fromType;
	}

	public TLType getToType() {
		return getType();
	}
	
	@Override
	public SimpleNode duplicate() {
		return new CastNode(this);
	}
	
	public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
		return visitor.visit(this, data);
	}
	
	@Override
	public String toString() {
		return "CastNode from '" + fromType.name() + "' to '" + getType().name() + "'";
	}
	
}
