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
package org.jetel.ctl.ASTnode;
import java.text.ParseException;

import org.jetel.ctl.ExpParser;
import org.jetel.ctl.LiteralParser;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.TransformLangParserConstants;
import org.jetel.ctl.TransformLangParserVisitor;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLTypePrimitive;

public class CLVFLiteral extends SimpleNode implements TransformLangParserConstants {
	
	private String valueImage; 
    private Object valueObj;
    private int tokenKind;
	
	public CLVFLiteral(int id) {
		super(id);
	}
	
	public CLVFLiteral(ExpParser p, int id) {
	    super(p, id);
	  }
	
	public CLVFLiteral(CLVFLiteral node) {
		super(node);
		this.valueImage = node.valueImage;
		this.valueObj = node.valueObj;
	}

	/** Accept the visitor. This method implementation is identical in all SimpleNode descendants. */
	@Override
	public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
		try {
			return visitor.visit(this, data);
		} catch (TransformLangExecutorRuntimeException e) {
			if (e.getNode() == null) {
				e.setNode(this);
			}
			throw e;
		} catch (RuntimeException e) {
			throw new TransformLangExecutorRuntimeException(this, null, e);
		}
	}
	
	public void setValue(int tokenKind, String valueImage) {
		this.tokenKind = tokenKind;
		this.valueImage = valueImage;
	}
	
	public void setValueDirect(Object value) {
		this.valueObj = value;
	}
	
	public int getTokenKind() {
		return tokenKind;
	}
	
	public String getValueImage() {
		return valueImage;
	}
	
	public void computeValue(LiteralParser parser) throws NumberFormatException, ParseException {
		switch (tokenKind) {
		case INTEGER_LITERAL:
			valueObj = parser.parseInt(valueImage);
			setType(TLTypePrimitive.INTEGER);
			break;
		case LONG_LITERAL:
			valueObj = parser.parseLong(valueImage);
			setType(TLTypePrimitive.LONG);
			break;
		case FLOATING_POINT_LITERAL:	
			final char distincterChar = valueImage.charAt(valueImage.length()-1);
			if ( distincterChar == 'D' || distincterChar == 'd') {
				// decimal literal -> remove trailing distincter
				valueObj = parser.parseBigDecimal(valueImage);
                setType(TLTypePrimitive.DECIMAL);
            } else {
                valueObj = parser.parseDouble(valueImage);
                setType(TLTypePrimitive.DOUBLE);
            }
			break;
		case STRING_LITERAL:
			valueObj = valueImage;
			setType(TLTypePrimitive.STRING);
			break;
		case BOOLEAN_LITERAL:
			valueObj = parser.parseBoolean(valueImage);
			setType(TLTypePrimitive.BOOLEAN);
			break;
		case DATE_LITERAL:
			valueObj = parser.parseDate(valueImage);
			setType(TLTypePrimitive.DATETIME);
			break;
		case DATETIME_LITERAL:
			valueObj = parser.parseDateTime(valueImage);
			setType(TLTypePrimitive.DATETIME);
			break;
		case NULL_LITERAL:
			valueObj = null;
			setType(TLType.NULL);
			break;
		}
	}
	
	public Object getValue() {
		return valueObj;
	}
	
	
	@Override
	public String toString() {
		return super.toString() + " value " + "\"" + valueObj + "\"";
	}
	
	@Override
	public SimpleNode duplicate() {
		return new CLVFLiteral(this);
	}
}
