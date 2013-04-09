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
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.jetel.ctl.ExpParser;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.TransformLangParserConstants;
import org.jetel.ctl.TransformLangParserVisitor;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLTypePrimitive;

public class CLVFLiteral extends SimpleNode implements TransformLangParserConstants {
	
    private static final DateFormat DATE_FORMATTER;
    private static final DateFormat DATE_TIME_FORMATTER;
    private static final Calendar calendar = Calendar.getInstance();
    
    static {
    	DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");
    	DATE_FORMATTER.setLenient(false);
    	DATE_TIME_FORMATTER =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	DATE_TIME_FORMATTER.setLenient(false);
    }
    
    
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
	
	public void computeValue() 
	throws NumberFormatException, ParseException {
		switch (tokenKind) {
		case INTEGER_LITERAL:
			if (valueImage.startsWith("0x")) {
				// hexadecimal literal -> skip 0x
				valueObj = Integer.parseInt(valueImage.substring(2),16);
			} else if (valueImage.startsWith("-0x")) { 
				// negative hexadecimal literal -> skip -0x
				valueObj = -Integer.parseInt(valueImage.substring(3),16);
			} else if (valueImage.startsWith("-0")) {
				// negative octal literal
				valueObj = -Integer.parseInt(valueImage.substring(2),8);
			} else if (valueImage.startsWith("0")) {
				// octal literal
				valueObj = Integer.parseInt(valueImage,8);
			} else {
				// decimal literal
				valueObj = Integer.parseInt(valueImage);
			}
			setType(TLTypePrimitive.INTEGER);
			break;
		case LONG_LITERAL:
			valueImage = stripDistincter(valueImage,'l','L');
			if (valueImage.startsWith("0x")) {
				// hexadecimal literal -> remove 0x prefix
				valueObj = Long.parseLong(valueImage.substring(2),16);
			} else if (valueImage.startsWith("-0x")) {
				valueObj = -Long.parseLong(valueImage.substring(3),16);
		    } else if (valueImage.startsWith("0")) {
				// octal literal -> 0 is the distincter, but Java handles that correctly
				valueObj = Long.parseLong(valueImage,8);
		    } else if (valueImage.startsWith("-0")) {
				// negative octal literal
				valueObj = -Long.parseLong(valueImage.substring(2),8);
			} else {
				valueObj = Long.parseLong(valueImage);
			}
			setType(TLTypePrimitive.LONG);
			break;
		case FLOATING_POINT_LITERAL:	
			final char distincterChar = valueImage.charAt(valueImage.length()-1);
			if ( distincterChar == 'D' || distincterChar == 'd') {
				// decimal literal -> remove trailing distincter
				final BigDecimal result = new BigDecimal(stripDistincter(valueImage,'d','D'),TransformLangExecutor.MAX_PRECISION);
				valueObj = result;
                setType(TLTypePrimitive.DECIMAL);
            }else{
                valueObj=Double.parseDouble(stripDistincter(valueImage,'f','F'));
                setType(TLTypePrimitive.DOUBLE);
            }
			break;
		case STRING_LITERAL:
			valueObj = valueImage;
			setType(TLTypePrimitive.STRING);
			break;
		case BOOLEAN_LITERAL:
			valueObj = Boolean.parseBoolean(valueImage);
			setType(TLTypePrimitive.BOOLEAN);
			break;
		case DATE_LITERAL:
			final ParsePosition p1 = new ParsePosition(0);
			Date date = null;
			synchronized (DATE_FORMATTER) { // DateFormat is not thread-safe
				date = DATE_FORMATTER.parse(valueImage, p1);
			}
			if (date == null) {
				throw new ParseException("Date literal '" + valueImage + "' could not be parsed.", 0);
			}
			if (p1.getIndex() < valueImage.length() - 1) {
				throw new ParseException("Date literal '" + valueImage + "' has invalid format.", p1.getErrorIndex()); 
			}
			//this code can be executed by multiple threads - so statically defined calendar has to be synchronised
			synchronized (calendar) {
				calendar.setTime(date);
				// set all time fields to zero
				calendar.set(Calendar.HOUR, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND,0);
				calendar.set(Calendar.MILLISECOND,0);
				valueObj = calendar.getTime();
			}
			setType(TLTypePrimitive.DATETIME);
			break;
		case DATETIME_LITERAL:
			final ParsePosition p2 = new ParsePosition(0);
			synchronized (DATE_TIME_FORMATTER) { // DateFormat is not thread-safe
				valueObj = DATE_TIME_FORMATTER.parse(valueImage,p2);
			}
			if (valueObj == null) {
				throw new ParseException("Date-time literal '" + valueImage + "' could not be parsed.", 0);
			}
			if (p2.getIndex() < valueImage.length() - 1) {
				throw new ParseException("Date-time literal '" + valueImage + "' has invalid format.", p2.getErrorIndex());
			}
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
	
	private String stripDistincter(String input, char... dist) {
		final char c = input.charAt(input.length()-1);
		for (int i=0; i<dist.length; i++) {
			if (c == dist[i]) {
				return input.substring(0,input.length()-1);
			}
		}
		
		return input;
	}
	
	
	@Override
	public SimpleNode duplicate() {
		return new CLVFLiteral(this);
	}
}
