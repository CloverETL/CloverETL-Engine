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
package org.jetel.interpreter.ASTnode;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.jetel.data.Defaults;
import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.CloverLong;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.interpreter.ExpParser;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.TransformLangParserConstants;
import org.jetel.interpreter.TransformLangParserVisitor;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLDateValue;
import org.jetel.interpreter.data.TLNullValue;
import org.jetel.interpreter.data.TLNumericValue;
import org.jetel.interpreter.data.TLStringValue;
import org.jetel.interpreter.data.TLValueType;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

public class CLVFLiteral extends SimpleNode implements TransformLangParserConstants {
	
    public static final String DECIMAL_DISTINCTER_LOWERCASE="d";
    public static final String DECIMAL_DISTINCTER_UPPERCASE="D";
    public static final String LONG_DISTINCTER_LOWERCASE="l";
    public static final String LONG_DISTINCTER_UPPERCASE="L";
    
    private static final DateFormat dateFormat = new SimpleDateFormat(Defaults.DEFAULT_DATE_FORMAT);
    private static final DateFormat dateTimeFormat = new SimpleDateFormat(Defaults.DEFAULT_DATETIME_FORMAT);
    
	String valueImage; 
    public Object valueObj;
	int literalType;
	
	public CLVFLiteral(int id) {
		super(id);
	}
	
	public CLVFLiteral(ExpParser p, int id) {
	    super(p, id);
	  }
	
	/** Accept the visitor. **/
	@Override
	public Object jjtAccept(TransformLangParserVisitor visitor, Object data) {
		return visitor.visit(this, data);
	}
	
	@Override
	@SuppressWarnings("STCAL")
	public void init() throws org.jetel.interpreter.TransformLangExecutorRuntimeException {
		super.init();
		try{
			switch(literalType){
			case FLOATING_POINT_LITERAL:
                if (valueImage.endsWith(DECIMAL_DISTINCTER_LOWERCASE) || 
                        valueImage.endsWith(DECIMAL_DISTINCTER_UPPERCASE)){
                    value=new TLNumericValue(TLValueType.DECIMAL,DecimalFactory.getDecimal(valueImage.substring(0,valueImage.length()-1)));
                }else{
                    value=new TLNumericValue(TLValueType.NUMBER,new CloverDouble( Double.parseDouble(valueImage)));
                }
				break;
			case STRING_LITERAL:
                 value=new TLStringValue(valueImage);
				break;
			case INTEGER_LITERAL:
                // determine size of Integere literal
                if (valueImage.endsWith(LONG_DISTINCTER_UPPERCASE) || 
                        valueImage.endsWith(LONG_DISTINCTER_LOWERCASE)) {
                    value=new TLNumericValue(TLValueType.LONG,new CloverLong(Long.parseLong(valueImage.substring(0,valueImage.length()-1))));
                } else {
                    // try to parse as INT first, if error then LONG
                    try {
                        value=new TLNumericValue(TLValueType.INTEGER,new CloverInteger(Integer.parseInt(valueImage)));
                    } catch (NumberFormatException ex) {
                        value=new TLNumericValue(TLValueType.LONG,new CloverLong(Long.parseLong(valueImage)));
                    }
                }
				break;
            case HEX_LITERAL:
            	 if (valueImage.endsWith(LONG_DISTINCTER_UPPERCASE) || 
                         valueImage.endsWith(LONG_DISTINCTER_LOWERCASE)) {
                     value=new TLNumericValue(TLValueType.LONG,new CloverLong(Long.parseLong(valueImage.substring(2,valueImage.length()-1),16)));
                 } else {
                // try to parse as INT first, if error then LONG
                try{
                    value=new TLNumericValue(TLValueType.INTEGER,new CloverInteger(  Integer.parseInt(valueImage.substring(2),16)));
                }catch(NumberFormatException ex){
                    value=new TLNumericValue(TLValueType.LONG,new CloverLong( Long.parseLong(valueImage.substring(2),16)));
                }
                 }
            break;
            case OCTAL_LITERAL:
            	if (valueImage.endsWith(LONG_DISTINCTER_UPPERCASE) || 
                        valueImage.endsWith(LONG_DISTINCTER_LOWERCASE)) {
                    value=new TLNumericValue(TLValueType.LONG,new CloverLong(Long.parseLong(valueImage.substring(0,valueImage.length()-1),8)));
                } else {
                // try to parse as INT first, if error then LONG
                try{
                    value=new TLNumericValue(TLValueType.INTEGER,new CloverInteger(Integer.parseInt(valueImage,8)));
                }catch(NumberFormatException ex){
                    value=new TLNumericValue(TLValueType.LONG,new CloverLong( Long.parseLong(valueImage,8)));
                }
                }
            break; 
			case DATE_LITERAL:
				//DateFormat dateFormat=new SimpleDateFormat(Defaults.DEFAULT_DATE_FORMAT);
                 value=new TLDateValue(dateFormat.parse(valueImage));
				break;
			case DATETIME_LITERAL:
				//DateFormat dateFormat2=new SimpleDateFormat(Defaults.DEFAULT_DATETIME_FORMAT);
                 value=new TLDateValue(dateTimeFormat.parse(valueImage));
				break;
			case BOOLEAN_LITERAL:
                 value=Boolean.parseBoolean(valueImage) ? TLBooleanValue.TRUE : TLBooleanValue.FALSE;
				break;
            case NULL_LITERAL:
                value=TLNullValue.getInstance();
                break;
			default:
				throw new TransformLangExecutorRuntimeException(this,new Object[0],"Can't handle datatype "
						+tokenImage[literalType]);
			}
		}catch(java.text.ParseException ex){
		    throw new TransformLangExecutorRuntimeException(this,new Object[0],"Parser exception ["+tokenImage[literalType]+"] : Unrecognized value: "+valueImage);
        }catch(NumberFormatException ex){
            throw new TransformLangExecutorRuntimeException(this,new Object[0],"Number format error ["+tokenImage[literalType]+"] : Unrecognized value: "+valueImage);
		}catch(Exception ex){
		    throw new TransformLangExecutorRuntimeException(this,new Object[0],ex.getClass().getName()+" : ["+tokenImage[literalType]+"] : Unrecognized value: "+valueImage);
		}
		valueObj=value.getValue();
	}
	
	public void setVal(int literalType, String valueImage){
		this.valueImage=valueImage;
		this.literalType=literalType;
        init();
	}

	public void setConstant(int value) {
		this.valueImage = String.valueOf(value);
		this.literalType = INTEGER_LITERAL;
		this.value = new TLNumericValue(TLValueType.INTEGER,new CloverInteger(value));
		this.valueObj = this.value.getValue();
	}
	
	public void setNegative(boolean negative) {
		if (negative) {
			if (value.type.isNumeric()) {
				((TLNumericValue)value).neg();
				valueObj = value.getValue();
				valueImage = "-" + valueImage;
			}else {
				throw new TransformLangExecutorRuntimeException(this,new Object[0], "Unary minus is not applicable on literal " + valueImage);
			}
		} 
	}
}
