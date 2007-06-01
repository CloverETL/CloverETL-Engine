/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-07  David Pavlis <david.pavlis@centrum.cz> and others.
 *    
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *    
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
 *    Lesser General Public License for more details.
 *    
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * Created on 2.4.2007
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.interpreter.extensions;

import org.jetel.data.primitive.CloverInteger;
import org.jetel.interpreter.Stack;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.data.TLContext;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.util.StringUtils;

public class StringLib extends TLFunctionLibrary {

    private static final String LIBRARY_NAME = "String";

    enum Function {
        CONCAT("concat"),
        UPPERCASE("uppercase"),
        LOWERCASE("lowercase"),
        LEFT("left"),
        RIGHT("right"),
        TRIM("trim"),
        LENGTH("length");
        
        public String name;
        
        private Function(String name) {
            this.name = name;
        }
        
        public static Function fromString(String s) {
            for(Function function : Function.values()) {
                if(s.equalsIgnoreCase(function.name) || s.equalsIgnoreCase(LIBRARY_NAME + "." + function.name)) {
                    return function;
                }
            }
            return null;
        }
    }

    public StringLib() {
        super();
    }
    
    public TLFunctionPrototype getFunction(String functionName) {
        switch(Function.fromString(functionName)) {
        case CONCAT: return new ConcatFunction();
        case UPPERCASE: return new UppercaseFunction();
        case LOWERCASE: return new LowerCaseFunction();
        case LEFT: return new LeftFunction();
        case RIGHT: return new RightFunction();
        case TRIM: return new TrimFunction();
        case LENGTH: return new LengthFunction();
        default: return null;
       }
    }
    


    // CONCAT
    class ConcatFunction extends TLFunctionPrototype {

    	public ConcatFunction() {
    		super("string", "concat", new TLValueType[] { TLValueType.STRING }, TLValueType.STRING);
    	}
    	
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            StringBuilder strBuf = (StringBuilder) context.getContext();
            strBuf.setLength(0);
            for (int i = 0; i < params.length; i++) {
                if (!params[i].isNull()) {
                    if (params[i].type == TLValueType.STRING) {
                        StringUtils.strBuffAppend(strBuf, params[i]
                                .getCharSequence());
                    } else {
                        StringUtils.strBuffAppend(strBuf, params[i]
                                .toString());
                    }
                } else {
                    throw new TransformLangExecutorRuntimeException(
                            params, "concat - wrong type of literal(s)");
                }
            }
            return new TLValue(TLValueType.STRING, strBuf);
        }
     }

    // UPPERCASE
    class UppercaseFunction extends TLFunctionPrototype {
    	
    	public UppercaseFunction(){
    		super("string", "uppercase", new TLValueType[] { TLValueType.STRING }, TLValueType.STRING);
    	}
    	
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            StringBuilder strBuf = (StringBuilder) context.getContext();
            strBuf.setLength(0);

            if (!params[0].isNull()
                    && params[0].type == TLValueType.STRING) {
                CharSequence seq = params[0].getCharSequence();
                strBuf.ensureCapacity(seq.length());
                for (int i = 0; i < seq.length(); i++) {
                    strBuf.append(Character.toUpperCase(seq.charAt(i)));
                }
            } else {
                throw new TransformLangExecutorRuntimeException(params,
                        "uppercase - wrong type of literal");
            }

            return new TLValue(TLValueType.STRING, strBuf);
        }

    }

    // LOWERCASE
    class LowerCaseFunction extends TLFunctionPrototype {

    	public LowerCaseFunction() {
			super("string", "lowercase",
					new TLValueType[] { TLValueType.STRING },
					TLValueType.STRING);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			StringBuilder strBuf = (StringBuilder) context.getContext();
			strBuf.setLength(0);

			if (!params[0].isNull() && params[0].type == TLValueType.STRING) {
				CharSequence seq = params[0].getCharSequence();
				strBuf.ensureCapacity(seq.length());
				for (int i = 0; i < seq.length(); i++) {
					strBuf.append(Character.toLowerCase(seq.charAt(i)));
				}
			} else {
				throw new TransformLangExecutorRuntimeException(params,
						"uppercase - wrong type of literal");
			}

			return new TLValue(TLValueType.STRING, strBuf);
		}

	}
    
    // SUBSTRING
    class SubstringFunction extends TLFunctionPrototype {
    	
    	public SubstringFunction(){
    		super("string", "substring", new TLValueType[] {TLValueType.STRING, TLValueType.INTEGER, TLValueType.INTEGER }, TLValueType.STRING);
    	}

    	@Override
        public TLValue execute(TLValue[] params, TLContext context) {
            StringBuilder strBuf = (StringBuilder) context.getContext();
            strBuf.setLength(0);
            int length, from;
            if (params[0].isNull() || params[1].isNull()
                    || params[2].isNull()) {
                throw new TransformLangExecutorRuntimeException(params,
                        "substring - NULL value not allowed");
            }

            try {
                length = params[2].getInt();
                from = params[1].getInt();
            } catch (Exception ex) {
                throw new TransformLangExecutorRuntimeException(params,
                        "substring - " + ex.getMessage());
            }

            if (params[0].type != TLValueType.STRING) {
                throw new TransformLangExecutorRuntimeException(params,
                        "substring - wrong type of literal(s)");
            }

            return new TLValue(TLValueType.STRING, StringUtils
                    .subString(strBuf, params[0].getCharSequence(),
                            from, length));
        }
    }

    // LEFT
    class LeftFunction extends TLFunctionPrototype {
    	
    	public LeftFunction(){
    		super("string", "left", new TLValueType[] { TLValueType.STRING, TLValueType.INTEGER }, TLValueType.STRING);
    	}

    	@Override
        public TLValue execute(TLValue[] params, TLContext context) {
            StringBuilder strBuf = (StringBuilder) context.getContext();
            strBuf.setLength(0);
            int length;
            if (params[0].isNull() || params[1].isNull()) {
                throw new TransformLangExecutorRuntimeException(params,
                        "left - NULL value not allowed");
            }

            try {
                length = params[1].getInt();
            } catch (Exception ex) {
                throw new TransformLangExecutorRuntimeException(params,
                        "left - " + ex.getMessage());
            }

            if (params[0].type != TLValueType.STRING) {
                throw new TransformLangExecutorRuntimeException(params,
                        "left - wrong type of literal(s)");
            }

            return new TLValue(TLValueType.STRING, StringUtils
                    .subString(strBuf, params[0].getCharSequence(), 0,
                            length));
        }
    }

    // RIGHT
    class RightFunction extends TLFunctionPrototype {
    	
    	public RightFunction(){
    		super("string", "right", new TLValueType[] { TLValueType.STRING, TLValueType.INTEGER }, TLValueType.STRING);
    	}
    
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            StringBuilder strBuf = (StringBuilder) context.getContext();
            strBuf.setLength(0);
            int length;
            if (params[0].isNull() || params[1].isNull()) {
                throw new TransformLangExecutorRuntimeException(params,
                        "right - NULL value not allowed");
            }

            try {
                length = params[1].getInt();
            } catch (Exception ex) {
                throw new TransformLangExecutorRuntimeException(params,
                        "right - " + ex.getMessage());
            }

            if (params[0].type != TLValueType.STRING) {
                throw new TransformLangExecutorRuntimeException(params,
                        "right - wrong type of literal(s)");
            }

            CharSequence src = params[0].getCharSequence();
            int from = src.length() - length;

            return new TLValue(TLValueType.STRING, StringUtils
                    .subString(strBuf, params[0].getCharSequence(),
                            from, length));
        }
    }

    // TRIM
    class TrimFunction extends TLFunctionPrototype {
    	
    	public TrimFunction(){
    		super("string", "trim", new TLValueType[] { TLValueType.STRING }, TLValueType.STRING);
    	}
    
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            StringBuilder strBuf = (StringBuilder) context.getContext();
            strBuf.setLength(0);
            if (params[0].type != TLValueType.STRING) {
                throw new TransformLangExecutorRuntimeException(params,
                        "trim - wrong type of literal");
            }
            strBuf.append(params[0].getCharSequence());
            StringUtils.trim(strBuf);

            if (strBuf.length() == 0)
                return Stack.EMPTY_STRING;

            return new TLValue(TLValueType.STRING, strBuf);

        }
    }

    
    // LENGTH
    class LengthFunction extends TLFunctionPrototype {
    	
    	public LengthFunction() {
    		super ("string", "length", new TLValueType[] { TLValueType.STRING }, TLValueType.INTEGER);
		} 
    
        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            CloverInteger intBuf = (CloverInteger) context.getContext();
            if (params[0].type != TLValueType.STRING) {
                throw new TransformLangExecutorRuntimeException(params,
                        "length - wrong type of literal");
            }
            intBuf.setValue(params[0].getCharSequence().length());
           
            return new TLValue(TLValueType.INTEGER, intBuf);

        }
    }
             
}
