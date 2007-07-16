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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.data.primitive.Numeric;
import org.jetel.interpreter.Stack;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.util.Compare;
import org.jetel.util.StringUtils;

public class StringLib extends TLFunctionLibrary {

    private static final String LIBRARY_NAME = "String";

    enum Function {
        CONCAT("concat"), UPPERCASE("uppercase"), LOWERCASE("lowercase"), LEFT(
                "left"), SUBSTRING("substring"), RIGHT("right"), TRIM("trim"), LENGTH(
                "length"), SOUNDEX("soundex"), REPLACE("replace"), SPLIT("split"),CHAR_AT("charat") ;

        public String name;

        private Function(String name) {
            this.name = name;
        }

        public static Function fromString(String s) {
            for (Function function : Function.values()) {
                if (s.equalsIgnoreCase(function.name)
                        || s.equalsIgnoreCase(LIBRARY_NAME + "."
                                + function.name)) {
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
        switch (Function.fromString(functionName)) {
        case CONCAT:
            return new ConcatFunction();
        case UPPERCASE:
            return new UppercaseFunction();
        case LOWERCASE:
            return new LowerCaseFunction();
        case SUBSTRING:
            return new SubstringFunction();
        case LEFT:
            return new LeftFunction();
        case RIGHT:
            return new RightFunction();
        case TRIM:
            return new TrimFunction();
        case LENGTH:
            return new LengthFunction();
        case SOUNDEX:
            return new SoundexFunction();
        case REPLACE:
            return new ReplaceFunction();
        case SPLIT:
            return new SplitFunction();
        case CHAR_AT:
        	return new CharAtFunction();
        default:
            return null;
        }
    }

    // CONCAT
    class ConcatFunction extends TLFunctionPrototype {

        public ConcatFunction() {
            super("string", "concat", new TLValueType[] { TLValueType.STRING },
                    TLValueType.STRING,999,2);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.value;
            strBuf.setLength(0);
            for (int i = 0; i < params.length; i++) {
                if (!params[i].isNull()) {
                    if (params[i].type == TLValueType.STRING) {
                        StringUtils.strBuffAppend(strBuf, params[i]
                                .getCharSequence());
                    } else {
                        StringUtils.strBuffAppend(strBuf, params[i].toString());
                    }
                } 
            }
            return val;
        }

        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
        }
    }

    // UPPERCASE
    class UppercaseFunction extends TLFunctionPrototype {

        public UppercaseFunction() {
            super("string", "uppercase",
                    new TLValueType[] { TLValueType.STRING },
                    TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.value;
            strBuf.setLength(0);

            if (!params[0].isNull() && params[0].type == TLValueType.STRING) {
                CharSequence seq = params[0].getCharSequence();
                strBuf.ensureCapacity(seq.length());
                for (int i = 0; i < seq.length(); i++) {
                    strBuf.append(Character.toUpperCase(seq.charAt(i)));
                }
            } else {
                throw new TransformLangExecutorRuntimeException(params,
                        "uppercase - wrong type of literal");
            }

            return val;
        }

        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
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
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.value;
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

            return val;
        }

        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
        }

    }

    // SUBSTRING
    class SubstringFunction extends TLFunctionPrototype {

        public SubstringFunction() {
            super("string", "substring", new TLValueType[] {
                    TLValueType.STRING, TLValueType.INTEGER,
                    TLValueType.INTEGER }, TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.value;
            strBuf.setLength(0);
            
            int length, from;
            if (params[0].isNull() || params[1].isNull() || params[2].isNull()) {
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

            StringUtils.subString(strBuf, params[0].getCharSequence(), from, length);
            return val;
        }

        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
        }
    }

    // LEFT
    class LeftFunction extends TLFunctionPrototype {

        public LeftFunction() {
            super("string", "left", new TLValueType[] { TLValueType.STRING,
                    TLValueType.INTEGER }, TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.value;
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

            StringUtils.subString(strBuf, params[0].getCharSequence(), 0, length);
            return val;
            
        }

        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
        }
    }

    // RIGHT
    class RightFunction extends TLFunctionPrototype {

        public RightFunction() {
            super("string", "right", new TLValueType[] { TLValueType.STRING,
                    TLValueType.INTEGER }, TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.value;
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

            StringUtils.subString(strBuf, src,from, length);
            return val;
        }

        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
        }
    }

    // TRIM
    class TrimFunction extends TLFunctionPrototype {

        public TrimFunction() {
            super("string", "trim", new TLValueType[] { TLValueType.STRING },
                    TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.value;
            strBuf.setLength(0);
            
            if (params[0].type != TLValueType.STRING) {
                throw new TransformLangExecutorRuntimeException(params,
                        "trim - wrong type of literal");
            }
            strBuf.append(params[0].getCharSequence());
            StringUtils.trim(strBuf);

            if (strBuf.length() == 0)
                return Stack.EMPTY_STRING;

            return val;

        }

        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
        }
    }

    // LENGTH
    class LengthFunction extends TLFunctionPrototype {

        public LengthFunction() {
            super("string", "length", new TLValueType[] { TLValueType.OBJECT },
                    TLValueType.INTEGER);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            Numeric intBuf = val.getNumeric();
            
            switch(params[0].type){
            case STRING:
            	intBuf.setValue(params[0].getCharSequence().length());
            	break;
            case LIST:
            	intBuf.setValue(params[0].getList().size());
            	break;
            case MAP:
            	intBuf.setValue(params[0].getMap().size());
            	break;
            	default:
                throw new TransformLangExecutorRuntimeException(params,
                        "length - wrong type of literal");
            }

            return val;

        }

        @Override
        public TLContext createContext() {
            return TLContext.createIntegerContext();
        }
    }

    // SOUNDEX
    class SoundexFunction extends TLFunctionPrototype {

        private static final int SIZE = 4;

        public SoundexFunction() {
            super("string", "soundex",
                    new TLValueType[] { TLValueType.STRING },
                    TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder targetStrBuf = (StringBuilder)val.value;
            targetStrBuf.setLength(0);
            
            if (params[0].type != TLValueType.STRING) {
                throw new TransformLangExecutorRuntimeException(params,
                        "soundex - wrong type of literal");
            }

            CharSequence src = params[0].getCharSequence();
            int length = src.length();
            char srcChars[] = new char[length];
            for (int i = 0; i < length; i++)
                srcChars[i]=Character.toUpperCase(src.charAt(i++));
            char firstLetter = srcChars[0];

            // convert letters to numeric code
            for (int i = 0; i < srcChars.length; i++) {
                switch (srcChars[i]) {
                case 'B':
                case 'F':
                case 'P':
                case 'V': {
                    srcChars[i] = '1';
                    break;
                }

                case 'C':
                case 'G':
                case 'J':
                case 'K':
                case 'Q':
                case 'S':
                case 'X':
                case 'Z': {
                    srcChars[i] = '2';
                    break;
                }

                case 'D':
                case 'T': {
                    srcChars[i] = '3';
                    break;
                }

                case 'L': {
                    srcChars[i] = '4';
                    break;
                }

                case 'M':
                case 'N': {
                    srcChars[i] = '5';
                    break;
                }

                case 'R': {
                    srcChars[i] = '6';
                    break;
                }

                default: {
                    srcChars[i] = '0';
                    break;
                }
                }
            }

            // remove duplicates
            targetStrBuf.append(firstLetter);
            char last = srcChars[0];
            for (int i = 1; i < srcChars.length; i++) {
                if (srcChars[i] != '0' && srcChars[i] != last) {
                    last = srcChars[i];
                    targetStrBuf.append(last);
                }
            }

            // pad with 0's or truncate
            for (int i = targetStrBuf.length(); i < SIZE; i++) {
                targetStrBuf.append('0');
            }
            targetStrBuf.setLength(SIZE);
            return val;

        }

        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
        }
    }

    
    //REPLACE
    
    class ReplaceFunction extends TLFunctionPrototype {

        public ReplaceFunction() {
            super("string", "replace", new TLValueType[] {
                    TLValueType.STRING, TLValueType.STRING,
                    TLValueType.STRING }, TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            RegexStore regex = (RegexStore) context.getContext();
            if (regex.pattern==null){
            	regex.initRegex(params[1].getString(), params[0].getCharSequence(),true,new TLValue(TLValueType.STRING,new StringBuffer()));
            }else{
            	// can we reuse pattern/matcher
            	if(regex.storedRegex!=params[1].getValue() &&  Compare.compare(regex.storedRegex,params[1].getCharSequence())!=0){
            		//we can't
            		regex.resetPattern(params[1].getString());
            	}
            	//            	 reset matcher
                regex.resetMatcher(params[0].getCharSequence());
            }
            
            String replacement=params[2].toString();
            StringBuffer sb=(StringBuffer)regex.result.getValue();
            sb.setLength(0);
            while ( regex.matcher.find()) {
                regex.matcher.appendReplacement(sb, replacement);
            }
            regex.matcher.appendTail(sb);
            
			return regex.result;

        }

        @Override
        public TLContext createContext() {
            TLContext<RegexStore> context = new TLContext<RegexStore>();
            context.setContext(new RegexStore());
            return context;
        }
        
        
    }

    
    class SplitFunction extends TLFunctionPrototype {

        public SplitFunction() {
            super("string", "split", new TLValueType[] {
                    TLValueType.STRING, TLValueType.STRING}, TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            RegexStore regex = (RegexStore) context.getContext();
            if (regex.pattern==null){
            	regex.initRegex(params[1].getString(), params[0].getCharSequence(),false,new TLValue(TLValueType.LIST,new ArrayList<TLValue>()));
            }else{
            	// can we reuse pattern/matcher
            	if(regex.storedRegex!=params[1].getValue() &&  Compare.compare(regex.storedRegex,params[1].getCharSequence())!=0){
            		//we can't
            		regex.resetPattern(params[1].getString());
            	}
            }
            
            String[] strArray=regex.pattern.split(params[0].getCharSequence());
            List<TLValue> list=regex.result.getList();
            list.clear();
            for(String item : strArray){
            	list.add(new TLValue(TLValueType.STRING,item));
            }
            
            return regex.result;

        }

        @Override
        public TLContext createContext() {
            TLContext<RegexStore> context = new TLContext<RegexStore>();
            context.setContext(new RegexStore());
            return context;
        }
        
        
    }

    //  CHAR AT
    class CharAtFunction extends TLFunctionPrototype {

        public CharAtFunction() {
            super("string", "char_at", new TLValueType[] { TLValueType.STRING ,
                    TLValueType.INTEGER}, TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.value;
            strBuf.setLength(0);
            
            if (params[1].isNull()) {
                throw new TransformLangExecutorRuntimeException(params,
                        Function.CHAR_AT.name()+" - NULL value not allowed");
            }
            
            if (!params[0].isNull()) {
                try {
                strBuf.append(params[0].getCharSequence().charAt(params[1].getInt()));
                }catch(IndexOutOfBoundsException ex) {
                    throw new TransformLangExecutorRuntimeException(params,
                            Function.CHAR_AT.name()+" - character index is out of bounds",ex);
                }catch(Exception ex) {
                    throw new TransformLangExecutorRuntimeException(params,
                            Function.CHAR_AT.name()+" - wrong type of literals",ex);
                }
            }
            return val;
        }

        @Override
        public TLContext createContext() {
            return TLContext.createStringContext();
        }
    }

    
	class RegexStore{
	    public Pattern pattern;
	    public Matcher matcher;
	    public String storedRegex;
	    public TLValue result;
	    
	    public void initRegex(String regex,CharSequence input,boolean initMatcher,TLValue value){
        	pattern=Pattern.compile(regex);
        	if (initMatcher) matcher=pattern.matcher(input);
        	storedRegex=regex;
        	result=value;
        }
	    
	    public void resetPattern(String regex){
	    	pattern=Pattern.compile(regex);
	    	matcher.usePattern(pattern);
	    }
	    
	    public void resetMatcher(CharSequence input){
	    	matcher.reset(input);
	    }
	}

}
