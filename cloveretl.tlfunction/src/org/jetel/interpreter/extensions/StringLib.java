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

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetel.data.primitive.Numeric;
import org.jetel.exception.JetelException;
import org.jetel.interpreter.TransformLangExecutorRuntimeException;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLContainerValue;
import org.jetel.interpreter.data.TLListValue;
import org.jetel.interpreter.data.TLMapValue;
import org.jetel.interpreter.data.TLNullValue;
import org.jetel.interpreter.data.TLNumericValue;
import org.jetel.interpreter.data.TLStringValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.MiscUtils;
import org.jetel.util.string.Compare;
import org.jetel.util.string.StringAproxComparator;
import org.jetel.util.string.StringUtils;

public class StringLib extends TLFunctionLibrary {

    private static final String LIBRARY_NAME = "String";
    
    enum Function {
        CONCAT("concat"), UPPERCASE("uppercase"), LOWERCASE("lowercase"), LEFT(
                "left"), SUBSTRING("substring"), RIGHT("right"), TRIM("trim"), LENGTH(
                "length"), SOUNDEX("soundex"), REPLACE("replace"), SPLIT("split"),CHAR_AT(
                "char_at"), IS_BLANK("is_blank"), IS_ASCII("is_ascii"), IS_NUMBER("is_number"),
                IS_INTEGER("is_integer"), IS_LONG("is_long"), IS_DATE("is_date"), 
                REMOVE_DIACRITIC("remove_diacritic"), REMOVE_BLANK_SPACE("remove_blank_space"), 
                GET_ALPHANUMERIC_CHARS("get_alphanumeric_chars"), TRANSLATE("translate"), 
                JOIN("join"), INDEX_OF("index_of"), COUNT_CHAR("count_char"), CHOP("chop"),
                FIND("find"),CUT("cut"), REMOVE_NONPRINTABLE("remove_nonprintable"),
                REMOVE_NONASCII("remove_nonascii"), EDIT_DISTANCE("edit_distance"), METAPHONE("metaphone"),
                NYSIIS("nysiis");

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
        case IS_BLANK:
        	return new IsBlankFunction();
        case IS_ASCII:
        	return new IsAsciiFunction();
        case IS_NUMBER:
        	return new IsNumberFunction();
        case IS_INTEGER:
        	return new IsIntegerFunction();
        case IS_LONG:
        	return new IsLongFunction();
        case IS_DATE:
        	return new IsDateFunction();
        case REMOVE_DIACRITIC:
        	return new RemoveDiacriticFunction();
        case REMOVE_BLANK_SPACE:
        	return new RemoveBlankSpaceFunction();
        case GET_ALPHANUMERIC_CHARS:
        	return new GetAlphanumericCharsFunction();
        case TRANSLATE:
        	return new TranslateFunction();
        case JOIN:
        	return new JoinFunction();
        case INDEX_OF:
        	return new IndexOfFunction();
        case COUNT_CHAR:
        	return new CountCharFunction();
        case FIND:
        	return new FindFunction();
        case CUT:
        	return new CutFunction();
        case CHOP:
        	return new ChopFunction();
        case REMOVE_NONPRINTABLE:
        	return new RemoveNonPrintableFunction();
        case REMOVE_NONASCII:
        	return new RemoveNonAsciiFunction();
        case EDIT_DISTANCE:
        	return new EditDistanceFunction();
        case METAPHONE:
        	return new MetaphoneFunction();
        case NYSIIS:
        	return new NYSIISFunction();
        default:
            return null;
        }
    }
    
    public  Collection<TLFunctionPrototype> getAllFunctions() {
    	List<TLFunctionPrototype> ret = new ArrayList<TLFunctionPrototype>();
    	Function[] fun = Function.values();
    	for (Function function : fun) {
    		ret.add(getFunction(function.name));
		}
    	
    	return ret;
    }

    // CONCAT
    class ConcatFunction extends TLFunctionPrototype {

        public ConcatFunction() {
            super("string", "concat", "Concatenates two strings", new TLValueType[] { TLValueType.STRING },
                    TLValueType.STRING,999,2);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.getValue();
            strBuf.setLength(0);
            for (int i = 0; i < params.length; i++) {
                if (params[i]!=TLNullValue.getInstance()) {
                    if (params[i].type == TLValueType.STRING) {
                        StringUtils.strBuffAppend(strBuf, ((TLStringValue)params[i])
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
            super("string", "uppercase", "Returns uppercase representation", 
                    new TLValueType[] { TLValueType.STRING },
                    TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.getValue();
            strBuf.setLength(0);

            if (params[0]!=TLNullValue.getInstance() && params[0].type == TLValueType.STRING) {
                CharSequence seq = ((TLStringValue)params[0]).getCharSequence();
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
            super("string", "lowercase", "Returns lowercase representation",
                    new TLValueType[] { TLValueType.STRING },
                    TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.getValue();
            strBuf.setLength(0);
            
            if (params[0]!=TLNullValue.getInstance() && params[0].type == TLValueType.STRING) {
                CharSequence seq = ((TLStringValue)params[0]).getCharSequence();
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
            super("string", "substring", "Returns a substring of a given string",
            		new TLValueType[] {
                    TLValueType.STRING, TLValueType.INTEGER,
                    TLValueType.INTEGER }, TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.getValue();
            strBuf.setLength(0);
            
            int length, from;
            if (params[0]==TLNullValue.getInstance() || params[1]==TLNullValue.getInstance() || params[2]==TLNullValue.getInstance()) {
                throw new TransformLangExecutorRuntimeException(params,
                        "substring - NULL value not allowed");
            }

            try {
                length = ((TLNumericValue)params[2]).getInt();
                from = ((TLNumericValue)params[1]).getInt();
            } catch (Exception ex) {
                throw new TransformLangExecutorRuntimeException(params,
                        "substring - " + ex.getMessage());
            }

            if (params[0].type != TLValueType.STRING) {
                throw new TransformLangExecutorRuntimeException(params,
                        "substring - wrong type of literal(s)");
            }

            StringUtils.subString(strBuf, ((TLStringValue)params[0]).getCharSequence(), from, length);
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
            super("string", "left", "Returns prefix of the specified length", 
            		new TLValueType[] { TLValueType.STRING,
                    TLValueType.INTEGER }, TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.getValue();
            strBuf.setLength(0);
            
            int length;
            if (params[0]==TLNullValue.getInstance() || params[1]==TLNullValue.getInstance()) {
                throw new TransformLangExecutorRuntimeException(params,
                        "left - NULL value not allowed");
            }

            try {
                length = ((TLNumericValue)params[1]).getInt();
            } catch (Exception ex) {
                throw new TransformLangExecutorRuntimeException(params,
                        "left - " + ex.getMessage());
            }

            if (params[0].type != TLValueType.STRING) {
                throw new TransformLangExecutorRuntimeException(params,
                        "left - wrong type of literal(s)");
            }

            StringUtils.subString(strBuf, ((TLStringValue)params[0]).getCharSequence(), 0, length);
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
            super("string", "right", "Returns suffix of the specified length", 
            		new TLValueType[] { TLValueType.STRING,
                    TLValueType.INTEGER }, TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.getValue();
            strBuf.setLength(0);
            
            int length;
            if (params[0]==TLNullValue.getInstance() || params[1]==TLNullValue.getInstance()) {
                throw new TransformLangExecutorRuntimeException(params,
                        "right - NULL value not allowed");
            }

            try {
                length = ((TLNumericValue)params[1]).getInt();
            } catch (Exception ex) {
                throw new TransformLangExecutorRuntimeException(params,
                        "right - " + ex.getMessage());
            }

            if (params[0].type != TLValueType.STRING) {
                throw new TransformLangExecutorRuntimeException(params,
                        "right - wrong type of literal(s)");
            }

            CharSequence src = ((TLStringValue)params[0]).getCharSequence();
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
            super("string", "trim", "Trims leading and trailing spaces", 
            		new TLValueType[] { TLValueType.STRING },
                    TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.getValue();
            strBuf.setLength(0);
            
            if (params[0].type != TLValueType.STRING) {
                throw new TransformLangExecutorRuntimeException(params,
                        "trim - wrong type of literal");
            }
            strBuf.append(((TLStringValue)params[0]).getCharSequence());
            StringUtils.trim(strBuf);

            if (strBuf.length() == 0)
                return TLStringValue.EMPTY;

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
            super("string", "length", "Returns legth of string or number of elements for complex types", 
            		new TLValueType[] { TLValueType.OBJECT },
                    TLValueType.INTEGER);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            Numeric intBuf = ((TLNumericValue)val).getNumeric();
            
            switch(params[0].type){
            case STRING:
            	intBuf.setValue(((TLStringValue)params[0]).getCharSequence().length());
            	break;
            case LIST:
            case MAP:
            case BYTE:
            case RECORD:
            	intBuf.setValue(((TLContainerValue)params[0]).getLength());
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
            super("string", "soundex", "Calculates string index based on its sound",
                    new TLValueType[] { TLValueType.STRING },
                    TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder targetStrBuf = (StringBuilder)val.getValue();
            targetStrBuf.setLength(0);
            
            if (params[0].type != TLValueType.STRING) {
                throw new TransformLangExecutorRuntimeException(params,
                        "soundex - wrong type of literal");
            }

            CharSequence src = ((TLStringValue)params[0]).getCharSequence();
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
            super("string", "replace", "Replaces matches of a regular expression", 
            		new TLValueType[] {
                    TLValueType.STRING, TLValueType.STRING,
                    TLValueType.STRING }, TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            RegexStore regex = (RegexStore) context.getContext();
            if (regex.pattern==null){
            	regex.initRegex(params[1].toString(), ((TLStringValue)params[0]).getCharSequence(),new TLStringValue());
            	regex.strbuf=new StringBuffer();
            }else{
            	// can we reuse pattern/matcher
            	if(regex.storedRegex!=params[1].getValue() &&  Compare.compare(regex.storedRegex,((TLStringValue)params[1]).getCharSequence())!=0){
            		//we can't
            		regex.resetPattern(params[1].toString());
            	}
            	//            	 reset matcher
                regex.resetMatcher(((TLStringValue)params[0]).getCharSequence());
            }
            
            String replacement=params[2].toString();
            regex.strbuf.setLength(0);
            while ( regex.matcher.find()) {
                regex.matcher.appendReplacement(regex.strbuf, replacement);
            }
            regex.matcher.appendTail(regex.strbuf);
            regex.result.setValue(regex.strbuf);
            
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
            super("string", "split", "Splits the string around regular expression matches",new TLValueType[] {
                    TLValueType.STRING, TLValueType.STRING}, TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            RegexStore regex = (RegexStore) context.getContext();
            if (regex.pattern==null){
            	regex.initRegex(params[1].toString(), ((TLStringValue)params[0]).getCharSequence(),new TLListValue());
            }else{
            	// can we reuse pattern/matcher
            	if(regex.storedRegex!=params[1].getValue() &&  Compare.compare(regex.storedRegex,((TLStringValue)params[1]).getCharSequence())!=0){
            		//we can't
            		regex.resetPattern(params[1].toString());
            	}
            }
            
            String[] strArray=regex.pattern.split(((TLStringValue)params[0]).getCharSequence());
            List<TLValue> list=((TLListValue)regex.result).getList();
            list.clear();
            for(String item : strArray){
            	list.add(new TLStringValue(item));
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
            super("string", "char_at", "Returns character at the specified index of a string",
            		new TLValueType[] { TLValueType.STRING ,
                    TLValueType.INTEGER}, TLValueType.STRING);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
            StringBuilder strBuf = (StringBuilder)val.getValue();
            strBuf.setLength(0);
            
            if (params[1]==TLNullValue.getInstance()) {
                throw new TransformLangExecutorRuntimeException(params,
                        Function.CHAR_AT.name()+" - NULL value not allowed");
            }
            
            if (params[0]!=TLNullValue.getInstance()) {
                try {
                strBuf.append(((TLStringValue)params[0]).getCharSequence().charAt(((TLNumericValue)params[1]).getInt()));
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

    //  IS BLANK
    class IsBlankFunction extends TLFunctionPrototype {

        public IsBlankFunction() {
            super("string", "is_blank", "Checks if the string contains only whitespace characters",new TLValueType[] { TLValueType.STRING }, 
            		TLValueType.BOOLEAN);
        }

        @Override
        public TLValue execute(TLValue[] params, TLContext context) {
            
            if (params[0]==TLNullValue.getInstance()) {
            	return TLBooleanValue.TRUE;
            }else if (!(params[0].type == TLValueType.STRING)){
                throw new TransformLangExecutorRuntimeException(params,
                "is_blank - wrong type of literal");
            }else{
            	if (StringUtils.isBlank(((TLStringValue)params[0]).getCharSequence())) 
            		return TLBooleanValue.TRUE;
            	else 
            		return TLBooleanValue.FALSE;
            }
         
        }
        
    }

        //  IS ASCII
     class IsAsciiFunction extends TLFunctionPrototype {

            public IsAsciiFunction() {
                super("string", "is_ascii", "Checks if the string contains only characters from the US-ASCII encoding",
                		new TLValueType[] { TLValueType.STRING }, 
                		TLValueType.BOOLEAN);
            }

            @Override
            public TLValue execute(TLValue[] params, TLContext context) {
     
                if (params[0]==TLNullValue.getInstance() || !(params[0].type == TLValueType.STRING)){
                    throw new TransformLangExecutorRuntimeException(params,
                    "is_ascii - wrong type of literal");
                }else{
                    if (StringUtils.isAscii(((TLStringValue)params[0]).getCharSequence())) 
                    	return TLBooleanValue.TRUE;
                    else 
                    	return TLBooleanValue.FALSE;
                }
            }

    }
    
     //  IS NUMBER
     class IsNumberFunction extends TLFunctionPrototype {

            public IsNumberFunction() {
                super("string", "is_number", "Checks if the string can be parsed into a double number", 
                		new TLValueType[] { TLValueType.STRING }, 
                		TLValueType.BOOLEAN);
            }

            @Override
            public TLValue execute(TLValue[] params, TLContext context) {
     
                if (params[0]==TLNullValue.getInstance() || !(params[0].type == TLValueType.STRING)){
                    throw new TransformLangExecutorRuntimeException(params,
                    "is_number - wrong type of literal");
                }else{
                    if (StringUtils.isNumber(((TLStringValue)params[0]).getCharSequence())) 
                    	return TLBooleanValue.TRUE;
                    else 
                    	return TLBooleanValue.FALSE;
                }
            }

    }

     //  IS INTEGER
     class IsIntegerFunction extends TLFunctionPrototype {

            public IsIntegerFunction() {
                super("string", "is_integer", "Checks if the string can be parsed into an integer number", 
                		new TLValueType[] { TLValueType.STRING }, 
                		TLValueType.BOOLEAN);
            }

            @Override
            public TLValue execute(TLValue[] params, TLContext context) {
                if (params[0]==TLNullValue.getInstance() || !(params[0].type == TLValueType.STRING)){
                    throw new TransformLangExecutorRuntimeException(params,
                    "is_integer - wrong type of literal");
                }else{
                    int numberType = StringUtils.isInteger(((TLStringValue)params[0]).getCharSequence());
                    if (numberType == 0 || numberType == 1) 
                    	return TLBooleanValue.TRUE;
                    else 
                    	return TLBooleanValue.FALSE;
                }
                
           }

    }

     //  IS LONG
     class IsLongFunction extends TLFunctionPrototype {

            public IsLongFunction() {
                super("string", "is_long", "Checks if the string can be parsed into a long number", 
                		new TLValueType[] { TLValueType.STRING }, 
                		TLValueType.BOOLEAN);
            }

            @Override
            public TLValue execute(TLValue[] params, TLContext context) {
                if (params[0]==TLNullValue.getInstance() || !(params[0].type == TLValueType.STRING)){
                    throw new TransformLangExecutorRuntimeException(params,
                    "is_integer - wrong type of literal");
                }else{
                    int numberType = StringUtils.isInteger(((TLStringValue)params[0]).getCharSequence());
                     if (numberType > -1 && numberType < 3) 
                    	return TLBooleanValue.TRUE;
                    else 
                    	return TLBooleanValue.FALSE;
                }
                
           }

    }

     //  IS DATE
     class IsDateFunction extends TLFunctionPrototype {

            public IsDateFunction() {
                super("string", "is_date", "Checks if the string can be parsed into a date", 
                		new TLValueType[] { TLValueType.STRING, TLValueType.STRING, TLValueType.OBJECT, TLValueType.BOOLEAN }, 
                		TLValueType.BOOLEAN, 4, 2);
            }

            @Override
            public TLValue execute(TLValue[] params, TLContext context) {
      
                if (params[0]==TLNullValue.getInstance() || params[1]==TLNullValue.getInstance()) {
                    throw new TransformLangExecutorRuntimeException(params,
                            Function.IS_DATE.name()+" - NULL value not allowed");
                }
                if (!(params[0].type == TLValueType.STRING && params[1].type == TLValueType.STRING)){
                    throw new TransformLangExecutorRuntimeException(params,
                    "is_date - wrong type of literal");
                }
                if (params.length == 3 && !(params[2].type == TLValueType.STRING || params[2].type == TLValueType.BOOLEAN)){
                    throw new TransformLangExecutorRuntimeException(params,
                    "is_date - wrong type of literal");
               }
                if (params.length == 4 && !(params[3].type == TLValueType.BOOLEAN)){
                    throw new TransformLangExecutorRuntimeException(params,
                    "is_date - wrong type of literal");
                }
                DateFormatStore formatter = (DateFormatStore) context.getContext();
                String pattern = params[1].toString();
                String locale = params.length > 2 && params[2].type == TLValueType.STRING ? 
                		params[2].toString() : null;
                
        		/*
				 * When parsing in lenient mode the parser is allowed to
				 * skip part of input string or accept illegal values
				 * for days in month (@see java.util.Calendar.setLenient())
				 * 
				 * In lenient mode we additionally allow an empty string to be treated 
				 * as a valid date.
				 * 
				 * Therefore we will use strict validation
				 * (lenient=false) by default and additionally check that whole input 
				 * was consumed in parsing (parsePostion = input.length).
				 * Empty string will be treated as an illegal date.
				 */		
                boolean lenient = false;
                if (params.length == 3 && params[2].type == TLValueType.BOOLEAN){
                	lenient = ((TLBooleanValue)params[2]).getBoolean();
                }else if (params.length == 4) {
                	lenient = ((TLBooleanValue)params[3]).getBoolean();
                }
                
                // empty string handling in lenient mode
                final String input = params[0].toString();
                if (lenient) {
                	if (input.length() == 0) {
                		return TLBooleanValue.TRUE;
                	}
                }
                
                if (formatter.formatter == null){
                	formatter.init(locale, pattern);
                }else if (locale != null) {
                	formatter.reset(locale, pattern);
                }else{
                	formatter.resetPattern(pattern);
                }

                formatter.setLenient(lenient);
                formatter.formatter.parse(input, formatter.position);
                if (!lenient) {
                	// strict: valid input must be non-empty & exact match to pattern
                	return TLBooleanValue.getInstance(
                			input.length() > 0 
                			&&
                			formatter.position.getIndex() == input.length()
                	);
                } else {
                	// lenient: return true if something was parsed
                	return TLBooleanValue.getInstance(formatter.position.getIndex() != 0); 
                }
            }

            @Override
            public TLContext createContext() {
                TLContext<DateFormatStore> context = new TLContext<DateFormatStore>();
                context.setContext(new DateFormatStore());
                return context;
            }
            
    }

     //  REMOVE DIACRITIC
     class RemoveDiacriticFunction extends TLFunctionPrototype {

         public RemoveDiacriticFunction() {
             super("string", "remove_diacritic", "Removes diacritic characters",
            		 new TLValueType[] { TLValueType.STRING }, 
            		 TLValueType.STRING);
         }

         @Override
         public TLValue execute(TLValue[] params, TLContext context) {
             TLValue val = (TLValue)context.getContext();

             if (!(params[0].type == TLValueType.STRING)){
                 throw new TransformLangExecutorRuntimeException(params,
                 "remove_diacritic - wrong type of literal");
             }else{
                 val.setValue(StringUtils.removeDiacritic(params[0].toString()));
             }
             return val;
         }

         @Override
         public TLContext createContext() {
             return TLContext.createStringContext();
         }
     }

     //  REMOVE BLANK SPACE
     class RemoveBlankSpaceFunction extends TLFunctionPrototype {

         public RemoveBlankSpaceFunction() {
             super("string", "remove_blank_space", "Removes whitespace characters", new TLValueType[] { TLValueType.STRING }, 
            		 TLValueType.STRING);
         }

         @Override
         public TLValue execute(TLValue[] params, TLContext context) {
             TLValue val = (TLValue)context.getContext();

             if (!(params[0].type == TLValueType.STRING)){
                 throw new TransformLangExecutorRuntimeException(params,
                 "remove_blank_space - wrong type of literal");
             }else{
                 val.setValue(StringUtils.removeBlankSpace(params[0].toString()));
             }
             return val;
         }

         @Override
         public TLContext createContext() {
             return TLContext.createStringContext();
         }
     }

     //  REMOVE NONPRINTABLE CHARS
     class RemoveNonPrintableFunction extends TLFunctionPrototype {

         public RemoveNonPrintableFunction() {
             super("string", "remove_nonprintable", "Removes nonprintable characters", new TLValueType[] { TLValueType.STRING }, 
            		 TLValueType.STRING);
         }

         @Override
         public TLValue execute(TLValue[] params, TLContext context) {
             TLValue val = (TLValue)context.getContext();

             if (!(params[0].type == TLValueType.STRING)){
                 throw new TransformLangExecutorRuntimeException(params,
                 "remove_nonprintable - wrong type of literal");
             }else{
                 val.setValue(StringUtils.removeNonPrintable(params[0].toString()));
             }
             return val;
         }

         @Override
         public TLContext createContext() {
             return TLContext.createStringContext();
         }
     }

     //  REMOVE NONASCII CHARS
     class RemoveNonAsciiFunction extends TLFunctionPrototype {

         public RemoveNonAsciiFunction() {
             super("string", "remove_nonascii", "Removes nonascii characters", new TLValueType[] { TLValueType.STRING }, 
            		 TLValueType.STRING);
         }

         @Override
         public TLValue execute(TLValue[] params, TLContext context) {
             TLValue val = (TLValue)context.getContext();

             if (!(params[0].type == TLValueType.STRING)){
                 throw new TransformLangExecutorRuntimeException(params,
                 "remove_nonascii - wrong type of literal");
             }else{
                 val.setValue(StringUtils.removeNonAscii(params[0].toString()));
             }
             return val;
         }

         @Override
         public TLContext createContext() {
             return TLContext.createStringContext();
         }
     }

     //  GET ALPHANUMERIC CHARS
     class GetAlphanumericCharsFunction extends TLFunctionPrototype {

         public GetAlphanumericCharsFunction() {
             super("string", "get_alphanumeric_chars", "Removes characters which are not alphanumeric", 
            		 new TLValueType[] { TLValueType.STRING,
            		 TLValueType.BOOLEAN, TLValueType.BOOLEAN}, TLValueType.STRING, 3, 1);
         }

         @Override
         public TLValue execute(TLValue[] params, TLContext context) {
             TLValue val = (TLValue)context.getContext();

             if (!(params[0].type == TLValueType.STRING)){
                 throw new TransformLangExecutorRuntimeException(params,
                 "get_alphanumeric_chars - wrong type of literal(s)");
             }else{
            	 if (params.length > 1) 
            		 if (!(params[1].type == TLValueType.BOOLEAN && 
            				 params[2].type == TLValueType.BOOLEAN)) 
            			 throw new TransformLangExecutorRuntimeException(params,
                                 "get_alphanumeric_chars - wrong type of literal(s)");
            	 boolean alpha = params.length > 1 ? params[1]==TLBooleanValue.TRUE : true;
            	 boolean numeric = params.length > 1 ? params[2]==TLBooleanValue.TRUE : true;
                 val.setValue(StringUtils.getOnlyAlphaNumericChars(params[0].toString(), alpha, numeric));
             }
             return val;
         }

         @Override
         public TLContext createContext() {
             return TLContext.createStringContext();
         }
     }

     //  TRANSLATE
     class TranslateFunction extends TLFunctionPrototype {

         public TranslateFunction() {
             super("string", "translate", "Replaces single characters according to a pattern",
            		 new TLValueType[] { TLValueType.STRING, 
            		 TLValueType.STRING, TLValueType.STRING}, TLValueType.STRING);
         }

         @Override
         public TLValue execute(TLValue[] params, TLContext context) {
             TLValue val = (TLValue)context.getContext();

             if (params[0]==TLNullValue.getInstance() || params[1]==TLNullValue.getInstance() || 
            		 params[0].type != TLValueType.STRING || params[1].type != TLValueType.STRING || ( params[2].type != TLValueType.STRING && params[2]!=TLNullValue.getInstance())) {
                 throw new TransformLangExecutorRuntimeException(params,
                         "translate - wrong type of literal(s)");
             }else{
                 val.setValue(StringUtils.translateSequentialSearch(((TLStringValue)params[0]).getCharSequence(), 
                		 ((TLStringValue)params[1]).getCharSequence(), params[2]!=TLNullValue.getInstance() ?  ((TLStringValue)params[2]).getCharSequence() : "" ));
             }
             return val;
         }

         @Override
         public TLContext createContext() {
             return TLContext.createStringContext();
         }
     }

     // JOIN
     class JoinFunction extends TLFunctionPrototype {
    	 
    	 Collection list;
    	 CharSequence delimiter;

         public JoinFunction() {
             super("string", "join", "Joins the parameters and separates them by a delimiter",
            		 new TLValueType[] { TLValueType.STRING },
                     TLValueType.STRING,999,3);
         }

         @Override
         public TLValue execute(TLValue[] params, TLContext context) {
             TLValue val = (TLValue)context.getContext();
             StringBuilder strBuf = (StringBuilder)val.getValue();
             strBuf.setLength(0);
             
             delimiter = params[0]==TLNullValue.getInstance() ? "" : ((TLStringValue)params[0]).getCharSequence();
             for (int i = 1; i < params.length; i++) {
                 if (params[i]!=TLNullValue.getInstance()) {
                     if (params[i].type == TLValueType.STRING) {
                         StringUtils.strBuffAppend(strBuf, ((TLStringValue)params[i])
                                 .getCharSequence());
                     }else if (params[i].type == TLValueType.LIST || 
                    		 params[i].type == TLValueType.MAP){
                    	 list = params[i].type == TLValueType.LIST ? ((TLListValue)params[i]).getList() : 
                    		 ((TLMapValue)params[i]).getMap().entrySet();
                    	 for (Iterator iter = list.iterator(); iter.hasNext();) {
							strBuf.append(iter.next());
							strBuf.append(delimiter);
						}
                         if (strBuf.length() > 0) {
                        	 strBuf.setLength(strBuf.length() - delimiter.length());
                         }
                     }else {
                         StringUtils.strBuffAppend(strBuf, params[i].toString());
                     }
						strBuf.append(delimiter);
                 } 
             }
             return val;
         }

         @Override
         public TLContext createContext() {
             return TLContext.createStringContext();
         }
     }
     
     //  INDEX OF
     class IndexOfFunction extends TLFunctionPrototype {

         public IndexOfFunction() {
             super("string", "index_of", "Finds the first occurence of a specified string", 
            		 new TLValueType[] { TLValueType.STRING, 
            		 TLValueType.STRING, TLValueType.INTEGER}, TLValueType.INTEGER, 3, 2);
         }

         @Override
         public TLValue execute(TLValue[] params, TLContext context) {
             TLValue val = (TLValue)context.getContext();
             Numeric result = (TLNumericValue)val;

             if (params[0]==TLNullValue.getInstance() || params[0].type != TLValueType.STRING || 
            	 params[1]==TLNullValue.getInstance() || params[1].type != TLValueType.STRING ||
            	 (params.length == 3 && params[2].type != TLValueType.INTEGER)) {
                 throw new TransformLangExecutorRuntimeException(params,
                 "index_of - wrong type of literal(s)");
             }else{
            	 int fromIndex = params.length < 3 ? 0 : ((TLNumericValue)params[2]).getInt();
            	 CharSequence pattern = ((TLStringValue)params[1]).getCharSequence();
				if (pattern.length() > 1) {
					result.setValue(StringUtils.indexOf(((TLStringValue)params[0]).getCharSequence(), 
							pattern, fromIndex));
				}else{
					result.setValue(StringUtils.indexOf(((TLStringValue)params[0]).getCharSequence(), 
							pattern.charAt(0), fromIndex));
				}
             }
             return val;
         }

         @Override
         public TLContext createContext() {
             return TLContext.createIntegerContext();
         }
     }


     // COUNT_CHAR
     //int count_char(string search_string, char what)
     //vrati pocet vyskytu "what" znaku v search_string
     class CountCharFunction extends TLFunctionPrototype {

		public CountCharFunction() {
			super("string", "count_char", "Calculates the number of occurences of the specified character",
					new TLValueType[] { TLValueType.STRING,
					TLValueType.STRING },
					TLValueType.INTEGER);
		}

		@Override
		public TLValue execute(TLValue[] params, TLContext context) {
			TLValue val = (TLValue) context.getContext();
			Numeric result = (TLNumericValue) val;

			if (params[0].type != TLValueType.STRING || params[1].type !=TLValueType.STRING) {
				throw new TransformLangExecutorRuntimeException(params,
						"count_char - wrong type of literal(s)");
			} else {
					result.setValue(StringUtils.count((CharSequence)params[0], ((CharSequence)params[1]).charAt(0)));
			}
			return val;
		}

		@Override
		public TLContext createContext() {
			return TLContext.createIntegerContext();
		}
	}

    class FindFunction extends TLFunctionPrototype {

         public FindFunction() {
             super("string", "find", "Finds and returns all occurences of regex in specified string",new TLValueType[] {
                     TLValueType.STRING, TLValueType.STRING}, TLValueType.LIST);
         }

         @Override
         public TLValue execute(TLValue[] params, TLContext context) {
             RegexStore regex = (RegexStore) context.getContext();
             if (regex.pattern==null){
             	regex.initRegex(params[1].toString(), ((TLStringValue)params[0]).getCharSequence(),new TLListValue());
             }else{
             	// can we reuse pattern/matcher
             	if(regex.storedRegex!=params[1].getValue() &&  Compare.compare(regex.storedRegex,((TLStringValue)params[1]).getCharSequence())!=0){
             		//we can't
             		regex.resetPattern(params[1].toString());
             	}
             }
             regex.resetMatcher(((TLStringValue)params[0]).getCharSequence());
             List<TLValue> list=((TLListValue)regex.result).getList();
             list.clear();
             // 1st occurence
             while (regex.matcher.find()){
            	 list.add(new TLStringValue(regex.matcher.group()));
            	 int i=0;
            	 while(i<regex.matcher.groupCount()){
            		 list.add(new TLStringValue(regex.matcher.group(++i)));
            	 }
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


     //CHOP
     class ChopFunction extends TLFunctionPrototype {
    	 
 		public ChopFunction() {
 			super("string", "chop", "Chops up the string from the new line separators or from given string",
 					new TLValueType[] { TLValueType.STRING,
 					TLValueType.STRING },
 					TLValueType.STRING, 2, 1);
 		}

 		@Override
 		public TLValue execute(TLValue[] params, TLContext context) {
            TLValue val = (TLValue)context.getContext();
 			if (params[0].type != TLValueType.STRING || 
 					(params.length == 2 && params[1].type !=TLValueType.STRING)) {
 				throw new TransformLangExecutorRuntimeException(params,
 						"chop - wrong type of literal(s)");
 			} 
 			
 			
			StringBuilder input = (StringBuilder)params[0].getValue();
			StringBuilder output = (StringBuilder)val.getValue();
			// erase the output value - this will also handle empty strings
			output.setLength(0); 

			// nothing to do for empty strings
			if (input.length() != 0) {

				// second parameter is pattern to chop from the end of the string
 				if (params.length > 1) {
 					output.append(input);
					StringBuilder pattern = (StringBuilder) params[1].getValue();
					int endIndex = input.length();
					int startIndex = endIndex - pattern.length();
					
					if (startIndex < 0) {
						// pattern is longer than our input -> no match
						return val;
					}
					
					if (input.substring(startIndex, endIndex).equals(pattern.toString())) {
						output.setLength(startIndex);
					}
				} else {
					int index = input.length()-1; // let's examine input from the end
					boolean done = false;
					while (index >= 0 && ! done) {
						switch (input.charAt(index)) {
						case '\r':
						case '\n':
							index--;
							continue;
						default:
							done = true;
							break;
						}
					}
					
					// index now points at the first non-newline character
					output.append(input.substring(0, index+1));
				}
 			}
 			return val;
 		}

 		@Override
 		public TLContext createContext() {
              return TLContext.createStringContext();
 		}
 	}

    
     class CutFunction extends TLFunctionPrototype {

         public CutFunction() {
             super("string", "cut", "Cuts substring from specified string based on list consisting of pairs position,length",new TLValueType[] {
                     TLValueType.STRING, TLValueType.LIST}, TLValueType.LIST);
         }

         @Override
         public TLValue execute(TLValue[] params, TLContext context) {
 			 TLListValue store=(TLListValue) context.getContext();
        	 List<TLValue> list = store.getList();
 			 if (params[0].type!=TLValueType.STRING || params[1].type!=TLValueType.LIST){
 				throw new TransformLangExecutorRuntimeException(params,
				"cut - wrong type of literal(s)");
	
 			 }
 			 List<TLValue> sourceList=(List<TLValue>)params[1].getValue();
 			 if (sourceList.size()%2!=0){
 				throw new TransformLangExecutorRuntimeException(params,
 						"cut - wrong length of intevals list");
 						
 			 }
 			 list.clear();
 			 CharSequence src=(TLStringValue)params[0];
 			 for(int i=0;i<sourceList.size();){
 				 int pos, length;
 				 try{
 					 pos=sourceList.get(i++).getNumeric().getInt();
 					 length=sourceList.get(i++).getNumeric().getInt();
 				 }catch(UnsupportedOperationException ex){
 					 throw new TransformLangExecutorRuntimeException("cut - wrong position or length definition at index "+i);
 				 }
 				 list.add(new TLStringValue(src.subSequence(pos, pos+length)));
 			 }
 			return store;

         }

         @Override
         public TLContext createContext() {
             return TLContext.createListContext();
         }
     }

     class EditDistanceFunction extends TLFunctionPrototype {

         public EditDistanceFunction() {
             super("string", "edit_distance", "Calculates edit distance between two strings",new TLValueType[] {
                     TLValueType.STRING, TLValueType.STRING, TLValueType.INTEGER, TLValueType.STRING, TLValueType.INTEGER}, 
                     TLValueType.INTEGER, 5, 2);
         }

         @Override
         public TLValue execute(TLValue[] params, TLContext context) {
 			 if (params[0].type!=TLValueType.STRING || params[1].type!=TLValueType.STRING){
 				throw new TransformLangExecutorRuntimeException(params,
				"edit_distance - wrong type of literal(s)");
	
 			 }
 			 ComparatorStore store = (ComparatorStore) context.getContext();
			 int strength = StringAproxComparator.IDENTICAL;
 			 String locale = null;
 			 int maxLetters = -1;
 			 if (params.length > 2) {
 				 if (params[2].type.isNumeric()) {
 					 strength = params[2].getNumeric().getInt();
 				 }else if (params[2].type == TLValueType.STRING) {
 					 locale = params[2].toString();
 				 }else {
 	 				throw new TransformLangExecutorRuntimeException(params,
 					"edit_distance - wrong type of literal(s)");
 				 }
 				 if (params.length > 3) {
 					 if (params[3].type == TLValueType.STRING) {
 						 locale = params[3].toString();
 					 }else if (params[3].type.isNumeric()) {
 						 maxLetters = params[3].getNumeric().getInt();
 				 	}else {
 		 				throw new TransformLangExecutorRuntimeException(params,
 						"edit_distance - wrong type of literal(s)");
 					 }
 				 }
 			 }
 			 if (strength != store.strength || locale != store.locale) {
				try {
					store.init(strength, locale);
				} catch (JetelException e) {
					throw new TransformLangExecutorRuntimeException(e
							.getMessage());
				}
			}
			if (maxLetters > -1 || params.length > 4) {
				if (maxLetters > -1 || params[4].type.isNumeric()) {
					store.comparator.setMaxLettersToChange(maxLetters > -1 ? maxLetters : params[4].getNumeric().getInt());
				}else {
	 				throw new TransformLangExecutorRuntimeException(params,
	 						"edit_distance - wrong type of literal(s)");
	 			}
			}
			int compResult = store.comparator.distance(params[0].toString(), params[1].toString());
			//we need to normalize it
 			store.value.setValue(compResult/store.comparator.getMaxCostForOneLetter());
 			
 			return store.value;
         }

         @Override
         public TLContext createContext() {
             TLContext<ComparatorStore> context = new TLContext<ComparatorStore>();
             context.setContext(new ComparatorStore());
             return context;
         }
     }
     
     class MetaphoneFunction extends TLFunctionPrototype {

         public MetaphoneFunction() {
             super("string", "metaphone", "Finds the metaphone value of a String", 
                     new TLValueType[] { TLValueType.STRING , TLValueType.INTEGER},
                     TLValueType.STRING, 2, 1);
         }

         @Override
         public TLValue execute(TLValue[] params, TLContext context) {
             TLValue val = (TLValue)context.getContext();
             StringBuilder strBuf = (StringBuilder)val.getValue();
             strBuf.setLength(0);

             if (params[0]!=TLNullValue.getInstance() && params[0].type == TLValueType.STRING) {
            	 if (params.length > 1) {
            		 if (params[1].getType().isNumeric()) {
            			 strBuf.append(StringUtils.metaphone(params[0].toString(), params[1].getNumeric().getInt()));
            		 }else{
                         throw new TransformLangExecutorRuntimeException(params,
                         "metaphone - wrong type of literal");
            		 }
            	 }else{
            		 strBuf.append(StringUtils.metaphone(params[0].toString()));
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

     class NYSIISFunction extends TLFunctionPrototype {

         public NYSIISFunction() {
             super("string", "NYSIIS", "Finds The New York State Identification and Intelligence System Phonetic Code", 
            		 new TLValueType[] { TLValueType.STRING }, 
            		 TLValueType.STRING);
         }

         @Override
         public TLValue execute(TLValue[] params, TLContext context) {
             TLValue val = (TLValue)context.getContext();

             if (!(params[0].type == TLValueType.STRING)){
                 throw new TransformLangExecutorRuntimeException(params,
                 "NYSIIS - wrong type of literal");
             }else{
                 val.setValue(StringUtils.NYSIIS(params[0].toString()));
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
	    public StringBuffer strbuf;
	    
	    public void initRegex(String regex,CharSequence input,TLValue value){
        	pattern=Pattern.compile(regex);
        	matcher=pattern.matcher(input);
        	storedRegex=regex;
        	result=value;
        	strbuf=null;
        }
	    
	    public void resetPattern(String regex){
	    	pattern=Pattern.compile(regex);
	    	matcher.usePattern(pattern);
	    }
	    
	    public void resetMatcher(CharSequence input){
	    	matcher.reset(input);
	    }
	}

     class DateFormatStore{
    	 
    	 SimpleDateFormat formatter;
    	 ParsePosition position;
    	 String locale;
    	 
    	 public void init(String locale, String pattern){
    		 formatter = (SimpleDateFormat)MiscUtils.createFormatter(DataFieldMetadata.DATE_FIELD, 
    				 locale, pattern);
    		 this.locale = locale;
    		 position = new ParsePosition(0);
    	 }
    	 
    	 public void reset(String newLocale, String newPattern) {
			if (!newLocale.equals(locale)) {
				formatter = (SimpleDateFormat) MiscUtils.createFormatter(
						DataFieldMetadata.DATE_FIELD, newLocale, newPattern);
				this.locale = newLocale;
			}
			resetPattern(newPattern);
		}
    	 
    	 public void resetPattern(String newPattern) {
			if (!newPattern.equals(formatter.toPattern())) {
				formatter.applyPattern(newPattern);
			}
			position.setIndex(0);
		}
    	 
    	 public void setLenient(boolean lenient){
    		 formatter.setLenient(lenient);
    	 }
     }
     
     class ComparatorStore {
    	 
    	 StringAproxComparator comparator;
		 int strength;
		 String locale;
    	 TLValue value = TLValue.create(TLValueType.INTEGER);
		 
    	 public void init(int strength, String locale) throws JetelException {
    		 boolean[] s = new boolean[4];
    		 Arrays.fill(s, false);
    		 s[4 - strength] = true;
    		 comparator = StringAproxComparator.createComparator(locale, s);
    		 this.strength = strength;
    		 this.locale = locale;
    	 }
    	 
     }
}
