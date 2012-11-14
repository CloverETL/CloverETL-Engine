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
package org.jetel.component;

import java.util.regex.Pattern;

import org.jetel.ctl.TransformLangExecutor;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.string.CommentsProcessor;

/**
 * This utility class helps to detect type of given source code - CTL1, CTL2, JAVA and JAVA_PREPROCESS.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 3.9.2012
 */
public class TransformLanguageDetector {

	public static enum TransformLanguage {
		JAVA,
		JAVA_PREPROCESS,
		CTL1,
		CTL2;
	}
	
    public static final Pattern PATTERN_CLASS = Pattern.compile("class\\s+\\w+"); 
    public static final Pattern PATTERN_TL_CODE = Pattern.compile("function\\s+((transform)|(generate))");
    public static final Pattern PATTERN_CTL_CODE = Pattern.compile("function\\s+[a-z]*\\s+((transform)|(generate))");
    public static final Pattern PATTERN_PARTITION_CODE = Pattern.compile("function\\s+getOutputPort"); 
    public static final Pattern PATTERN_CTL_PARTITION_CODE = Pattern.compile("function\\s+[a-z]*\\s+getOutputPort");
    
    public static final Pattern PATTERN_PREPROCESS_1 = Pattern.compile("\\$\\{out\\."); 
    public static final Pattern PATTERN_PREPROCESS_2 = Pattern.compile("\\$\\{in\\.");

    /**
     * Guesses type of transformation code based on
     * code itself - looks for certain patterns within the code.
     */
    public static TransformLanguage guessLanguage(String transformationCode) {
    	String commentsStripped = CommentsProcessor.stripComments(transformationCode);
      
    	// First, try to identify the starting string
    	
    	if (getPattern(WrapperTL.TL_TRANSFORM_CODE_ID).matcher(transformationCode).find() ||
    			getPattern(WrapperTL.TL_TRANSFORM_CODE_ID2).matcher(transformationCode).find()) {
    		return TransformLanguage.CTL1;
        }
        
        if (getPattern(TransformLangExecutor.CTL_TRANSFORM_CODE_ID).matcher(transformationCode).find()) {
        	return TransformLanguage.CTL2;
        }
        
        if (PATTERN_CTL_CODE.matcher(commentsStripped).find() 
        		|| PATTERN_CTL_PARTITION_CODE.matcher(commentsStripped).find()) {
            return TransformLanguage.CTL2;
        }
        
        if (PATTERN_TL_CODE.matcher(commentsStripped).find() 
        		|| PATTERN_PARTITION_CODE.matcher(commentsStripped).find()) {
            return TransformLanguage.CTL1;
        }
        
        if (PATTERN_CLASS.matcher(commentsStripped).find()) {
            return TransformLanguage.JAVA;
        }
        if (PATTERN_PREPROCESS_1.matcher(commentsStripped).find() || 
                PATTERN_PREPROCESS_2.matcher(commentsStripped).find() ){
            // semi-java code which has to be preprocessed
            return TransformLanguage.JAVA_PREPROCESS;
        }
        
        return null;
    }

    private static Pattern getPattern(String hashBang) {
    	return Pattern.compile("^\\s*" + hashBang);
    }

}
