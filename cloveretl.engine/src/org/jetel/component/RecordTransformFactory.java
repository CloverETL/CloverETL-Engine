/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (C) 2002-06  David Pavlis <david.pavlis@centrum.cz> and others.
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
 * Created on 27.6.2006
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
package org.jetel.component;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.jetel.interpreter.ParseException;
import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.ASTnode.CLVFDirectMapping;
import org.jetel.interpreter.ASTnode.CLVFFunctionDeclaration;
import org.jetel.interpreter.ASTnode.CLVFStart;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CodeParser;
import org.jetel.util.compile.DynamicJavaCode;
import org.jetel.util.file.FileUtils;

public class RecordTransformFactory {

    
    public static final int TRANSFORM_JAVA_SOURCE=1;
    public static final int TRANSFORM_CLOVER_TL=2;
    public static final int TRANSFORM_JAVA_PREPROCESS=3;
    
    public static final Pattern PATTERN_CLASS = Pattern.compile("class\\s+\\w+"); 
    public static final Pattern PATTERN_TL_CODE = Pattern.compile("function\\s+transform");
    public static final Pattern PATTERN_DENORMALIZE_CODE = Pattern.compile("function\\s+getOutputRecord");
    public static final Pattern PATTERN_PARTITION_CODE = Pattern.compile("function\\s+getOutputPort"); 
    
    public static final Pattern PATTERN_PREPROCESS_1 = Pattern.compile("\\$\\{out\\."); 
    public static final Pattern PATTERN_PREPROCESS_2 = Pattern.compile("\\$\\{in\\."); 
    
    public static RecordTransform createTransform(String transform, String transformClass, 
    		String transformURL, String charset, Node node, 
    		DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata, 
    		Properties transformationParameters, ClassLoader classLoader) throws ComponentNotReadyException {
        RecordTransform transformation = null;
        Log logger = LogFactory.getLog(node.getClass());
        
        //without these parameters we cannot create transformation
        if(transform == null && transformClass == null && transformURL == null) {
            throw new ComponentNotReadyException("Record transformation is not defined.");
        }
        
        if (transformClass != null) {
            //get transformation from link to the compiled class
            transformation = RecordTransformFactory.loadClass(logger, transformClass);
        }else if (transform == null && transformURL != null){
        	transform = FileUtils.getStringFromURL(node.getGraph().getProjectURL(), transformURL, charset);
        }
        if (transformClass == null) {
            
            switch (guessTransformType(transform)) {
            case TRANSFORM_JAVA_SOURCE:
                // try compile transform parameter as java code
				// try preprocessing if applicable
                transformation = RecordTransformFactory.loadClassDynamic(
                        logger, null, transform, inMetadata, outMetadata, classLoader, false);
                break;
            case TRANSFORM_CLOVER_TL:
                transformation = new RecordTransformTL(transform, logger);
                break;
            case TRANSFORM_JAVA_PREPROCESS:
                transformation = RecordTransformFactory.loadClassDynamic(
                        logger, "Transform" + node.getId(), transform,
                        inMetadata, outMetadata, classLoader, true);
                break;
            default:
                // logger.error("Can't determine transformation code type at
                // component ID :"+node.getId());
                throw new ComponentNotReadyException(
                        "Can't determine transformation code type at component ID :" + node.getId());
            }
        }

        transformation.setGraph(node.getGraph());

        // init transformation
        if (!transformation.init(transformationParameters, inMetadata, outMetadata)) {
            throw new ComponentNotReadyException("Error when initializing tranformation function !");
        }
        
        return transformation;
    }
    
    
    public static RecordTransform loadClass(Log logger, String transformClass) throws ComponentNotReadyException {
        //TODO parsing url from transformClass parameter
        return loadClass(logger, transformClass, null, null);
    }
    
    /**
     * @param logger
     * @param transformClassName
     * @param libraryPaths
     * @return
     * @throws ComponentNotReadyException
     */
    public static RecordTransform loadClass(Log logger,
            String transformClassName, URL contextURL, String[] libraryPaths)
            throws ComponentNotReadyException {
        RecordTransform transformation = null;
        // try to load in transformation class & instantiate
        try {
            transformation =  (RecordTransform)Class.forName(transformClassName).newInstance();
        }catch(InstantiationException ex){
            throw new ComponentNotReadyException("Can't instantiate transformation class: "+ex.getMessage());
        }catch(IllegalAccessException ex){
            throw new ComponentNotReadyException("Can't instantiate transformation class: "+ex.getMessage());
        }catch (ClassNotFoundException ex) {
            // let's try to load in any additional .jar library (if specified)
            if (libraryPaths == null) {
                throw new ComponentNotReadyException(
                        "Can't find specified transformation class: "
                                + transformClassName);
            }
            URL[] myURLs = new URL[libraryPaths.length];
            // try to create URL directly, if failed probably the protocol is
            // missing, so use File.toURL
            for (int i = 0; i < libraryPaths.length; i++) {
                try {
                    // valid url
                    myURLs[i] = FileUtils.getFileURL(contextURL, libraryPaths[i]);
                } catch (MalformedURLException e) {
                    throw new ComponentNotReadyException("Malformed URL: " + e.getMessage());
                }
            }
            try {
                URLClassLoader classLoader = new URLClassLoader(myURLs, Thread
                        .currentThread().getContextClassLoader());
                transformation = (RecordTransform) Class.forName(
                        transformClassName, true, classLoader).newInstance();
            } catch (ClassNotFoundException ex1) {
                throw new ComponentNotReadyException("Can not find class: "
                        + ex1);
            } catch (Exception ex3) {
                throw new ComponentNotReadyException(ex3.getMessage());
            }
        }
        return transformation;
    }

    /**
     * @param logger
     * @param className
     * @param transformCode
     * @param inMetadata
     * @param outMetadata
     * @return
     * @throws ComponentNotReadyException
     */
    public static RecordTransform loadClassDynamic(Log logger,
            String className, String transformCode,
            DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata, ClassLoader classLoader,
            boolean addTransformCodeStub)
            throws ComponentNotReadyException {
        DynamicJavaCode dynamicTransformCode;
        // creating dynamicTransformCode from internal transformation format
        CodeParser codeParser = new CodeParser(inMetadata, outMetadata);
        codeParser.setSourceCode(transformCode);
        codeParser.parse();
        if (addTransformCodeStub)
        	codeParser.addTransformCodeStub("Transform" + className);

        dynamicTransformCode = new DynamicJavaCode(codeParser.getSourceCode(), classLoader);
        return loadClassDynamic(logger,dynamicTransformCode);
    }
    
    /**
     * @param logger
     * @param dynamicTransformCode
     * @return
     * @throws ComponentNotReadyException
     */
    public static RecordTransform loadClassDynamic(Log logger,DynamicJavaCode dynamicTransformCode)
            throws ComponentNotReadyException {
        logger.info(" (compiling dynamic source) ");
        // use DynamicJavaCode to instantiate transformation class
        Object transObject = null;
        try {
            transObject = dynamicTransformCode.instantiate();
        } catch (RuntimeException ex) {
            logger.debug(dynamicTransformCode.getCompilerOutput());
            logger.debug(dynamicTransformCode.getSourceCode());
            throw new ComponentNotReadyException(
                    "Transformation code is not compilable.\n" + "reason: "
                            + ex.getMessage());
        }
        if (transObject instanceof RecordTransform) {
            return (RecordTransform) transObject;
        } else {
            throw new ComponentNotReadyException(
                    "Provided transformation class doesn't implement RecordTransform.");
        }

    }
    
    /**
     * Guesses type of transformation code based on
     * code itself - looks for certain patterns within the code
     * 
     * @param transform
     * @return  guessed transformation type or -1 if can't determine
     */
    public static int guessTransformType(String transform){
      
        if (transform.indexOf(WrapperTL.TL_TRANSFORM_CODE_ID) != -1){
            // clover internal transformation language
            return TRANSFORM_CLOVER_TL;
        }
        if (PATTERN_TL_CODE.matcher(transform).find() 
        		|| PATTERN_DENORMALIZE_CODE.matcher(transform).find()
        		|| PATTERN_PARTITION_CODE.matcher(transform).find()){
            // clover internal transformation language
            return TRANSFORM_CLOVER_TL;
        }
        
        if (PATTERN_CLASS.matcher(transform).find()){
            // full java source code
            return TRANSFORM_JAVA_SOURCE;
        }
        if (PATTERN_PREPROCESS_1.matcher(transform).find() || 
                PATTERN_PREPROCESS_2.matcher(transform).find() ){
            // semi-java code which has to be preprocessed
            return TRANSFORM_JAVA_PREPROCESS;
        }
        
        return -1;
    }
    
    private static boolean isSimpleTransformNode(CLVFStart record) {
    	
        int numTopChildren = record.jjtGetNumChildren();
        
        /* Detection of simple mapping format */
        if (record.jjtHasChildren()) {
    		for (int i = 0; i < numTopChildren; i++) {
    			org.jetel.interpreter.ASTnode.Node node = record.jjtGetChild(i);
    			if (i < numTopChildren - 1) {
    				if (!(node instanceof org.jetel.interpreter.ASTnode.CLVFVarDeclaration)) {
//    					System.out.println("Non var-declaration encountered.");
    					return false;
    				}
    			} else {
    				if (!(node instanceof org.jetel.interpreter.ASTnode.CLVFFunctionDeclaration)) {
//    					System.out.println("Non function-declaration encountered.");
    					return false;
    				}
    				if (!((CLVFFunctionDeclaration)node).name.equals("transform")) {
//    					System.out.println("The single function must be named 'trasnform'");
    					return false;
    				}
    				CLVFFunctionDeclaration transFunction = (CLVFFunctionDeclaration)node;
    				int numTransChildren = transFunction.jjtGetNumChildren();
    				for (int j = 0; j < numTransChildren; j++) {
    					org.jetel.interpreter.ASTnode.Node fNode = transFunction.jjtGetChild(j);
    					if (!(fNode instanceof CLVFDirectMapping)) {
//    						System.out.println("Function transform() must contain direct mappings only");
    						return false;
    					}
    				}
    			}
    		}
    	}
        return true;
    }
    
    public static boolean isSimpleTransform(DataRecordMetadata[] inMeta,
    		DataRecordMetadata[] outMeta, String transform) {
    	
    	TransformLangParser parser = new TransformLangParser(inMeta, outMeta, transform);
    	CLVFStart record = null;
    	
        try {
            record = parser.Start();
            record.init();
        } catch (ParseException e) {
            System.out.println("Error when parsing expression: " + e.getMessage().split(System.getProperty("line.separator"))[0]);
            return false;
        }
    	return isSimpleTransformNode(record);
    }
    
}
    