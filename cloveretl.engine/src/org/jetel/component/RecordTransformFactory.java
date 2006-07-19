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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CodeParser;
import org.jetel.util.DynamicJavaCode;

public class RecordTransformFactory {

    
    public static RecordTransform createTransform(String transform, String transformClass, Node node, DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata, Properties transformationParameters) throws ComponentNotReadyException {
        RecordTransform transformation = null;
        Log logger = LogFactory.getLog(node.getClass());
        
        //without these parameters we cannot create transformation
        if(transform == null && transformClass == null) {
            throw new ComponentNotReadyException("Record transformation is not defined.");
        }
        
        if (transformClass != null) {
            //get transformation from link to the compiled class
            transformation = RecordTransformFactory.loadClass(logger, transformClass);
        } else {
            //try compile transform parameter as java code
            try {
                DynamicJavaCode dynaTransCode = new DynamicJavaCode(transform);
                transformation = RecordTransformFactory.loadClassDynamic(logger, dynaTransCode);
            } catch(Exception e) {
                logger.info("Transform parameter is not valid java code (Reason: " + e.getMessage() + ")");
                logger.info("Trying to parse transform parameter as internal clover script language.");
            }
         
            //trying to parse transform parameter as internal clover script language
            if(transformation == null) {
                if (transform.indexOf(RecordTransformTL.TL_TRANSFORM_CODE_ID) != -1){
                    transformation = new RecordTransformTL(logger, transform);
                } else {
                    transformation = RecordTransformFactory.loadClassDynamic(
                                        logger,
                                        "Transform" + node.getId(),
                                        transform,
                                        inMetadata,
                                        outMetadata);
                }
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
        return loadClass(logger, transformClass, null);
    }
    
    /**
     * @param logger
     * @param transformClassName
     * @param libraryPaths
     * @return
     * @throws ComponentNotReadyException
     */
    public static RecordTransform loadClass(Log logger,
            String transformClassName, String[] libraryPaths)
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
                    myURLs[i] = new URL(libraryPaths[i]);
                } catch (MalformedURLException ex1) {
                    try {
                        // probably missing protocol prefix, try to load it as a
                        // file
                        myURLs[i] = new File(libraryPaths[i]).toURI().toURL();
                    } catch (MalformedURLException ex2) {
                        throw new ComponentNotReadyException("Malformed URL: "
                                + ex1.getMessage());
                    }
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
            DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata)
            throws ComponentNotReadyException {
        DynamicJavaCode dynamicTransformCode;
        // creating dynamicTransformCode from internal transformation format
        CodeParser codeParser = new CodeParser(inMetadata, outMetadata);
        codeParser.setSourceCode(transformCode);
        codeParser.parse();
        codeParser.addTransformCodeStub("Transform" + className);

        dynamicTransformCode = new DynamicJavaCode(codeParser.getSourceCode());
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
        dynamicTransformCode.setCaptureCompilerOutput(true);
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
}
    