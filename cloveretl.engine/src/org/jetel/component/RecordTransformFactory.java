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

import static org.jetel.ctl.TransformLangParserTreeConstants.JJTASSIGNMENT;
import static org.jetel.ctl.TransformLangParserTreeConstants.JJTFIELDACCESSEXPRESSION;
import static org.jetel.ctl.TransformLangParserTreeConstants.JJTFUNCTIONDECLARATION;
import static org.jetel.ctl.TransformLangParserTreeConstants.JJTIMPORTSOURCE;
import static org.jetel.ctl.TransformLangParserTreeConstants.JJTRETURNSTATEMENT;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.jetel.component.TransformLanguageDetector.TransformLanguage;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.ITLCompiler;
import org.jetel.ctl.MetadataErrorDetail;
import org.jetel.ctl.NavigatingVisitor;
import org.jetel.ctl.TLCompiler;
import org.jetel.ctl.TLCompilerFactory;
import org.jetel.ctl.TLUtils;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.ASTnode.CLVFBlock;
import org.jetel.ctl.ASTnode.CLVFFieldAccessExpression;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.MissingFieldException;
import org.jetel.graph.ContextProvider;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.CloverClassPath;
import org.jetel.interpreter.ParseException;
import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.ASTnode.CLVFDirectMapping;
import org.jetel.interpreter.ASTnode.CLVFFunctionDeclaration;
import org.jetel.interpreter.ASTnode.CLVFStart;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.plugin.PluginDescriptor;
import org.jetel.util.CodeParser;
import org.jetel.util.classloader.MultiParentClassLoader;
import org.jetel.util.compile.ClassLoaderUtils;
import org.jetel.util.compile.DynamicCompiler;
import org.jetel.util.compile.DynamicJavaClass;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.CommentsProcessor;

/**
 * @deprecated use {@link TransformFactory} instead
 * @deprecated use {@link TransformLanguageDetector} instead
 * @deprecated use {@link TLUtils} instead
 * @deprecated use {@link ClassLoaderUtils} instead
 */
@Deprecated
public class RecordTransformFactory {

    
	/**
	 * @deprecated use {@link TransformLanguage#JAVA} instead
	 */
	@Deprecated
    public static final int TRANSFORM_JAVA_SOURCE=1;
	/**
	 * @deprecated use {@link TransformLanguage#CTL1} instead
	 */
	@Deprecated
    public static final int TRANSFORM_CLOVER_TL=2;
	/**
	 * @deprecated use {@link TransformLanguage#JAVA_PREPROCESS} instead
	 */
	@Deprecated
    public static final int TRANSFORM_JAVA_PREPROCESS=3;
	/**
	 * @deprecated use {@link TransformLanguage#CTL2} instead
	 */
	@Deprecated
    public static final int TRANSFORM_CTL = 4;
    
	/**
	 * @deprecated use {@link TransformLanguageDetector#PATTERN_CLASS} instead
	 */
	@Deprecated
    public static final Pattern PATTERN_CLASS = Pattern.compile("class\\s+\\w+"); 
	/**
	 * @deprecated use {@link TransformLanguageDetector#PATTERN_TL_CODE} instead
	 */
	@Deprecated
    public static final Pattern PATTERN_TL_CODE = Pattern.compile("function\\s+((transform)|(generate))");
	/**
	 * @deprecated use {@link TransformLanguageDetector#PATTERN_CTL_CODE} instead
	 */
	@Deprecated
    public static final Pattern PATTERN_CTL_CODE = Pattern.compile("function\\s+[a-z]*\\s+((transform)|(generate))");
	/**
	 * @deprecated use {@link TransformLanguageDetector#PATTERN_PARTITION_CODE} instead
	 */
	@Deprecated
    public static final Pattern PATTERN_PARTITION_CODE = Pattern.compile("function\\s+getOutputPort"); 
	/**
	 * @deprecated use {@link TransformLanguageDetector#PATTERN_CTL_PARTITION_CODE} instead
	 */
	@Deprecated
    public static final Pattern PATTERN_CTL_PARTITION_CODE = Pattern.compile("function\\s+[a-z]*\\s+getOutputPort");
    
	/**
	 * @deprecated use {@link TransformLanguageDetector#PATTERN_PREPROCESS_1} instead
	 */
	@Deprecated
    public static final Pattern PATTERN_PREPROCESS_1 = Pattern.compile("\\$\\{out\\."); 
	/**
	 * @deprecated use {@link TransformLanguageDetector#PATTERN_PREPROCESS_2} instead
	 */
	@Deprecated
    public static final Pattern PATTERN_PREPROCESS_2 = Pattern.compile("\\$\\{in\\.");
    
    /**
     * Verifier class checking if a CTL function only contains direct mappings.
     * Direct mapping is an assignment statement where left hand side contains an output field reference.
     * 
     * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
     */
    private static class SimpleTransformVerifier extends NavigatingVisitor {
    	private final String functionName;
    	private final org.jetel.ctl.ASTnode.Node ast;
    	
    	/**
    	 * Allocates verifier which will verify that the <code>functionName</code>
    	 * contains only simple mappings
    	 * 
    	 * @param functionName	function to validate
    	 */
    	public SimpleTransformVerifier(String functionName, org.jetel.ctl.ASTnode.Node ast) {
    		this.functionName = functionName;
    		this.ast = ast;
    		
    	}
    	
    	/**
    	 * Scans AST tree for function and checks it only contains direct mappings
    	 * (i.e. assignments where LHS is an output field reference)
    	 * 
    	 * @return	true if function is simple, false otherwise
    	 */
    	public boolean check() {
    		return (Boolean)ast.jjtAccept(this, null);
    	}
    	
    	@Override
    	public Object visit(org.jetel.ctl.ASTnode.CLVFStart node, Object data) {
    		// functions can only be declared in start or import nodes
    		for (int i=0; i<node.jjtGetNumChildren(); i++) {
    			final SimpleNode child = (SimpleNode)node.jjtGetChild(i); 
    			final int id = child.getId();
    			switch (id) {
    			case JJTIMPORTSOURCE:
    				// scan imports
    				child.jjtAccept(this, data);
    				break;
    			case JJTFUNCTIONDECLARATION:
    				if (((org.jetel.ctl.ASTnode.CLVFFunctionDeclaration)child).getName().equals(functionName)) {
    					// scan statements in function body 
    					return child.jjtGetChild(2).jjtAccept(this, data);
    				}
    				break;
    			
    			}
    		}
    		
    		return false;
    	}

    	@Override
    	public Object visit(CLVFBlock node, Object data) {
    		// we must have come here as the block is 'transform' function body
    		for (int i=0; i<node.jjtGetNumChildren(); i++) {
    			final SimpleNode child = (SimpleNode)node.jjtGetChild(i);

    			// statement must be an assignment and a direct mapping into output field
    			if (child.getId() != JJTASSIGNMENT && child.getId() != JJTRETURNSTATEMENT) {
    				// not an assignment - fail quickly
    				return false;
    			}
    			
    			if (child.getId() != JJTRETURNSTATEMENT) {
	    			// check if direct mapping
	    			final SimpleNode lhs = (SimpleNode)child.jjtGetChild(0);
	    			if (lhs.getId() != JJTFIELDACCESSEXPRESSION) {
	    				// not a mapping
	    				return false;
	    			}
	    			if (!((CLVFFieldAccessExpression) lhs).isOutput()) {
	    				// lhs must be an output field
	    				return false;
	    			}
    			}
    		}
    		
    		// all statements are direct mappings
    		return true;
    	}
    }

	/**
	 * @deprecated use {@link TransformFactory#createTransform()} instead
	 */
	@Deprecated
    public static RecordTransform createTransform(String transform, String transformClass, String transformUrl,
    		String charset, Node node, DataRecordMetadata inMetaData[], DataRecordMetadata outMetadata[])
    	throws ComponentNotReadyException, MissingFieldException {
    	
    	return createTransform(transform, transformClass, transformUrl, charset, node, inMetaData, outMetadata,
    			ClassLoaderUtils.createNodeClassLoader(node), node.getGraph().getRuntimeContext().getClassPath());
    }

    /**
     * 
     * @param transform
     * @param transformClass
     * @param transformURL
     * @param charset
     * @param node
     * @param inMetadata
     * @param outMetadata
     * @param transformationParameters
     * @param classLoader
     * @param classPath 
     * @return
     * @throws MissingFieldException if the CTL transformation tried to access non-existing field
     * @throws ComponentNotReadyException
     */
    private static RecordTransform createTransform(String transform, String transformClass, String transformURL,
    		String charset, Node node, DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata,
    		ClassLoader classLoader, CloverClassPath classPath) throws ComponentNotReadyException, MissingFieldException {
    	
    	//if classpath wasn't passed, empty one is automatically prepared
    	if (classPath == null) {
    		classPath = new CloverClassPath();
    	}
    	
    	// create transformation
        RecordTransform transformation = null;
        Log log = LogFactory.getLog(node.getClass());
        Logger logger = Logger.getLogger(node.getClass());
        
        //without these parameters we cannot create transformation
        if (transform == null && transformClass == null && transformURL == null) {
            throw new ComponentNotReadyException("Record transformation is not defined.");
        }
        
        //resolve references at given transformation string 
        if (transform != null) {
        	PropertyRefResolver refResolver = node.getPropertyRefResolver();
        	transform = refResolver.resolveRef(transform, RefResFlag.SPEC_CHARACTERS_OFF);
        }
        
        if (transformClass != null) {
            //get transformation from link to the compiled class
            transformation = (RecordTransform)loadClassInstance(transformClass, classLoader);
        }else if (transform == null && transformURL != null){
        	transform = FileUtils.getStringFromURL(node.getGraph().getRuntimeContext().getContextURL(), transformURL, charset);
        	PropertyRefResolver refResolver = node.getPropertyRefResolver();
        	transform = refResolver.resolveRef(transform, RefResFlag.SPEC_CHARACTERS_OFF);
        }
        if (transformClass == null) {
            
            switch (guessTransformType(transform)) {
            case TRANSFORM_JAVA_SOURCE:
                // try compile transform parameter as java code
				// try preprocessing if applicable
                transformation = RecordTransformFactory.loadClassDynamic(
                        log, null, transform, inMetadata, outMetadata, node, false);
                break;
            case TRANSFORM_CLOVER_TL:
                transformation = new RecordTransformTL(transform, logger);
                break;
            case TRANSFORM_CTL:
            	final ITLCompiler compiler = 
            		TLCompilerFactory.createCompiler(node.getGraph(),inMetadata,outMetadata,"UTF-8");
            	List<ErrorMessage> msgs = compiler.compile(transform, CTLRecordTransform.class, node.getId());
            	if (compiler.errorCount() > 0) {
            		String report = ErrorMessage.listToString(msgs, logger);
            		String message = "CTL code compilation finished with " + compiler.errorCount() + " errors" + report;
            		for (ErrorMessage msg: msgs) {
            			if (msg.getDetail() instanceof MetadataErrorDetail) {
            				MetadataErrorDetail detail = (MetadataErrorDetail) msg.getDetail();
                			throw new MissingFieldException(message, detail.isOutput(), detail.getRecordId(), detail.getFieldName());
            			}
            		}
        			throw new ComponentNotReadyException(message);
            	}
            	Object ret = compiler.getCompiledCode();
            	if (ret instanceof TransformLangExecutor) {
            		// setup interpreted runtime
            		transformation = new CTLRecordTransformAdapter((TransformLangExecutor)ret, logger);
            	} else if (ret instanceof RecordTransform){
            		transformation = (RecordTransform)ret;
            	} else {
            		// this should never happen as compiler always generates correct interface
            		throw new ComponentNotReadyException("Invalid type of record transformation");
            	}
                break;
            case TRANSFORM_JAVA_PREPROCESS:
                transformation = RecordTransformFactory.loadClassDynamic(log,
                		"Transform" + node.getId(), transform, inMetadata, outMetadata, node, true);
                break;
            default:
                // logger.error("Can't determine transformation code type at
                // component ID :"+node.getId());
                throw new ComponentNotReadyException(
                        "Can't determine transformation code type at component ID :" + node.getId());
            }
        }
        transformation.setNode(node);
    	
        return transformation;
    }
    
    /**
     * Answers instance of given <tt>transformClassName</tt>.
     * 
     * @param transformClassName
     * @param loader  class loader to lookup class by name
     * @return
     * @throws ComponentNotReadyException
	 * @deprecated use {@link ClassLoaderUtils#loadClassInstance(String, ClassLoader)} instead
	 */
	@Deprecated
    public static Object loadClassInstance(String transformClassName, ClassLoader loader)
    	throws ComponentNotReadyException {
    	try {
    		Class<?> klass = Class.forName(transformClassName, true, loader);
    		return klass.newInstance();
    	} catch (ClassNotFoundException e) {
    		throw new ComponentNotReadyException("Cannot find class: " + transformClassName, e);
    	} catch (IllegalAccessException e) {
    		throw new ComponentNotReadyException("Cannot instantiate class: " + transformClassName, e);
    	} catch (InstantiationException e) {
    		throw new ComponentNotReadyException("Cannot instantiate class: " + transformClassName, e);
    	} catch (ExceptionInInitializerError e) {
    		throw new ComponentNotReadyException("Cannot initialize class: " + transformClassName, e);
    	} catch (LinkageError e) {
    		throw new ComponentNotReadyException("Cannot link class: " + transformClassName, e);
    	} catch (SecurityException e) {
    		throw new ComponentNotReadyException("Cannot instantiate class: " + transformClassName, e);
    	}
    }
    
	/**
	 * @deprecated use {@link ClassLoaderUtils#loadClassInstance(String, Node)} instead
	 */
	@Deprecated
    public static Object loadClassInstance(String transformClassName) throws ComponentNotReadyException {
    	
    	Node node = ContextProvider.getNode();
    	if (node == null) {
    		return loadClassInstance(transformClassName, Thread.currentThread().getContextClassLoader());
    	} else {
    		return loadClassInstance(transformClassName, node);
    	}
    }
    
    /**
     * Answers instance of class with given <tt>transformClassName</tt>. The supplied node
     * is used as a context to fetch its class loader and the runtime class path as well.
     * 
     * @param transformClassName
     * @param node
     * @return
     * @throws ComponentNotReadyException
	 * @deprecated use {@link ClassLoaderUtils#loadClassInstance(String, Node)} instead
	 */
	@Deprecated
    public static Object loadClassInstance(String transformClassName, Node node) throws ComponentNotReadyException {
    	return loadClassInstance(transformClassName, ClassLoaderUtils.createNodeClassLoader(node));
    }
    
	/**
	 * @deprecated use {@link ClassLoaderUtils#loadClassInstance(Class, String, Node)} instead
	 */
	@Deprecated
    public static <T> T loadClassInstance(String transformClassName, Class<T> expectedType, Node node)
    	throws ComponentNotReadyException {
    	
    	Object instance = loadClassInstance(transformClassName, node);
    	try {
    		return expectedType.cast(instance);
    	} catch (ClassCastException e) {
    		throw new ComponentNotReadyException(node, "Provided class '" + transformClassName + "' does not extend/implement " + expectedType.getName());
    	}
    }
    
    private static ClassLoader getCTLLibsClassLoader() {
    	
    	Set<ClassLoader> loaders = new LinkedHashSet<ClassLoader>();
    	for (PluginDescriptor plugin : DynamicCompiler.getCTLRelatedPlugins()) {
    		loaders.add(plugin.getClassLoader());
    	}
    	return new MultiParentClassLoader(loaders.toArray(new ClassLoader[loaders.size()]));
    }

    /**
     * @param logger
     * @param className
     * @param transformCode
     * @param inMetadata
     * @param outMetadata
     * @return
     * @throws ComponentNotReadyException
	 * @deprecated use {@link TransformFactory#createTransform()} instead
	 */
	@Deprecated
    public static RecordTransform loadClassDynamic(Log logger, String className, String transformCode,
            DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata, Node node, boolean addTransformCodeStub) throws ComponentNotReadyException {
        // creating dynamicTransformCode from internal transformation format
        CodeParser codeParser = new CodeParser(inMetadata, outMetadata);
        if (!addTransformCodeStub) 
        	// we must turn this off, because we don't have control about the rest of Java source
        	// and thus we cannot create the declaration of symbolic constants
        	codeParser.setUseSymbolicNames(false);
        codeParser.setSourceCode(transformCode);
        codeParser.parse();
        if (addTransformCodeStub)
        	codeParser.addTransformCodeStub("Transform" + className);

        return loadClassDynamic(codeParser.getSourceCode(), node);
    }
    
    /**
     * @param logger
     * @param dynamicTransformCode
     * @return
     * @throws ComponentNotReadyException
	 * @deprecated use {@link TransformFactory#createTransform()} instead
	 */
	@Deprecated
    public static RecordTransform loadClassDynamic(String sourceCode, Node node)
            throws ComponentNotReadyException {
    	
        return DynamicJavaClass.instantiate(sourceCode, RecordTransform.class, node);
    }
    
    private static Pattern getPattern(String hashBang) {
    	return Pattern.compile("^\\s*" + hashBang);
    }
    
    /**
     * Guesses type of transformation code based on
     * code itself - looks for certain patterns within the code
     * 
     * @param transform
     * @return  guessed transformation type or -1 if can't determine
	 * @deprecated use {@link TransformLanguageDetector#guessLanguage(String)} instead
	 */
	@Deprecated
    public static int guessTransformType(String transform){
    	
    	String commentsStripped = CommentsProcessor.stripComments(transform);
      
    	// First, try to identify the starting string
    	
    	if (getPattern(WrapperTL.TL_TRANSFORM_CODE_ID).matcher(transform).find() ||
    			getPattern(WrapperTL.TL_TRANSFORM_CODE_ID2).matcher(transform).find()) {
    		return TRANSFORM_CLOVER_TL;
        }
        
        if (getPattern(TransformLangExecutor.CTL_TRANSFORM_CODE_ID).matcher(transform).find()) {
        	// new CTL implementation
        	return TRANSFORM_CTL;
        }
        
        if (PATTERN_CTL_CODE.matcher(commentsStripped).find() 
        		|| PATTERN_CTL_PARTITION_CODE.matcher(commentsStripped).find()){
            // clover internal transformation language
            return TRANSFORM_CTL;
        }
        
        if (PATTERN_TL_CODE.matcher(commentsStripped).find() 
        		|| PATTERN_PARTITION_CODE.matcher(commentsStripped).find()){
            // clover internal transformation language
            return TRANSFORM_CLOVER_TL;
        }
        
        if (PATTERN_CLASS.matcher(commentsStripped).find()){
            // full java source code
            return TRANSFORM_JAVA_SOURCE;
        }
        if (PATTERN_PREPROCESS_1.matcher(commentsStripped).find() || 
                PATTERN_PREPROCESS_2.matcher(commentsStripped).find() ){
            // semi-java code which has to be preprocessed
            return TRANSFORM_JAVA_PREPROCESS;
        }
        
        return -1;
    }

	// Following old TL parser functions are now deprecated 
    @Deprecated
	private static boolean isTLSimpleTransformFunctionNode(CLVFStart record, String functionName, int functionParams) {
		int numTopChildren = record.jjtGetNumChildren();
		
		/* Detection of simple mapping inside transform(idx) function */
		if (record.jjtHasChildren()) {
			for (int i = 0; i < numTopChildren; i++) {
				org.jetel.interpreter.ASTnode.Node node = record.jjtGetChild(i);
				if (node instanceof org.jetel.interpreter.ASTnode.CLVFFunctionDeclaration) {
					if (((CLVFFunctionDeclaration)node).name.equals(functionName)) {
						CLVFFunctionDeclaration transFunction = (CLVFFunctionDeclaration)node;
						if (transFunction.numParams != functionParams) 
							return false;
						int numTransChildren = transFunction.jjtGetNumChildren();
	    				for (int j = 0; j < numTransChildren; j++) {
	    					org.jetel.interpreter.ASTnode.Node fNode = transFunction.jjtGetChild(j);
	    					if (!(fNode instanceof CLVFDirectMapping)) {
//	    						System.out.println("Function transform(idx) must contain direct mappings only");
	    						return false;
	    					}
	    				}
						return true;
					}
				}
			}		
		}  
		return false;
	}
    
    /**
	 * @deprecated use {@link TLUtils#isTLSimpleTransform(DataRecordMetadata[], DataRecordMetadata[], String)} instead
	 */
	@Deprecated
    public static boolean isTLSimpleTransform(DataRecordMetadata[] inMeta,
    		DataRecordMetadata[] outMeta, String transform) {
    	
    	return isTLSimpleFunction(inMeta, outMeta, transform, "transform");
    }
    
    /**
	 * @deprecated use {@link TLUtils#isTLSimpleFunction(DataRecordMetadata[], DataRecordMetadata[], String, String)} instead
	 */
	@Deprecated
    public static boolean isTLSimpleFunction(DataRecordMetadata[] inMeta,
    		DataRecordMetadata[] outMeta, String transform, String funtionName) {
    	
    	TransformLangParser parser = new TransformLangParser(inMeta, outMeta, transform);
    	CLVFStart record = null;
    	
        try {
            record = parser.Start();
            record.init();
        } catch (ParseException e) {
            System.out.println("Error when parsing expression: " + e.getMessage().split(System.getProperty("line.separator"))[0]);
            return false;
        }
    	return isTLSimpleTransformFunctionNode(record, funtionName, 0);
    }

    /**
	 * @deprecated use {@link TLUtils#isTLSimpleDenormalizer(DataRecordMetadata[], DataRecordMetadata[], String)} instead
	 */
	@Deprecated
    public static boolean isTLSimpleDenormalizer(DataRecordMetadata[] inMeta,
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
    	return isTLSimpleTransformFunctionNode(record, "transform", 0);
    }
    
    /**
	 * @deprecated use {@link TLUtils#isTLSimpleNormalizer(DataRecordMetadata[], DataRecordMetadata[], String)} instead
	 */
	@Deprecated
    public static boolean isTLSimpleNormalizer(DataRecordMetadata[] inMeta,
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
    	return isTLSimpleTransformFunctionNode(record, "transform", 1);
    }
   
    
    /**
	 * @deprecated use {@link TLUtils#isSimpleFunction(TransformationGraph, DataRecordMetadata[], DataRecordMetadata[], String, String)} instead
	 */
	@Deprecated
    public static boolean isSimpleFunction(TransformationGraph graph, DataRecordMetadata[] inMeta,
    		DataRecordMetadata[] outMeta, String code, String functionName) {
    	
    	TLCompiler compiler = new TLCompiler(graph,inMeta,outMeta);
    	List<ErrorMessage> msgs = compiler.validate(code);
    	if (compiler.errorCount() > 0) {
    		for (ErrorMessage msg : msgs) {
    			System.out.println(msg);
    		}
    		System.out.println("CTL code compilation finished with " + compiler.errorCount() + " errors");
    		return false;
    	}


    	final SimpleTransformVerifier verifier = new SimpleTransformVerifier(functionName,compiler.getStart());
    	return verifier.check();
    }
    
}
    
    