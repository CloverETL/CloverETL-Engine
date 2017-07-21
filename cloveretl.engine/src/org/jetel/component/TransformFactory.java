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

import java.util.List;

import org.apache.log4j.Logger;
import org.jetel.component.TransformLanguageDetector.TransformLanguage;
import org.jetel.ctl.CTLAbstractTransform;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.ITLCompiler;
import org.jetel.ctl.MetadataErrorDetail;
import org.jetel.ctl.TLCompilerFactory;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.data.Defaults;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.LoadClassException;
import org.jetel.exception.MissingFieldException;
import org.jetel.graph.Node;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CodeParser;
import org.jetel.util.compile.ClassLoaderUtils;
import org.jetel.util.compile.DynamicJavaClass;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;

/**
 * This class is used to instantiate a class based on source code.
 * Supported languages are CTL1, CTL2, java and pre-processed java.
 * Instance of this factory can be created only by two factory methods<br>
 * <li>
 * 		{@link #createTransformFactory(TransformDescriptor)} - creates factory based on given TransformDescriptor
 * (provides necessary information about instantiated interface)
 * </li>
 * <li>
 * 		{@link #createTransformFactory(Class)} - creates factory for the given class - class needs to have 
 * non-parametric constructor
 * </li>
 * Other necessary information about instantiated class (source code, inMetadata and outMetadata for CTL transformation, ...)
 * are provided by setters. Instance is created by {@link #createTransform()} method.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.8.2012
 */
public class TransformFactory<T> {

	/** Descriptor of instantiated transformation/class */
	private TransformDescriptor<T> transformDescriptor;
	/** Source code of transformation */
	private String transform;
	/** URL to source code of transformation */
	private String transformUrl;
	/** Class name of instantiated transformation */
	private String transformClass;
	/** Charset of source code file defined in transformUrl */
	private String charset;
	/** Component for which the transformation is instantiated */
	private Node component;
	/** Input metadata of transformation, used for CTL compilation */
	private DataRecordMetadata[] inMetadata;
	/** Output metadata of transformation, used for CTL compilation */
	private DataRecordMetadata[] outMetadata;
	
	private TransformFactory(TransformDescriptor<T> transformDescriptor) {
		this.transformDescriptor = transformDescriptor;
	}
	
	/**
	 * @param transformDescriptor
	 * @return {@link TransformFactory} for the given {@link TransformDescriptor}
	 */
	public static <T> TransformFactory<T> createTransformFactory(TransformDescriptor<T> transformDescriptor) {
		return new TransformFactory<T>(transformDescriptor);
	}

	/**
	 * {@link TransformFactory} returned by this method is limited and only java code implementation is supported.
	 * @param transformClass
	 * @return {@link TransformFactory} for the given class
	 */
	public static <T> TransformFactory<T> createTransformFactory(final Class<T> transformClass) {
		return new TransformFactory<T>(new TransformDescriptor<T>() {
			@Override
			public Class<T> getTransformClass() {
				return transformClass;
			}

			@Override
			public T createCTL1Transform(String transformCode, Logger logger) {
				throw new UnsupportedOperationException("CTL1 is not supported in '" + transformClass.getName() + "'.");
			}

			@Override
			public Class<? extends CTLAbstractTransform> getCompiledCTL2TransformClass() {
				throw new UnsupportedOperationException("CTL2 is not supported in '" + transformClass.getName() + "'.");
			}

			@Override
			public T createInterpretedCTL2Transform(TransformLangExecutor executor, Logger logger) {
				throw new UnsupportedOperationException("CTL2 is not supported in '" + transformClass.getName() + "'.");
			}
		});
	}

	private void validateSettings() {
        //without these parameters we cannot create transformation
        if (StringUtils.isEmpty(transform)
        		&& StringUtils.isEmpty(transformClass)
        		&& StringUtils.isEmpty(transformUrl)) {
        	throw new JetelRuntimeException("Transformation is not defined.");
        }
        if (component == null) {
        	throw new JetelRuntimeException("Component is not defined.");
        }
	}
	
	/**
	 * Configuration check, mainly invoked from {@link Node#checkConfig(ConfigurationStatus)}.
	 * Only CTL1 and CTL2 code is compiled to ensure correctness of all settings.
	 * Java code is not validated.
	 * @param status
	 * @return
	 */
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		try {
			validateSettings();
		} catch (Exception e) {
        	status.add(new ConfigurationProblem(e, Severity.ERROR, component, Priority.NORMAL, null));
		}
		
        String checkTransform = null;
        if (StringUtils.isEmpty(transformClass)) {
	        if (!StringUtils.isEmpty(transform)) {
	        	checkTransform = transform;
	        } else if (!StringUtils.isEmpty(transformUrl)) {
	        	if (charset == null) {
	        		charset = Defaults.DEFAULT_SOURCE_CODE_CHARSET;
	        	}
	        	checkTransform = FileUtils.getStringFromURL(component.getGraph().getRuntimeContext().getContextURL(), transformUrl, charset);
	        }
	        // only the transform and transformURL parameters are checked, transformClass is ignored
	        if (checkTransform != null) {
	        	TransformLanguage transformLanguage = TransformLanguageDetector.guessLanguage(checkTransform);
	        	if (transformLanguage == TransformLanguage.CTL1
	        			|| transformLanguage == TransformLanguage.CTL2) {
	        		// only CTL is checked
	        		
	    			try {
						createTransform();
					} catch (JetelRuntimeException e) {
						// report CTL error as a warning
						status.add(new ConfigurationProblem(e, Severity.WARNING, component, Priority.NORMAL, null));
					}
	        	}
	        }
        }

		return status;
	}

    /**
     * Core method of the factory.
     * @return instance of transformation class
     * @throws MissingFieldException if the CTL transformation tried to access non-existing field
     * @throws LoadClassException transformation cannot be instantiated
     */
    public T createTransform() {
		validateSettings();

        T transformation = null;
    	if (!StringUtils.isEmpty(transform)) {
    		//transform has highest priority
    		transformation = createTransformFromCode(transform);
    	} else if (!StringUtils.isEmpty(transformUrl)) {
    		//load transformation code from an URL
    		if (charset == null) {
        		charset = Defaults.DEFAULT_SOURCE_CODE_CHARSET;
        	}
        	String transformCode = FileUtils.getStringFromURL(component.getGraph().getRuntimeContext().getContextURL(), transformUrl, charset);
        	PropertyRefResolver refResolver = component.getPropertyRefResolver();
        	transformCode = refResolver.resolveRef(transformCode, RefResFlag.SPEC_CHARACTERS_OFF);
    		transformation = createTransformFromCode(transformCode);
    	} else if (!StringUtils.isEmpty(transformClass)) {
    		transformation = ClassLoaderUtils.loadClassInstance(transformDescriptor.getTransformClass(), transformClass, component);
    	} else {
    		throw new JetelRuntimeException("Transformation is not defined.");
    	}
    	
        if (transformation instanceof Transform) {
        	((Transform) transformation).setNode(component);
        }
        
    	return transformation;
    }

    /**
     * Creates transform based on the given source code.
     */
    private T createTransformFromCode(String transformCode) {
    	T transformation = null;
    	
        switch (TransformLanguageDetector.guessLanguage(transformCode)) {
        case JAVA:
        	transformCode = preprocessJavaCode(transformCode, inMetadata, outMetadata, component, false);
            transformation = DynamicJavaClass.instantiate(transformCode, transformDescriptor.getTransformClass(), component);
            break;
        case JAVA_PREPROCESS:
        	transformCode = preprocessJavaCode(transformCode, inMetadata, outMetadata, component, true);
            transformation = DynamicJavaClass.instantiate(transformCode, transformDescriptor.getTransformClass(), component);
            break;
        case CTL1:
        	transformation = transformDescriptor.createCTL1Transform(transformCode, component.getLog());
            break;
        case CTL2:
        	if (charset == null) {
        		charset = Defaults.DEFAULT_SOURCE_CODE_CHARSET;
        	}
        	final ITLCompiler compiler = 
        		TLCompilerFactory.createCompiler(component.getGraph(), inMetadata, outMetadata, charset);
        	List<ErrorMessage> msgs = compiler.compile(transformCode, transformDescriptor.getCompiledCTL2TransformClass(), component.getId());
        	if (compiler.errorCount() > 0) {
        		String report = ErrorMessage.listToString(msgs, null); // message does not need to be logged here, will be thrown up as part of an exception
        		String message = "CTL code compilation finished with " + compiler.errorCount() + " errors." + report;
        		for (ErrorMessage msg: msgs) {
        			if (msg.getDetail() instanceof MetadataErrorDetail) {
        				MetadataErrorDetail detail = (MetadataErrorDetail) msg.getDetail();
            			throw new MissingFieldException(message, detail.isOutput(), detail.getRecordId(), detail.getFieldName());
        			}
        		}
    			throw new LoadClassException(message);
        	}
        	Object ret = compiler.getCompiledCode();
        	if (ret instanceof TransformLangExecutor) {
        		// setup interpreted runtime
        		transformation = transformDescriptor.createInterpretedCTL2Transform((TransformLangExecutor) ret, component.getLog());
        	} else if (transformDescriptor.getTransformClass().isInstance(ret)) {
        		transformation = transformDescriptor.getTransformClass().cast(ret);
        	} else {
        		// this should never happen as compiler always generates correct interface
        		throw new LoadClassException("Invalid type of record transformation");
        	}
            break;
        default:
            throw new LoadClassException("Can't determine transformation code.");
        }
        
        return transformation;
    }
    
    /**
     * Java code is pre-processed by {@link CodeParser} before compilation.
     */
    private static String preprocessJavaCode(String transformCode, DataRecordMetadata[] inMetadata, DataRecordMetadata[] outMetadata, Node node, boolean addTransformCodeStub) {
        // creating dynamicTransformCode from internal transformation format
        CodeParser codeParser = new CodeParser(inMetadata, outMetadata);
        if (!addTransformCodeStub) 
        	// we must turn this off, because we don't have control about the rest of Java source
        	// and thus we cannot create the declaration of symbolic constants
        	codeParser.setUseSymbolicNames(false);
        codeParser.setSourceCode(transformCode);
        codeParser.parse();
        if (addTransformCodeStub) {
        	codeParser.addTransformCodeStub("Transform" + node.getId());
        }
        return codeParser.getSourceCode();
    }

	/**
	 * Sets transformation code.
	 */
	public void setTransform(String transform) {
		this.transform = transform;
	}
	
	/**
	 * Sets transformation class name.
	 */
	public void setTransformClass(String transformClass) {
		this.transformClass = transformClass;
	}
	
	/**
	 * Sets URL where the transformation code can be loaded.
	 */
	public void setTransformUrl(String transformUrl) {
		this.transformUrl = transformUrl;
	}

	/**
	 * Sets charset of external definition of transformation code defined in transformUrl
	 * or charset that should be used for import 
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}
	
	/**
	 * Sets component which requests the transformation instantiation. 
	 */
	public void setComponent(Node component) {
		this.component = component;
	}

	/**
	 * Sets input metadata of transformation necessary for CTL compilation.
	 */
	public void setInMetadata(DataRecordMetadata... inMetadata) {
		this.inMetadata = inMetadata;
	}

	/**
	 * Sets input metadata of transformation necessary for CTL compilation.
	 */
	public void setInMetadata(List<DataRecordMetadata> inMetadata) {
		this.inMetadata = inMetadata.toArray(new DataRecordMetadata[inMetadata.size()]);
	}

	/**
	 * Sets output metadata of transformation necessary for CTL compilation.
	 */
	public void setOutMetadata(DataRecordMetadata... outMetadata) {
		this.outMetadata = outMetadata;
	}

	/**
	 * Sets output metadata of transformation necessary for CTL compilation.
	 */
	public void setOutMetadata(List<DataRecordMetadata> outMetadata) {
		this.outMetadata = outMetadata.toArray(new DataRecordMetadata[outMetadata.size()]);
	}

	/**
	 * @return true if the transformation code or class name is specified
	 */
	public boolean isTransformSpecified() {
		return !StringUtils.isEmpty(transform) || !StringUtils.isEmpty(transformClass) || !StringUtils.isEmpty(transformUrl);
	}

}
