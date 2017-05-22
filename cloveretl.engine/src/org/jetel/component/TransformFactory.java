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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.jetel.component.TransformLanguageDetector.TransformLanguage;
import org.jetel.ctl.CTLAbstractTransform;
import org.jetel.ctl.ErrorMessage;
import org.jetel.ctl.ITLCompiler;
import org.jetel.ctl.ITLCompilerFactory;
import org.jetel.ctl.MetadataErrorDetail;
import org.jetel.ctl.TLCompilerFactory;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.data.Defaults;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.LoadClassException;
import org.jetel.exception.MissingFieldException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.compile.ClassLoaderUtils;
import org.jetel.util.compile.DynamicJavaClass;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;

/**
 * This class is used to instantiate a class based on source code.
 * Supported languages are CTL2 and java.
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
	/** Optional: Attribute of the component for which the transformation is instantiated */
	private String attributeName;

	private String transformSourceId;
	/** Input metadata of transformation, used for CTL compilation */
	private DataRecordMetadata[] inMetadata;
	/** Output metadata of transformation, used for CTL compilation */
	private DataRecordMetadata[] outMetadata;
	/** Customizable compiler factory */
	private ITLCompilerFactory compilerFactory = new DefaultCompilerFactory();
	
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
	 * Only CTL2 code is compiled to ensure correctness of all settings.
	 * Java code is not validated.
	 * @param status
	 * @return
	 */
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		try {
			validateSettings();
		} catch (Exception e) {
        	status.addError(component, null, e);
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

	        	if (transformLanguage == TransformLanguage.UNKNOWN) {
	        		String messagePrefix = attributeName != null ? attributeName + ": can't" : "Can't";
	        		status.addWarning(component, attributeName, messagePrefix + " determine transformation language");
	        	} else if (!transformLanguage.isSupported()) {
					status.addError(component, null, transformLanguage.getName() + " is not a supported language any more, please convert your code to CTL2.");
	        	} else if (transformLanguage == TransformLanguage.CTL2) {
	        		// only CTL is checked
	        		T transform = null;
	    			try {
						transform = createTransform();
					} catch (JetelRuntimeException e) {
						// report CTL error as a warning
						status.addWarning(component, null, e);
					} finally {
						if (transform instanceof Freeable) {
							((Freeable)transform).free();
						}
					}
	        	}
	        }
        }

		return status;
	}

	/**
	 * Uses specified ClassLoader in case the transform is defined by a class
	 */
    public T createTransform(ClassLoader cl) {
		validateSettings();

        T transformation = null;
    	if (!StringUtils.isEmpty(transform)) {
    		//transform has highest priority
    		transformation = createTransformFromCode(transform, transformSourceId != null ? transformSourceId : createPropertyTransformSourceId());
    	} else if (!StringUtils.isEmpty(transformUrl)) {
    		//load transformation code from an URL
    		if (charset == null) {
        		charset = Defaults.DEFAULT_SOURCE_CODE_CHARSET;
        	}
    		URL contextURL = component.getGraph().getRuntimeContext().getContextURL();
        	String transformCode = FileUtils.getStringFromURL(contextURL, transformUrl, charset);
        	PropertyRefResolver refResolver = component.getPropertyRefResolver();
        	transformCode = refResolver.resolveRef(transformCode, RefResFlag.SPEC_CHARACTERS_OFF);
        	String sourceId;
			try {
				sourceId = FileUtils.getFileURL(contextURL, transformUrl).toString();
			} catch (MalformedURLException e) {
				LogFactory.getLog(CTLAbstractTransform.class).warn("Incorrect format of debug source ID", e);
				sourceId = null;
			}
    		transformation = createTransformFromCode(transformCode, sourceId);
    	} else if (!StringUtils.isEmpty(transformClass)) {
    		if (cl != null) {
    			transformation = ClassLoaderUtils.loadClassInstance(transformDescriptor.getTransformClass(), transformClass, cl);
    		} else {
    			transformation = ClassLoaderUtils.loadClassInstance(transformDescriptor.getTransformClass(), transformClass, component);
    		}
    	} else {
    		throw new JetelRuntimeException("Transformation is not defined.");
    	}
    	
        if (transformation instanceof Transform) {
        	((Transform) transformation).setNode(component);
        }
        
    	return transformation;
    }
    
    /**
     * Creates source id based on graph's path, component ID and property name
     * @return
     */
	private String createPropertyTransformSourceId() {
		if (attributeName != null && component.getGraph() != null && component.getGraph().getRuntimeContext() != null) {
			String jobUrl = component.getGraph().getRuntimeContext().getJobUrl();
			if (jobUrl != null) {
				return TransformUtils.createCTLSourceId(jobUrl, TransformUtils.COMPONENT_ID_PARAM, component.getId(),
						TransformUtils.PROPERTY_NAME_PARAM, attributeName);
			}
		}
		return null;
	}
    
    /**
     * Core method of the factory.
     * @return instance of transformation class
     * @throws MissingFieldException if the CTL transformation tried to access non-existing field
     * @throws LoadClassException transformation cannot be instantiated
     */
    public T createTransform() {
    	return createTransform(null);
    }
    
    /**
     * Creates transform based on the given source code.
     */
    private T createTransformFromCode(String transformCode, String sourceId) {
    	T transformation = null;
    	
    	TransformLanguage language = TransformLanguageDetector.guessLanguage(transformCode);
    	
        switch (language) {
        case JAVA:
            transformation = DynamicJavaClass.instantiate(transformCode, transformDescriptor.getTransformClass(), component);
            break;
        case JAVA_PREPROCESS:
        	throw new JetelRuntimeException("CTLLite is not a supported language any more, please convert your code to CTL2.");
        case CTL1:
        	throw new JetelRuntimeException("CTL1 is not a supported language any more, please convert your code to CTL2.");
        case CTL2:
        	if (charset == null) {
        		charset = Defaults.DEFAULT_SOURCE_CODE_CHARSET;
        	}
        	final ITLCompiler compiler = 
        		compilerFactory.createCompiler(component.getGraph(), inMetadata, outMetadata, charset);
        	String id = component.getId();
        	if (!StringUtils.isEmpty(attributeName)) {
        		id += "_" + attributeName;
        	}
        	
        	compiler.setSourceId(sourceId);

        	List<ErrorMessage> msgs = compiler.compile(transformCode, transformDescriptor.getCompiledCTL2TransformClass(), id);
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
        case UNKNOWN:
    		throw new LoadClassException("Can't determine transformation language.");
        default:
            throw new LoadClassException("Can't determine transformation code.");
        }
        
        return transformation;
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
	 * Sets the name of the component attribute which requests the transformation instantiation. 
	 */
	public void setAttributeName(String attributeName) {
		this.attributeName = attributeName;
	}
	
	public void setTransformSourceId(String transformSourceId) {
		this.transformSourceId = transformSourceId;
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

	public void setCompilerFactory(ITLCompilerFactory compilerFactory) {
		this.compilerFactory = compilerFactory;
	}

	/**
	 * Default {@link ITLCompilerFactory} implementation,
	 * selects the compiler with maximum priority from registered compilers.
	 * 
	 * @author krivanekm (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 14. 1. 2015
	 */
	public static class DefaultCompilerFactory implements ITLCompilerFactory {

		@Override
		public ITLCompiler createCompiler(TransformationGraph graph, DataRecordMetadata[] inMetadata,
				DataRecordMetadata[] outMetadata, String encoding) {
			
			return TLCompilerFactory.createCompiler(graph, inMetadata, outMetadata, encoding);
		}

	}
}
