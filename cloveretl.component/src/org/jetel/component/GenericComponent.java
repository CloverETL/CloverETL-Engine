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

import java.nio.charset.Charset;

import org.apache.log4j.Logger;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.compile.CompilationException;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Element;

/**
 * Generic component, also called Hercules.
 * 
 * @author Kokon, salamonp (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5. 1. 2015
 */
public class GenericComponent extends Node /*implements MetadataProvider*/ {

	public final static String COMPONENT_TYPE = "GENERIC_COMPONENT";
	private static final Logger logger = Logger.getLogger(GenericComponent.class);

	private static final String XML_GENERIC_TRANSFORM_CLASS_ATTRIBUTE = "genericTransformClass";
	private static final String XML_GENERIC_TRANSFORM_ATTRIBUTE = "genericTransform";
	private static final String XML_GENERIC_TRANSFORM_URL_ATTRIBUTE = "genericTransformURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";

    private String genericTransformCode = null;
	private String genericTransformClass = null;
	private String genericTransformURL = null;
	private String charset = null;
	
	private GenericTransform genericTransform = null;
	
	/**
	 * Just for blank reading records that the transformation left unread.
	 */
	private DataRecord[] inRecords;

	public GenericComponent(String id) {
		super(id);
	}
	
	private void initRecords() {
		DataRecordMetadata[] inMeta = getInMetadataArray();
		inRecords = new DataRecord[inMeta.length];
		for (int i = 0; i < inRecords.length; i++) {
			inRecords[i] = DataRecordFactory.newRecord(inMeta[i]);
		}
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) {
			return;
		}
		super.init();
		initRecords();
		genericTransform = getTransformFactory().createTransform();
		
		executeWithTransformClassLoader(new CustomRunnable() {
			@Override
			public void run() {
				genericTransform.init();
			}
		});
	}

    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	
    	executeWithTransformClassLoader(new CustomRunnable() {
			@Override
			public void run() throws ComponentNotReadyException {
				genericTransform.preExecute();
			}
		});
    }

	@Override
	public Result execute() throws Exception {
		try {
			CustomRunnable executeRunnable = new CustomRunnable() {
				@Override
				public void run() {
					try {
						genericTransform.execute();
					} catch (Exception e) {
						this.exception = e;
					}
				}
			};
			executeWithTransformClassLoader(executeRunnable);
			if (executeRunnable.exception != null) {
				throw executeRunnable.exception;
			}
		} catch (final Exception e) {
			if (ExceptionUtils.instanceOf(e, InterruptedException.class)) {
				// return as fast as possible when interrupted
				return Result.ABORTED;
			}
			executeWithTransformClassLoader(new CustomRunnable() {
				@Override
				public void run() {
					genericTransform.executeOnError(e);
				}
			});
		}
		
		for (int i = 0; i < inRecords.length; i++) {
			boolean firstUnreadRecord = true;
			while (readRecord(i, inRecords[i]) != null) {
				if (firstUnreadRecord) {
					firstUnreadRecord = false;
					logger.warn(COMPONENT_TYPE + ": Component had unread records on input port " + i);
				}
				// blank read
			}
		}
		
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

    @Override
    public void postExecute() throws ComponentNotReadyException {
		try {
			executeWithTransformClassLoader(new CustomRunnable() {
				@Override
				public void run() throws ComponentNotReadyException {
					genericTransform.postExecute();
				}
			});
		} finally {
			// call to super must be last because super.postExecute() closes transformation's classloader
			super.postExecute();
		}
	}
    
	@Override
	public synchronized void free() {
		super.free();
		try {
			if (genericTransform != null) {
				executeWithTransformClassLoader(new CustomRunnable() {
					@Override
					public void run() {
						genericTransform.free();
					}
				});
			}
		} catch (ComponentNotReadyException e) {
			throw new JetelRuntimeException(e);
		}
	}

	/** 
	 * You can turn on CTL support in this method. Just change the commented lines accordingly.
	 * @return
	 */
	private TransformFactory<GenericTransform> getTransformFactory() {
		
		/** This is Java only version */
		TransformFactory<GenericTransform> transformFactory = TransformFactory.createTransformFactory(GenericTransform.class);
		
		/** This is Java and CTL version */
		/*
		TransformFactory<GenericTransform> transformFactory = TransformFactory.createTransformFactory(GenericTransformDescriptor.newInstance());
		transformFactory.setInMetadata(getInMetadata());
    	transformFactory.setOutMetadata(getOutMetadata());
    	*/
		
		transformFactory.setTransform(genericTransformCode);
		transformFactory.setTransformClass(genericTransformClass);
		transformFactory.setTransformUrl(genericTransformURL);
		transformFactory.setCharset(charset);
		transformFactory.setComponent(this);
		transformFactory.setAttributeName(XML_GENERIC_TRANSFORM_ATTRIBUTE);
		return transformFactory;
	}
	
	@Override
	public ConfigurationStatus checkConfig(final ConfigurationStatus status) {
		super.checkConfig(status);
		
		if (charset != null && !Charset.isSupported(charset)) {
        	status.addError(this, XML_CHARSET_ATTRIBUTE, "Charset " + charset + " not supported!");
        }
		
		try {
			if (genericTransform == null) {
				genericTransform = getTransformFactory().createTransform();
			}
			executeWithTransformClassLoader(new CustomRunnable() {
				@Override
				public void run() {
					genericTransform.checkConfig(status); // delegating to implemented method
				}
			});
		} catch (org.jetel.exception.LoadClassException e) {
			if (ExceptionUtils.instanceOf(e, CompilationException.class)) {
				status.addWarning(this, null, e);
			} else {
				status.addWarning(this, null, "Make sure to set classpath correctly.", e);
			}
		} catch (ComponentNotReadyException e) {
			throw new JetelRuntimeException(e);
		}
        return status;
	}
	
	/**
	 * Creates new instance of this Component from XML definition.
	 * @param graph
	 * @param xmlElement
	 * @return
	 * @throws XMLConfigurationException
	 * @throws AttributeNotFoundException
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		GenericComponent genericComponent = new GenericComponent(xattribs.getString(XML_ID_ATTRIBUTE));
        genericComponent.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE, null));
        genericComponent.setGenericTransformCode(xattribs.getStringEx(XML_GENERIC_TRANSFORM_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
        genericComponent.setGenericTransformURL(xattribs.getStringEx(XML_GENERIC_TRANSFORM_URL_ATTRIBUTE, null, RefResFlag.URL));
        genericComponent.setGenericTransformClass(xattribs.getString(XML_GENERIC_TRANSFORM_CLASS_ATTRIBUTE, null));
        
		return genericComponent;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getGenericTransformCode() {
		return genericTransformCode;
	}

	public void setGenericTransformCode(String genericTransformCode) {
		this.genericTransformCode = genericTransformCode;
	}

	public String getGenericTransformClass() {
		return genericTransformClass;
	}

	public void setGenericTransformClass(String genericTransformClass) {
		this.genericTransformClass = genericTransformClass;
	}

	public String getGenericTransformURL() {
		return genericTransformURL;
	}

	public void setGenericTransformURL(String genericTransformURL) {
		this.genericTransformURL = genericTransformURL;
	}
	
	/**
	 * fix CLO-8964
	 * Runs given runnable with transformation's classloader set as thread context class loader.
	 * This ensures that 3rd party code which uses context class loader has access to correct classpath.
	 */
	private void executeWithTransformClassLoader(CustomRunnable runnable) throws ComponentNotReadyException {
		ClassLoader formerClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(genericTransform.getClass().getClassLoader());
			runnable.run();
		} finally {
			Thread.currentThread().setContextClassLoader(formerClassLoader);
		}
	}
	
	/** Runnable that throws ComponentNotReadyException */
	private static abstract class CustomRunnable {
		protected Exception exception;
		public abstract void run() throws ComponentNotReadyException;
	}
	
	// Currently hercules can't propagate metadata.
	// When CLO-6437 is resolved, we can uncomment these methods (don't forget to implement MetadataProvider in AbstractGenericTransform!)
	/*
	@Override
	public MVMetadata getInputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		if (genericTransform == null && (genericTransformCode != null || genericTransformURL != null || genericTransformClass != null)) {
			genericTransform = getTransformFactory().createTransform();
		}

		if (genericTransform instanceof MetadataProvider) {
			return ((MetadataProvider) genericTransform).getInputMetadata(portIndex, metadataPropagationResolver);
		}
		return null;
	}

	@Override
	public MVMetadata getOutputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		if (genericTransform == null && (genericTransformCode != null || genericTransformURL != null || genericTransformClass != null)) {
			genericTransform = getTransformFactory().createTransform();
		}
		
		if (genericTransform instanceof MetadataProvider) {
			return ((MetadataProvider) genericTransform).getOutputMetadata(portIndex, metadataPropagationResolver);
		}
		return null;
	}
	*/
}
