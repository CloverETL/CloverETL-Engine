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
import java.util.Properties;

import org.apache.log4j.Logger;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Element;


/**
 * Generic component, also called as Hercules.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5. 1. 2015
 */
public class GenericComponent extends Node {

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

	private Properties additionalAttributes = null;

	public GenericComponent(String id) {
		super(id);
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) return;
		super.init();

		genericTransform = getTransformFactory().createTransform();

		genericTransform.setAdditionalAttributes(additionalAttributes);
		
		genericTransform.init();
	}

	/**
    /* (non-Javadoc)
     * @see org.jetel.graph.Node#preExecute()
     */
    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	
    	genericTransform.preExecute();
    }

	@Override
	public Result execute() throws Exception {
		try {
			genericTransform.execute();
		} catch (Exception e) {
			genericTransform.executeOnError(e);
		}
		
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#postExecute(org.jetel.graph.TransactionMethod)
     */
    @Override
    public void postExecute() throws ComponentNotReadyException {
    	super.postExecute();
    	
    	genericTransform.postExecute();
    }
    
	@Override
	public synchronized void free() {	
		super.free();
		
		if (genericTransform != null) {
			genericTransform.free();
		}
	}

	private TransformFactory<GenericTransform> getTransformFactory() {
		TransformFactory<GenericTransform> transformFactory = TransformFactory.createTransformFactory(GenericTransform.class);
		transformFactory.setTransform(genericTransformCode);
		transformFactory.setTransformClass(genericTransformCode);
		transformFactory.setTransformUrl(genericTransformURL);
		transformFactory.setCharset(charset);
		transformFactory.setComponent(this);
		return transformFactory;
	}
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {		
		super.checkConfig(status);
		
		if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem("Charset " + charset + " not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL, XML_CHARSET_ATTRIBUTE));
        }
		
		//check GenericTransform
		getTransformFactory().checkConfig(status);
		
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
//		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
//		
//		Properties additionalAttributes;
//		
//		GenericComponent genericComponent = new GenericComponent(xattribs.getString(XML_ID_ATTRIBUTE));
//
//        genericComponent.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE, null));
//        
//        genericComponent.set                xattribs.getStringEx(XML_GENERIC_TRANSFORM_ATTRIBUTERUNNABLE_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF), 
//                xattribs.getString(XML_RUNNABLECLASS_ATTRIBUTE, null),
//                xattribs.getStringEx(XML_RUNNABLEURL_ATTRIBUTE, null, RefResFlag.URL),
//                internalProperties);
//		
//		return javaExecute;
		return null;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	/**
	 * @return the genericTransformCode
	 */
	public String getGenericTransformCode() {
		return genericTransformCode;
	}

	/**
	 * @param genericTransformCode the genericTransformCode to set
	 */
	public void setGenericTransformCode(String genericTransformCode) {
		this.genericTransformCode = genericTransformCode;
	}

	/**
	 * @return the genericTransformClass
	 */
	public String getGenericTransformClass() {
		return genericTransformClass;
	}

	/**
	 * @param genericTransformClass the genericTransformClass to set
	 */
	public void setGenericTransformClass(String genericTransformClass) {
		this.genericTransformClass = genericTransformClass;
	}

	/**
	 * @return the genericTransformURL
	 */
	public String getGenericTransformURL() {
		return genericTransformURL;
	}

	/**
	 * @param genericTransformURL the genericTransformURL to set
	 */
	public void setGenericTransformURL(String genericTransformURL) {
		this.genericTransformURL = genericTransformURL;
	}
	
}
