/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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
*/
package org.jetel.component.ws;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.List;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.Source;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.ws.exception.MessageValidationException;
import org.jetel.component.ws.exception.SendingMessegeException;
import org.jetel.component.ws.exception.WSMessengerConfigurationException;
import org.jetel.component.ws.proxy.AsyncMessenger;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.formatter.XmlTemplateFormatter;
import org.jetel.data.formatter.provider.FormatterProvider;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.extension.PortDefinition;
import org.jetel.metadata.DataRecordMetadata;

/**
 * The component WSPureXMLReader is responsible for coordination of XML processing activities 
 * related with communication of WebService provider. The request message of the appropriate
 * remote operation chosen by the user may be processed as pure XML document or template 
 * document based on XML format. The template document aims the requirement for customized 
 * input data transformation. The presumed response message is consequently processed 
 * by parser component. 
 * @author Pavel Pospichal
 */
public class WSPureXMLReader {

    private static Log defaultLogger = LogFactory.getLog(WSPureXMLReader.class);

    private static final String OUTPUT_CHARSET_DEFAULT = "UTF-8";
    private final static String TEMP_FILE_PREFIX_DEFAULT = "cloverETL";
    private final static String TEMP_FILE_SUFFIX_DEFAULT = "";
    
    private Log logger = defaultLogger;

    private Parser parser;
    private XmlTemplateFormatter currentFormatter;	// for former constructor
    private FormatterProvider formatterGetter;		// creates new formatter
    private OMElement requestMessageElement;
    
    private boolean isRequestMessageGenerated;
    
    private AsyncMessenger messenger;
    private FileChannel tempXmlStorage;
    private File temporaryFile;
    private String tempFilePrefix = TEMP_FILE_PREFIX_DEFAULT;
    private String tempFileSuffix = TEMP_FILE_SUFFIX_DEFAULT;
    
    private URL wsdlLocation;
    private QName operationQName;
    
    private boolean validateMessageOnRequest = false;
    private boolean validateMessageOnResponse = false;

    private int recordCounter;
    private long xmlDocumentSize;
    
    private String proxyHostLocation = null;
    private int proxyHostPort;

    public WSPureXMLReader(Parser parser, URL wsdlLocation, QName operationQName) {
        this.parser = parser;
        this.wsdlLocation = wsdlLocation;
        this.operationQName = operationQName;
    }
    
    public WSPureXMLReader(Parser parser, XmlTemplateFormatter formatter, URL wsdlLocation, QName operationQName) {
        this(parser, wsdlLocation, operationQName);
        this.currentFormatter = formatter;
    }

    public WSPureXMLReader(Parser parser, FormatterProvider formatterGetter, URL wsdlLocation, QName operationQName) {
        this(parser, wsdlLocation, operationQName);
        this.formatterGetter = formatterGetter;
    }
    
    public void checkConfig(DataRecordMetadata metadata) throws ComponentNotReadyException {
        parser.init(metadata);

        try {
            if (messenger == null) {
                messenger = new AsyncMessenger(wsdlLocation);
            }
            messenger.setOperationQName(operationQName);
            messenger.init();
        } catch(WSMessengerConfigurationException cce) {
            throw new ComponentNotReadyException("Unable to configure Web Service proxy.", cce);
        }
	}

    public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
        parser.init(metadata);
        recordCounter = 0;
        xmlDocumentSize = 0;
        
		isRequestMessageGenerated = currentFormatter != null || formatterGetter != null;
        
        if (isRequestMessageGenerated) {
        	initializeFormatter();
        }
        initializeWSMessenger();
    }
    
    private void initializeWSMessenger() throws ComponentNotReadyException {
    	try {
            if (messenger == null) {
                messenger = new AsyncMessenger(wsdlLocation);
            }

            if (proxyHostLocation != null) {
                messenger.setProxyHostLocation(proxyHostLocation);
                messenger.setProxyHostPort(proxyHostPort);
            }

            messenger.setOperationQName(operationQName);
            messenger.setValidateMessageOnRequest(validateMessageOnRequest);
            messenger.setValidateMessageOnResponse(validateMessageOnResponse);
            messenger.init();
        } catch(WSMessengerConfigurationException cce) {
            throw new ComponentNotReadyException("Unable to establish Web Service proxy.", cce);
        }
    }
    
    private void initializeFormatter() throws ComponentNotReadyException {
        /**
         * Result Xml document is stored in temporary file because huge data
         * volume is expacted.
         */
        tempXmlStorage = prepareTemporaryXmlStorage();
        /*
        try {
            xmlDocBuffer = tempXmlStorage.map(FileChannel.MapMode.READ_WRITE, 0, XML_DOCUMENT_BUFFER_SIZE_DAFAULT);
            if (logger.isDebugEnabled()) {
                logger.debug("Memory-mapped XML document buffer established of size "+xmlDocBuffer.capacity()+"B.");
            }
        } catch(IOException ioe) {
            throw new ComponentNotReadyException("Unable to establish memory-mapped buffer for generated XML document.", ioe);
        }
        */
        //OutputStream bufferedOutputStream = ByteBufferUtils.newOutputStream(tempXmlStorage, XML_DOCUMENT_BUFFER_SIZE_DAFAULT);

        if (currentFormatter == null && formatterGetter != null) {
            Formatter formatter = formatterGetter.getNewFormatter();
            if (!(formatter instanceof XmlTemplateFormatter)) {
                throw new ComponentNotReadyException("Unsupported type of XML formatter.");
            }
            currentFormatter = (XmlTemplateFormatter) formatter;
        }

        if (currentFormatter == null) {
            throw new ComponentNotReadyException("Missing data formatter definition.");
        }

        //currentFormatter.setDataTarget(bufferedOutputStream);
        currentFormatter.setDataTarget(tempXmlStorage);
        currentFormatter.setUseRootElement(false);
        currentFormatter.setCharset(OUTPUT_CHARSET_DEFAULT);
        currentFormatter.init(null);
    }
    
    private FileChannel prepareTemporaryXmlStorage() throws ComponentNotReadyException {
        FileChannel fileXmlStorage = null;

        try {
            // Target XML data are deposited to temporary file
            temporaryFile = File.createTempFile(tempFilePrefix, tempFileSuffix);
            temporaryFile.deleteOnExit();
            logger.debug("Temporary file location: "+temporaryFile.getAbsolutePath());

            fileXmlStorage = new RandomAccessFile(temporaryFile, "rw").getChannel();

        } catch(IOException ioe) {
            throw new ComponentNotReadyException(ioe);
        }

        return fileXmlStorage;
    }

    public void prepareDataSource(Source requestMessageSource) throws JetelException {
    	
    	if (isRequestMessageGenerated) throw new JetelException("The component is not configured for processing plain XML document.");
    	
    	try {
    		if (requestMessageSource != null) {
        		// the request message is prepared by external entity
        		XMLStreamReader xmlReader = XMLInputFactory.newInstance().createXMLStreamReader(requestMessageSource);
        		requestMessageElement = new StAXOMBuilder(xmlReader).getDocumentElement();
        	} else {
        		// there is no request message
        		requestMessageElement = null;
        	}
    	} catch(Exception e) {
    		throw new JetelException("Unable to prepare request message from plain XML document.", e);
    	}
    	
    }
    
    public void prepareDataSource(Collection<PortDefinition> portDefinitions) throws JetelException {
                    
        
    	if (!isRequestMessageGenerated) throw new JetelException("The component is not configured for generating XML document based on template document.");
    	
    	try {
			// the request message is generated based on template document
			// write record to XML storage region
			try {
		        int writeBytes = currentFormatter.write(portDefinitions);
		        xmlDocumentSize += writeBytes;
			} finally {
				currentFormatter.finish();
			}	
		        
		    if (tempXmlStorage != null && tempXmlStorage.isOpen()) {
		        tempXmlStorage.close();
		    }
		
		    tempXmlStorage = new RandomAccessFile(temporaryFile, "r").getChannel();
		
		    /*
		    try {
		        xmlDocBuffer = tempXmlStorage.map(FileChannel.MapMode.READ_ONLY, 0, tempXmlStorage.size());
		        if (logger.isDebugEnabled()) {
		            logger.debug("Memory-mapped XML document buffer established of size " + xmlDocBuffer.capacity() + "B.");
		        }
		    } catch (IOException ioe) {
		        throw new IOException("Unable to establish memory-mapped buffer for generated XML document.", ioe);
		    }
		    */
		    
		    //InputStream inputStream = ByteBufferUtils.createInputStream(xmlDocBuffer);
		    InputStream inputStream = Channels.newInputStream(tempXmlStorage);
		    XMLStreamReader xmlReader = XMLInputFactory.newInstance().createXMLStreamReader(inputStream);
		    
		    requestMessageElement = new StAXOMBuilder(xmlReader).getDocument().getOMDocumentElement();
		
		    // send established XML document as payload using configured WS messenger
    	} catch (XMLStreamException se) {
            throw new JetelException("Unable to establish reader for generated XML document: " + se.getMessage());
        } catch(Exception e) {
    		throw new JetelException("Unable to prepare request message from template document.", e);
    	}
    }
    
    public void invokeRemoteService() throws JetelException {
    	OMElement responseRootElement = null;
    	
    	try {
    		if (requestMessageElement != null) {
    			responseRootElement = messenger.sendPayload(requestMessageElement);
    		} else {
	            List<OMElement> response = messenger.sendPayloadElements(null);
	            if (response != null && response.size()!=0) {
	                if (response.size() != 1) throw new JetelException("Received invalid message content.");
	                responseRootElement = response.get(0);
	            }
    		}
    	} catch (SendingMessegeException sme) {
            throw new JetelException("Unable to send payload to remote service: " + sme.getMessage());
        } catch (MessageValidationException mve) {
            throw new JetelException("Unable to send payload to remote service: " + mve.getMessage());
        } catch (Exception e) {
            throw new JetelException("Unable to process user request message: " + e.getMessage());
        } finally {
        	try {
        		messenger.terminate();
        	} catch(WSMessengerConfigurationException cce) {
        		throw new JetelException("Unable to terminate connection to remote service.", cce);
        	}
        }
        
        if (logger.isInfoEnabled()) {
            logger.info("Web Service message delivered successfully.");
            logger.info("Transfered payload size : " + xmlDocumentSize + "B");
        }
        
        if (responseRootElement == null) {
        	parser.close();
        	return;
        }
        
    	// currently no support for StAX; as temporary solution serialize into tmp file
        File tmpFile = null;
        FileChannel channel = null;
        
        try {
            tmpFile = File.createTempFile("clover", "");
            tmpFile.deleteOnExit();
            channel = new RandomAccessFile(tmpFile, "rw").getChannel();
            responseRootElement.serialize(Channels.newOutputStream(channel));
        } catch (Exception e) {
            throw new JetelException("Unable to store received XML response.",e);
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch(IOException ioe) {}
            }
        }
        
        //parser.setDataSource(responseRootElement);
        // responsible for closing opened channel
        try {
            parser.setDataSource(new BufferedInputStream(new FileInputStream(tmpFile)));
        } catch (Exception e) {
            throw new JetelException("Unable to set data source to XPathParser.", e);
        }
    	
    }

    public DataRecord getNext() throws JetelException {
        //use parser to get next record
        DataRecord rec = parser.getNext();
        
        return rec;
	}

    public DataRecord getNext(DataRecord record) throws JetelException {

        //use parser to get next record
        DataRecord rec = parser.getNext(record);
        
        return rec;
	}

    public void close() throws JetelException {
        try {
            if (parser != null) {
            	parser.close();
            	parser = null;
            }
            
            if (currentFormatter != null) {
                currentFormatter.close();
                currentFormatter = null;
            }

            // close temporary file and erase it.
            if (tempXmlStorage != null && tempXmlStorage.isOpen()) {
                tempXmlStorage.close();
                tempXmlStorage = null;
            }

            // terminate messenger activities
            if (messenger != null) {
                messenger.terminate();
                messenger = null;
            }
        } catch (IOException ioe) {
            logger.warn("Exception thrown during releasing allocated resources.", ioe);
        } catch(WSMessengerConfigurationException cce) {
        	logger.warn("Exception thrown during teminating WS messenger.", cce);
        }
        
        logger.debug("Web Service writer closed.");

    }

    public synchronized void abort() {
        if (messenger != null) {
            messenger.setInterrupted(true);
        }
    }

    public boolean isValidateMessageOnRequest() {
        return validateMessageOnRequest;
    }

    public void setValidateMessageOnRequest(boolean validateMessageOnRequest) {
        this.validateMessageOnRequest = validateMessageOnRequest;
    }

    public boolean isValidateMessageOnResponse() {
        return validateMessageOnResponse;
    }

    public void setValidateMessageOnResponse(boolean validateMessageOnResponse) {
        this.validateMessageOnResponse = validateMessageOnResponse;
    }

    public String getProxyHostLocation() {
        return proxyHostLocation;
    }

    public void setProxyHostLocation(String proxyHostLocation) {
        this.proxyHostLocation = proxyHostLocation;
    }

    public int getProxyHostPort() {
        return proxyHostPort;
    }

    public void setProxyHostPort(int proxyHostPort) {
        this.proxyHostPort = proxyHostPort;
    }
    
    /**
	 * @return the tempFilePrefix
	 */
	public String getTempFilePrefix() {
		return tempFilePrefix;
	}

	/**
	 * @param tempFilePrefix the tempFilePrefix to set
	 */
	public void setTempFilePrefix(String tempFilePrefix) {
		this.tempFilePrefix = tempFilePrefix;
	}

	/**
	 * @return the tempFileSuffix
	 */
	public String getTempFileSuffix() {
		return tempFileSuffix;
	}

	/**
	 * @param tempFileSuffix the tempFileSuffix to set
	 */
	public void setTempFileSuffix(String tempFileSuffix) {
		this.tempFileSuffix = tempFileSuffix;
	}

	/**
	 * @return the logger
	 */
	public Log getLogger() {
		return logger;
	}

	/**
	 * @param logger the logger to set
	 */
	public void setLogger(Log logger) {
		this.logger = logger;
	}
 
}
