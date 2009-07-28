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
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
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
import org.jetel.component.ws.exception.SendingMessegeException;
import org.jetel.component.ws.exception.WSMessengerConfigurationException;
import org.jetel.component.ws.proxy.AsyncMessenger;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;

/**
 *
 * @author Pavel Pospichal
 */
public class WSPureXMLReader {

    private static Log defaultLogger = LogFactory.getLog(WSPureXMLReader.class);

    private Log logger = defaultLogger;

    private Parser parser;
    private URL wsdlLocation;
    private QName operationQName;

    private AsyncMessenger messenger;

    private boolean validateMessageOnRequest = false;
    private boolean validateMessageOnResponse = false;

    private String proxyHostLocation = null;
    private int proxyHostPort;

    public WSPureXMLReader(Parser parser, URL wsdlLocation, QName operationQName) {
        this.parser = parser;
        this.wsdlLocation = wsdlLocation;
        this.operationQName = operationQName;
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

    public void prepareDataSource(Source requestMessageSource) throws ComponentNotReadyException {
        try {
            OMElement responseRootElement = null;
            XMLStreamReader reader = null;
            
            try {
                if (requestMessageSource != null) {
                    reader = XMLInputFactory.newInstance().createXMLStreamReader(requestMessageSource);
                    OMElement request = new StAXOMBuilder(reader).getDocumentElement();
                    responseRootElement = messenger.sendPayload(request);
                } else {
                    List<OMElement> response = messenger.sendPayloadElements(null);
                    if (response == null || response.size() != 1) {
                        throw new ComponentNotReadyException("Received invalid InOut message content.");
                    }
                    responseRootElement = response.get(0);
                }

                
                if (responseRootElement == null) {
                    throw new SendingMessegeException("Received invalid message content.");
                }

            } catch (Exception e) {
                throw new ComponentNotReadyException("Unable to process user request message.");
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (XMLStreamException e) {
                    }
                }
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
                throw new ComponentNotReadyException("Unable to store received XML response.",e);
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
                throw new ComponentNotReadyException("Unable to set data source to XPathParser.", e);
            }
        } /*catch (MessageValidationException mve) {
            throw new ComponentNotReadyException("Unable to establish data source.", mve);
        } */finally {
            try {
            messenger.terminate();
        } catch(WSMessengerConfigurationException cce) {
            throw new ComponentNotReadyException("Unable to terminate connection to remote service.", cce);
        }
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

    public void releaseDataSource() throws JetelException {
        try {
            parser.close();
            messenger.terminate();
        } catch(WSMessengerConfigurationException cce) {
            throw new JetelException("Unable to release", cce);
        }

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
 
}
