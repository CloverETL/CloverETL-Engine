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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.Collection;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.axiom.om.OMDocument;
import org.apache.axiom.om.impl.builder.StAXBuilder;
import org.apache.axiom.om.impl.builder.StAXOMBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.ws.exception.MessageValidationException;
import org.jetel.component.ws.exception.SendingMessegeException;
import org.jetel.component.ws.exception.WSMessengerConfigurationException;
import org.jetel.component.ws.proxy.AsyncMessenger;
import org.jetel.data.formatter.Formatter;
import org.jetel.data.formatter.XmlTemplateFormatter;
import org.jetel.data.formatter.provider.FormatterProvider;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.extension.PortDefinition;
import org.jetel.metadata.DataRecordMetadata;

/**
 *
 * @author Pavel Pospichal
 */
public class WSPureBatchXMLWriter {

    private static Log defaultLogger = LogFactory.getLog(WSPureBatchXMLWriter.class);

    private final static String TEMP_FILE_PREFIX_DEFAULT = "cloverETL";
    private final static String TEMP_FILE_SUFFIX_DEFAULT = "";
    private static final String OUTPUT_CHARSET_DEFAULT = "UTF-8";
    private final static int XML_DOCUMENT_BUFFER_SIZE_DAFAULT = 16*1024; // 16kB

    private Log logger = defaultLogger;

    private URL wsdlLocation;
    private QName operationQName;

    private AsyncMessenger messenger;
    private FileChannel tempXmlStorage;
    private File temporaryFile;
    private MappedByteBuffer xmlDocBuffer;
    private String tempFilePrefix = TEMP_FILE_PREFIX_DEFAULT;
    private String tempFileSuffix = TEMP_FILE_SUFFIX_DEFAULT;

    private XmlTemplateFormatter currentFormatter;				// for former constructor
    private FormatterProvider formatterGetter;		// creates new formatter
    private DataRecordMetadata metadata;

	private int recordCounter;
    private long xmlDocumentSize;
    
    private String proxyHostLocation = null;
    private int proxyHostPort;

    public WSPureBatchXMLWriter(XmlTemplateFormatter formatter, URL wsdlLocation, QName operationQName) {
        this.currentFormatter = formatter;
        this.wsdlLocation = wsdlLocation;
        this.operationQName = operationQName;
    }

    public WSPureBatchXMLWriter(FormatterProvider formatterGetter, URL wsdlLocation, QName operationQName) {
        this.formatterGetter = formatterGetter;
        this.wsdlLocation = wsdlLocation;
        this.operationQName = operationQName;
    }

    /**
     * Initializes underlying formatter with a given metadata.
     *
     * @param metadata
     * @throws ComponentNotReadyException
     * @throws IOException
     */
    public void init(DataRecordMetadata metadata) throws ComponentNotReadyException {
    	this.metadata = metadata;
        recordCounter = 0;
        xmlDocumentSize = 0;

        initializeFormatter();
        initializeWSMessenger();
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
        currentFormatter.setRootElement("root");
        currentFormatter.setCharset(OUTPUT_CHARSET_DEFAULT);
        currentFormatter.setComponentId("I");
        currentFormatter.setGraphName("T");
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

    private void initializeWSMessenger() throws ComponentNotReadyException {
        try {
            if (messenger == null) {
                messenger = new AsyncMessenger(wsdlLocation);
            }

            if ((proxyHostLocation != null)) {
                messenger.setProxyHostLocation(proxyHostLocation);
                messenger.setProxyHostPort(proxyHostPort);
            }
            messenger.setOperationQName(operationQName);
            messenger.init();
        } catch(WSMessengerConfigurationException cce) {
            throw new ComponentNotReadyException("Unable to establish Web Service proxy.", cce);
        }
    }

    /**
     * Writes given record via formatter into destination file(s).
     * @param record
     * @throws IOException
     * @throws ComponentNotReadyException
     */
    public void write(Collection<PortDefinition> portDefinitions) throws IOException, ComponentNotReadyException {

        // write record to XML storage region
        int writeBytes = currentFormatter.write(portDefinitions);
        xmlDocumentSize += writeBytes;
        recordCounter++;
    }

    /**
	 * Reset writer for next graph execution.
     * @throws ComponentNotReadyException
     */
	public void reset() throws ComponentNotReadyException {

		close();

    	init(metadata);
	}

    /**
     * Closes underlying formatter.
     */
    public void close() {

        try {
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
        } catch (WSMessengerConfigurationException mce) {
            logger.warn("Exception thrown during teminating WS messenger.", mce);
        }

        logger.debug("Web Service writer closed.");
    }

    /**
     * XML document is prepared in temporary file to be sent as payload
     * @throws java.io.IOException
     */
    public void finish() throws IOException {
        XMLStreamReader xmlReader = null;

        try {
            currentFormatter.finish();
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
            XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();

            //InputStream inputStream = ByteBufferUtils.createInputStream(xmlDocBuffer);
            InputStream inputStream = Channels.newInputStream(tempXmlStorage);
            xmlReader = xmlInputFactory.createXMLStreamReader(inputStream);

            StAXBuilder builder = new StAXOMBuilder(xmlReader);
            OMDocument document = builder.getDocument();
            //File tmpFile = File.createTempFile("sd", "axk");
            //document.serialize(new FileOutputStream(tmpFile));
            // send established XML document as payload using configured WS messenger
            /*
            List<OMElement> payload = new ArrayList<OMElement>();
            Iterator<OMElement> els = document.getOMDocumentElement().getChildElements();
            while (els.hasNext()) {
                payload.add(els.next());
            }
            logger.debug("Payload size: "+payload.size());
            messenger.sendPayloadElements(payload);
            */

            // control response??
            messenger.sendPayload(document.getOMDocumentElement());
            
        } catch (XMLStreamException se) {
            throw new IOException("Unable to establish reader for generated XML document: " + se.getMessage());
        } catch (SendingMessegeException sme) {
            throw new IOException("Unable to send payload to remote service: " + sme.getMessage());
        } catch (MessageValidationException mve) {
            throw new IOException("Unable to send payload to remote service: " + mve.getMessage());
        } finally {
            try {
                if (xmlReader != null) {
                    xmlReader.close();
                }
            } catch (XMLStreamException se) {
                logger.warn("Exception thrown during closing XML documnet reader.",se);
            } 

        }
        if (logger.isInfoEnabled()) {
            logger.info("Web Service message delivered successfully.");
            logger.info("Transfered payload size : " + xmlDocumentSize + "B");
        }

    }

    public synchronized void abort() {
        if (messenger != null) {
            messenger.setInterrupted(true);
        }
    }

    public Log getLogger() {
        return logger;
    }

    public void setLogger(Log logger) {
        this.logger = logger;
    }

    public String getTempFilePrefix() {
        return tempFilePrefix;
    }

    public void setTempFilePrefix(String tempFilePrefix) {
        this.tempFilePrefix = tempFilePrefix;
    }

    public String getTempFileSuffix() {
        return tempFileSuffix;
    }

    public void setTempFileSuffix(String tempFileSuffix) {
        this.tempFileSuffix = tempFileSuffix;
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
