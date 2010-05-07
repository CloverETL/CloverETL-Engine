/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 */
package org.jetel.component;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.StringDataField;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.InputPort;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.OutputPortDirect;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 24.6.2009
 */
public class HttpConnector extends Node {

    private final static Log logger = LogFactory.getLog(HttpConnector.class);

	private final static String COMPONENT_TYPE = "HTTP_CONNECTOR";

    private final static String XML_URL_ATTRIBUTE = "url";
    private final static String XML_REQUEST_METHOD_ATTRIBUTE = "requestMethod";
    private final static String XML_REQUEST_CONTENT_ATTRIBUTE = "requestContent";
    private final static String XML_INPUT_FILEURL_ATTRIBUTE = "inFileUrl";
    private final static String XML_OUTPUT_FILEURL_ATTRIBUTE = "outFileUrl";
    private final static String XML_APPEND_OUTPUT_ATTRIBUTE = "appendOutput";
    private final static String XML_HEADER_PROPERTIES_ATTRIBUTE = "headerProperties";
    private final static String XML_INPUT_PORT_FIELD_NAME = "inputField";
    private final static String XML_OUTPUT_PORT_FIELD_NAME = "outputField";
    private final static String XML_CHARSET_ATTRIBUTE = "charset";

    
    private final static String XML_STORE_RESPONSE_TO_TEMP_FILE = "responseAsFileName";
    /** 
     * If specified, the component will write response to a temporary file and send the file name in output field
     * User can then read with "indirect" reading. If not specified, the response is passed by value
     */
    private final static String XML_TEMPORARY_DIRECTORY = "responseDirectory";
    
    private final static String XML_TEMPORARY_FILE_PREFIX = "responseFilePrefix";
    
    private final static boolean DEFAULT_APPEND_OUTPUT = false;
    
    private final static int IN_PORT = 0;
    private final static int OUT_PORT = 0;

    
    private interface ResponseWriter {
    	public void writeResponse(String response) throws IOException;
    }
    
    
    /**
     * Writer that sends reponse directly stored in String field
     */
    private class ResponseByValueWriter implements ResponseWriter {

    	private final DataField outputField;
    	
    	public ResponseByValueWriter(DataField outputField) {
    		this.outputField = outputField;
    	}
    	
		public void writeResponse(String response) throws IOException {
			outputField.setValue(response);
		}
    }
    
    private class ResponseByFileNameWriter implements ResponseWriter {
    	
    	private final DataField outputField;
    	private final String prefix;
    	private final File directory;
    	
    	public ResponseByFileNameWriter(DataField outputField, String prefix, File tempDirectory) {
    		this.outputField = outputField;
    		this.prefix = prefix;
    		this.directory = tempDirectory;
    		
    	}

		public void writeResponse(String response) throws IOException {
			// write response to a temporary file
			final File tempFile = File.createTempFile(this.prefix, ".tmp", this.directory);
			final BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
			writer.write(response);
			writer.flush();
			writer.close();
			
			// populate output field with the temporary file name
			outputField.setValue(tempFile.getAbsolutePath());
		}
    	
    	
    	
    }
    
    private String rawUrl;
    private String requestMethod;
	private String inputFileUrl;
    private String outputFileUrl;
    private boolean appendOutput;
    private String rawRequestProperties;
    private Properties requestProperties;
    private String requestContent;
    private String inputFieldName;
    private String outputFieldName;
    private String charset;
    
    private boolean storeResponseToTempFile;
    private String temporaryDirectory;
    private String temporaryFilePrefix;
    
    
    private HttpURLConnection httpConnection;

	private InputPortDirect inPort;
	private OutputPortDirect outPort;

	private DataRecord inRecord;
	private DataRecord outRecord;

	private StringDataField outField;

	private StringDataField inField;

	private ResponseWriter responseWriter;
	
    
	public HttpConnector(String id) {
		super(id);
	}

	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();

		// input port initialization
		inPort = getInputPortDirect(IN_PORT);
		if (inPort != null) {
			if (inputFieldName != null) {
				logger.warn("Input file specified will be ignored, because an input port is connected.");
			}
	        inRecord = new DataRecord(inPort.getMetadata());
	        inRecord.init();
	        if (inputFieldName == null) {
	        	inField = (StringDataField) inRecord.getField(0);
	        } else {
	        	inField = (StringDataField) inRecord.getField(inputFieldName);
	        }
		}

		// output port initialization
		outPort = getOutputPortDirect(OUT_PORT);
		if (outPort != null) {
			if (outputFieldName != null) {
				logger.warn("Output file specified will be ignored, because an output port is connected.");
			}
	        outRecord = new DataRecord(outPort.getMetadata());
	        outRecord.init();
	        if (outputFieldName == null) {
	        	outField = (StringDataField) outRecord.getField(0);
	        } else {
	        	outField = (StringDataField) outRecord.getField(outputFieldName);
	        }
		}
		

		// create response writer based on configuration - do it only when output port is connected
		if (outField != null) {
			// output port is connected (see validation above)
			if (getStoreResponseToTempFile()) {
				try {
					// resolve temporary directory if a relative path is used
					final File tmpDir = new File(FileUtils.getFile(getGraph().getProjectURL(), temporaryDirectory));
					responseWriter = new ResponseByFileNameWriter(outField,temporaryFilePrefix,tmpDir);
				} catch (MalformedURLException e) {
					throw new ComponentNotReadyException("Unable to resolve directory to store temporary response files",e);
				}
			} else {
				// response is written to the output field
				responseWriter = new ResponseByValueWriter(outField);
			}
		} 
	}
	
	/**
	 * Initializes the HTTP connection for a new request.
	 * 
	 * @throws ComponentNotReadyException
	 */
	private void initHTTPConnection() throws ComponentNotReadyException {
		//create URL
		URL url = null;
		try {
			url = new URL(rawUrl);
		} catch (MalformedURLException e) {
			throw new ComponentNotReadyException(this, "Invalid URL component attribute.", e);
		}
		String protocol = url.getProtocol();
		if (!protocol.equals("http") && !protocol.equals("https")) {
			throw new ComponentNotReadyException(this, "Given URL has incompatible protocol: " + protocol);
		}
		
		//open connection based on URL
		URLConnection urlConnection = null;
		try {
			urlConnection = url.openConnection();
		} catch (IOException e) {
			throw new ComponentNotReadyException(this, "Unable to connect to given URL.", e);
		}
		
		//is the opened connection HTTP connection?
		if (!(urlConnection instanceof HttpURLConnection)) {
			throw new ComponentNotReadyException(this, "Given URL is not valid http location.");
		}
		
		httpConnection = (HttpURLConnection) urlConnection;

		if (!StringUtils.isEmpty(requestMethod)) {
			try {
				httpConnection.setRequestMethod(requestMethod);
			} catch (ProtocolException e) {
				throw new ComponentNotReadyException(this, "Unsupported request method: " + requestMethod, e);
			}
		}

		//load request properties
		if (!StringUtils.isEmpty(rawRequestProperties)) {
			requestProperties = new Properties();
			try {
				requestProperties.load(new ByteArrayInputStream(rawRequestProperties.getBytes("ISO-8859-1")));
			} catch (Exception e) {
				throw new ComponentNotReadyException(this, "Unexpected exception during request properties reading.", e);
			}
		
			//pass request properties to the http connection
			for (Entry<Object, Object> entry : requestProperties.entrySet()) {
				httpConnection.addRequestProperty((String) entry.getKey(), (String) entry.getValue());
			}
		}
		
		if (!StringUtils.isEmpty(inputFileUrl) || inPort != null || !StringUtils.isEmpty(requestContent)) {
			httpConnection.setDoOutput(true);
		}

	}
	
	@Override
	public Result execute() throws Exception {
		if (inPort != null && outPort != null) {
			// input and output ports are connected, so they will be used
			
			while (inPort.readRecord(inRecord) != null && runIt) {
				// create a new HTTP connection
				initHTTPConnection();

				httpConnection.connect();
				
				String input = inField.toString();
				if (input != null) {
					// send the input field content to the HTTP connection
					sendInput(input);
				}
				
				
				if (outPort != null) {
					responseWriter.writeResponse(getRequestResult());
					outPort.writeRecord(outRecord);
				}
				
				SynchronizeUtils.cloverYield();
			}
			
			broadcastEOF();
		} else {
			// input and output ports are not connected, so they will be ignored and only 1 request will be made
			
			// create a new HTTP connection
			initHTTPConnection();

			//prepare input
			if (!StringUtils.isEmpty(requestContent)) {
				sendInput(requestContent);
			} else if (!StringUtils.isEmpty(inputFileUrl)) {
				ReadableByteChannel inputFile = FileUtils.getReadableChannel(getGraph() != null ? getGraph().getProjectURL() : null, inputFileUrl);
				WritableByteChannel outputConnection = Channels.newChannel(httpConnection.getOutputStream());
				
				copy(inputFile, outputConnection);

				inputFile.close();
				outputConnection.close();
			}

			httpConnection.connect();
			
			WritableByteChannel outputFile = null;
			if (!StringUtils.isEmpty(outputFileUrl)) {
				outputFile = FileUtils.getWritableChannel(getGraph() != null ? getGraph().getProjectURL() : null, outputFileUrl, appendOutput);
			} else {
				outputFile = Channels.newChannel(System.out);
			}
			ReadableByteChannel inputConnection = Channels.newChannel(httpConnection.getInputStream());

			copy(inputConnection, outputFile);
		}

		
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/**
	 * Writes to the HTTP connection.
	 * 
	 * @param input
	 * @throws IOException
	 */
	private void sendInput(String input) throws IOException {
		byte[] bytes;
		if (charset == null) {
			bytes = input.getBytes(Defaults.DataParser.DEFAULT_CHARSET_DECODER);
		} else {
			bytes = input.getBytes(charset);
		}
		ByteArrayInputStream is = new ByteArrayInputStream(bytes);
		OutputStream os = httpConnection.getOutputStream();

		copy(is, os);
		
		is.close();
		os.close();
	}

	/**
	 * @return result of the HTTP request
	 * @throws IOException
	 */
	private String getRequestResult() throws IOException {
		InputStream result = httpConnection.getInputStream();

		BufferedReader reader;
		if (charset == null) {
			reader = new BufferedReader(new InputStreamReader(result));
		} else {
			reader = new BufferedReader(new InputStreamReader(result, charset));
		}
		StringBuilder sb = new StringBuilder();
		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				sb.append(line + "\n");
			}
		} catch (IOException e) {
			throw e;
		} finally {
			try {
				result.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return sb.toString();
	}


	public static HttpConnector fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		try {
			HttpConnector httpConnector = new HttpConnector(xattribs.getString(XML_ID_ATTRIBUTE));

			httpConnector.setUrl(xattribs.getString(XML_URL_ATTRIBUTE));
			httpConnector.setRequestMethod(xattribs.getString(XML_REQUEST_METHOD_ATTRIBUTE, null));
			httpConnector.setInputFileUrl(xattribs.getStringEx(XML_INPUT_FILEURL_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
			httpConnector.setOutputFileUrl(xattribs.getStringEx(XML_OUTPUT_FILEURL_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
			httpConnector.setAppendOutput(xattribs.getBoolean(XML_APPEND_OUTPUT_ATTRIBUTE, DEFAULT_APPEND_OUTPUT));
			httpConnector.setRequestProperties(xattribs.getString(XML_HEADER_PROPERTIES_ATTRIBUTE, null));
			httpConnector.setRequestContent(xattribs.getString(XML_REQUEST_CONTENT_ATTRIBUTE, null));
			httpConnector.setInputFieldName(xattribs.getString(XML_INPUT_PORT_FIELD_NAME, null));
			httpConnector.setOutputFieldName(xattribs.getString(XML_OUTPUT_PORT_FIELD_NAME, null));
			httpConnector.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE, null));
			httpConnector.setStoreResponseToTempFile(xattribs.getBoolean(XML_STORE_RESPONSE_TO_TEMP_FILE, false));
			httpConnector.setTemporaryDirectory(xattribs.getString(XML_TEMPORARY_DIRECTORY, System.getProperty("java.io.tmpdir")));
			httpConnector.setTemporaryFilePrefix(xattribs.getString(XML_TEMPORARY_FILE_PREFIX, "http-response-"));
			
			
			return httpConnector;
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":"
					+ xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ")
					+ ":" + ex.getMessage(), ex);
		}
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		//check number of input/output ports
		if(!checkInputPorts(status, 0, 1)
				|| !checkOutputPorts(status, 0, 1)) {
			return status;
		}
		
		if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }

		InputPort inPort = getInputPort(IN_PORT); 
		if (inPort != null) {
			DataFieldMetadata inField;
			if (!StringUtils.isEmpty(inputFieldName)) {
				inField = inPort.getMetadata().getField(inputFieldName);
			} else {
				inField = inPort.getMetadata().getField(0);
			}
			if (inField == null) {
				status.add(new ConfigurationProblem(
						"Input field name '" + inputFieldName + "' does not exist in input metadata.", 
						Severity.ERROR, this, Priority.NORMAL, XML_INPUT_PORT_FIELD_NAME));
			} else if (inField.getType() != DataFieldMetadata.STRING_FIELD) {
				status.add(new ConfigurationProblem(
						"Input field '" + outputFieldName + "' has incompatible type '" + inField.getTypeAsString() + "'. Field has to be String.", 
						Severity.ERROR, this, Priority.NORMAL, XML_INPUT_PORT_FIELD_NAME));
			}
		}

		OutputPort outPort = getOutputPort(OUT_PORT); 
		if (outPort != null) {
			DataFieldMetadata outField;
			if (!StringUtils.isEmpty(outputFieldName)) {
				outField = outPort.getMetadata().getField(outputFieldName);
			} else {
				outField = outPort.getMetadata().getField(0);
			}
			if (outField == null) {
				status.add(new ConfigurationProblem(
						"Output field name '" + outputFieldName + "' does not exist in output metadata.", 
						Severity.ERROR, this, Priority.NORMAL, XML_OUTPUT_PORT_FIELD_NAME));
			} else if (outField.getType() != DataFieldMetadata.STRING_FIELD) {
				status.add(new ConfigurationProblem(
						"Output field '" + outputFieldName + "' has incompatible type '" + outField.getTypeAsString() + "'. Field has to be String.", 
						Severity.ERROR, this, Priority.NORMAL, XML_OUTPUT_PORT_FIELD_NAME));
			}
		}
		
		if ((inPort != null && outPort == null)
				|| (inPort == null && outPort != null)) {
			status.add(new ConfigurationProblem(
					"Both input and output ports must be connected, or none of them", 
					Severity.ERROR, this, Priority.NORMAL));
		}

		if (!StringUtils.isEmpty(getRequestContent()) && !StringUtils.isEmpty(getInputFileUrl())) {
			status.add(new ConfigurationProblem(
					"You can set either 'Request content' or 'Input file URL'.", 
					Severity.ERROR, this, Priority.NORMAL));
		}
		
		// check existence of the temporary directory; if specified
		if (getStoreResponseToTempFile()) {
			
			// output port must be connected so that we can write file names into
			if (outPort == null) {
				status.add(new ConfigurationProblem(
						"An output port must be connected in order to write response temporary file names.",
						Severity.ERROR,this,Priority.NORMAL));
			}
			
			// when temporary directory should be used, it must exist
			if (!StringUtils.isEmpty(getTemporaryDirectory())) {
				
				try {
				File tmpDir = new File(FileUtils.getFile(getGraph().getProjectURL(), getTemporaryDirectory()));;
				if (!tmpDir.exists()) {
					status.add(new ConfigurationProblem(
						"Directory to store response temporary files does not exist: " + tmpDir.getAbsolutePath() ,
						Severity.ERROR,this,Priority.NORMAL));
				}
				} catch (MalformedURLException e) {
					status.add(new ConfigurationProblem(
						"Unable to resolve directory to store response files: " + e.getMessage(),
						Severity.ERROR,this,Priority.NORMAL
					));
				}
			}
		}
		
        return status;
	}
	
    public void setUrl(String rawUrl) {
		this.rawUrl = rawUrl;
	}

    public void setRequestMethod(String requestMethod) {
		this.requestMethod = requestMethod;
	}

    public String getRequestMethod() {
		return requestMethod;
	}

    public void setInputFileUrl(String inputFileUrl) {
		this.inputFileUrl = inputFileUrl;
	}

    public String getInputFileUrl() {
		return inputFileUrl;
	}

	public void setOutputFileUrl(String outputFileUrl) {
		this.outputFileUrl = outputFileUrl;
	}

    public String getOutputFileUrl() {
		return outputFileUrl;
	}

	public void setAppendOutput(boolean appendOutput) {
    	this.appendOutput = appendOutput;
    }
    
	public boolean isAppendOutput() {
		return appendOutput;
	}

	public void setRequestProperties(String rawRequestProperties) {
		this.rawRequestProperties = rawRequestProperties;
	}

	public String getRequestContent() {
		return requestContent;
	}
	
	public void setRequestContent(String requestContent) {
		this.requestContent = requestContent;
	}
	
	public String getInputFieldName() {
		return inputFieldName;
	}

	public void setInputFieldName(String inputFieldName) {
		this.inputFieldName = inputFieldName;
	}

	public String getOutputFieldName() {
		return outputFieldName;
	}

	public void setOutputFieldName(String outputFieldName) {
		this.outputFieldName = outputFieldName;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	
	public void setTemporaryDirectory(String temporaryDirectory) {
		this.temporaryDirectory = temporaryDirectory;
	}
	
	public String getTemporaryDirectory() {
		return temporaryDirectory;
	}
	
	public void setTemporaryFilePrefix(String temporaryFilePrefix) {
		this.temporaryFilePrefix = temporaryFilePrefix;
	}
	
	public String getTemporaryFilePrefix() {
		return temporaryFilePrefix;
	}
	
	public void setStoreResponseToTempFile(boolean storeResponseToTempFile) {
		this.storeResponseToTempFile = storeResponseToTempFile;
	}
	
	public boolean getStoreResponseToTempFile() {
		return storeResponseToTempFile;
	}

	
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}
	
	

    /**
     * Read all available bytes from one channel and copy them to the other.
     * @param in
     * @param out
     * @throws IOException
     */
    public static void copy(ReadableByteChannel in, WritableByteChannel out) throws IOException {
    	// First, we need a buffer to hold blocks of copied bytes.
    	ByteBuffer buffer = ByteBuffer.allocateDirect(32 * 1024);

    	// Now loop until no more bytes to read and the buffer is empty
    	while (in.read(buffer) != -1 || buffer.position() > 0) {
    		// The read() call leaves the buffer in "fill mode". To prepare
    		// to write bytes from the bufferwe have to put it in "drain mode"
    		// by flipping it: setting limit to position and position to zero
    		buffer.flip();

    		// Now write some or all of the bytes out to the output channel
    		out.write(buffer);

    		// Compact the buffer by discarding bytes that were written,
    		// and shifting any remaining bytes. This method also
    		// prepares the buffer for the next call to read() by setting the
    		// position to the limit and the limit to the buffer capacity.
    		buffer.compact();
    	}
    }

    private static final int IO_BUFFER_SIZE = 4 * 1024;  

    /**
     * Read all available bytes from one stream and copy them to the other.
     * @param in
     * @param out
     * @throws IOException
     */
    public static void copy(InputStream in, OutputStream out) throws IOException {
    	byte[] b = new byte[IO_BUFFER_SIZE];
    	int read;
    	while ((read = in.read(b)) != -1) {
    		out.write(b, 0, read);
    	}
    }

}
