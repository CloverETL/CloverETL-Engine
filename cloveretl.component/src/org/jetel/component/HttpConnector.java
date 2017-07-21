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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthPolicy;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.StringDataField;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.OutputPortDirect;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.protocols.proxy.ProxyHandler;
import org.jetel.util.stream.StreamUtils;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  *  <h3>HttpConnector Component</h3>
 * 
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>HttpConnector</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td>Others</td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Component provides a functionality for sending http requests. URL can be set or retrieved from input port. Implemented http methods are 
 * GET and POST. Place holders can be used at URL when input port is connected. The format of placeholder is *{placeholder_name}. Input fields 
 * not used for substitution of placeholders can be added to the URL as parameters (they can be add to the query string or method body if the 
 * POST method is used. Ignored fields specify which input fields can't be add as parameters. If POST method is used input fields can be added 
 * as multipart entities. Component allows HTTP Authentication (basic and digest). Authentication is proceed if username and password is set.)</td></tr>
 * <tr><td></td></tr>
 * </table>
 *  <br>  
 *  <table border="1">
 *  <tr><td><b>type</b></td><td>"HTTP_CONNECTOR"</td></tr>
 *  <tr><td><b>url</b></td><td>URL for http request. Place holders can be used when input port is connected. Place holder format is *{}</td>
 *  <tr><td><b>urlInputField</b></td><td>URL for http request from metadata field. Place holders can be used.</td>
 *  <tr><td><b>requestMethod</b></td><td>Http request method. GET and POST are implemented. (values: POST/GET)</td>
 *  <tr><td><b>addInputFieldsAsParametres</b></td><td>Specifies whether parameters are added to the URL. (values: true/false)</td>
 *  <tr><td><b>addInputFieldsAsParametresTo</b></td><td>Specifies whether input fields should be add to the query string or method body. 
 *  Parameters can be added to the method body in case that POST method is used. (values: Query/Body)</td>
 *  <tr><td><b>ignoredFields</b></td><td>Specifies which input fields aren't added as parameters. List of input fields separated by ; is expected.</td>
 *  <tr><td><b>multipartEntities</b></td><td>Specifies which input fields are added to the request as multipart entities. Multipart entities can be added 
 *  to the request in case that POST method is used. List of input fields separated by ; is expected.</td>
 *  <tr><td><b>headerProperties</b></td><td>Additional http header properties.</td>
 *  <tr><td><b>charset</b></td><td>Character encoding of the output file (if not specified, then ISO-8859-1 is used)</td>
 *  <tr><td><b>inputField</b></td><td>The input field whose content is sent as the request.</td>
 *  <tr><td><b>requestContent</b></td><td>The text field whose content is sent as the request.</td>
 *  <tr><td><b>inFileUrl</b></td><td>Input file.</td>
 *  <tr><td><b>outFileUrl</b></td><td>Output file.</td>
 *  <tr><td><b>append</b></td><td>Whether to append data at the end if output file exists or replace it (values: true/false)</td>
 *  <tr><td><b>authenticationMethod</b></td><td>HTTP Authentication method. Authentication is done if username and password is entered. 
 *  (values: BASIC/DIGEST/ANY)</td>
 *  <tr><td><b>username</b></td><td>Username for http authentication</td>
 *  <tr><td><b>password</b></td><td>Password for http authentication</td>
 *  <tr><td><b>responseAsFileName</b></td><td>If specified, the component will write response to a temporary file and send the file name in output field User
 *  can then read with "indirect" reading. If not specified, the response is passed by value. (values true/false)</td>
 *  <tr><td><b>responseDirectory</b></td><td>Directory for response files.</td>
 *  <tr><td><b>responseFilePrefix</b></td><td>Prefix for response files</td>
 *  </tr>
 *  </table>  
 *
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz) (c) Javlin Consulting (www.javlinconsulting.cz),
 * 
 * @created 24.6.2009
 * @version 15.3.2010
 */

public class HttpConnector extends Node {

	private static final String GET = "GET";
	private static final String POST = "POST";

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
	private final static String XML_AUTHENTICATION_METHOD_ATTRIBUTE = "authenticationMethod";
	private final static String XML_USERNAME_ATTRIBUTE = "username";
	private final static String XML_PASSWORD_ATTRIBUTE = "password";
	private final static String XML_ADD_INPUT_FIELDS_AS_PARAMETERS_ATTRIBUTE = "addInputFieldsAsParameters";
	private final static String XML_ADD_INPUT_FIELDS_AS_PARAMETERS_TO_ATTRIBUTE = "addInputFieldsAsParametersTo";
	private final static String XML_IGNORED_FIELDS_ATTRIBUTE = "ignoredFields";
	private final static String XML_MULTIPART_ENTITIES_FIELDS_LIST_ATTRIBUTE = "multipartEntities";
	private final static String XML_URL_FROM_INPUT_FIELD_ATTRIBUTE = "urlInputField";

	private final static String XML_STORE_RESPONSE_TO_TEMP_FILE = "responseAsFileName";
	/**
	 * If specified, the component will write response to a temporary file and send the file name in output field User
	 * can then read with "indirect" reading. If not specified, the response is passed by value
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

		@Override
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

		@Override
		public void writeResponse(String response) throws IOException {
			// write response to a temporary file
			final File tempFile = File.createTempFile(this.prefix, ".tmp", this.directory);
			//final BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
			final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempFile),charset));
			
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
	private String authenticationMethod;
	private String username;
	private String password;
	private boolean addInputFieldsAsParameters;
	private String addInputFieldsAsParametersTo;
	private String ignoredFields;
	private String multipartEntities;
	private String urlInputField;

	private boolean storeResponseToTempFile;
	private String temporaryDirectory;
	private String temporaryFilePrefix;
	private String rawUrlToProceed;
	private StringPart[] stringParts;
	private NameValuePair[] body;
	private String preparedQueryString;

	private HttpClient httpClient;
	private GetMethod getMethod;
	private PostMethod postMethod;

	private InputPortDirect inPort;
	private OutputPortDirect outPort;

	private DataRecord inRecord;
	private DataRecord outRecord;

	private StringDataField outField;

	private StringDataField inField;

	private ResponseWriter responseWriter;

	private DataRecord record;

	private UsernamePasswordCredentials creds;
	
	public HttpConnector(String id) {
		super(id);
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized())
			return;
		super.init();
        //If charset isnt filled, we used default systems
		if(StringUtils.isEmpty(this.charset)) {
		    this.charset = Charset.defaultCharset().name();	
		}
		
		// input port initialization
		inPort = getInputPortDirect(IN_PORT);
		if (inPort != null) {
			inRecord = DataRecordFactory.newRecord(inPort.getMetadata());
			inRecord.init();
			if (inputFieldName != null) {
				inField = (StringDataField) inRecord.getField(inputFieldName);
			}
		}

		// output port initialization
		outPort = getOutputPortDirect(OUT_PORT);
		if (outPort != null) {
			outRecord = DataRecordFactory.newRecord(outPort.getMetadata());
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
					final File tmpDir = new File(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), temporaryDirectory));
					responseWriter = new ResponseByFileNameWriter(outField, temporaryFilePrefix, tmpDir);
				} catch (MalformedURLException e) {
					throw new ComponentNotReadyException("Unable to resolve directory to store temporary response files", e);
				}
			} else {
				// response is written to the output field
				responseWriter = new ResponseByValueWriter(outField);
			}
		}

		//filter multipart entities (ignored fields are removed from multipart entities record)
		if (!StringUtils.isEmpty(ignoredFields) && !StringUtils.isEmpty(multipartEntities)) {
			List<String> multipartEntitiesList = new ArrayList<String>();
			StringTokenizer tokenizer = new StringTokenizer(multipartEntities, ";");
			while (tokenizer.hasMoreElements()) {
				multipartEntitiesList.add(tokenizer.nextToken());
			}
			tokenizer = new StringTokenizer(ignoredFields, ";");
			while (tokenizer.hasMoreTokens()) {
				multipartEntitiesList.remove(tokenizer.nextToken());
			}
			if (!multipartEntitiesList.isEmpty()) {
				multipartEntities = "";
				for (String entity : multipartEntitiesList) {
					multipartEntities += entity + ";";
				}
				multipartEntities = multipartEntities.substring(0, multipartEntities.length() - 1);
			} else {
				multipartEntities = null;
			}
		}
	}


	@Override
	public Result execute() throws Exception {

		if (inPort != null && outPort != null) {
			// input and output ports are connected, so they will be used
			for (record = inPort.readRecord(inRecord); record != null && runIt; record = inPort.readRecord(inRecord)) {

				//urlInputField is set so it will be used (url field is ignored)
				if (!StringUtils.isEmpty(getUrlInputField())) {
					setUrl(record.getField(getUrlInputField()).toString());
				}
				//test whether there are suitable fields for placeholders at metadata
				if (isPossibleToMapVariables()) {
					prepareHttpRequest();
				} else {
					throw new ComponentNotReadyException("Invalid input. Can't create URL or map fields to URL");
				}
				// initialize HTTP connection
				initHTTPConnection();
				if (inField != null) {
				    sendInput(inField.toString());
				} else if(this.requestContent != null) {
				    sendInput(this.requestContent);
				} else {
					// execute selected method
					int statusCode = 0;
					if (requestMethod.equals(POST)) {
						statusCode = httpClient.executeMethod(postMethod);
					} else if (requestMethod.equals(GET)) {
						statusCode = httpClient.executeMethod(getMethod);
					}
					// warn if the http response code isn't 200
					if (statusCode != 200) {
						logger.warn("Returned code for http request "+rawUrlToProceed+" is " + statusCode);
					}
				}
				
				if (outPort != null) {
					responseWriter.writeResponse(getRequestResult());
					outPort.writeRecord(outRecord);
				}

		        if (requestMethod.equals(POST)) {
					postMethod.releaseConnection();
				} else if (requestMethod.equals(GET)) {
					getMethod.releaseConnection();
				}

				SynchronizeUtils.cloverYield();
			}

			broadcastEOF();
		} else {
			// input and output ports are not connected, so they will be ignored and only 1 request will be made
			rawUrlToProceed = rawUrl;
			//initialize http connection
			initHTTPConnection();
			
			if (!StringUtils.isEmpty(requestContent)) {
				//some http request is entered
				sendInput(requestContent);
			} else if (!StringUtils.isEmpty(inputFileUrl)) {
				//input file with http request is entered
		        String contentToSend = FileUtils.getStringFromURL(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, inputFileUrl, null);
				sendInput(contentToSend);
			} else {
				//http request is defined by parameters (method, url, query, ...)
				int statusCode = 0;
				if (requestMethod.equals(POST)) {
				    statusCode = httpClient.executeMethod(postMethod);
				} else if (requestMethod.equals(GET)) {
					statusCode = httpClient.executeMethod(getMethod);
				}
				if (statusCode != 200) {
					logger.warn("Returned code for http request "+rawUrlToProceed+" is " + statusCode);
				}
			}

			WritableByteChannel outputFile = null;
			if (!StringUtils.isEmpty(outputFileUrl)) {
				outputFile = FileUtils.getWritableChannel(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, outputFileUrl, appendOutput);
			} else {
				outputFile = Channels.newChannel(System.out);
			}

			ReadableByteChannel inputConnection = null;
		
			if (requestMethod.equals(POST)) {
			    inputConnection = Channels.newChannel(postMethod.getResponseBodyAsStream());
			} else if (requestMethod.equals(GET)) {
				inputConnection = Channels.newChannel(getMethod.getResponseBodyAsStream());
			}
			
			if (inputConnection != null) {
				StreamUtils.copy(inputConnection, outputFile);
			}
			
	       if (requestMethod.equals(POST)) {
				postMethod.releaseConnection();
			} else if (requestMethod.equals(GET)) {
				getMethod.releaseConnection();
			}
		}

		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

    public static HttpConnector fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		try {
			HttpConnector httpConnector = new HttpConnector(xattribs.getString(XML_ID_ATTRIBUTE));

			httpConnector.setUrl(xattribs.getString(XML_URL_ATTRIBUTE, null));
			httpConnector.setRequestMethod(xattribs.getString(XML_REQUEST_METHOD_ATTRIBUTE, GET));
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
			httpConnector.setAuthenticationMethod(xattribs.getString(XML_AUTHENTICATION_METHOD_ATTRIBUTE, "BASIC"));
			httpConnector.setUsername(xattribs.getString(XML_USERNAME_ATTRIBUTE, null));
			httpConnector.setPassword(xattribs.getString(XML_PASSWORD_ATTRIBUTE, null));
			httpConnector.setAddInputFieldsAsParameters(xattribs.getBoolean(XML_ADD_INPUT_FIELDS_AS_PARAMETERS_ATTRIBUTE, false));
			httpConnector.setAddInputFieldsAsParametersTo(xattribs.getString(XML_ADD_INPUT_FIELDS_AS_PARAMETERS_TO_ATTRIBUTE, "QUERY"));
			httpConnector.setIgnoredFields(xattribs.getString(XML_IGNORED_FIELDS_ATTRIBUTE, null));
			httpConnector.setMultipartEntities(xattribs.getString(XML_MULTIPART_ENTITIES_FIELDS_LIST_ATTRIBUTE, null));
			httpConnector.setUrlInputField(xattribs.getString(XML_URL_FROM_INPUT_FIELD_ATTRIBUTE, null));

			return httpConnector;
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ") + ":" + ex.getMessage(), ex);
		}
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		// check number of input/output ports
		if (!checkInputPorts(status, 0, 1) || !checkOutputPorts(status, 0, 1)) {
			return status;
		}

		if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem(
            		"Charset "+charset+" not supported!", 
            		ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }

		//check whether both URL and input field URL isn't entered
		if (!StringUtils.isEmpty(rawUrl) && !StringUtils.isEmpty(urlInputField)) {
			status.add(new ConfigurationProblem("Both URL and URL from input field is entered. URL from input field will be used.", Severity.WARNING, this, Priority.NORMAL));
		}
		
		//check whether some URL is entered
		if (StringUtils.isEmpty(rawUrl) && StringUtils.isEmpty(urlInputField)) {
			status.add(new ConfigurationProblem("No URL to proceed.", Severity.WARNING, this, Priority.NORMAL));
		}
		
		if (!authenticationMethod.equals("BASIC") && !authenticationMethod.equals("DIGEST") && !authenticationMethod.equals("ANY")) {
			status.add(new ConfigurationProblem("Unsupported authentication method: "+authenticationMethod, Severity.ERROR, this, Priority.NORMAL));
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
				status.add(new ConfigurationProblem("Input field name '" + inputFieldName + "' does not exist in input metadata.", 
						Severity.ERROR, this, Priority.NORMAL, XML_INPUT_PORT_FIELD_NAME));
			} else if (inField.getType() != DataFieldMetadata.STRING_FIELD) {
				status.add(new ConfigurationProblem("Input field '" + outputFieldName + "' has incompatible type '" + 
						inField.getTypeAsString() + "'. Field has to be String.", Severity.ERROR, this, Priority.NORMAL, XML_INPUT_PORT_FIELD_NAME));
			}

			//check whether multipart entities and ignored list is entered just when possible or reasonable
			if (!addInputFieldsAsParameters) {
				if (!StringUtils.isEmpty(multipartEntities)) {
					status.add(new ConfigurationProblem("To add input fields as parameters is disabled and multipart entities list isn't empty.", 
							Severity.WARNING, this, Priority.NORMAL));
				}
				if (!StringUtils.isEmpty(ignoredFields)) {
					status.add(new ConfigurationProblem("To add input fields as parameters is disabled and ignored fields list isn't empty.", 
							Severity.WARNING, this, Priority.NORMAL));
				}
			} else {
				if (!StringUtils.isEmpty(multipartEntities) && !requestMethod.equals(POST)) {
					status.add(new ConfigurationProblem("Multipart entities list isn't empty but the request method isn't POST.", 
							Severity.WARNING, this, Priority.NORMAL));
				}
			}
			
			// check whether multipart entities list contains just values from metadata
			if (!StringUtils.isEmpty(multipartEntities)) {
				List<String> multipartEntitiesList = new ArrayList<String>();
				StringTokenizer tokenizer = new StringTokenizer(multipartEntities, ";");
				while (tokenizer.hasMoreElements()) {
					multipartEntitiesList.add(tokenizer.nextToken());
				}
				String[] metadataNames = inPort.getMetadata().getFieldNamesArray();
				for (String metadataName : metadataNames) {
					multipartEntitiesList.remove(metadataName);
				}
				if (!multipartEntitiesList.isEmpty()) {
					status.add(new ConfigurationProblem("Given multipart entities list contains value not defined at metadata: " + 
							multipartEntitiesList.get(0), Severity.ERROR, this, Priority.NORMAL));
				}
			}

			// check whether ignored fields list contains just values from metadata
			if (!StringUtils.isEmpty(ignoredFields)) {
				List<String> ignoredList = new ArrayList<String>();
				StringTokenizer tokenizer = new StringTokenizer(ignoredFields, ";");
				while (tokenizer.hasMoreElements()) {
					ignoredList.add(tokenizer.nextToken());
				}
				String[] metadataNames = inPort.getMetadata().getFieldNamesArray();
				for (String metadataName : metadataNames) {
					ignoredList.remove(metadataName);
				}
				if (!ignoredList.isEmpty()) {
					status.add(new ConfigurationProblem("Given ignored fields list contains value not defined at metadata: " + ignoredList.get(0), Severity.ERROR, this, Priority.NORMAL));
				}
			}
			
			//test whether is specified where the input fields should be added (query or body)
			if (addInputFieldsAsParameters) {
				if (StringUtils.isEmpty(addInputFieldsAsParametersTo)) {
					if (requestMethod.equals(POST)) {
						status.add(new ConfigurationProblem("Add input fields as parametres must be specified.", Severity.ERROR, this, Priority.NORMAL));
					}
				} else if (addInputFieldsAsParametersTo.equals("BODY") && requestMethod.equals(GET)) {
					status.add(new ConfigurationProblem("Add input fields as parametres must be specified.", Severity.ERROR, this, Priority.NORMAL));
				}
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
				status.add(new ConfigurationProblem("Output field name '" + outputFieldName + "' does not exist in output metadata.", Severity.ERROR, this, Priority.NORMAL, XML_OUTPUT_PORT_FIELD_NAME));
			} else if (outField.getType() != DataFieldMetadata.STRING_FIELD) {
				status.add(new ConfigurationProblem("Output field '" + outputFieldName + "' has incompatible type '" + outField.getTypeAsString() + "'. Field has to be String.", Severity.ERROR, this, Priority.NORMAL, XML_OUTPUT_PORT_FIELD_NAME));
			}
		}

		if ((inPort != null && outPort == null) || (inPort == null && outPort != null)) {
			status.add(new ConfigurationProblem("Both input and output ports must be connected, or none of them", Severity.ERROR, this, Priority.NORMAL));
		}
		
		if (inPort == null && outPort == null && StringUtils.isEmpty(outputFileUrl)) {
			status.add(new ConfigurationProblem("Output port isn't connected and output file is not set.", Severity.WARNING, this, Priority.NORMAL));
		}

		if (inPort != null && !StringUtils.isEmpty(inputFileUrl)) {
			status.add(new ConfigurationProblem("'Input file URL' will be ignored because input port is connected.", Severity.WARNING, this, Priority.NORMAL));
		}

		if (outPort != null && !StringUtils.isEmpty(outputFileUrl)) {
			status.add(new ConfigurationProblem("'Output file URL' will be ignored because output port is connected.", Severity.WARNING, this, Priority.NORMAL));
		}

		if (!StringUtils.isEmpty(getRequestContent()) && !StringUtils.isEmpty(getInputFileUrl())) {
			status.add(new ConfigurationProblem("You can set either 'Request content' or 'Input file URL'.", Severity.ERROR, this, Priority.NORMAL));
		}

		// check existence of the temporary directory; if specified
		if (getStoreResponseToTempFile()) {

			// output port must be connected so that we can write file names into
			if (outPort == null) {
				status.add(new ConfigurationProblem("An output port must be connected in order to write response temporary file names.", Severity.ERROR, this, Priority.NORMAL));
			}

			// when temporary directory should be used, it must exist
			if (!StringUtils.isEmpty(getTemporaryDirectory())) {

				try {
					File tmpDir = new File(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), getTemporaryDirectory()));
					;
					if (!tmpDir.exists()) {
						status.add(new ConfigurationProblem("Directory to store response temporary files does not exist: " + tmpDir.getAbsolutePath(), Severity.ERROR, this, Priority.NORMAL));
					}
				} catch (MalformedURLException e) {
					status.add(new ConfigurationProblem("Unable to resolve directory to store response files: " + e.getMessage(), Severity.ERROR, this, Priority.NORMAL));
				}
			}
		}

		//check whether both user name and password for BASIC http auth are entered or none of them
		if ((!StringUtils.isEmpty(getUsername()) && StringUtils.isEmpty(getPassword())) || (StringUtils.isEmpty(getUsername()) && !StringUtils.isEmpty(getPassword()))) {
			status.add(new ConfigurationProblem("Both username and password must be entered or none of them.", Severity.ERROR, this, Priority.NORMAL));
		}
		return status;
	}
	

	/**
	 * Initializes the HTTP connection for a new request.
	 * 
	 * @throws ComponentNotReadyException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void initHTTPConnection() throws ComponentNotReadyException, IOException, InterruptedException {

		// check whether the given URL is valid
		if (StringUtils.isEmpty(getUrlInputField())) {
			URL url = null;
			String protocol = null;
			try {
				url = new URL(rawUrl);
				protocol = url.getProtocol();
				if (!protocol.equals("http") && !protocol.equals("https")) {
					throw new ComponentNotReadyException("Given URL has incompatible protocol: " + protocol);
				}
			} catch (MalformedURLException e) {
				throw new ComponentNotReadyException("Given URL has incompatible protocol: " + protocol);
			}
		}
		
		httpClient = new HttpClient();

		//
		// proxy usage
		//

		String proxyUrlString = FileURLParser.getInnerAddress(rawUrlToProceed);

		if (proxyUrlString != null) {
			URL proxyUrl = null;

			try {
				proxyUrl = new URL(null, proxyUrlString, new ProxyHandler());
			} catch (MalformedURLException exception) {
				throw new ComponentNotReadyException("Malformed proxy URL!", exception);
			}

			httpClient.getHostConfiguration().setProxy(proxyUrl.getHost(), proxyUrl.getPort());
		}

		//
		// authentication
		//

		if (username != null && password != null) {
			//create credentials
			creds = new UsernamePasswordCredentials(username, password);
			httpClient.getState().setCredentials(AuthScope.ANY, creds);
			
			//set authentication method
			String authMethod = null;
			if (authenticationMethod.equals("BASIC")) {
				//basic http authentication
				authMethod = AuthPolicy.BASIC;
			} else if (authenticationMethod.equals("DIGEST")) {
				//digest http authentication
				authMethod = AuthPolicy.DIGEST;
			} else if (authenticationMethod.equals("ANY")) {
				//one of the possible authentication method will be used
				authMethod = "ANY";
			}
			List<String> authPrefs = null;
			if (authMethod.equals("ANY")) {
				authPrefs = new ArrayList<String>(2);
				authPrefs.add(AuthPolicy.BASIC);
				authPrefs.add(AuthPolicy.DIGEST);
			} else {
				authPrefs = new ArrayList<String>(1);
				authPrefs.add(authMethod);
			}
			authPrefs.add(authMethod);
			httpClient.getParams().setParameter(AuthPolicy.AUTH_SCHEME_PRIORITY, authPrefs);
		}

		if (requestMethod.equals(POST)) {
			//request method is post
			if( logger.isDebugEnabled() ){
				logger.debug("Creating POST request to " + rawUrlToProceed);
			}
			postMethod = new PostMethod(FileURLParser.getOuterAddress(rawUrlToProceed));
			postMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
			//do authentication
			if (username != null && password != null) {
				postMethod.setDoAuthentication(true);
			}
			//set multipart request entity if any
			if (stringParts != null) {
				postMethod.setRequestEntity(new MultipartRequestEntity(stringParts, postMethod.getParams()));
			}
			//set query string if any
			if (!StringUtils.isEmpty(preparedQueryString)) {
				if (!StringUtils.isEmpty(postMethod.getQueryString())) {
					postMethod.setQueryString(postMethod.getQueryString()+"&"+preparedQueryString);
				} else {
					postMethod.setQueryString(preparedQueryString);
				}
			}
			//set request body if any
			if (body != null) {
				postMethod.setRequestBody(body);
			
			}
		} else if (requestMethod.equals(GET)) {
			//request method is post
			if( logger.isDebugEnabled() ) {
				logger.debug("Creating GET request to " + rawUrlToProceed);
			}
			getMethod = new GetMethod(FileURLParser.getOuterAddress(rawUrlToProceed));
			getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));
			//do authentication
			if (username != null && password != null) {
				getMethod.setDoAuthentication(true);
			}
			//set query string if any
			if (!StringUtils.isEmpty(preparedQueryString)) {
				if (!StringUtils.isEmpty(getMethod.getQueryString())) {
					getMethod.setQueryString(getMethod.getQueryString()+"&"+preparedQueryString);
				} else {
					getMethod.setQueryString(preparedQueryString);
				}
			}
		} else {
			//another request method than get or post
			throw new ComponentNotReadyException(this, "Unsupported request method: " + requestMethod);
		}

		// load request properties
		if (!StringUtils.isEmpty(rawRequestProperties)) {
			requestProperties = new Properties();
			try {
				requestProperties.load(new StringReader(rawRequestProperties));
			} catch (Exception e) {
				throw new ComponentNotReadyException(this, "Unexpected exception during request properties reading.", e);
			}

			// pass request properties to the http connection
			for (Entry<Object, Object> entry : requestProperties.entrySet()) {
				// httpConnection.addRequestProperty((String) entry.getKey(), (String) entry.getValue());
				if (requestMethod.equals(POST)) {
					postMethod.getParams().setParameter((String) entry.getKey(), (String) entry.getValue());
				} else if (requestMethod.equals(GET)) {
					getMethod.getParams().setParameter((String) entry.getKey(), (String) entry.getValue());
				}
			}
			
			for(Entry<Object, Object> entry : requestProperties.entrySet()) {
				if (requestMethod.equals(POST)) {
					postMethod.addRequestHeader((String) entry.getKey(), (String) entry.getValue());
				} else if (requestMethod.equals(GET)) {
					getMethod.addRequestHeader((String) entry.getKey(), (String) entry.getValue());
				}
			}
		}
		

	}

	/**
	 * Prepares all parts of http request.
	 * @throws ComponentNotReadyException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void prepareHttpRequest() throws ComponentNotReadyException, IOException, InterruptedException {

		//parse URL
		//placeholder format: *{placeholder_name}
		String tempUrl = "";
		StringTokenizer st = new StringTokenizer(rawUrl, "*");
		while (st.hasMoreTokens()) {
			tempUrl += st.nextToken();
		}

		//substitute placeholders
		rawUrlToProceed = "";
		Set<String> substituedPlaceHolders = new HashSet<String>();
		while (tempUrl.indexOf("{") > 0 && tempUrl.length() > 0) {
			rawUrlToProceed += tempUrl.substring(0, tempUrl.indexOf("{"));
			String propertyName = tempUrl.substring(tempUrl.indexOf("{") + 1, tempUrl.indexOf("}"));

			DataField field = record.getField(propertyName);
			if (field != null) {
				rawUrlToProceed += field.toString();
				substituedPlaceHolders.add(propertyName);
			} else {
				rawUrlToProceed += "*";
			}
			tempUrl = tempUrl.substring(tempUrl.indexOf("}") + 1, tempUrl.length());
		}
		rawUrlToProceed += tempUrl;

		//there are some input fields which should be add to the request
		if (addInputFieldsAsParameters) {
			//find out which metadata fields weren't used for substitution of placeholders
			List<String> unusedMetadata = new ArrayList<String>();
			DataRecordMetadata metadata = inPort.getMetadata();
			String[] metadataNames = metadata.getFieldNamesArray();
			for (String metadataName : metadataNames) {
				if (!substituedPlaceHolders.contains(metadataName)) {
					unusedMetadata.add(metadataName);
				}
			}

			//find out which metadata fields shoud be ignored
			if (ignoredFields != null) {
				StringTokenizer tokenizer = new StringTokenizer(ignoredFields, ";");
				while (tokenizer.hasMoreTokens()) {
					unusedMetadata.remove(tokenizer.nextToken());
				}
			}
			
			if (requestMethod.equals(POST) && addInputFieldsAsParametersTo.equals("BODY")) {
				//request method is post and some metadata fields are add to the method body
				body = new NameValuePair[unusedMetadata.size()];
				int i = 0;
				for (String property : unusedMetadata) {
					NameValuePair pair = new NameValuePair(property, record.getField(property).toString());
					body[i] = pair;
				}
			} else {
				//metadata fields are add to the query string
				preparedQueryString = "";
				for (String property : unusedMetadata) {
					NameValuePair pair = new NameValuePair(property, record.getField(property).toString());
					preparedQueryString += pair.getName() + "=" + pair.getValue() + "&";
				}
				if (preparedQueryString.length() > 0) {
					preparedQueryString = preparedQueryString.substring(0, preparedQueryString.length() - 1);
				}
			}
		}

		//parse multipart entities
		if (multipartEntities != null) {
			StringTokenizer parts = new StringTokenizer(multipartEntities, ";");
			stringParts = new StringPart[parts.countTokens()];
			for (int i = 0; parts.hasMoreTokens(); i++) {
				String token = parts.nextToken();
				String value = record.getField(token).toString();
				stringParts[i] = new StringPart(token, value);
			}
		}

		if (rawUrlToProceed.indexOf("*{") > 0) {
			//some placeholder wasn't substituted. This should never happen.
			throw new ComponentNotReadyException("Invalid URL.");
		}
	}

	/**
	 * 
	 * @return true if and only if there are suitable metadata fields for substitution of all placeholders
	 */
	private boolean isPossibleToMapVariables() {

		boolean possibleToMapVariables = true;
		try {
			String tempUrl = "";
			if (rawUrl.indexOf("*") > 0) {
				StringTokenizer st = new StringTokenizer(rawUrl, "*");
				while (st.hasMoreTokens()) {
					tempUrl += st.nextToken();
				}
			} else {
				tempUrl = rawUrl;
			}

			Set<String> variablesAtUrl = new HashSet<String>();
			while (tempUrl.indexOf("{") > 0 && tempUrl.length() > 0) {
				String propertyName = tempUrl.substring(tempUrl.indexOf("{") + 1, tempUrl.indexOf("}"));
				tempUrl = tempUrl.substring(tempUrl.indexOf("}") + 1, tempUrl.length());
				variablesAtUrl.add(propertyName);
			}

			Set<String> variablesAtMetadata = new HashSet<String>();
			DataRecordMetadata metadata = inPort.getMetadata();
			String[] metadataNames = metadata.getFieldNamesArray();
			for (String metadataName : metadataNames) {
				variablesAtMetadata.add(metadataName);
			}

			for (String urlPlaceholder : variablesAtUrl) {
				if (!variablesAtMetadata.contains(urlPlaceholder)) {
					possibleToMapVariables = false;
					break;
				}
			}
		} catch (Exception e) {
			possibleToMapVariables = false;
		}
		return possibleToMapVariables;
	}
	

	/**
	 * @return String representation of result of the HTTP request
	 * @throws IOException
	 */
	private String getRequestResult() throws IOException {
		InputStream result = null;
		
	    if (requestMethod.equals(POST)) {
			result = postMethod.getResponseBodyAsStream();
		} else if (requestMethod.equals(GET)) {
			result = getMethod.getResponseBodyAsStream();
		}

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
		    logger.error("Unable to read request result. Caused by: "+e.getMessage());
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
		
		StringRequestEntity entity = new StringRequestEntity(input);
		if(requestMethod.equals(POST))  {
		   this.postMethod.setRequestEntity(entity);
		   int resultCode = this.httpClient.executeMethod(postMethod);
		   if(resultCode != 200) {
			   logger.warn("Returned code for http request "+rawUrlToProceed+" is " + resultCode);
		   }
		}
		
	

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
	 * @param username
	 *            the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @param password
	 *            the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param addInputFieldsAsParameters
	 *            the addInputFieldsAsParameters to set
	 */
	public void setAddInputFieldsAsParameters(boolean addInputFieldsAsParameters) {
		this.addInputFieldsAsParameters = addInputFieldsAsParameters;
	}

	/**
	 * @return the addInputFieldsAsParameters
	 */
	public boolean getAddInputFieldsAsParameters() {
		return addInputFieldsAsParameters;
	}

	/**
	 * @param ignoredFields
	 *            the ignoredFields to set
	 */
	public void setIgnoredFields(String ignoredFields) {
		this.ignoredFields = ignoredFields;
	}

	/**
	 * @return the ignoredFields
	 */
	public String getIgnoredFields() {
		return ignoredFields;
	}

	/**
	 * @param multipartField
	 *            the multipartField to set
	 */
	public void setMultipartEntities(String multipartEntities) {
		this.multipartEntities = multipartEntities;
	}

	/**
	 * @return the multipartField
	 */
	public String getMultipartEntities() {
		return multipartEntities;
	}

	/**
	 * @param urlInputField the urlInputField to set
	 */
	public void setUrlInputField(String urlInputField) {
		this.urlInputField = urlInputField;
	}

	/**
	 * @return the urlInputField
	 */
	public String getUrlInputField() {
		return urlInputField;
	}

	/**
	 * @param addInputFieldsAsParametersTo the addInputFieldsAsParametersTo to set
	 */
	public void setAddInputFieldsAsParametersTo(String addInputFieldsAsParametersTo) {
		this.addInputFieldsAsParametersTo = addInputFieldsAsParametersTo;
	}

	/**
	 * @return the addInputFieldsAsParametersTo
	 */
	public String getAddInputFieldsAsParametersTo() {
		return addInputFieldsAsParametersTo;
	}

	/**
	 * @param authenticationMethod the authenticationMethod to set
	 */
	public void setAuthenticationMethod(String authenticationMethod) {
		this.authenticationMethod = authenticationMethod;
	}

	/**
	 * @return the authenticationMethod
	 */
	public String getAuthenticationMethod() {
		return authenticationMethod;
	}

}