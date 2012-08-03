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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import oauth.signpost.OAuthConsumer;
import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.params.AuthPolicy;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.DefaultTargetAuthenticationHandler;
import org.apache.http.impl.conn.SingleClientConnManager;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Level;
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
import org.jetel.exception.TempFileCreationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.graph.runtime.tracker.ReformatComponentTokenTracker;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CTLMapping;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
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
 *  <tr><td><b>consumerKey</b></td><td>Consumer key to be used with OAuth authentication</td>
 *  <tr><td><b>consumerSecret</b></td><td>Consumer secret to be used with OAuth authentication</td>
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
	/**
	 * 
	 */
	private static final String DUMMY_DELIMITER = ";";

	private final static Log logger = LogFactory.getLog(HttpConnector.class);
	
	/**
	 * Type of this component.
	 */
	private final static String COMPONENT_TYPE = "HTTP_CONNECTOR";
	
	
    /** 
     * The port index used for data record input
     */
    private static final int INPUT_PORT_NUMBER = 0;

    /** 
     * The port index used for data record output for tokens with successful status
     */
    private static final int STANDARD_OUTPUT_PORT_NUMBER = 0;

    /** 
     * The port index used for optional data record output for tokens with non-successful status
     */
    private static final int ERROR_OUTPUT_PORT_NUMBER = 1;
	
    /**
     * Valid authentication methods.
     */
    private static final Set<String> SUPPORTED_AUTHENTICATION_METHODS = new HashSet<String>();
    static {
    	SUPPORTED_AUTHENTICATION_METHODS.add("BASIC");
    	SUPPORTED_AUTHENTICATION_METHODS.add("DIGEST");
    	SUPPORTED_AUTHENTICATION_METHODS.add("ANY");
    }
    
    /**
     * Valid authentication methods.
     */
    private static final Set<String> SUPPORTED_REQUEST_METHODS = new HashSet<String>();
    static {
    	SUPPORTED_REQUEST_METHODS.add("GET");
    	SUPPORTED_REQUEST_METHODS.add("POST");
    }
    
    /**
     * Value representing the GET method of performing HTTP request.
     */
	private static final String GET = "GET";

	/**
     * Value representing the POST method of performing HTTP request.
     */
	private static final String POST = "POST";


	
	/**
	 * The name of an XML attribute representing target URL.
	 */
	private final static String XML_URL_ATTRIBUTE = "url";

	/**
	 * The name of an XML attribute representing request method.
	 */
	private final static String XML_REQUEST_METHOD_ATTRIBUTE = "requestMethod";

	/**
	 * The name of an XML attribute representing request content.
	 */
	private final static String XML_REQUEST_CONTENT_ATTRIBUTE = "requestContent";

	/**
	 * The name of an XML attribute representing input file URL.
	 */
	private final static String XML_INPUT_FILEURL_ATTRIBUTE = "inFileUrl";

	/**
	 * The name of an XML attribute representing output file URL.
	 */
	private final static String XML_OUTPUT_FILEURL_ATTRIBUTE = "outFileUrl";
	
	/**
	 * The name of an XML attribute representing if the output should be appended.
	 */
	private final static String XML_APPEND_OUTPUT_ATTRIBUTE = "appendOutput";
	
	/**
	 * The name of an XML attribute representing additional header parameters.
	 */
	private final static String XML_HEADER_PROPERTIES_ATTRIBUTE = "headerProperties";

	/**
	 * The name of an XML attribute representing input field name, holding request content.
	 */
	private final static String XML_INPUT_PORT_FIELD_NAME = "inputField";

	/**
	 * The name of an XML attribute representing output field name, where the response should be written.
	 */
	private final static String XML_OUTPUT_PORT_FIELD_NAME = "outputField";

	/**
	 * The name of an XML attribute representing character set to use.
	 */
	private final static String XML_CHARSET_ATTRIBUTE = "charset";

	/**
	 * The name of an XML attribute representing authentication method.
	 */
	private final static String XML_AUTHENTICATION_METHOD_ATTRIBUTE = "authenticationMethod";

	/**
	 * The name of an XML attribute representing user name.
	 */
	private final static String XML_USERNAME_ATTRIBUTE = "username";

	/**
	 * The name of an XML attribute representing password.
	 */
	private final static String XML_PASSWORD_ATTRIBUTE = "password";

	/**
	 * The name of an XML attribute representing flag indicating, whether fields of the input metadata should be added as parameters to the query.
	 */
	private final static String XML_ADD_INPUT_FIELDS_AS_PARAMETERS_ATTRIBUTE = "addInputFieldsAsParameters";

	/**
	 * The name of an XML attribute representing a way in which the fields of the input metadataq are added as parameters to the request.
	 */
	private final static String XML_ADD_INPUT_FIELDS_AS_PARAMETERS_TO_ATTRIBUTE = "addInputFieldsAsParametersTo";
	
	/**
	 * The name of an XML attribute representing fields, that shouldn't be added as parameters to the request.
	 */
	private final static String XML_IGNORED_FIELDS_ATTRIBUTE = "ignoredFields";

	/**
	 * The name of an XML attribute representing multipart entities.
	 */
	private final static String XML_MULTIPART_ENTITIES_FIELDS_LIST_ATTRIBUTE = "multipartEntities";
	
	/**
	 * The name of an XML attribute representing field holding a target URL.
	 */
	private final static String XML_URL_FROM_INPUT_FIELD_ATTRIBUTE = "urlInputField";

	/**
	 * The name of an XML attribute representing flag indicating, whether the response should be stored in temporary file.
	 */
	private final static String XML_STORE_RESPONSE_TO_TEMP_FILE = "responseAsFileName";

	/**
	 * The name of an XML attribute representing prefix of the temporary files, used to store responses
	 */
	private final static String XML_TEMPORARY_FILE_PREFIX = "responseFilePrefix";


	/** 
     * The name of an XML attribute used to define input mapping.
     */
	public static final String XML_INPUT_MAPPING_ATTRIBUTE = "inputMapping";

    /** 
     * The name of an XML attribute used to define output mapping to the first standard output port 
     */
	public static final String XML_STANDARD_OUTPUT_MAPPING_ATTRIBUTE = "standardOutputMapping";

    /** 
     * The name of an XML attribute used to define output mapping to the second error output port
     */
	public static final String XML_ERROR_OUTPUT_MAPPING_ATTRIBUTE = "errorOutputMapping";

    /** the name of an XML attribute for error output redirection */
	public static final String XML_REDIRECT_ERROR_OUTPUT = "redirectErrorOutput";
	
	/**
	 * The name of an XML attribute representing consumer key, used for OAuth authentication
	 */
	private final static String XML_CONSUMER_KEY_ATTRIBUTE = "consumerKey";

	/**
	 * The name of an XML attribute representing consumer secret, used for OAuth authentication
	 */
	private final static String XML_CONSUMER_SECRET_ATTRIBUTE = "consumerSecret";
	
	
	/**
	 * Default value of the 'append output' flag 
	 */
	private final static boolean DEFAULT_APPEND_OUTPUT = false;

    /**
     * Input record name.
     */
    private static final String INPUT_RECORD_NAME = "Input record";

    /**
     * Output record name.
     */
    private static final String STANDARD_OUTPUT_RECORD_NAME = "Standard output record";

    /**
     * Output record name.
     */
    private static final String ERROR_OUTPUT_RECORD_NAME = "Error output record";
    
    /*  === Result metadata ===  */

	/**
	 * Name of the result record
	 */
    private static final String RESULT_RECORD_NAME = "Result";
	
	/**
	 * Result field representing the content of the response
	 */
    private static final int RP_CONTENT_INDEX = 0;
    private static final String RP_CONTENT_NAME = "content";

	/**
	 * Result field representing the URL of an output file
	 */
    private static final int RP_OUTPUTFILE_INDEX = 1;
    private static final String RP_OUTPUTFILE_NAME = "outputFilePath";
    
	/**
	 * Result field representing the status code of the response
	 */
    private static final int RP_STATUS_CODE_INDEX = 2;
    private static final String RP_STATUS_CODE_NAME = "statusCode";

    /**
     * Result field representing the header of the response
     */
    private static final int RP_HEADER_INDEX = 3;
    private static final String RP_HEADER_NAME = "header";

   	/**
	 * Result field representing the error message (can have a value only if error port is redirected to std port)
	 */
    private static final int RP_MESSAGE_INDEX = 4;
    private static final String RP_MESSAGE_NAME = "errorMessage";

    
    /*  === Error metadata ===  */

	/**
	 * Name of the error record
	 */
    private static final String ERROR_RECORD_NAME = "Error";
	
	/**
	 * Error field representing the error message
	 */
    private static final int EP_MESSAGE_INDEX = 0;
    private static final String EP_MESSAGE_NAME = RP_MESSAGE_NAME;

    
    
    private interface ResponseWriter {
		public void writeResponse(HttpResponse response) throws IOException;
	}

	/**
	 * Writer that sends response directly stored in String field
	 */
	private class ResponseByValueWriter implements ResponseWriter {

		private final DataField outputField;

		public ResponseByValueWriter(DataField outputField) {
			this.outputField = outputField;
		}

		@Override
		public void writeResponse(HttpResponse response) throws IOException {
			if (outputField != null) {
				outputField.setValue(getResponseContent(response));
			}
		}
	}

	private abstract class AbstractResponseFileWriter implements ResponseWriter {

		private final DataField fileURLField;

		public AbstractResponseFileWriter(DataField fileURLField) {
			this.fileURLField = fileURLField;
		}

		/** Returns a file output channel. The method is guaranteed to be called before the 
		 *  {@link #getFileOutputPath()}, so that the implementation may cache the data.
		 * 
		 * @return a file output channel
		 */
		abstract protected WritableByteChannel getFileOutputChannel() throws IOException;

		/** Returns an output file path. The method is guaranteed to be called after the 
		 *  {@link #getFileOutputChannel()}, so that the implementation may cache the data.
		 * 
		 * @return an output file path
		 */
		abstract protected String getFileOutputPath() throws IOException;
		
		@Override
		public void writeResponse(HttpResponse response) throws IOException {
			WritableByteChannel outputChannel = getFileOutputChannel();
			
			if (outputChannel == null) {
				outputChannel = Channels.newChannel(System.out);
			}
			
			ReadableByteChannel inputConnection = Channels.newChannel(response.getEntity().getContent());
			
			if (inputConnection != null) {
				try {
					StreamUtils.copy(inputConnection, outputChannel);
				} finally {
					inputConnection.close();
				}
			}
			
			// populate output field with the temporary file name
			if (fileURLField != null) {
				fileURLField.setValue(getFileOutputPath());
			}
		}
	}
	
	private class ResponseTempFileWriter extends AbstractResponseFileWriter {

		private final String prefix;
		private File tempFile = null;

		public ResponseTempFileWriter(DataField fileURLField, String prefix) {
			super(fileURLField);
			this.prefix = prefix;
		}

		@Override
		protected WritableByteChannel getFileOutputChannel() throws IOException {
			tempFile = null;
			try {
				tempFile = getGraph().getAuthorityProxy().newTempFile(prefix, ".tmp", -1);
				
				return Channels.newChannel(new FileOutputStream(tempFile, appendOutputToUse));
			} catch (TempFileCreationException e) {
				throw new IOException(e);
			}
		}		

		@Override
		protected String getFileOutputPath() throws IOException {
			if (tempFile != null) {
				return tempFile.getAbsolutePath();
			}
			
			return null;
		}
	}

	private class ResponseFileWriter extends AbstractResponseFileWriter {

		private final String fileName;

		public ResponseFileWriter(DataField fileURLField, String fileName) {
			super(fileURLField);
			
			this.fileName = fileName;
		}

		
		@Override
		protected WritableByteChannel getFileOutputChannel() throws IOException {
			return FileUtils.getWritableChannel(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, fileName, appendOutputToUse);
		}		

		@Override
		protected String getFileOutputPath() throws IOException {
			File file = FileUtils.getJavaFile(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, fileName); 
			if (file != null) {
				return file.getAbsolutePath();
			}
			
			return null;
		}
	}
	
	public static class HTTPConnectorException extends Exception {
		private static final long serialVersionUID = 1L;

		/**
		 * 
		 */
		public HTTPConnectorException() {
			super();
		}

		/**
		 * @param message
		 * @param cause
		 */
		public HTTPConnectorException(String message, Throwable cause) {
			super(message, cause);
		}

		/**
		 * @param message
		 */
		public HTTPConnectorException(String message) {
			super(message);
		}

		/**
		 * @param cause
		 */
		public HTTPConnectorException(Throwable cause) {
			super(cause);
		}
	}
	
	/** Class representing a result of the request.
	 * 
	 * @author Tomas Laurincik (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 8.6.2012
	 */
	protected static class RequestResult {
		private HttpResponse response;
		private Exception exception;
		
		public HttpResponse getResponse() {
			return response;
		}
		
		public void setResponse(HttpResponse response) {
			this.response = response;
		}
		
		public Exception getException() {
			return exception;
		}
		
		public void setException(Exception exception) {
			this.exception = exception;
		}
	}
	
	
    /* === Component context ===  */
    
	/**
     * only optional input port or <code>null</code> if no input edge is assigned
     */
    private InputPort inputPort;
    
    /**
     * standard output port or <code>null</code> if no output edge is assigned
     */
    private OutputPort standardOutputPort;
    
    /**
     * optional error output port or <code>null</code> if no error output edge is assigned
     */
    private OutputPort errorOutputPort;
    
    /**
     * Is the optional input port attached.
     */
    private boolean hasInputPort;
    
    /**
     * Is the optional standard output port attached.
     */
    private boolean hasStandardOutputPort;
    
    /**
     * Is the optional error output port attached.
     */
    protected boolean hasErrorOutputPort;
    
	/**
     * Record which is used for input mapping as other configuration attributes.
     */
    protected DataRecord inputParamsRecord;	

    /**
     * Record with run status information.
     */
    protected DataRecord resultRecord;
    
    /**
     * Record with run status information.
     */
    protected DataRecord errorRecord;
    
    /**
     * Input port record or null if input port is not attached.
     */
    protected DataRecord inputRecord;
	
    /**
     * Input records for input mapping transformation.
     */
    protected DataRecord[] inputMappingInRecords;

    /**
     * Output records for input mapping transformation.
     */
    protected DataRecord[] inputMappingOutRecords;

    /**
     * Input records for output mapping transformation - standard output mapping.
     */
    protected DataRecord[] standardOutputMappingInRecords;

    /**
     * Input records for output mapping transformation - error output mapping.
     */
    protected DataRecord[] errorOutputMappingInRecords;

    /**
     * Output records for standard output mapping transformation.
     */
    protected DataRecord[] standardOutputMappingOutRecords;

    /**
     * Output records for error output mapping transformation.
     */
    protected DataRecord[] errorOutputMappingOutRecords;	
    
    
	/*  === Component base properties === */

	/**
	 * Name of the run configuration record
	 */
    private static final String ATTRIBUTES_RECORD_NAME = "Attributes";
    
	/** 
	 * URL to which the HTTPConnector should connect. 
	 */
	private String rawUrl;
	private String rawUrlToUse;
    private static final int IP_URL_INDEX = 0;
    private static final String IP_URL_NAME = "URL";
	
	/** 
	 * Field of the input metadata containing the target URL. 
	 */
	private String urlInputField;
//	private String urlInputFieldToUse;
//    private static final int IP_URL_FIELD_INDEX = 1;
//    private static final String IP_URL_FIELD_NAME = "urlInputField";

	/** 
	 * Request method to be used.
	 */
	private String requestMethod;
	private String requestMethodToUse;
    private static final int IP_REQUEST_METHOD_INDEX = 1;
    private static final String IP_REQUEST_METHOD_NAME = "requestMethod";
    	
	/** 
	 * <code>true</code> if the fields of the input metadata should be added as parameters to the request. 
	 */
	private boolean addInputFieldsAsParameters;
	private Boolean addInputFieldsAsParametersToUse;
	private static final int IP_ADD_INPUT_FIELDS_AS_PARAMETERS_INDEX = 2;
    private static final String IP_ADD_INPUT_FIELDS_AS_PARAMETERS_NAME = "addInputFieldsAsParameters";
    
	/** 
	 * String representing a way, the parameters are passed (QUERY, BODY, ...).
	 */
	private String addInputFieldsAsParametersTo;
	private String addInputFieldsAsParametersToToUse;
	private static final int IP_ADD_INPUT_FIELDS_AS_PARAMETERS_TO_INDEX = 3;
    private static final String IP_ADD_INPUT_FIELDS_AS_PARAMETERS_TO_NAME = "addInputFieldsAsParametersTo";
    
	/** 
	 * Fields that should not be added as parameters to the request. 
	 */
	private String ignoredFields;
	private String ignoredFieldsToUse;
	private static final int IP_IGNORED_FIELDS_INDEX = 4;
    private static final String IP_IGNORED_FIELDS_NAME = "ignoredFields";

    /** 
	 *  String representing properties, that should be used as additional HTTP headers.
	 */
	private String rawRequestProperties;
	private String rawRequestPropertiesToUse;
	private static final int IP_REQUEST_PROPERTIES_INDEX = 5;
    private static final String IP_REQUEST_PROPERTIES_NAME = "additionalHTTPHeaderProperties";
    
    /**  
	 * Character set that should be used.
	 */
	private String charset;
	private String charsetToUse;
    private static final int IP_CHARSET_INDEX = 6;
    private static final String IP_CHARSET_NAME = "charset";
        
	/** 
	 * Name of the field of the input metadata, that contains the request content. 
	 */
	private String inputFieldName;
//	private String inputFieldNameToUse;
//	private static final int IP_INPUT_FIELD_NAME_INDEX = 5;
//    private static final String IP_INPUT_FIELD_NAME_NAME = "inputFieldName";

	/**  
	 * Name of field of the output metadata, that should be populated with the response.
	 */
	private String outputFieldName;
//	private String outputFieldNameToUse;
//    private static final int IP_OUTPUT_FIELD_NAME_INDEX = 6;
//    private static final String IP_OUTPUT_FIELD_NAME_NAME = "outputFieldName";
    	
    /** 
	 * String representing request content. 
	 */
	private String requestContent;
	private String requestContentToUse;
	private static final int IP_REQUEST_CONTENT_INDEX = 7;
    private static final String IP_REQUEST_CONTENT_NAME = "requestContent";

	/**  
	 * URL of the file containing request content.
	 */
	private String inputFileUrl;
	private String inputFileUrlToUse;
    private static final int IP_INPUT_FILE_URL_INDEX = 8;
    private static final String IP_INPUT_FILE_URL_NAME = "inputFileUrl";
    

	/**  
	 * URL of the file where the response should be stored.
	 */
	private String outputFileUrl;
	private String outputFileUrlToUse;
	private static final int IP_OUTPUT_FILE_URL_INDEX = 9;
    private static final String IP_OUTPUT_FILE_URL_NAME = "outputFileUrl";
   
	/**  
	 * <code>True</code>, if the response should be appended to the output file, <code>false</code> if the file should be overwritten.
	 */
	private boolean appendOutput;
	private Boolean appendOutputToUse;
	private static final int IP_APPEND_OUTPUT_INDEX = 10;
    private static final String IP_APPEND_OUTPUT_NAME = "appendOutput";
   	
	/** 
	 * Authentication method to be used (BASIC, DIGEST, ...).
	 */
	private String authenticationMethod;
	private String authenticationMethodToUse;
	private static final int IP_AUTHENTICATION_METHOD_INDEX = 11;
    private static final String IP_AUTHENTICATION_METHOD_NAME = "authenticationMethod";
    
	/** 
	 * User 	name to be used for authentication.
	 */
	private String username;
	private String usernameToUse;
	private static final int IP_USERNAME_INDEX = 12;
    private static final String IP_USERNAME_NAME = "username";
    
	/** 
	 * Password to be used for authentication.
	 */
    private String password;
	private String passwordToUse;
	private static final int IP_PASSWORD_INDEX = 13;
    private static final String IP_PASSWORD_NAME = "password";
    
	/**
	 * Consumer key used for OAuth authentication
	 */
	private String consumerKey;
	private String consumerKeyToUse;
	private static final int IP_CONSUMER_KEY_INDEX = 14;
    private static final String IP_CONSUMER_KEY_NAME = "consumerKey";
 
	/**
	 * Consumer secret used for OAuth authentication
	 */
	private String consumerSecret;
	private String consumerSecretToUse;
    private static final int IP_CONSUMER_SECRET_INDEX = 15;
    private static final String IP_CONSUMER_SECRET_NAME = "consumerSecret";
    
	/**
	 * <code>true</code> if the responses should be stored to temporary files, <code>false</code> otherwise.  
	 */
	private boolean storeResponseToTempFile;
	private Boolean storeResponseToTempFileToUse;
	private static final int IP_STORE_RESPONSE_TO_TEMP_INDEX = 16;
    private static final String IP_STORE_RESPONSE_TO_TEMP_NAME = "storeResponseToTempFile";
    
	/** 
	 * Prefix used for the temporary files with responses.
	 */
	private String temporaryFilePrefix;
	private String temporaryFilePrefixToUse;
	private static final int IP_TEMP_FILE_PREFIX_INDEX = 17;
    private static final String IP_TEMP_FILE_PREFIX_NAME = "temporaryFilePrefix";
    
	/**
	 * String representing fields used as multipart entities.
	 */
	private String multipartEntities;
	private String multipartEntitiesToUse;
	private static final int IP_MULTIPART_ENTITIES_INDEX = 18;
    private static final String IP_MULTIPART_ENTITIES_NAME = "multipartEntities";
    


	/* === Job flow related properties ===  */
	
	/** 
	 * Source code of the input mapping.
	 */
	private String inputMapping; 

	/** 
	 * Source code of the error output mapping.
	 */
	private String errorOutputMapping;

	/** 
	 * Source code of the output mapping.
	 */
    private String standardOutputMapping;

    /**
     * Enables redirection of error output to standard output. Disabled by default.
     */
    private boolean redirectErrorOutput;
	

	
    /* === Processed properties ===  */
    
    /** 
     * Structure holding the user authentication details.
     */
	private UsernamePasswordCredentials creds;

	/**
	 * Standard output field.
	 */
	private StringDataField outField;
	
	/** 
	 * Parsed request properties - used as the additional HTTP headers. 
	 */
	private Properties requestProperties;
	
	/**
	 * Fields that should be ignored
	 */
	private Set<String> ignoredFieldsSet = new HashSet<String>();
	
	/**
	 * Runtime for input mapping.
	 */
    protected CTLMapping inputMappingTransformation;
    
	/**
	 * Runtime for input mapping.
	 */
    protected CTLMapping standardOutputMappingTransformation;

	/**
	 * Runtime for input mapping.
	 */
    protected CTLMapping errorOutputMappingTransformation;

	
    /* === Component state properties per record ===  */
	
    /**
     * Standard output port record or null if standard output port is not attached.
     */
	private DataRecord standardOutputRecord;
    
    /**
     * Error output port record or null if error output port is not attached.
     */
	private DataRecord errorOutputRecord;
	
    /**
     * Result of the processing of one record.
     */
    private RequestResult result;
	
    /* === Tools used ===  */
	
	/**
	 * Resolver for substituting field references.
	 */
	private PropertyRefResolver refResolver = new PropertyRefResolver();;

	/**
	 * Utility class for writing the response content to file or output port.
	 */
	private ResponseWriter responseWriter;
	
	/**
	 * HttpClient used for HTTP communication.
	 */
	private DefaultHttpClient httpClient;

	/**
	 * OAuth consumer used to sign requests when OAuth is used.
	 */
	private OAuthConsumer oauthConsumer;
	
	/**
	 * Request interceptor used for logging.
	 */
	private HttpRequestInterceptor requestLoggingInterceptor = new HttpRequestInterceptor() {
		
				@Override
				public void process(HttpRequest paramHttpRequest, HttpContext paramHttpContext) throws HttpException, IOException {
					logger.debug("Sending HTTP request:\n\n" + buildRequestLogString(paramHttpRequest));
				}
			};
	
	/**
	 * Response interceptor used for logging.
	 */
	private HttpResponseInterceptor responseLoggingInterceptor = new HttpResponseInterceptor() {
		
				@Override
				public void process(HttpResponse paramHttpResponse, HttpContext paramHttpContext) throws HttpException, IOException {
					logger.debug("Received HTTP response:\n\n" + buildResponseLogString(paramHttpResponse));
				}
			};
	
	
	/** Helper class holding a name and body part for multipart message.
	 * 
	 * @author Tomas Laurincik (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 5.6.2012
	 */
	protected static class PartWithName {
		public String name;
		public StringBody value;

		public PartWithName(String name, StringBody value) {
			super();
			this.name = name;
			this.value = value;
		}
	}

	/** Bean representing configuration of one HTTP request
	 * 
	 * @author Tomas Laurincik (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 4.6.2012
	 */
	protected static class HTTPRequestConfiguration {
		private String target;
		private String proxy;
		private Map<String, String> parameters = new HashMap<String, String>();
		private Map<String, String> multipartEntities = new HashMap<String, String>();
		private String content;
		
		private URL targetURL;
		private URL proxyURL;
		

		public HTTPRequestConfiguration() {
			super();
		}

		public void prepare() throws ComponentNotReadyException {
			if (getTarget() != null) {
				try {
					targetURL = new URL(getTarget());
				} catch (MalformedURLException e) {
					throw new ComponentNotReadyException("Given target URL '" + getTarget() + "' is invalid.");
				}
			}

			if (getProxy() != null) {
				try {
					proxyURL = new URL(getProxy());
				} catch (MalformedURLException e) {
					throw new ComponentNotReadyException("Given proxy URL '" + getProxy() + "' is invalid.");
				}
			}
		}
		
		public String getTarget() {
			return target;
		}

		public void setTarget(String target) {
			this.target = target;
		}

		public String getProxy() {
			return proxy;
		}

		public void setProxy(String proxy) {
			this.proxy = proxy;
		}

		public Map<String, String> getParameters() {
			return parameters;
		}

		public void setParameters(Map<String, String> parameters) {
			this.parameters = parameters;
		}

		public Map<String, String> getMultipartEntities() {
			return multipartEntities;
		}

		public void setMultipartEntities(Map<String, String> multipartEntities) {
			this.multipartEntities = multipartEntities;
		}
		
		public String getContent() {
			return content;
		}

		public void setContent(String content) {
			this.content = content;
		}
		
		public URL getTargetURL() {
			return targetURL;
		}

		public void setTargetURL(URL targetURL) {
			this.targetURL = targetURL;
		}

		public URL getProxyURL() {
			return proxyURL;
		}

		public void setProxyURL(URL proxyURL) {
			this.proxyURL = proxyURL;
		}

		public void copyTo(HTTPRequestConfiguration configuration) {
			configuration.setTarget(target);
			configuration.setProxy(proxy);
			configuration.setContent(content);
			configuration.setParameters(new HashMap<String, String>(parameters));
			configuration.setMultipartEntities(new HashMap<String, String>(multipartEntities));
			
			configuration.setProxyURL(proxyURL);
			configuration.setTargetURL(targetURL);
		}
	}
	
    /** Creates new instance of the HTTPConnector with given id
     * 
     * @param id
     */
	public HttpConnector(String id) {
		super(id);
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized())
			return;
		
		super.init();
		
		// if charset isn't filled, we use the system default
		if (StringUtils.isEmpty(this.charset)) {
		    this.charset = Charset.defaultCharset().name();	
		}

		tryToInit(false);
		
		// create response writer based on the configuration
		if (getOutputFileUrl() != null) {
			responseWriter = new ResponseFileWriter(outField, outputFileUrl);
			
		} else if (getStoreResponseToTempFile()) {
			responseWriter = new ResponseTempFileWriter(outField, temporaryFilePrefix);
			
		} else {
			responseWriter = new ResponseByValueWriter(outField);
		}

		// build properties from raw request properties
		if (!StringUtils.isEmpty(rawRequestProperties)) {
			requestProperties = new Properties();
			try {
				requestProperties.load(new StringReader(rawRequestProperties));
			} catch (Exception e) {
				throw new ComponentNotReadyException(this, "Unexpected exception during request properties reading.", e);
			}
		}
	}

	/** Extracts a set of ignored field names from given string representation.
	 * 
	 * @param rawIgnoredFields
	 * @return a set of ignored field names from given string representation.
	 */
	private Set<String> extractIgnoredFields(String rawIgnoredFields) {
		Set<String> result = new HashSet<String>();

		//find out which metadata fields shoud be ignored
		if (!StringUtils.isEmpty(ignoredFieldsToUse)) {
			StringTokenizer tokenizer = new StringTokenizer(ignoredFieldsToUse, ";");
			while (tokenizer.hasMoreTokens()) {
				result.add(tokenizer.nextToken());
			}
		}		
		
		return result;
	}
	
	/** Returns a Boolean value of given field in input record.
	 * 
	 * @param index
	 * @return a Boolean value of given field in input record.
	 */
	private Boolean getBooleanInputParameterValue(int index, Boolean defaultValue) {
		Object value = inputParamsRecord.getField(index).getValue();
		
		if (Boolean.FALSE.equals(value)) {
			return false;
		} else if (Boolean.TRUE.equals(value)) {
			return true;
		} 
		
		return defaultValue;
	}	

	/** Returns a string value of the given field in input record.
	 * 
	 * @param index
	 * @return a string value of the given field.
	 */
	private String getStringInputParameterValue(int index) {
		Object value = inputParamsRecord.getField(index).getValue();
		if (value != null) {
			return value.toString();
		}
		
		return null;
	}
	
	/** Processes the input records. The values contained in the fields of the record are set as a 
	 *  properties of this components.
	 * 
	 */
	protected void processInputParamsRecord() {
		rawUrlToUse = getStringInputParameterValue(IP_URL_INDEX);
//		urlInputFieldToUse = getStringInputParameterValue(IP_URL_FIELD_INDEX);
		requestMethodToUse = getStringInputParameterValue(IP_REQUEST_METHOD_INDEX);
		requestContentToUse = getStringInputParameterValue(IP_REQUEST_CONTENT_INDEX);
		inputFileUrlToUse = getStringInputParameterValue(IP_INPUT_FILE_URL_INDEX);
//		inputFieldNameToUse = getStringInputParameterValue(IP_INPUT_FIELD_NAME_INDEX);
//		outputFieldNameToUse = getStringInputParameterValue(IP_OUTPUT_FIELD_NAME_INDEX);
		outputFileUrlToUse = getStringInputParameterValue(IP_OUTPUT_FILE_URL_INDEX);
		appendOutputToUse = getBooleanInputParameterValue(IP_APPEND_OUTPUT_INDEX, Boolean.FALSE);
		charsetToUse = getStringInputParameterValue(IP_CHARSET_INDEX);
		rawRequestPropertiesToUse = getStringInputParameterValue(IP_REQUEST_PROPERTIES_INDEX);
		addInputFieldsAsParametersToUse = getBooleanInputParameterValue(IP_ADD_INPUT_FIELDS_AS_PARAMETERS_INDEX, Boolean.FALSE);
		addInputFieldsAsParametersToToUse = getStringInputParameterValue(IP_ADD_INPUT_FIELDS_AS_PARAMETERS_TO_INDEX);
		ignoredFieldsToUse = getStringInputParameterValue(IP_IGNORED_FIELDS_INDEX);
		authenticationMethodToUse = getStringInputParameterValue(IP_AUTHENTICATION_METHOD_INDEX);
		usernameToUse = getStringInputParameterValue(IP_USERNAME_INDEX);
		passwordToUse = getStringInputParameterValue(IP_PASSWORD_INDEX);
		storeResponseToTempFileToUse = getBooleanInputParameterValue(IP_STORE_RESPONSE_TO_TEMP_INDEX, Boolean.FALSE);
		temporaryFilePrefixToUse = getStringInputParameterValue(IP_TEMP_FILE_PREFIX_INDEX);
		multipartEntitiesToUse = getStringInputParameterValue(IP_MULTIPART_ENTITIES_INDEX);
		consumerKeyToUse = getStringInputParameterValue(IP_CONSUMER_KEY_INDEX);
		consumerSecretToUse = getStringInputParameterValue(IP_CONSUMER_SECRET_INDEX);		
	}
	

	/** Resets the records in a given array.
	 * 
	 * @param records
	 */
	private static void resetRecords(DataRecord... records) {
		for (DataRecord record : records) {
			if (record != null) {
				record.reset();
			}
		}
	}	
	
	/** Returns <code>true</code> if the record does not have a field with the given name, <code>false</code> otherwise.
	 * 
	 * @param record
	 * @return <code>true</code> if the record does not have a field with the given name, <code>false</code> otherwise.
	 */
	private DataField getFieldSafe(DataRecord record, String name) {
		if (name == null) {
			return null;
		}
		
		for (String fieldName : record.getMetadata().getFieldNamesArray()) {
			if (name.equals(fieldName)) {
				return record.getField(name);
			}
		}
		
		return null;
	}
	
	
	/** Pre-process the record.
	 * 
	 * @throws ComponentNotReadyException
	 */
	private void preProcessForRecord() throws ComponentNotReadyException {
		ignoredFieldsSet = extractIgnoredFields(ignoredFieldsToUse);

//		if (inputFieldNameToUse != null) {
//			inField = (StringDataField) getFieldSafe(inputRecord, inputFieldNameToUse);
//			if (inField == null) {
//				throw new ComponentNotReadyException("Unknown input field name '" + inputFieldNameToUse + "'.");
//			}
//		}
//
//		// BACKWARD COMPATIBILITY: get the output field, where the content should be sent.
//		if (standardOutputRecord != null) {
//			if (outputFieldNameToUse == null) {
//				outField = (StringDataField) standardOutputRecord.getField(0);
//			} else {
//				outField = (StringDataField) getFieldSafe(standardOutputRecord, outputFieldNameToUse);
//				if (outField == null) {
//					throw new ComponentNotReadyException("Unknown output field name '" + outputFieldNameToUse + "'.");
//				}
//			}
//		}
		
		// create response writer based on the configuration
		if (outputFileUrlToUse != null) {
			responseWriter = new ResponseFileWriter(resultRecord != null? resultRecord.getField(RP_OUTPUTFILE_INDEX) : null, outputFileUrlToUse);
			
		} else if (storeResponseToTempFileToUse) {
			responseWriter = new ResponseTempFileWriter(resultRecord != null? resultRecord.getField(RP_OUTPUTFILE_INDEX) : null, temporaryFilePrefixToUse);
			
		} else {
			responseWriter = new ResponseByValueWriter(resultRecord != null? resultRecord.getField(RP_CONTENT_INDEX) : null);
		}

		// filter multipart entities (ignored fields are removed from multipart entities record)
		if (!ignoredFieldsSet.isEmpty() && !StringUtils.isEmpty(multipartEntities)) {
			List<String> multipartEntitiesList = new ArrayList<String>();
			StringTokenizer tokenizer = new StringTokenizer(multipartEntities, ";");
			while (tokenizer.hasMoreElements()) {
				multipartEntitiesList.add(tokenizer.nextToken());
			}

			// remove ignored fields
			multipartEntitiesList.removeAll(ignoredFieldsSet);
			
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
		
		
		// build properties from raw request properties
		if (!StringUtils.isEmpty(rawRequestPropertiesToUse)) {
			requestProperties = new Properties();
			try {
				requestProperties.load(new StringReader(rawRequestPropertiesToUse));
			} catch (Exception e) {
				throw new ComponentNotReadyException(this, "Unexpected exception during request properties reading.", e);
			}
		}
	}

	/** Builds a string from given request.
	 * 
	 * @param message
	 * @return a string from given request.
	 */
	private String buildRequestLogString(HttpRequest message) {
		if (message == null) {
			return "<null>";
		}
		
		StringBuilder result = new StringBuilder();
		if (message.getRequestLine() != null) {
			result.append(message.getRequestLine().toString()).append("\n");
		}
		
		if (message.getAllHeaders() != null) {
			for (Header header : message.getAllHeaders()) {
				result.append(header.toString()).append("\n");
			}
		}
		
		if (message instanceof HttpEntityEnclosingRequest) {
			HttpEntity entity = ((HttpEntityEnclosingRequest) message).getEntity();
			if (entity != null) {
				if (entity.isRepeatable()) {
					try {
						ByteArrayOutputStream entityBytes = new ByteArrayOutputStream();
						entity.writeTo(entityBytes);
						result.append(new String(entityBytes.toByteArray(), entity.getContentEncoding() == null?  Charset.defaultCharset().name() : entity.getContentEncoding().getValue()));
					} catch (IOException e) {
						// ignore
					}
				} else {
					result.append("\n<content not repeatable>\n");
				}
			}
		}
		
		return result.toString();
	}
	
	/** Builds a string from given request.
	 * 
	 * @param message
	 * @return a string from given request.
	 */
	private String buildResponseLogString(HttpResponse message) {
		if (message == null) {
			return "<null>";
		}
		
		StringBuilder result = new StringBuilder();
		
		if (message.getStatusLine() != null) {
			result.append(message.getStatusLine().toString()).append("\n");
		}
		
		if (message.getAllHeaders() != null) {
			for (Header header : message.getAllHeaders()) {
				result.append(header.toString()).append("\n");
			}
		}
		
		if (message instanceof BasicHttpResponse) {
			HttpEntity entity = ((BasicHttpResponse) message).getEntity();
			if (entity != null) {
				if (entity.isRepeatable()) {
					try {
						ByteArrayOutputStream entityBytes = new ByteArrayOutputStream();
						entity.writeTo(entityBytes);
						result.append(new String(entityBytes.toByteArray(), entity.getContentEncoding() == null?  Charset.defaultCharset().name() : entity.getContentEncoding().getValue()));
					} catch (IOException e) {
						// ignore
					}
				} else {
					result.append("\n<content not repeatable>\n");
				}
			}
		}
		
		return result.toString();
	}
	
	
	/** Prepares the request content. The content is read from the input field, component property or input file.
	 * 
	 * @return the request content.
	 */
	private String prepareRequestContent() {
		DataField inField = getFieldSafe(inputRecord, inputFieldName);

		if (inField != null) {
		    return inField.toString();
		    
		} if (!StringUtils.isEmpty(requestContentToUse)) {
			return requestContentToUse;
			
		} else if (!StringUtils.isEmpty(inputFileUrlToUse)) {
			//input file with http request is entered
	        String contentToSend = FileUtils.getStringFromURL(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, inputFileUrlToUse, null);
	        
			return contentToSend;
		} 
		
		return null;
	}
	
	/** Try to initialize the component.
	 * 
	 * @throws ComponentNotReadyException
	 */
	protected void tryToInit(boolean runningFromCheckConfig) throws ComponentNotReadyException {
		//find the attached ports (input and output)
		inputPort = getInputPortDirect(INPUT_PORT_NUMBER);
		standardOutputPort = getOutputPort(STANDARD_OUTPUT_PORT_NUMBER);
		errorOutputPort = getOutputPort(ERROR_OUTPUT_PORT_NUMBER);

		//which ports are attached?
		hasInputPort = (inputPort != null);
		hasStandardOutputPort = (standardOutputPort != null);
		hasErrorOutputPort = (errorOutputPort != null);
		
		if (redirectErrorOutput && hasErrorOutputPort) {
			throw new ComponentNotReadyException("Error output is redirected to standard output port, but error port has an edge connected");
		}

		// create input mapping transformation
    	inputMappingTransformation = new CTLMapping("Input mapping", this);
    	inputMappingTransformation.setTransformation(inputMapping);
    	inputMappingTransformation.setClasspath(getGraph().getRuntimeContext().getClassPath());
    	inputMappingTransformation.setClassLoader(this.getClass().getClassLoader());
        
		// create standard output mapping transformation
    	standardOutputMappingTransformation = new CTLMapping("Standard output mapping", this);
    	standardOutputMappingTransformation.setTransformation(standardOutputMapping);
    	standardOutputMappingTransformation.setClasspath(getGraph().getRuntimeContext().getClassPath());
    	standardOutputMappingTransformation.setClassLoader(this.getClass().getClassLoader());
        
		// create error output mapping transformation
    	errorOutputMappingTransformation = new CTLMapping("Error output mapping", this);
    	errorOutputMappingTransformation.setTransformation(errorOutputMapping);
    	errorOutputMappingTransformation.setClasspath(getGraph().getRuntimeContext().getClassPath());
    	errorOutputMappingTransformation.setClassLoader(this.getClass().getClassLoader());

		DataRecordMetadata paramsMetadata = createInputParametersMetadata();
		inputParamsRecord = DataRecordFactory.newRecord(paramsMetadata);
		inputParamsRecord.init();    	
		
		//create internal result data record
		if (hasStandardOutputPort) {
			DataRecordMetadata resultMetadata = createResultMetadata();
			resultRecord = DataRecordFactory.newRecord(resultMetadata);
			resultRecord.init();
		}

		//create internal error data record
		if (hasErrorOutputPort || redirectErrorOutput) {
			DataRecordMetadata errorMetadata = createErrorMetadata();
			errorRecord = DataRecordFactory.newRecord(errorMetadata);
			errorRecord.init();
		}
		
		//create input data record, if necessary
		if (hasInputPort) {
			inputRecord = DataRecordFactory.newRecord(inputPort.getMetadata());
			inputRecord.init();
		}
		
		//create output data records, if necessary
		if (hasStandardOutputPort) {
			standardOutputRecord = DataRecordFactory.newRecord(standardOutputPort.getMetadata());
			standardOutputRecord.init();
			standardOutputRecord.reset();
			if (outputFieldName != null) {
				outField = (StringDataField) standardOutputRecord.getField(outputFieldName);
			} else if (standardOutputMapping == null) {
				outField = (StringDataField) standardOutputRecord.getField(0);
			}
		}
		
		if (hasErrorOutputPort) {
			errorOutputRecord = DataRecordFactory.newRecord(errorOutputPort.getMetadata());
			errorOutputRecord.init();
			errorOutputRecord.reset();
		}

		//create input records for input mapping
		if (hasInputPort) {
			inputMappingInRecords = new DataRecord[] { inputRecord };
			inputMappingTransformation.addInputRecord(INPUT_RECORD_NAME, inputRecord);
		} else {
			inputMappingInRecords = new DataRecord[] { };
		}

		//create output records for input mapping
		List<DataRecord> inputMappingOutRecordsList = new ArrayList<DataRecord>();
		inputMappingOutRecordsList.add(inputParamsRecord);
		inputMappingOutRecords = inputMappingOutRecordsList.toArray(new DataRecord[0]);
		
		if (inputParamsRecord != null) {
			inputMappingTransformation.addOutputRecord(ATTRIBUTES_RECORD_NAME, inputParamsRecord);
		}

		//create input records for standard output mapping
		standardOutputMappingInRecords = new DataRecord[] { inputRecord, resultRecord, inputParamsRecord };
		standardOutputMappingTransformation.addInputRecord(INPUT_RECORD_NAME, inputRecord);
		standardOutputMappingTransformation.addInputRecord(RESULT_RECORD_NAME, resultRecord);
		standardOutputMappingTransformation.addInputRecord(ATTRIBUTES_RECORD_NAME, inputParamsRecord);
		
		//create input records for error output mapping
		errorOutputMappingInRecords = new DataRecord[] { inputRecord, errorRecord, inputParamsRecord };
		errorOutputMappingTransformation.addInputRecord(INPUT_RECORD_NAME, inputRecord);
		errorOutputMappingTransformation.addInputRecord(ERROR_RECORD_NAME, errorRecord);
		errorOutputMappingTransformation.addInputRecord(ATTRIBUTES_RECORD_NAME, inputParamsRecord);
		
		//create output records for standard output mapping
		if (hasStandardOutputPort) {
			standardOutputMappingOutRecords = new DataRecord[] { standardOutputRecord };
			standardOutputMappingTransformation.addOutputRecord(STANDARD_OUTPUT_RECORD_NAME, standardOutputRecord);
		} else {
			standardOutputMappingOutRecords = new DataRecord[] { };
		}

		//create output records for error output mapping
		if (hasErrorOutputPort) {
			errorOutputMappingOutRecords = new DataRecord[] { null, errorOutputRecord };
			errorOutputMappingTransformation.addOutputRecord(ERROR_OUTPUT_RECORD_NAME, errorOutputRecord);
		} else {
			errorOutputMappingOutRecords = new DataRecord[] { };
		}

		//preset default values into runtime variables
	    if (!runningFromCheckConfig) {
	    	initExecutionParametersFromComponentAttributes();
	    }
		
		inputMappingTransformation.init(XML_INPUT_MAPPING_ATTRIBUTE);
		standardOutputMappingTransformation.init(XML_STANDARD_OUTPUT_MAPPING_ATTRIBUTE);
	    errorOutputMappingTransformation.init(XML_ERROR_OUTPUT_MAPPING_ATTRIBUTE);
	}	
	
	/**
	 * 
	 */
	private void initExecutionParametersFromComponentAttributes() {
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_URL_NAME, rawUrl);
//		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_URL_FIELD_NAME, urlInputField);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_REQUEST_METHOD_NAME, requestMethod);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_REQUEST_CONTENT_NAME, requestContent);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_INPUT_FILE_URL_NAME, inputFileUrl);
//		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_INPUT_FIELD_NAME_NAME, inputFieldName);
//		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_OUTPUT_FIELD_NAME_NAME, outputFieldName);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_OUTPUT_FILE_URL_NAME, outputFileUrl);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_APPEND_OUTPUT_NAME, appendOutput);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_CHARSET_NAME, charset);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_REQUEST_PROPERTIES_NAME, rawRequestProperties);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_ADD_INPUT_FIELDS_AS_PARAMETERS_NAME, addInputFieldsAsParameters);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_ADD_INPUT_FIELDS_AS_PARAMETERS_TO_NAME, addInputFieldsAsParametersTo);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_IGNORED_FIELDS_NAME, ignoredFields);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_AUTHENTICATION_METHOD_NAME, authenticationMethod);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_USERNAME_NAME, username);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_PASSWORD_NAME, password);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_STORE_RESPONSE_TO_TEMP_NAME, storeResponseToTempFile);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_TEMP_FILE_PREFIX_NAME, temporaryFilePrefix);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_MULTIPART_ENTITIES_NAME, multipartEntities);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_CONSUMER_KEY_NAME, consumerKey);
		inputMappingTransformation.setDefaultOutputValue(ATTRIBUTES_RECORD_NAME, IP_CONSUMER_SECRET_NAME, consumerSecret);		
		
	}

	/** Maps values in <code>toRecord</code> from records contained in <code>fromRecords</code> by name.
	 * 
	 * @param fromRecords
	 * @param toRecord
	 */
	private static void mapByName(DataRecord[] fromRecords, DataRecord toRecord) {
		if (fromRecords == null) {
			return;
		}
		
		for (DataRecord record : fromRecords) {
			toRecord.copyFieldsByName(record);
		}
	}
	
	/** Processes given request configuration. New request is built based on given configuration, the request is then sent to the 
	 *  target URL and the response is processed.
	 * 
	 * @param configuration - configuration specifying the request
	 * @throws Exception
	 */
	private void process() throws Exception {
		HTTPRequestConfiguration configuration = prepareConfigurationForRecord();
		
		HttpResponse response = buildAndSendRequest(configuration);
		
		// warn if the HTTP response code isn't 200
		if (response != null && response.getStatusLine().getStatusCode() != 200) {
			logger.warn("Returned code for http request " + configuration.getTarget() + " is " + response.getStatusLine().getStatusCode());
		}

		result.setResponse(response);
		
		// process the response
		processResponse();
	}
	
	
	/** Builds a request based on the given configuration and sends it, using a HTTP client
	 * 
	 * @param configuration
	 * @return response retrieved
	 * 
	 * @throws ComponentNotReadyException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private HttpResponse buildAndSendRequest(HTTPRequestConfiguration configuration) throws Exception {
		initHTTPClient(configuration);
		
		HttpRequestBase method = prepareMethod(configuration);
		
		// sign the request before sending it 
		if (oauthConsumer != null) {
			oauthConsumer.sign(method);
		}
		
		HttpResponse response = httpClient.execute(method);
		
		
		
		return response;
	}
	
	/** Processes the given response. The response is written to the output port (if connected) or to an output file (if specified). Otherwise, the 
	 *  content of the response is written to the standard output. 
	 * 
	 * @param response
	 * 
	 * @throws ComponentNotReadyException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void processResponse() throws ComponentNotReadyException, IOException, InterruptedException {
		// output port is connected - write the response there
		logProcessing();
		
		HttpResponse response = result.getResponse();
		responseWriter.writeResponse(response);
		
		if (resultRecord != null) {
			resultRecord.getField(RP_STATUS_CODE_INDEX).setValue(response.getStatusLine().getStatusCode());
			
			Header[] headers = response.getAllHeaders();
			Map<String, String> headersMap = new HashMap<String, String>(headers.length);
			for (Header header: headers) {
				headersMap.put(header.getName(), header.getValue());
			}
			resultRecord.getField(RP_HEADER_INDEX).setValue(headersMap);
		} 
	}
	
	@Override
	public Result execute() throws Exception {
		boolean success = true;
		if (hasInputPort) {
			while (inputPort.readRecord(inputRecord) != null && runIt && success) {
				success &= executeForRecord();
				
				SynchronizeUtils.cloverYield();
			}
		} else {
			if (tokenTracker != null) {
				//in case no input port is attached - dummy token is prepared and initialized and all logging messages
				//are tracked on this dummy token
				inputRecord = DataRecordFactory.newToken("input");
				tokenTracker.initToken(inputRecord);
			}
			
			// no input port connected - only one request will be sent
			success &= executeForRecord();
		}

		broadcastEOF();

        return (runIt ? (success ? Result.FINISHED_OK : Result.ERROR) : Result.ABORTED);		
	}

	/** Logs success.
	 * 
	 */
	private void logSuccess() {
		tokenTracker.logMessage(inputRecord, Level.INFO, "Executed sucessfully.", null);
	}

	/** Logs error.
	 * 
	 */
	private void logError(Exception e) {
		tokenTracker.logMessage(inputRecord, Level.ERROR, e.toString(), null);
	}
	
	/** Logs progress.
	 * 
	 */
	private void logProcessing() {
		tokenTracker.logMessage(inputRecord, Level.INFO, "Processing response...", null);
	}
	
	/** Method that send the request based on the given record on input (or dummy record, if input port is not connected)
	 * 
	 * @return
	 * @throws Exception
	 */
	protected boolean executeForRecord() throws Exception {
		initForRecord();
		
		try {
			mapInput();
	
			process();
			
			logSuccess();

		} catch (Exception e) {
			result.setException(e);
		} 

		boolean success = mapOutput();
		
		if (!success) {
			logError(result.getException());
		}
		
		return success;
	}	
	
	/** Prepares the state of the component for processing of a next record.
	 * 
	 */
	private void initForRecord() {
		// reset all the records used
		resetRecords(inputParamsRecord, resultRecord, errorRecord);
		resetRecords(standardOutputMappingOutRecords);
		resetRecords(errorOutputMappingOutRecords);
		
		result = new RequestResult();
	}
	
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		inputMappingTransformation.preExecute();
		standardOutputMappingTransformation.preExecute();
		errorOutputMappingTransformation.preExecute();
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		inputMappingTransformation.postExecute();
		standardOutputMappingTransformation.postExecute();
		errorOutputMappingTransformation.postExecute();
	}
	
	/** Method that fills the parameter record based on the given transformation (or by name if no explicit transformation specified)
	 * 
	 */
	protected void mapInput() {
		resetRecords(inputMappingOutRecords);
		inputMappingTransformation.execute();
		processInputParamsRecord();

	}	

//	/** Maps fields based on the configuration to preserve compatibility.
//	 * 
//	 * @throws Exception
//	 */
//	private void mapLegacyInput() {
//		if (inputFieldName != null) {
//			inputParamsRecord.getField(IP_INPUT_FIELD_NAME_INDEX).setValue(inputFieldName);
//		}		
//	}
	
	/** Prepares configuration based on record read from the input port.
	 * 
	 * @return a configuration based on record read from the input port.
	 * @throws ComponentNotReadyException
	 */
	private HTTPRequestConfiguration prepareConfigurationForRecord() throws ComponentNotReadyException {
		preProcessForRecord();
		
		// get the URL template
		String urlFromField = rawUrlToUse;
		if (!StringUtils.isEmpty(urlInputField) && !attributeMapped(IP_URL_NAME)) {
			// urlInputField is set so it will be used (url field is ignored)
			urlFromField = inputRecord.getField(urlInputField).toString();
		}

		Set<String> fieldsToIgnore = new HashSet<String>(ignoredFieldsSet);
		
		// prepare the URL (substitute variables) and fill the fields to ignore set
		String finalURL = prepareURL(urlFromField, fieldsToIgnore);

		// now build the configuration
		HTTPRequestConfiguration configuration = new HTTPRequestConfiguration();
		
		configuration.setTarget(FileURLParser.getOuterAddress(finalURL));
		configuration.setProxy(FileURLParser.getInnerAddress(finalURL));

		// prepare the parameters, ignore fields that are set as ignored + fields used to resolve URL placeholders
		configuration.setParameters(prepareRequestParameters(fieldsToIgnore));
		// prepare the multipart entities
		configuration.setMultipartEntities(prepareMultipartEntities());
		
		configuration.setContent(prepareRequestContent());
		
		configuration.prepare();
		
		return configuration;
	}	
	
	/** Maps the output.
	 *
	 * @return success
	 * @throws Exception
	 */
	private boolean mapOutput() throws Exception {
		if (result.getException() == null) {
			
			mapStandardOutput();
			return true;
			
		} else {

			if (isLegacyErrorHandling()
					|| (!redirectErrorOutput && !hasErrorOutputPort)
					|| (redirectErrorOutput && !hasStandardOutputPort)) {
				return false;
			}
				
			if (redirectErrorOutput) {
				mapStandardOutput();
			} else {
				mapErrorOutput();
			}
			return true;
			
		}
	}

	private boolean isLegacyErrorHandling() {
		return standardOutputMapping == null && errorOutputMapping == null && !redirectErrorOutput && !hasErrorOutputPort;
	}


	/** Method that fill the standard output record based on the given output mapping
	 * 
	 * @throws Exception
	 */
	private void mapStandardOutput() throws Exception {
		if (hasStandardOutputPort) {
			populateInputParamsRecord();
			populateResultRecordError();
			
			if (outField != null) {
				outField.setNull(true);
			}
			
			if (standardOutputMapping != null) {
				standardOutputMappingTransformation.execute();
				
				mapOverridingOutput();
			} else {
//					//output transformation is not specified - default star mapping is performed
//					// POSSIBLE PROBLEM FOR BACKWARD COMPATIBILITY
//					mapByName(standardOutputMappingInRecords, standardOutputRecord);
//					
//					// as the output transformation is not specified, we must assume, that this is a 
//					// usage of HTTPConnector, that is not aware of output mapping. Thus map the fields
//					// in an old way to preserve compatibility.
				mapLegacyOutput();
			}
			
			standardOutputPort.writeRecord(standardOutputRecord);
		}
	}

	/** Maps fields based on the configuration to preserve compatibility with old output field property.
	 * 
	 * @throws Exception
	 */
	private void mapOverridingOutput() throws Exception {
		// User defined the output field explicitly - set the value there, if the value was
		// not set by a mapping
		if (outputFieldName != null && outField != null && outField.isNull()) {
			outField.setValue(resultRecord.getField(RP_CONTENT_INDEX));
		}		
	}
	
	
	/** Maps fields based on the configuration to preserve compatibility.
	 * 
	 * @throws Exception
	 */
	private void mapLegacyOutput() throws Exception {
		// output field connected - set the value there.
		// NOTE: no output mapping is defined.
		if (outField != null) {
			if (outputFileUrlToUse != null || storeResponseToTempFileToUse) {
				// if we are storing the response to file, set the file URL into output field
				outField.setValue(resultRecord.getField(RP_OUTPUTFILE_INDEX));
				
			} else {
				// otherwise store the content
				outField.setValue(resultRecord.getField(RP_CONTENT_INDEX));
			}			
		}		
	}
	
	/** Method that fill the error output record based on the given error output mapping (or by name, if no explicit transformation specified)
	 * 
	 * @throws Exception
	 */
	private void mapErrorOutput() throws Exception {
		if (hasErrorOutputPort) {
			populateErrorRecord();
			populateInputParamsRecord();
			if (errorOutputMapping != null) {
				errorOutputMappingTransformation.execute();
			} else {
				//output transformation is not specified - default star mapping is performed
				mapByName(errorOutputMappingInRecords, errorOutputRecord);
			}
			errorOutputPort.writeRecord(errorOutputRecord);
		} else if (hasStandardOutputPort) {
			mapStandardOutput();
		}
	}
	
	/** Creates an instance of the HTTPConnector based on the given XML definition.
	 * 
	 * @param graph
	 * @param xmlElement
	 * @return an instance of the HTTPConnector based on the given XML definition.
	 * @throws XMLConfigurationException
	 */
    public static HttpConnector fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		try {
			HttpConnector httpConnector = new HttpConnector(xattribs.getString(XML_ID_ATTRIBUTE));

			/** base properties */
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
			httpConnector.setTemporaryFilePrefix(xattribs.getString(XML_TEMPORARY_FILE_PREFIX, "http-response-"));
			httpConnector.setAuthenticationMethod(xattribs.getString(XML_AUTHENTICATION_METHOD_ATTRIBUTE, "BASIC"));
			httpConnector.setUsername(xattribs.getString(XML_USERNAME_ATTRIBUTE, null));
			httpConnector.setPassword(xattribs.getString(XML_PASSWORD_ATTRIBUTE, null));
			httpConnector.setAddInputFieldsAsParameters(xattribs.getBoolean(XML_ADD_INPUT_FIELDS_AS_PARAMETERS_ATTRIBUTE, false));
			httpConnector.setAddInputFieldsAsParametersTo(xattribs.getString(XML_ADD_INPUT_FIELDS_AS_PARAMETERS_TO_ATTRIBUTE, "QUERY"));
			httpConnector.setIgnoredFields(xattribs.getString(XML_IGNORED_FIELDS_ATTRIBUTE, null));
			httpConnector.setMultipartEntities(xattribs.getString(XML_MULTIPART_ENTITIES_FIELDS_LIST_ATTRIBUTE, null));
			httpConnector.setUrlInputField(xattribs.getString(XML_URL_FROM_INPUT_FIELD_ATTRIBUTE, null));
			httpConnector.setConsumerKey(xattribs.getString(XML_CONSUMER_KEY_ATTRIBUTE, null));
			httpConnector.setConsumerSecret(xattribs.getString(XML_CONSUMER_SECRET_ATTRIBUTE, null));

			/** job flow related properties */
			httpConnector.setInputMapping(xattribs.getStringEx(XML_INPUT_MAPPING_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
			httpConnector.setStandardOutputMapping(xattribs.getStringEx(XML_STANDARD_OUTPUT_MAPPING_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
			httpConnector.setErrorOutputMapping(xattribs.getStringEx(XML_ERROR_OUTPUT_MAPPING_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
    		httpConnector.setRedirectErrorOutput(xattribs.getBoolean(XML_REDIRECT_ERROR_OUTPUT, false));
			
			
			return httpConnector;
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ") + ":" + ex.getMessage(), ex);
		}
	}

    private boolean attributeMappedFromInput(String attributeName) {
    	if (inputMapping == null) {
    		return false;
    	}
    	
    	return inputMapping.contains("$out.0." + attributeName + " = $in.");
    }

    private boolean attributeMapped(String attributeName) {
    	if (inputMapping == null) {
    		return false;
    	}
    	
    	return inputMapping.contains("$out.0." + attributeName + " =");
    }
    
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		// check number of input/output ports
		if (!checkInputPorts(status, 0, 1) || !checkOutputPorts(status, 0, 2)) {
			return status;
		}

		// check character set
		if (charset != null && !Charset.isSupported(charset)) {
        	status.add(new ConfigurationProblem("Charset " + charset + " not supported!", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
        }

		InputPort inputPort = getInputPort(INPUT_PORT_NUMBER);
		if (inputPort == null) {
			// no input port, but URL from field defined
			if (!StringUtils.isEmpty(urlInputField)) {
				status.add(new ConfigurationProblem("'URL from input field' attribute is set, but no input port is connected.", Severity.WARNING, this, Priority.NORMAL));
			}
			
			// no input port, but URL mapped from input
			if (attributeMappedFromInput(IP_URL_NAME)) {
				status.add(new ConfigurationProblem("URL is mapped from input field, but no input port is connected.", Severity.WARNING, this, Priority.NORMAL));
			} 
			
			// no URL defined (in the situation without an input port, the URL can be only set using 'URL' attribute or mapped in Input Mapping)
			if (StringUtils.isEmpty(rawUrl) && !attributeMapped(IP_URL_NAME)) {
				status.add(new ConfigurationProblem("No URL defined - please set the 'URL' attribute or map URL in Input Mapping.", Severity.ERROR, this, Priority.NORMAL));
			}
		}
		
		// input mapping does not define the URL
		if (!attributeMapped(IP_URL_NAME)) {

			// check whether both URL and input field URL isn't entered (note: the case when no input port is connected is handled above)
			if (!StringUtils.isEmpty(rawUrl) && !StringUtils.isEmpty(urlInputField)) {
				status.add(new ConfigurationProblem("Both URL and URL from input field is entered. URL from input field will be used.", Severity.WARNING, this, Priority.NORMAL));
			}
			
			// no URL defined - check only if the input port is not null (the case of no input port is checked above)
			if (inputPort != null && StringUtils.isEmpty(rawUrl) && StringUtils.isEmpty(urlInputField)) {
				status.add(new ConfigurationProblem("No URL defined - please set the 'URL' attribute or map URL in Input Mapping.", Severity.ERROR, this, Priority.NORMAL));
			}
			
		} else {
			// URL field set and URL mapped - mapping will be used
			if (!StringUtils.isEmpty(urlInputField)) {
				status.add(new ConfigurationProblem("URL is mapped in 'Input Mapping' and 'URL from input field' attribute is set. 'URL from input field' will be ignored.", Severity.WARNING, this, Priority.NORMAL));
			}
			
		}
		 
		// Unknown request method
		if (!SUPPORTED_REQUEST_METHODS.contains(requestMethod)) {
			status.add(new ConfigurationProblem("Unsupported request method: " + requestMethod, Severity.ERROR, this, Priority.NORMAL));
		}
		
		// Unknown authentication method
		if (!SUPPORTED_AUTHENTICATION_METHODS.contains(authenticationMethod)) {
			status.add(new ConfigurationProblem("Unsupported authentication method: " + authenticationMethod, Severity.ERROR, this, Priority.NORMAL));
		}
		
		if (inputPort != null) {
			DataFieldMetadata inField;
			if (!StringUtils.isEmpty(inputFieldName)) {
				inField = inputPort.getMetadata().getField(inputFieldName);
			} else {
				inField = inputPort.getMetadata().getField(0);
			}
			
			// Input field is set, but does not exist
			if (inputFieldName != null && inField == null) {
				status.add(new ConfigurationProblem("Input field name '" + inputFieldName + "' does not exist in input metadata.", 
						Severity.ERROR, this, Priority.NORMAL, XML_INPUT_PORT_FIELD_NAME));

			// Input field is set, but has incompatible type
			} else if (inputFieldName != null && inField.getDataType() != DataFieldType.STRING) {
				status.add(new ConfigurationProblem("Input field '" + inputFieldName + "' has incompatible type '" + 
						inField.getDataType().toString() + "'. Field has to be String.", Severity.ERROR, this, Priority.NORMAL, XML_INPUT_PORT_FIELD_NAME));
			}

			//check whether multipart entities and ignored list is entered just when possible or reasonable
			if (!addInputFieldsAsParameters) {
				if (!StringUtils.isEmpty(multipartEntities)) {
					status.add(new ConfigurationProblem("'Multipart entities' attribute will be ignored, because 'Add input fields as parameters' attribute is set to 'false'.", 
							Severity.WARNING, this, Priority.NORMAL));
				}
				if (!StringUtils.isEmpty(ignoredFields)) {
					status.add(new ConfigurationProblem("'Ignored fields' attribute will be ignored, because 'Add input fields as parameters' attribute is set to 'false'.", 
							Severity.WARNING, this, Priority.NORMAL));
				}
			} else {
				if (!StringUtils.isEmpty(multipartEntities) && !requestMethod.equals(POST)) {
					status.add(new ConfigurationProblem("Multipart entities can only be used with a POST request method.", 
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
				String[] metadataNames = inputPort.getMetadata().getFieldNamesArray();
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
				String[] metadataNames = inputPort.getMetadata().getFieldNamesArray();
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
						status.add(new ConfigurationProblem("Add input fields as parameters must be specified.", Severity.ERROR, this, Priority.NORMAL));
					}
				} else if (addInputFieldsAsParametersTo.equals("BODY") && requestMethod.equals(GET)) {
					status.add(new ConfigurationProblem("Cannot add fields as parameters to body of HTTP request when GET method is used.", Severity.ERROR, this, Priority.NORMAL));
				}
			}
		}

		OutputPort outputPort = getOutputPort(STANDARD_OUTPUT_PORT_NUMBER);
		if (outputPort != null) {
			DataFieldMetadata outField = null;
			if (!StringUtils.isEmpty(outputFieldName)) {
				outField = outputPort.getMetadata().getField(outputFieldName);
			} else if (standardOutputMapping == null) {
				outField = outputPort.getMetadata().getField(0);
			}
			
			
			if (outputFieldName != null && outField == null) {
				status.add(new ConfigurationProblem("Output field name '" + outputFieldName + "' does not exist in output metadata.", Severity.ERROR, this, Priority.NORMAL, XML_OUTPUT_PORT_FIELD_NAME));
				
			}
			if (outField != null && outField.getDataType() != DataFieldType.STRING) {
				if (outputFieldName != null) {
					status.add(new ConfigurationProblem("Output field '" + outputFieldName + "' has incompatible type '" + outField.getDataType().toString() + "'. The field has to be 'string'.", Severity.ERROR, this, Priority.NORMAL, XML_OUTPUT_PORT_FIELD_NAME));
				} else {
					status.add(new ConfigurationProblem("'Output field' not specified -> using the first field in output metadata as output, but it has incompatible type '" + outField.getDataType().toString() + "'. The field has to be 'string'.", Severity.ERROR, this, Priority.NORMAL, XML_OUTPUT_PORT_FIELD_NAME));
				}
			}
		}
		
		if (inputPort == null && outputPort == null && StringUtils.isEmpty(outputFileUrl)) {
			status.add(new ConfigurationProblem("Output port isn't connected and output file is not set.", Severity.WARNING, this, Priority.NORMAL));
		}

		if (inputPort != null && !StringUtils.isEmpty(inputFileUrl)) {
			status.add(new ConfigurationProblem("'Input file URL' will be ignored because input port is connected.", Severity.WARNING, this, Priority.NORMAL));
		}

		if (storeResponseToTempFile && outputFileUrl != null) {
			status.add(new ConfigurationProblem("Only one of 'Output file URL' and 'Store response file URL to output field' may be used.", Severity.ERROR, this, Priority.NORMAL));
		}
		
			
		if (!StringUtils.isEmpty(requestContent) && !StringUtils.isEmpty(inputFileUrl)) {
			status.add(new ConfigurationProblem("You can set either 'Request content' or 'Input file URL'.", Severity.ERROR, this, Priority.NORMAL));
		}

		// check existence of the temporary directory; if specified
		if (getStoreResponseToTempFile()) {

			// output port must be connected so that we can write file names into
			if (outputPort == null) {
				status.add(new ConfigurationProblem("An output port must be connected in order to write response temporary file names.", Severity.ERROR, this, Priority.NORMAL));
			}
		}

		//check whether both user name and password for BASIC http auth are entered or none of them
		if ((!StringUtils.isEmpty(getUsername()) && StringUtils.isEmpty(getPassword())) || (StringUtils.isEmpty(getUsername()) && !StringUtils.isEmpty(getPassword()))) {
			status.add(new ConfigurationProblem("Both username and password must be entered or none of them.", Severity.ERROR, this, Priority.NORMAL));
		}
		
		// check restrictions of the GET method
		if (requestMethod.equals(GET)) {
			// no content allowed in GET request (actually, it is not invalid for GET request to contain content according to standard, but the HTTPClient library does not support it 
			// as it is not used in practice)
			if (!StringUtils.isEmpty(requestContent)) {
				status.add(new ConfigurationProblem("Request content not allowed when GET method is used.", Severity.ERROR, this, Priority.NORMAL, XML_REQUEST_CONTENT_ATTRIBUTE));
			}
			if (!StringUtils.isEmpty(inputFieldName)) {
				status.add(new ConfigurationProblem("Input field not allowed when GET method is used.", Severity.ERROR, this, Priority.NORMAL, XML_INPUT_PORT_FIELD_NAME));
			}
			if (!StringUtils.isEmpty(inputFileUrl)) {
				status.add(new ConfigurationProblem("Input file URL not allowed when GET method is used.", Severity.ERROR, this, Priority.NORMAL, XML_INPUT_FILEURL_ATTRIBUTE));
			}
		}
		
		if (redirectErrorOutput && standardOutputMapping == null) {
			status.add(new ConfigurationProblem("Fields of error records redirected to standard output port will be empty unless Standard output mapping is defined", Severity.WARNING, this, Priority.NORMAL, XML_REDIRECT_ERROR_OUTPUT));
		}
		
        try {
        	tryToInit(true);
        } catch (Exception e) {
        	status.add("Initialization failed. " + e.getMessage(), Severity.ERROR, this, Priority.NORMAL);
        }
		
        
		return status;
	}
	
	/** Validates given prepared HTTP configuration.
	 * 
	 * @param configuration
	 * @return prepared configuration
	 * @throws ComponentNotReadyException
	 */
	private void validateConfiguration(HTTPRequestConfiguration configuration) throws ComponentNotReadyException {
		// check if the target URL is not empty
		if (StringUtils.isEmpty(configuration.getTarget())) {
			throw new ComponentNotReadyException("Invalid target URL - no URL provided"); 
		}

		// check if the protocol is HTTP or HTTPS
		String protocol = configuration.getTargetURL().getProtocol();
		if (!protocol.equals("http") && !protocol.equals("https")) {
			throw new ComponentNotReadyException("Given URL has incompatible protocol: " + protocol);
		}
	}

	/** Prepares a POST method for given configuration.
	 * 
	 * @param configuration
	 * @return a POST method for given configuration.
	 * 
	 * @throws UnsupportedEncodingException
	 */
	private HttpPost preparePostMethod(HTTPRequestConfiguration configuration) throws UnsupportedEncodingException {
		//request method is post
		if( logger.isDebugEnabled() ){
			logger.debug("Creating POST request to " + configuration.getTarget());
		}
		
		HttpPost postMethod = new HttpPost(configuration.getTarget());
		
		//set multipart request entity if any
		if (!configuration.getMultipartEntities().isEmpty()) {
			MultipartEntity entity = new MultipartEntity();
			for (PartWithName stringPart : buildMultiPart(configuration.getMultipartEntities())) {
				entity.addPart(stringPart.name, stringPart.value);
			}
			
			postMethod.setEntity(entity);
		}
		
		
		// process parameters
		if ("BODY".equals(addInputFieldsAsParametersToToUse)) {
			//set request body if any
			//FIXME: this replaces the multipart entity set in the previous step
			//we leave it as-is to make the behavior compatible with older version using httpClient 3.x
			//which also cleared the previously set multipart entity.
			postMethod.setEntity(new UrlEncodedFormEntity(Arrays.asList(buildNameValuePairs(configuration.getParameters())), charsetToUse));
			
		} else { // if (addInputFieldsAsParametersTo.equals("QUERY")) {
			String preparedQuery = buildQueryString(configuration.getParameters());
			
			if (!StringUtils.isEmpty(preparedQuery)) {
				addQuery(preparedQuery, postMethod);
			}
		}
		
		// process content
		if (!StringUtils.isEmpty(configuration.getContent())) {
			StringEntity entity = new StringEntity(configuration.getContent(), charsetToUse == null? Defaults.DataParser.DEFAULT_CHARSET_DECODER: charsetToUse);
			postMethod.setEntity(entity);
		}
		
		return postMethod;
	}
	
	/** Prepares a GET method for given configuration.
	 * 
	 * @param configuration
	 * @return a GET method for given configuration.
	 * 
	 * @throws UnsupportedEncodingException
	 */
	private HttpGet prepareGetMethod(HTTPRequestConfiguration configuration) throws UnsupportedEncodingException {
		// request method is post
		if( logger.isDebugEnabled() ) {
			logger.debug("Creating GET request to " + configuration.getTarget());
		}
		
		HttpGet getMethod = new HttpGet(configuration.getTarget());

		// process parameters
		String preparedQuery = buildQueryString(configuration.getParameters());
		
		//set query string if any
		if (!StringUtils.isEmpty(preparedQuery)) {
			addQuery(preparedQuery, getMethod);
		}

		
		return getMethod;
	}

	/** Prepares a HTTP method (based on the given configuration) to be used for a request.
	 * 
	 * @param configuration
	 * @return a HTTP method (based on the given configuration) to be used for a request.
	 * @throws UnsupportedEncodingException
	 * @throws ComponentNotReadyException
	 */
	private HttpRequestBase prepareMethod(HTTPRequestConfiguration configuration) throws UnsupportedEncodingException, ComponentNotReadyException {
		HttpRequestBase method = null;
		
		// configure the request method
		if (POST.equals(requestMethodToUse)) {
			method = preparePostMethod(configuration);
			
		} else if (GET.equals(requestMethodToUse)) {
			method = prepareGetMethod(configuration);
			
		} else {
			//another request method than get or post
			throw new ComponentNotReadyException(this, "Unsupported request method: " + requestMethodToUse);
		}
		
		// add resolved header parameters
		if (!StringUtils.isEmpty(rawRequestPropertiesToUse)) {
			addHeaderParameters(method);
		}
		
		return method;
	}
	
	/** Adds a query string to a given method URI.
	 * 
	 * @param query - a query string to be added
	 * @param method - a method to be updated
	 */
	private void addQuery(String query, HttpRequestBase method) {
		if (!StringUtils.isEmpty(method.getURI().getQuery())) {
			method.setURI(changeQueryString(method.getURI(), method.getURI().getRawQuery() + "&" + query));				
		} else {
			method.setURI(changeQueryString(method.getURI(), query));				
		}
	}
	
	/** Builds a query string from given parameters map.
	 * 
	 * @param parameters
	 * @return a query string built from given parameters map.
	 * 
	 * @throws UnsupportedEncodingException
	 */
	private String buildQueryString(Map<String, String> parameters) throws UnsupportedEncodingException {
		if (parameters == null) {
			return "";
		}
		
		StringBuilder result = new StringBuilder();
		Iterator<Entry<String, String>> it = parameters.entrySet().iterator();
		while(it.hasNext()) {
			Entry<String, String> parameter = it.next();
			
			result.append(URLEncoder.encode(parameter.getKey(), "UTF-8")).append('=').append(URLEncoder.encode(parameter.getValue(), "UTF-8"));
			if (it.hasNext()) {
				result.append('&');
			}
		}
		
		return result.toString();
	}
	 
	/** Builds a name-value pairs from given parameters map.
	 * 
	 * @param parameters
	 * @return a name-value pairs built from given parameters map.
	 */
	private NameValuePair[] buildNameValuePairs(Map<String, String> parameters) {
		if (parameters == null) {
			return null;
		}
		
		int index = 0;
		NameValuePair[] result = new NameValuePair[parameters.size()];
		for (Entry<String, String> parameter : parameters.entrySet()) {
			result[index] = new BasicNameValuePair(parameter.getKey(), parameter.getValue());
			index++;
		}
		
		return result;
	}
	
	/** Builds an array of content parts for given entity map.
	 * 
	 * @param multipartEntities
	 * @return an array of content parts for given entity map.
	 * 
	 * @throws UnsupportedEncodingException
	 */
	private PartWithName[] buildMultiPart(Map<String, String> multipartEntities) throws UnsupportedEncodingException {
		if (multipartEntities == null) {
			return null;
		}
		
		int index = 0;
		PartWithName[] result = new PartWithName[multipartEntities.size()];
		for (Entry<String, String> parameter : multipartEntities.entrySet()) {
			result[index] = new PartWithName(parameter.getKey(), new StringBody(parameter.getValue()));
			index++;
		}
		
		return result;
	}
	
	/**
	 * Initializes the HTTP client for a new request. Only properties that don't depend on the record should be used here.
	 * 
	 * @throws ComponentNotReadyException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	private void initHTTPClient(HTTPRequestConfiguration configuration) throws ComponentNotReadyException, IOException, InterruptedException {
		validateConfiguration(configuration);
		
		// TODO: need to use SingleClientConnManager, because of the bug introduced in httpclient-4.2 (https://issues.apache.org/jira/browse/HTTPCLIENT-1193). When it is fixed, 
		// the HTTP client can be constructed as follows:
		// httpClient = new DefaultHttpClient(); 
		httpClient = new DefaultHttpClient(new SingleClientConnManager());
		httpClient.setHttpRequestRetryHandler(new DefaultHttpRequestRetryHandler(3, false));
		
		httpClient.addRequestInterceptor(requestLoggingInterceptor);
		httpClient.addResponseInterceptor(responseLoggingInterceptor);
		
		// configure proxy 
		if (configuration.getProxyURL() != null) {
			
			HttpHost proxy = new HttpHost(configuration.getProxyURL().getHost(), configuration.getProxyURL().getPort());
			httpClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);			
		}

		// configure authentication
		if (usernameToUse != null && passwordToUse != null) {
			//create credentials
			creds = new UsernamePasswordCredentials(usernameToUse, passwordToUse); 
			httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, creds);
			
			//set authentication method
			String authMethod = null;
			if ("BASIC".equals(authenticationMethodToUse)) {
				//basic http authentication
				authMethod = AuthPolicy.BASIC;
			} else if ("DIGEST".equals(authenticationMethodToUse)) {
				//digest http authentication
				authMethod = AuthPolicy.DIGEST;
			} else if ("ANY".equals(authenticationMethodToUse)) {
				//one of the possible authentication method will be used
				authMethod = "ANY";
			}
			final List<String> authPrefs = new ArrayList<String>();
			if (authMethod.equals("ANY")) {
				authPrefs.add(AuthPolicy.BASIC);
				authPrefs.add(AuthPolicy.DIGEST);
			} else {
				authPrefs.add(authMethod);
			}
			authPrefs.add(authMethod);
			
			httpClient.setTargetAuthenticationHandler(new DefaultTargetAuthenticationHandler() {

				@Override
				protected List<String> getAuthPreferences(HttpResponse response, HttpContext context) {
					return authPrefs;
				}
				
			});
		}

		// configure OAuth authentication
		if (!StringUtils.isEmpty(consumerKeyToUse) && !StringUtils.isEmpty(consumerSecretToUse)){
			// Consumer instance that should be used (used for OAuth authentication)
			oauthConsumer = new CommonsHttpOAuthConsumer(consumerKeyToUse, consumerSecretToUse);				
		}		
	}

	/** Changes the query string in given URI and returns the updated URI.
	 * 
	 * @param uri
	 * @param newQuery
	 * @return the updated URI.
	 */
	private static URI changeQueryString(URI uri, String newQuery) {
		try {
			URI newURI = URIUtils.createURI(uri.getScheme(), uri.getHost(), uri.getPort(), uri.getPath(), newQuery, uri.getFragment());
			return newURI;
		} catch (URISyntaxException e) {
		}
		return null;
	}
	
	/** Sets the additional header parameters and resolving references in context of given record.
	 * 
	 * @param record - record to be used to resolve references
	 * @throws ComponentNotReadyException
	 */
	private void addHeaderParameters(HttpRequestBase method) throws ComponentNotReadyException {
		if (inputRecord != null) {
			Properties fieldValues = new Properties();

			Iterator<DataField> it = inputRecord.iterator();
			while (it.hasNext()) {
				DataField field = it.next();
				fieldValues.setProperty(field.getMetadata().getName(), field.toString());
			}
		
			setRefProperties(fieldValues);
		}
		
		// pass request properties to the http connection
		for (Entry<Object, Object> entry : requestProperties.entrySet()) {
			String value = refResolver.resolveRef((String) entry.getValue());
			
			// check if the value is fully resolved
			if (PropertyRefResolver.containsProperty(value)) {
				throw new ComponentNotReadyException(this, "Could not resolve all references in additional HTTP header: '" + (String)entry.getValue() + "' (resolved as '" + value + "')");
			}
			
			method.getParams().setParameter((String) entry.getKey(), value);
			method.addHeader((String) entry.getKey(), value);
		}
	}	
	
	/** Creates a map representing request parameters.
	 * 
	 * @param fieldsToIgnore
	 * @return a map representing request parameters.
	 */
	private Map<String, String> prepareRequestParameters(Set<String> fieldsToIgnore) {
		Map<String, String> parameters = new HashMap<String, String>();
		
		//there are some input fields which should be added to the request
		if (addInputFieldsAsParametersToUse) {
			//find out which metadata fields weren't used for substitution of placeholders
			List<String> unusedMetadata = new ArrayList<String>();
			DataRecordMetadata metadata = inputPort.getMetadata();
			String[] metadataNames = metadata.getFieldNamesArray();
			for (String metadataName : metadataNames) {
				if (!fieldsToIgnore.contains(metadataName)) {
					unusedMetadata.add(metadataName);
				}
			}

			for (String property : unusedMetadata) {
				parameters.put(property, inputRecord.getField(property).toString());
			}
		}

		return parameters;
	}
		
	/** Build a map representing multi-part entities.
	 * 
	 * @return a map representing multi-part entities.
	 */
	private Map<String, String> prepareMultipartEntities() {
		Map<String, String> multipartEntitiesMap = new HashMap<String, String>();

		//parse multipart entities
		if (multipartEntities != null) {
			StringTokenizer parts = new StringTokenizer(multipartEntities, ";");
			while(parts.hasMoreTokens()) {
				String token = parts.nextToken();
				String value = inputRecord.getField(token).toString();

				multipartEntitiesMap.put(token, value);
			}
		}
		
		return multipartEntitiesMap;
	}
	
	/**
	 * Prepares URL based on the given URL template. The substituted placeholders are added to the 
	 * set given as parameter. 
	 * 
	 * @throws ComponentNotReadyException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private String prepareURL(String urlTemplate, Set<String> substituedPlaceHolders) throws ComponentNotReadyException {
		if (urlTemplate == null) {
			throw new ComponentNotReadyException("Invalid URL: null");
		}
		
		// FIXME: is this check really necessary? The URL is checked at the end for unresolved references 
		if (!isPossibleToMapVariables(urlTemplate)) {
			throw new ComponentNotReadyException("Invalid input. Can't create URL or map fields to URL");
		}
		
		//parse URL
		//placeholder format: *{placeholder_name}
		String tempUrl = "";
		StringTokenizer st = new StringTokenizer(urlTemplate, "*");
		while (st.hasMoreTokens()) {
			tempUrl += st.nextToken();
		}

		// substitute placeholders
		String rawUrlToProceed = "";
		while (tempUrl.indexOf("{") > 0 && tempUrl.length() > 0) {
			rawUrlToProceed += tempUrl.substring(0, tempUrl.indexOf("{"));
			String propertyName = tempUrl.substring(tempUrl.indexOf("{") + 1, tempUrl.indexOf("}"));

			DataField field = inputRecord.getField(propertyName);
			if (field != null) {
				rawUrlToProceed += field.toString();
				substituedPlaceHolders.add(propertyName);
			} else {
				rawUrlToProceed += "*";
			}
			tempUrl = tempUrl.substring(tempUrl.indexOf("}") + 1, tempUrl.length());
		}
		rawUrlToProceed += tempUrl;

		if (rawUrlToProceed.indexOf("*{") > 0) {
			//some placeholder wasn't substituted. This should never happen.
			throw new ComponentNotReadyException("Invalid URL.");
		}

		return rawUrlToProceed;
	}

	/** Checks  if there are suitable metadata fields for substitution of all placeholders in given string.
	 * 
	 * @return true if and only if there are suitable metadata fields for substitution of all placeholders
	 */
	private boolean isPossibleToMapVariables(String urlTemplate) {

		boolean possibleToMapVariables = true;
		try {
			String tempUrl = "";
			if (urlTemplate.indexOf("*") > 0) {
				StringTokenizer st = new StringTokenizer(urlTemplate, "*");
				while (st.hasMoreTokens()) {
					tempUrl += st.nextToken();
				}
			} else {
				tempUrl = urlTemplate;
			}

			Set<String> variablesAtUrl = new HashSet<String>();
			while (tempUrl.indexOf("{") > 0 && tempUrl.length() > 0) {
				String propertyName = tempUrl.substring(tempUrl.indexOf("{") + 1, tempUrl.indexOf("}"));
				tempUrl = tempUrl.substring(tempUrl.indexOf("}") + 1, tempUrl.length());
				variablesAtUrl.add(propertyName);
			}

			Set<String> variablesAtMetadata = new HashSet<String>();
			if (inputPort != null) {
				DataRecordMetadata metadata = inputPort.getMetadata();
				String[] metadataNames = metadata.getFieldNamesArray();
				for (String metadataName : metadataNames) {
					variablesAtMetadata.add(metadataName);
				}
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
	
	/** Sets the properties to be used by reference resolver
	 * 
	 * @param props
	 */
	private void setRefProperties(Properties props) {
		if (refResolver.getProperties() != null) {
			refResolver.getProperties().clear();
		}
		refResolver.addProperties(props);
	}

	/** Retrieves a string representation of result of the HTTP request 
	 * 
	 * @return String representation of result of the HTTP request
	 * 
	 * @throws IOException
	 */
	private String getResponseContent(HttpResponse response) throws IOException {
		InputStream result = null;
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			result = entity.getContent();
		}

		BufferedReader reader;
		if (charsetToUse == null) {
			reader = new BufferedReader(new InputStreamReader(result));
		} else {
			reader = new BufferedReader(new InputStreamReader(result, charsetToUse));
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
	
	/** Populates the input parameter record with the parameters used.
	 * 
	 */
	protected void populateInputParamsRecord() {
		inputParamsRecord.getField(IP_URL_INDEX).setValue(rawUrlToUse);
//		inputParamsRecord.getField(IP_URL_FIELD_INDEX).setValue(urlInputFieldToUse);
		inputParamsRecord.getField(IP_REQUEST_METHOD_INDEX).setValue(requestMethodToUse);
		inputParamsRecord.getField(IP_REQUEST_CONTENT_INDEX).setValue(requestContentToUse);
		inputParamsRecord.getField(IP_INPUT_FILE_URL_INDEX).setValue(inputFileUrlToUse);
//		inputParamsRecord.getField(IP_INPUT_FIELD_NAME_INDEX).setValue(inputFieldNameToUse);
//		inputParamsRecord.getField(IP_OUTPUT_FIELD_NAME_INDEX).setValue(outputFieldNameToUse);
		inputParamsRecord.getField(IP_OUTPUT_FILE_URL_INDEX).setValue(outputFileUrlToUse);
		inputParamsRecord.getField(IP_APPEND_OUTPUT_INDEX).setValue(appendOutputToUse);
		inputParamsRecord.getField(IP_CHARSET_INDEX).setValue(charsetToUse);
		inputParamsRecord.getField(IP_REQUEST_PROPERTIES_INDEX).setValue(rawRequestPropertiesToUse);
		inputParamsRecord.getField(IP_ADD_INPUT_FIELDS_AS_PARAMETERS_INDEX).setValue(addInputFieldsAsParametersToUse);
		inputParamsRecord.getField(IP_ADD_INPUT_FIELDS_AS_PARAMETERS_TO_INDEX).setValue(addInputFieldsAsParametersToToUse);
		inputParamsRecord.getField(IP_IGNORED_FIELDS_INDEX).setValue(ignoredFieldsToUse);
		inputParamsRecord.getField(IP_AUTHENTICATION_METHOD_INDEX).setValue(authenticationMethodToUse);
		inputParamsRecord.getField(IP_USERNAME_INDEX).setValue(usernameToUse);
		inputParamsRecord.getField(IP_PASSWORD_INDEX).setValue(passwordToUse);
		inputParamsRecord.getField(IP_STORE_RESPONSE_TO_TEMP_INDEX).setValue(storeResponseToTempFileToUse);
		inputParamsRecord.getField(IP_TEMP_FILE_PREFIX_INDEX).setValue(temporaryFilePrefixToUse);
		inputParamsRecord.getField(IP_MULTIPART_ENTITIES_INDEX).setValue(multipartEntitiesToUse);
		inputParamsRecord.getField(IP_CONSUMER_KEY_INDEX).setValue(consumerKeyToUse);
		inputParamsRecord.getField(IP_CONSUMER_SECRET_INDEX).setValue(consumerSecretToUse);
	}

	/** Populates the error record.
	 *  
	 */
	protected void populateErrorRecord() {
		populateErrorField(errorRecord, EP_MESSAGE_INDEX);
	}
	
	private void populateResultRecordError() {
		populateErrorField(resultRecord, RP_MESSAGE_INDEX);
	}
	
	private void populateErrorField(DataRecord record, int errorFieldIndex) {
		Exception ex = result.getException();
		if (ex != null) {
			record.getField(errorFieldIndex).setValue(ex.toString());
		}
	}
	
	/** Creates input parameters metadata for mapping dialogs.
	 * 
	 * @return input parameter metadata used by the component's mapping dialogs
	 */
	public static DataRecordMetadata createUIInputParametersMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(ATTRIBUTES_RECORD_NAME);
		
		metadata.addField(new DataFieldMetadata(IP_URL_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(new DataFieldMetadata(IP_REQUEST_METHOD_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(new DataFieldMetadata(IP_REQUEST_CONTENT_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(new DataFieldMetadata(IP_INPUT_FILE_URL_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		
		return metadata;
	}
	
	/** Creates input parameters metadata.
	 * 
	 * @return input parameter metadata used by this component
	 */
	public static DataRecordMetadata createInputParametersMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(ATTRIBUTES_RECORD_NAME);
		
		metadata.addField(IP_URL_INDEX, new DataFieldMetadata(IP_URL_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
//		metadata.addField(IP_URL_FIELD_INDEX, new DataFieldMetadata(IP_URL_FIELD_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_REQUEST_METHOD_INDEX, new DataFieldMetadata(IP_REQUEST_METHOD_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_ADD_INPUT_FIELDS_AS_PARAMETERS_INDEX, new DataFieldMetadata(IP_ADD_INPUT_FIELDS_AS_PARAMETERS_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(IP_ADD_INPUT_FIELDS_AS_PARAMETERS_TO_INDEX, new DataFieldMetadata(IP_ADD_INPUT_FIELDS_AS_PARAMETERS_TO_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_IGNORED_FIELDS_INDEX, new DataFieldMetadata(IP_IGNORED_FIELDS_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_REQUEST_PROPERTIES_INDEX, new DataFieldMetadata(IP_REQUEST_PROPERTIES_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_CHARSET_INDEX, new DataFieldMetadata(IP_CHARSET_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_REQUEST_CONTENT_INDEX, new DataFieldMetadata(IP_REQUEST_CONTENT_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_INPUT_FILE_URL_INDEX, new DataFieldMetadata(IP_INPUT_FILE_URL_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
//		metadata.addField(IP_INPUT_FIELD_NAME_INDEX, new DataFieldMetadata(IP_INPUT_FIELD_NAME_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
//		metadata.addField(IP_OUTPUT_FIELD_NAME_INDEX, new DataFieldMetadata(IP_OUTPUT_FIELD_NAME_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_OUTPUT_FILE_URL_INDEX, new DataFieldMetadata(IP_OUTPUT_FILE_URL_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_APPEND_OUTPUT_INDEX, new DataFieldMetadata(IP_APPEND_OUTPUT_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(IP_AUTHENTICATION_METHOD_INDEX, new DataFieldMetadata(IP_AUTHENTICATION_METHOD_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_USERNAME_INDEX, new DataFieldMetadata(IP_USERNAME_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_PASSWORD_INDEX, new DataFieldMetadata(IP_PASSWORD_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_CONSUMER_KEY_INDEX, new DataFieldMetadata(IP_CONSUMER_KEY_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_CONSUMER_SECRET_INDEX, new DataFieldMetadata(IP_CONSUMER_SECRET_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_STORE_RESPONSE_TO_TEMP_INDEX, new DataFieldMetadata(IP_STORE_RESPONSE_TO_TEMP_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(IP_TEMP_FILE_PREFIX_INDEX, new DataFieldMetadata(IP_TEMP_FILE_PREFIX_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_MULTIPART_ENTITIES_INDEX, new DataFieldMetadata(IP_MULTIPART_ENTITIES_NAME, DataFieldType.STRING, DUMMY_DELIMITER));

		return metadata;
	}
	
	/** Creates result metadata.
	 * 
	 * @return result metadata used by this component
	 */
	public static DataRecordMetadata createResultMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(RESULT_RECORD_NAME);
		
		metadata.addField(RP_CONTENT_INDEX, new DataFieldMetadata(RP_CONTENT_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(RP_OUTPUTFILE_INDEX, new DataFieldMetadata(RP_OUTPUTFILE_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(RP_STATUS_CODE_INDEX, new DataFieldMetadata(RP_STATUS_CODE_NAME, DataFieldType.INTEGER, DUMMY_DELIMITER));
		
		DataFieldMetadata header = new DataFieldMetadata(RP_HEADER_NAME, DataFieldType.STRING, DUMMY_DELIMITER);
		header.setContainerType(DataFieldContainerType.MAP);
		metadata.addField(RP_HEADER_INDEX, header);
		
		metadata.addField(RP_MESSAGE_INDEX, new DataFieldMetadata(RP_MESSAGE_NAME, DataFieldType.STRING, DUMMY_DELIMITER));

		return metadata;
	}

	/** Creates error metadata.
	 * 
	 * @return error metadata used by this component
	 */
	public static DataRecordMetadata createErrorMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(ERROR_RECORD_NAME);
		
		metadata.addField(EP_MESSAGE_INDEX, new DataFieldMetadata(EP_MESSAGE_NAME, DataFieldType.STRING, DUMMY_DELIMITER));

		return metadata;
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

	public String getInputMapping() {
		return inputMapping;
	}

	public void setInputMapping(String inputMapping) {
		this.inputMapping = inputMapping;
	}

	public String getStandardOutputMapping() {
		return standardOutputMapping;
	}

	public void setStandardOutputMapping(String standardOutputMapping) {
		this.standardOutputMapping = standardOutputMapping;
	}

	public String getErrorOutputMapping() {
		return errorOutputMapping;
	}

	public void setErrorOutputMapping(String errorOutputMapping) {
		this.errorOutputMapping = errorOutputMapping;
	}	
	
	public void setUsername(String username) {
		this.username = username;
	}

	public String getUsername() {
		return username;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getPassword() {
		return password;
	}

	public void setAddInputFieldsAsParameters(boolean addInputFieldsAsParameters) {
		this.addInputFieldsAsParameters = addInputFieldsAsParameters;
	}

	public boolean getAddInputFieldsAsParameters() {
		return addInputFieldsAsParameters;
	}

	public void setIgnoredFields(String ignoredFields) {
		this.ignoredFields = ignoredFields;
	}

	public String getIgnoredFields() {
		return ignoredFields;
	}

	public void setMultipartEntities(String multipartEntities) {
		this.multipartEntities = multipartEntities;
	}

	public String getMultipartEntities() {
		return multipartEntities;
	}

	public void setUrlInputField(String urlInputField) {
		this.urlInputField = urlInputField;
	}

	public String getUrlInputField() {
		return urlInputField;
	}

	public void setAddInputFieldsAsParametersTo(String addInputFieldsAsParametersTo) {
		this.addInputFieldsAsParametersTo = addInputFieldsAsParametersTo;
	}

	public String getAddInputFieldsAsParametersTo() {
		return addInputFieldsAsParametersTo;
	}

	public void setAuthenticationMethod(String authenticationMethod) {
		this.authenticationMethod = authenticationMethod;
	}

	public String getAuthenticationMethod() {
		return authenticationMethod;
	}

	public String getConsumerKey() {
		return consumerKey;
	}

	public void setConsumerKey(String consumerKey) {
		this.consumerKey = consumerKey;
	}

	public String getConsumerSecret() {
		return consumerSecret;
	}

	public void setConsumerSecret(String consumerSecret) {
		this.consumerSecret = consumerSecret;
	}	
	
	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}
	
	private void setRedirectErrorOutput(boolean redirectErrorOutput) {
		this.redirectErrorOutput = redirectErrorOutput;
	}

	@Override
	protected ComponentTokenTracker createComponentTokenTracker() {
		return new ReformatComponentTokenTracker(this);
	}
}