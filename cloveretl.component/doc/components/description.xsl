<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:output method="html" encoding="ISO-8859-2"/>
	
	
<xsl:template name="singleDesc">
		 		  
	<a name="{componentName}" />
    <h1>
      <xsl:value-of select="componentName"/>
    </h1>
	
	
 <table border="1" cellspacing="0" cellpadding="5">
      <tbody>       
        <tr align="left" valign="top">
          <td align="left" valign="top">
            <strong>
			Name
			</strong>
            
          </td>
          <td align="left" valign="top">
            <p>
              <xsl:value-of select="componentName"/>
            </p>
          </td>
          
        </tr>     
		<tr align="left" valign="top">
          <td align="left" valign="top">
            <strong>
			Category
			</strong>
            
          </td>
          <td align="left" valign="top">
            <p>
              <xsl:value-of select="category"/>
            </p>
          </td>
          
        </tr>  
		<tr align="left" valign="top">
          <td align="left" valign="top">
            <strong>
			Last updated
			</strong>
            
          </td>
          <td align="left" valign="top">
            <p>
              <xsl:value-of select="lastUpdated"/>
            </p>
          </td>
          
        </tr> 
		<tr align="left" valign="top">
          <td align="left" valign="top">
            <strong>
			Since version
			</strong>
            
          </td>
          <td align="left" valign="top">
            <p>
              <xsl:value-of select="sinceVersion"/>
            </p>
          </td>
          
        </tr> 
		<tr align="left" valign="top">
          <td align="left" valign="top">
            <strong>
			Brief Description
			</strong>
            
          </td>
          <td align="left" valign="top">
            <p>
              <xsl:value-of select="briefDesc"/>
            </p>
          </td>
          
        </tr>
		  
		
		   
		<tr align="left" valign="top">
          <td align="left" valign="top">
            <strong>
			Full description
			</strong>
            
          </td>
          <td align="left" valign="top">
            <p>
			     <xsl:apply-templates select="fullDesc"/>
            </p>
          </td>
          
        </tr>   
		 
		  
		
       
      </tbody>
    </table>
<br/>

<strong>Inputs</strong>
 <table border="1" cellspacing="0" cellpadding="5">
      <tbody>       
	<tr>
		<th>
			<p>
			Number
			</p>
		</th>
		<th>
			<p>
			Description
			</p>
		</th>
		<th>
			<p>
			Detailed description
			</p>
		</th>
		<th>
			<p>
			Mandatory
			</p>
		</th>
	</tr>
		  
		  <xsl:for-each select="inputPorts/port">
        <tr align="left" valign="top">
          <td align="left" valign="top">
			  <xsl:choose>
				<xsl:when test="boolean(portMetadata)">
					<a href="#_inport{portName}"> 
					<xsl:value-of select="portName"/>
					</a>
				</xsl:when>
				<xsl:otherwise><xsl:value-of select="portName"/></xsl:otherwise>
			</xsl:choose>		
			  			  			  
             <p>
            <xsl:value-of select="portName"/>
			</p>
          </td>
	  <td align="left" valign="top">
             <p>
            <xsl:value-of select="portDesc"/>
		</p>
          </td>
	  <td align="left" valign="top">
             <p>
            <xsl:value-of select="portLongDesc"/>
		</p>
          </td>
	  <td align="left" valign="top">
             <p>
            <xsl:value-of select="portMandatory"/>
		</p>
          </td>
          
        </tr>
		</xsl:for-each>
       
       
      </tbody>
    </table>
<br/>
	
<xsl:for-each select="inputPorts/port">
	<xsl:if test="boolean(portMetadata)">
		<a name="_inport{portName}"> <strong> Metadata for port <xsl:value-of select="portName"/> </strong> </a>
		
		<table border="1" cellspacing="0" cellpadding="5">
		  <tbody>       
				<tr>
					<th>
						<p>
						Type
						</p>
					</th>
					<th>
						<p>
						Nullable
						</p>
					</th>
					<th>
						<p>
						Description
						</p>
					</th>					
				</tr>
			  
			  <xsl:for-each select="portMetadata/metaField">
				<tr align="left" valign="top">
				  <td align="left" valign="top">
					 <p>
					<xsl:value-of select="fieldType"/>
					</p>
				  </td>
			  <td align="left" valign="top">
					 <p>
					<xsl:value-of select="fieldNullable"/>
				</p>
				  </td>
			  <td align="left" valign="top">
					 <p>
					<xsl:value-of select="fieldDesc"/>
				</p>
				  </td>							 
				</tr>
			</xsl:for-each>
		   
		   
		  </tbody>
    </table>
		
		
	</xsl:if>
</xsl:for-each>

<strong>Outputs</strong>
  <table border="1" cellspacing="0" cellpadding="5">
      <tbody>       
	<tr>
		<th>
			<p>
			Number
			</p>
		</th>
		<th>
			<p>
			Description
			</p>
		</th>
		<th>
			<p>
			Detailed description
			</p>
		</th>
		<th>
			<p>
			Mandatory
			</p>
		</th>
	</tr>
		  
		  <xsl:for-each select="outputPorts/port">
        <tr align="left" valign="top">
          <td align="left" valign="top">
             <p>				 
				<xsl:choose>
				<xsl:when test="boolean(portMetadata)">
					<a href="#_outport{portName}"> 
					<xsl:value-of select="portName"/>
					</a>
				</xsl:when>
				<xsl:otherwise><xsl:value-of select="portName"/></xsl:otherwise>
				</xsl:choose>				 			            
			</p>
          </td>
	  <td align="left" valign="top">
             <p>
            <xsl:value-of select="portDesc"/>
		</p>
          </td>
	  <td align="left" valign="top">
             <p>
            <xsl:value-of select="portLongDesc"/>
		</p>
          </td>
	  <td align="left" valign="top">
             <p>
            <xsl:value-of select="portMandatory"/>
		</p>
          </td>
          
        </tr>
		</xsl:for-each>
       
       
      </tbody>
    </table>
<br/>
	
<xsl:for-each select="outputPorts/port">
	<xsl:if test="boolean(portMetadata)">
		<a name="_outport{portName}"> <strong> Metadata for port <xsl:value-of select="portName"/> </strong> </a>
		
		<table border="1" cellspacing="0" cellpadding="5">
		  <tbody>       
				<tr>
					<th>
						<p>
						Type
						</p>
					</th>
					<th>
						<p>
						Nullable
						</p>
					</th>
					<th>
						<p>
						Description
						</p>
					</th>					
				</tr>			  
			  <xsl:for-each select="portMetadata/metaField">
				<tr align="left" valign="top">
				  <td align="left" valign="top">
					 <p>
					<xsl:value-of select="fieldType"/>
					</p>
				  </td>
			  <td align="left" valign="top">
					 <p>
					<xsl:value-of select="fieldNullable"/>
				</p>
				  </td>
			  <td align="left" valign="top">
					 <p>
					<xsl:value-of select="fieldDesc"/>
				</p>
				  </td>							 
				</tr>
			</xsl:for-each>
		   		   
		  </tbody>
    </table>
		
		
	</xsl:if>
</xsl:for-each>
	
		 	
<strong>Attributes</strong>
    <table border="1" cellspacing="0" cellpadding="5">
      <tbody>
        <tr>
          <th>
            <p>
              Name
            </p>
          </th>
		  <th>
            <p>
              Type
            </p>
          </th>
          <th>
            <p>
              Mandatory
            </p>
          </th>
		   <th>
            <p>
              Description
            </p>
          </th>
		   <th>
            <p>
              Default value
            </p>
          </th>
        
       </tr>
		   	
	   <xsl:for-each select="attribute">
		  
		<a name="{attrName}"/>
		   
        <tr align="left" valign="top">
          <td align="left" valign="top">
            <p>
              <xsl:value-of select="attrName"/>
            </p>
          </td>
		  <td align="left" valign="top">
			  <xsl:choose>
				  <xsl:when test="starts-with(normalize-space(string(attrType)), 'enumeration')">
					              <xsl:value-of select="attrType"/>
				  </xsl:when>
				  <xsl:otherwise>
					  <a href="TypesDoc.html#{attrType}"> 
					  <xsl:value-of select="attrType"/>
					  </a>
				  </xsl:otherwise>			
			  </xsl:choose>              
          </td>
          <td align="left" valign="top">
            <p>
             <xsl:value-of select="attrIsMandatory"/>
            </p>
          </td>
	  <td align="left" valign="top">
            <p>
             <xsl:value-of select="attrDesc"/>
            </p>
          </td>
	  <td align="left" valign="top">
            <p>
              <xsl:value-of select="attrDefaultVal"/>
            </p>
          </td>
          
        </tr>
		</xsl:for-each>
        
      </tbody>
    </table>
<br/>		  
		  	  
		  
<strong>Examples</strong>
 <xsl:for-each select="example">
    <p>
       <img  src="{exampleImg}" alt="Example" border="0"/>
    </p>

XML graph notation:
<pre>
	<xsl:value-of select="exampleXml"/>
</pre>
<p>
	<xsl:value-of select="exampleDesc"/>
</p>
	 
	

</xsl:for-each>				 

	<br/>
		   
</xsl:template>	  	 	   
<!-- singleDesc -->

	
	
<xsl:template match="/componentsDoc">
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1"/>
    <meta http-equiv="Content-Style-Type" content="text/css"/>   
    <title>
      Description of the components
    </title>
    <link rel="stylesheet" href="stylesheet.css" charset="ISO-8859-1" type="text/css"/> 
  </head>
		
  <body>
	  <a name="readers"> <h2> Readers </h2> </a>
	  <br/>	  
		<xsl:for-each select="componentDescription[category='readers']">			
			
			<xsl:call-template name="singleDesc"/>
		</xsl:for-each>
	  
	  <a name="writers"> <h2> Writers </h2> </a>	  
	  <br/>	  
		<xsl:for-each select="componentDescription[category='writers']">
			
			
			<xsl:call-template name="singleDesc"/>					
		</xsl:for-each>
	  
	  <a name="transformers"> <h2> Transformers </h2> </a>	  
	  <br/>	  
		<xsl:for-each select="componentDescription[category='transformers']">
			
			<br/>
			<xsl:call-template name="singleDesc"/>					
		</xsl:for-each>
	  
	  <a name="joiners"> <h2> Joiners </h2> </a>	  
	  <br/>	  
		<xsl:for-each select="componentDescription[category='joiners']">
			
			<br/>
			<xsl:call-template name="singleDesc"/>					
		</xsl:for-each>
	  
	  <a name="others"> <h2> Others </h2> </a>	  
	  <br/>	  
		<xsl:for-each select="componentDescription[category='others']">
			
			<br/>
			<xsl:call-template name="singleDesc"/>					
		</xsl:for-each>
	  
	  <a name="deprecated"> <h2> Deprecated </h2> </a>	  
	  <br/>	  
		<xsl:for-each select="componentDescription[category='deprecated']">
			
			<br/>
			<xsl:call-template name="singleDesc"/>					
		</xsl:for-each>
	  
	  <!--
	  <xsl:for-each select="document('ComponentCategories.xml')/categories/category">
		<h2> <xsl:value-of select="@name"/> </h2>
			<xsl:for-each select="componentDescription[category='{name}']">
				<a href="ComponentsDoc.html#{componentName}"> <xsl:value-of select="componentName"/> </a>
				<br/>
				<xsl:call-template name="singleDesc"/>					
			</xsl:for-each>
	  </xsl:for-each>
		  -->  
  </body>
		
</html>

</xsl:template>
	
<xsl:template match="attrLink">
	<a href="ComponentsDoc.html#{.}"> <xsl:value-of select="."/> </a>		
</xsl:template>
	

	
</xsl:stylesheet>