<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:output method="html" encoding="ISO-8859-2"/>
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

	<xsl:for-each select="componentDescription">
		<a href="ComponentsDoc.html#{componentName}"> Component <xsl:value-of select="componentName"/> </a>
		<br/>
		
	</xsl:for-each>	
	  
	  
	  <xsl:for-each select="componentDescription">
		  
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
              <xsl:value-of select="fullDesc"/>
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
		  
        <tr align="left" valign="top">
          <td align="left" valign="top">
            <p>
              <xsl:value-of select="attrName"/>
            </p>
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

<pre>
	<xsl:value-of select="exampleXml"/>
</pre>
<p>
	<xsl:value-of select="exampleDesc"/>
</p>
	 
</xsl:for-each>

	<br/>		   
	</xsl:for-each>	

  </body>
</html>

</xsl:template>
</xsl:stylesheet>