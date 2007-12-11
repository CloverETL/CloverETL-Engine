<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE xsl:stylesheet [ <!ENTITY nbsp "&#160;"> ]>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">


<xsl:output method="html" encoding="ISO-8859-2"/>
	
	
<xsl:template name="singleDesc">
<a name="{componentName}" />
<h2><xsl:value-of select="componentName"/></h2>
	
 <table class="componentDesc">
      <tbody>
        <tr colspan="2">        	
        	<td><xsl:apply-templates select="briefDesc"/></td>
        </tr>
        <tr><td>&nbsp;</td></tr>
        <tr>
          <td class="head">Component type</td>
          <td>          
              <xsl:value-of select="componentType"/>
          </td>
        </tr>     
		<tr>
          <td class="head">Category</td>
          <td>
              <xsl:value-of select="category"/>
          </td>
        </tr>  
		<tr>
          <td class="head">Last updated</td>
          <td>
              <xsl:value-of select="lastUpdated"/>
          </td>          
        </tr> 
		<tr>
		  <td class="head">Since version</td>
          <td>
            <xsl:value-of select="sinceVersion"/>
          </td>
        </tr> 
      </tbody>
    </table>

<p>
	<xsl:apply-templates select="fullDesc"/>
</p>
&nbsp;<br/>

 <table class="inputs">
      <tbody>       
		<tr>
		  <td class="title" colspan="4">
		    Inputs
		  </td>
		</tr>
		<tr>
			<td class="head-framed">
				Number
			</td>
			<td class="head-framed">
				Description
			</td>
			<td class="head-framed">
				Detailed description
			</td>
			<td class="head-framed">
				Mandatory
			</td>
		</tr>
		  
		  <xsl:for-each select="inputPorts/port">
        <tr>
          <td class="framed">
			  <xsl:choose>
				<xsl:when test="boolean(portMetadata)">
					<a href="#_inport{portName}"> 
					<xsl:value-of select="portName"/>
					</a>
				</xsl:when>
				<xsl:otherwise><xsl:value-of select="portName"/></xsl:otherwise>
			</xsl:choose>		
            <xsl:value-of select="portName"/>
            &nbsp;
          </td>
	  	  <td class="framed">
	  	    <xsl:apply-templates select="portDesc"/>&nbsp;
          </td>
	  	  <td class="framed">
            <xsl:apply-templates select="portLongDesc"/>&nbsp;
          </td>
	  	  <td class="framed">
            <xsl:value-of select="portMandatory"/>&nbsp;
          </td>          
        </tr>
		</xsl:for-each>
       
       
      </tbody>
    </table>
			
  <table class="outputs">
      <tbody>
		<tr>
		  <td class="title" colspan="4">
		    Outputs
		  </td>
		</tr>
		<tr>
			<td class="head-framed">
				Number
			</td>
			<td class="head-framed">
				Description
			</td>
			<td class="head-framed">
				Detailed description
			</td>
			<td class="head-framed">
				Mandatory
			</td>
		</tr>
             		  
		  <xsl:for-each select="outputPorts/port">
        <tr>
          <td class="framed">
				<xsl:choose>
				<xsl:when test="boolean(portMetadata)">
					<a href="#_outport{portName}"> 
					<xsl:value-of select="portName"/>
					</a>
				</xsl:when>
				<xsl:otherwise><xsl:value-of select="portName"/></xsl:otherwise>
				</xsl:choose>
				&nbsp;            
          </td>
	  	  <td class="framed">
            <xsl:apply-templates select="portDesc"/>&nbsp;
          </td>
	  	  <td class="framed">
            <xsl:apply-templates select="portLongDesc"/>&nbsp;
          </td>
	  	  <td class="framed">
            <xsl:value-of select="portMandatory"/>&nbsp;
          </td>
        </tr>
		</xsl:for-each>

    	</tbody>
    </table>

<div class="clear"/>
&nbsp;<br/>
	
<xsl:for-each select="inputPorts/port">
	<xsl:if test="boolean(portMetadata)">
		<a name="_inport{portName}"/>
		
		<table class="fullwidth">
		  <tbody>       
		  	    <tr>
		  	    	<td class="title" colspan="3">
		  	    		Metadata for port <xsl:value-of select="portName"/>
		  	    	</td>
		  	    </tr>
				<tr>
					<td class="head-framed">
						Type
					</td>
					<td class="head-framed">
						Nullable
					</td>
					<td class="head-framed">
						Description
					</td>					
				</tr>
			  
			  <xsl:for-each select="portMetadata/metaField">
				<tr>
				  <td class="framed">
					<xsl:value-of select="fieldType"/>
					&nbsp;
				  </td>
			  	  <td class="framed">
					<xsl:value-of select="fieldNullable"/>
					&nbsp;
				  </td>
			      <td class="framed">
					<xsl:apply-templates select="fieldDesc"/>
					&nbsp;
				  </td>							 
				</tr>
			</xsl:for-each>		   
		   
		  </tbody>
    </table>		
	</xsl:if>
</xsl:for-each>

	
<xsl:for-each select="outputPorts/port">
	<xsl:if test="boolean(portMetadata)">
		<a name="_outport{portName}"/>
		
		<table class="fullwidth">
		  <tbody>       
		  	    <tr>
		  	    	<td class="title" colspan="3">
		  	    		Metadata for port <xsl:value-of select="portName"/>
		  	    	</td>
		  	    </tr>
				<tr>
					<td class="head-framed">
						Type
					</td>
					<td class="head-framed">
						Nullable
					</td>
					<td class="head-framed">
						Description
					</td>					
				</tr>
			  <xsl:for-each select="portMetadata/metaField">
				<tr>
				  <td class="framed">
					<xsl:value-of select="fieldType"/>
					&nbsp;
				  </td>
			      <td class="framed">
					<xsl:value-of select="fieldNullable"/>
					&nbsp;
				  </td>
			  	  <td class="framed">
					<xsl:apply-templates select="fieldDesc"/>
					&nbsp;
				  </td>							 
				</tr>
			</xsl:for-each>
		   		   
		  </tbody>
    </table>
		
		
	</xsl:if>
</xsl:for-each>
	
&nbsp;<br/>
		 	
    <table class="fullwidth">
      <tbody>
        <tr>
          <td class="title" colspan="5">
          Attributes
          </td>
        </tr>
        <tr>
          <td class="head-framed">
              Name
          </td>
		  <td class="head-framed">
              Type
          </td>
          <td class="head-framed">
              Mandatory
          </td>
		  <td class="head-framed">
              Description
          </td>
		  <td class="head-framed">
              Default value
          </td>
        
       </tr>
		   	
	   <xsl:for-each select="attribute">
		  
		<a name="{attrName}"/>
		   
        <tr>
          <td class="framed">
              <xsl:value-of select="attrName"/>
              &nbsp;
          </td>
		  <td class="framed">
		  <xsl:variable name="normalizedAttrType"><xsl:value-of select="normalize-space(string(attrType))"/></xsl:variable>
		    <xsl:choose>
		    	<xsl:when test="count(document('TypesDoc.xml',document(''))/typesDoc/attributeType[attributeTypeName=$normalizedAttrType])>0">		    	
					<a href="TypesDoc.html#{$normalizedAttrType}"> 
						<xsl:value-of select="$normalizedAttrType"/>
					</a>	    	
				</xsl:when>
				
				<xsl:otherwise>
		    		<xsl:value-of select="$normalizedAttrType"/>
				</xsl:otherwise>
		    </xsl:choose>
		    &nbsp;
          </td>
          <td class="framed">
             <xsl:value-of select="attrIsMandatory"/>
             &nbsp;
          </td>
	  	  <td class="framed">
             <xsl:apply-templates select="attrDesc"/>             
             &nbsp;
          </td>
	  	  <td class="framed">
              <xsl:value-of select="attrDefaultVal"/>
              &nbsp;
          </td>
        </tr>
		</xsl:for-each>
        
      </tbody>
    </table>
		  
		  	  

<div class="title">Examples</div>

<xsl:for-each select="example">
	
<xsl:if test="normalize-space(exampleImg)">
		<img  src="{exampleImg}" alt="Example" border="0"/>
</xsl:if>

<xsl:if test="normalize-space(exampleXml)">
XML graph notation:
<pre class="programlisting">
	<xsl:apply-templates select="exampleXml"/>
</pre>
</xsl:if>

<xsl:if test="normalize-space(exampleDesc)">
	<p class="framed">
		<xsl:apply-templates select="exampleDesc"/>
	</p>
</xsl:if>
	 
</xsl:for-each>				 
   
</xsl:template>	  	 	   
<!-- singleDesc -->


	
<xsl:template match="/Container">
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1"/>
    <meta http-equiv="Content-Style-Type" content="text/css"/>   
    <title>
      Description of the components
    </title>
    <link href="help.css" rel="stylesheet" charset="ISO-8859-1" type="text/css"/> 
  </head>
		
  <body>
	  <a name="readers"> <h1> Readers </h1> </a>
		<xsl:for-each select="componentsDoc/componentDescription[category='readers']">
			<xsl:sort select="componentName"/>		 			
			<xsl:call-template name="singleDesc"/>
		</xsl:for-each>
	  
	  <a name="writers"> <h1> Writers </h1> </a>	  
		<xsl:for-each select="componentsDoc/componentDescription[category='writers']">
			<xsl:sort select="componentName"/>		
			<xsl:call-template name="singleDesc"/>					
		</xsl:for-each>
	  
	  <a name="transformers"> <h1> Transformers </h1> </a>	  
		<xsl:for-each select="componentsDoc/componentDescription[category='transformers']">
			<xsl:sort select="componentName"/>		
			<xsl:call-template name="singleDesc"/>					
		</xsl:for-each>
	  
	  <a name="joiners"> <h1> Joiners </h1> </a>	  
		<xsl:for-each select="componentsDoc/componentDescription[category='joiners']">
			<xsl:sort select="componentName"/>		
			<xsl:call-template name="singleDesc"/>					
		</xsl:for-each>
	  
	  <a name="others"> <h1> Others </h1> </a>	  
		<xsl:for-each select="componentsDoc/componentDescription[category='others']">
			<xsl:sort select="componentName"/>		
			<xsl:call-template name="singleDesc"/>					
		</xsl:for-each>
	  
	  <a name="deprecated"> <h1> Deprecated </h1> </a>	  
		<xsl:for-each select="componentsDoc/componentDescription[category='deprecated']">
			<xsl:sort select="componentName"/>		
			<xsl:call-template name="singleDesc"/>					
		</xsl:for-each>


		<xsl:if test="count(componentsDoc/componentDescription[category!='readers' and category!='writers' and category!='joiners' and category!='transformers' and category!='others' and category!='deprecated'])>0">		    	
  		<a name="uncategorized"> <h1> Category Unkown </h1> </a>	  
			<xsl:for-each select="componentsDoc/componentDescription[category!='readers' and category!='writers' and category!='joiners' and category!='transformers' and category!='others' and category!='deprecated']">
			<xsl:sort select="componentName"/>
				<xsl:call-template name="singleDesc"/>					
			</xsl:for-each>
		</xsl:if>

	  
	  <!--
	  <xsl:for-each select="document('ComponentCategories.xml')/categories/category">
		<h2> <xsl:value-of select="@name"/> </h2>
			<xsl:for-each select="componentsDoc/componentDescription[category='{name}']">
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

<xsl:template match="fullDesc|briefDesc|portDesc|portLongDesc|attrDesc|fieldDesc|exampleDesc|exampleXml">
<xsl:apply-templates/>
</xsl:template>

<xsl:template match="fullDesc//*|briefDesc//*|portDesc//*|portLongDesc//*|attrDesc//*|fieldDesc//*|exampleDesc//*|exampleXml//*">
<xsl:copy>
<xsl:apply-templates/>
</xsl:copy>
</xsl:template>

</xsl:stylesheet>