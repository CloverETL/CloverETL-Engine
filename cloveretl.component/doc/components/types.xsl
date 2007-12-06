<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:output method="html" encoding="ISO-8859-2"/>

<xsl:template name="singleType">
    <a name="{attributeTypeName}" />
    <tr>
    	<td><xsl:value-of select="attributeTypeName"/></td>
    	<td><xsl:value-of select="description"/></td>
    	<td><xsl:value-of select="example"/></td>
   	</tr>
</xsl:template>
	
<xsl:template match="/typesDoc">
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1"/>
    <meta http-equiv="Content-Style-Type" content="text/css"/>   
    <title>
      Attributes data types
    </title>
	<link rel="stylesheet" href="stylesheet.css" charset="ISO-8859-1" type="text/css"/>
  </head>
		
  <body>
  <h2>Attribute data types</h2>
  <table class="componentDesc" border="1" cellspacing="0" cellpadding="5" ><tbody>
  		<th>Type</th>
  		<th>Description</th>
  		<th>Example</th>
	  	<xsl:for-each select="attributeType">
			<xsl:call-template name="singleType"/>					
		</xsl:for-each>
  </tbody></table>
	  
  </body>
		
</html>

</xsl:template>
	
	
</xsl:stylesheet>