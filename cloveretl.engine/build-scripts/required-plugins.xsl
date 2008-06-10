<?xml version="1.0" encoding="UTF-8" ?>
<xsl:stylesheet 
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	version="1.0">
 	
 	<xsl:param name="separator"/>
 	<xsl:param name="dist"/>
	<xsl:output method="text"/> 	
 	
	<xsl:template match="*">
		<xsl:text>required.plugins=</xsl:text>
        <xsl:apply-templates select="/plugin/requires" mode="list"/>
        
		<xsl:text disable-output-escaping="yes"><![CDATA[
]]></xsl:text>
		
		<xsl:text>required.plugins.dist=</xsl:text>
        <xsl:apply-templates select="/plugin/requires" mode="cp"/>
	</xsl:template>

	<xsl:template match="/plugin/requires" mode="list">
        <xsl:apply-templates select="*" mode="list"/>
	</xsl:template>

	<xsl:template match="import" mode="list">
		<xsl:if test="position() != 1">
			<xsl:value-of select="$separator"/>
		</xsl:if>
		<xsl:value-of select="@plugin-id"/>
	</xsl:template>	

	<xsl:template match="/plugin/requires" mode="cp">
        <xsl:apply-templates select="*" mode="cp"/>
	</xsl:template>

	<xsl:template match="import" mode="cp">
		<xsl:text>;</xsl:text>
		<xsl:value-of select="$dist"/>
		<xsl:text>/plugins/</xsl:text>
		<xsl:value-of select="@plugin-id"/>
		<xsl:text>/cloveretl.</xsl:text>
		<xsl:value-of select="substring-after(@plugin-id,'org.jetel.')"/>
		<xsl:text>.jar</xsl:text>
	</xsl:template>	
</xsl:stylesheet>
