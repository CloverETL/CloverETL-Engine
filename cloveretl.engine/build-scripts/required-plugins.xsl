<?xml version="1.0" encoding="UTF-8" ?>
<xsl:stylesheet 
	xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
	xmlns:fn="http://www.w3.org/2005/02/xpath-functions"
	version="2.0">
 	
 	<xsl:param name="separator"/>
 	<xsl:param name="dist"/>
 	<xsl:param name="home"/>
	<xsl:output method="text"/> 	
 	
	<xsl:template match="*">
		<xsl:text>required.plugins=</xsl:text>
        <xsl:apply-templates select="/plugin/requires" mode="list"/>
		<xsl:text disable-output-escaping="yes"><![CDATA[
]]></xsl:text>
		
		<xsl:text>required.plugins.dist=</xsl:text>
        <xsl:apply-templates select="/plugin/requires" mode="cp"/>
		<xsl:text disable-output-escaping="yes"><![CDATA[
]]></xsl:text>

		<xsl:text>required.plugins.classes=</xsl:text>
        <xsl:apply-templates select="/plugin/requires" mode="classes"/>
		<xsl:text disable-output-escaping="yes"><![CDATA[
]]></xsl:text>
		<xsl:text>required.plugins.dirs=</xsl:text>
        <xsl:apply-templates select="/plugin/requires" mode="dirs"/>
		<xsl:text disable-output-escaping="yes"><![CDATA[
]]></xsl:text>
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
		<!-- escape \ for build on windows -->
		<xsl:call-template name="escapeBackslash"><xsl:with-param name="string"><xsl:value-of select="$dist" /></xsl:with-param></xsl:call-template>
		<xsl:text>/plugins/</xsl:text>
		<xsl:value-of select="@plugin-id"/>
		<xsl:text>/cloveretl.</xsl:text>
	
		<xsl:if test="starts-with(@plugin-id,'org.jetel.')">
			<xsl:value-of select="substring-after(@plugin-id,'org.jetel.')"/>
		</xsl:if>

		<xsl:if test="starts-with(@plugin-id,'org.jetel.license')">
		  <xsl:text>.engine</xsl:text>
		</xsl:if>

		<xsl:if test="starts-with(@plugin-id,'com.opensys.clover.')">
			<xsl:value-of select="substring-after(@plugin-id,'com.opensys.clover.')"/>
			<xsl:text>.commercial</xsl:text>
		</xsl:if>
	
		<xsl:text>.jar</xsl:text>
	</xsl:template>	
	

	<xsl:template match="/plugin/requires" mode="classes">
        <xsl:apply-templates select="*" mode="classes"/>
	</xsl:template>
	<xsl:template match="import" mode="classes">
		<xsl:text>;</xsl:text>
		<!-- escape \ for build on windows -->
		<xsl:call-template name="escapeBackslash"><xsl:with-param name="string"><xsl:value-of select="$home" /></xsl:with-param></xsl:call-template>
		<xsl:text>/cloveretl.</xsl:text>

		<xsl:if test="starts-with(@plugin-id,'org.jetel.')">
			<xsl:value-of select="substring-after(@plugin-id,'org.jetel.')"/>
		</xsl:if>

		<xsl:if test="starts-with(@plugin-id,'com.opensys.clover.')">
			<xsl:value-of select="substring-after(@plugin-id,'com.opensys.clover.')"/>
			<xsl:text>.commercial</xsl:text>
		</xsl:if>
		
		<xsl:text>/build/classes</xsl:text>
	</xsl:template>	

	<xsl:template match="/plugin/requires" mode="dirs">
        <xsl:apply-templates select="*" mode="dirs"/>
	</xsl:template>
	<xsl:template match="import" mode="dirs">
		<xsl:if test="position() != 1">
			<xsl:value-of select="$separator"/>
		</xsl:if>
		<!-- escape \ for build on windows -->
		<xsl:text>cloveretl.</xsl:text>

		<xsl:if test="starts-with(@plugin-id,'org.jetel.')">
			<xsl:value-of select="substring-after(@plugin-id,'org.jetel.')"/>
		</xsl:if>

		<xsl:if test="starts-with(@plugin-id,'com.opensys.clover.')">
			<xsl:value-of select="substring-after(@plugin-id,'com.opensys.clover.')"/>
			<xsl:text>.commercial</xsl:text>
		</xsl:if>
	</xsl:template>	
		

	<xsl:template name="escapeBackslash">
		<xsl:param name="string" />
		<xsl:if test="contains($string, '\')"><xsl:value-of select="substring-before($string, '\')" />\\<xsl:call-template name="escapeBackslash"><xsl:with-param name="string"><xsl:value-of select="substring-after($string, '\')" /></xsl:with-param></xsl:call-template>
		</xsl:if>
		<xsl:if test="not(contains($string, '\'))"><xsl:value-of select="$string" />
		</xsl:if>
	</xsl:template>
	
</xsl:stylesheet>
