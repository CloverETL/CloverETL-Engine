<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<xsl:output method="xml" encoding="utf-8" indent="yes"/>

<xsl:template match="/">
<actors>
	<xsl:for-each select="//actor[not(@actor_id=following::actor/@actor_id)]">
		<xsl:sort select="@actor_id" data-type="number"/>
        <actor actor_id="{@actor_id}">
        	<first_name><xsl:value-of select="first_name"/></first_name>
        	<last_name><xsl:value-of select="last_name"/></last_name>
        <xsl:for-each select="//actor[@actor_id=current()/@actor_id]">
        	<movie>
        		<title><xsl:value-of select="../title"/></title>
        		<description><xsl:value-of select="../description"/></description>
        	</movie>
        </xsl:for-each>
        </actor>
    </xsl:for-each>
</actors>
</xsl:template>
</xsl:stylesheet>
