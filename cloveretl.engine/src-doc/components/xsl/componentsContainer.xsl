<xsl:stylesheet version="1.0" 
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:template match="/">
    <Container>      
      <xsl:copy-of select="document('../../../../cloveretl.component/doc/components/ComponentsDoc.xml',document(''))"/>        
      <xsl:copy-of select="document('../../../../cloveretl.bulkloader/doc/components/BulkLoaderDoc.xml',document(''))"/>
    </Container>
  </xsl:template>
</xsl:stylesheet>
