<!--
MIT License

Copyright (c) 2017 Bibliome

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


    yatea2tab.xslt - converts an YaTeA XML output into CSV with one term candidate per line.
    
    Author
        Robert Bossy

    History:
        2008-??-??  :  creation of this script
	2008-10-31  :  ???
	2010-05-19  :  added parameters 'filter' and 'merge'

    Usage:
        xsltproc [-\-stringparam PARAM VALUE] yatea2tab.xslt candidates.xml

    Parameters:
        filter : if equals 'yes', only print term candidates with 'DISMISSED' attribute equal to 'FALSE', default: 'no'.
	merge  : if equals 'yes', only print term candidates with 'MERGE_TYPE' attribute equal to 'UNIQ' or 'MERGING', default: 'no'.
	corpus : if not empty, reads the corpus XML file generated by yatea and includes occurrences in the output, default: ''.
-->


<xsl:stylesheet
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:exsl="http://exslt.org/common"
     extension-element-prefixes="exsl"
     version="1.0">

  <xsl:output method="text" />

  <xsl:param name="filter">no</xsl:param>
  <xsl:param name="merge">no</xsl:param>
  <xsl:param name="corpus"/>
  <xsl:param name="sep"><xsl:text>, </xsl:text></xsl:param>
  <xsl:param name="tab"><xsl:text>	</xsl:text></xsl:param>
  <xsl:param name="nl"><xsl:text>
</xsl:text></xsl:param>

  <xsl:variable name="docs">
    <xsl:if test="$corpus">
      <xsl:value-of select="document($corpus)"/>
    </xsl:if>
  </xsl:variable>

  <xsl:key name="term-by-id" match="TERM_CANDIDATE" use="./ID" />

  <xsl:template match="TERM_EXTRACTION_RESULTS">
    <xsl:value-of select="concat(
			  'ID', $tab,
			  'PREVALIDATION', $tab,
			  'VALIDATION', $tab,
			  '#OCC', $tab,
			  'SURFACE FORM', $tab,
			  'LEMMA', $tab,
			  'POS', $tab,
			  'HEAD LEMMA', $tab,
			  'HEAD++ SURFACE FORM', $tab,
			  'MODIFIER SURFACE FORM', $tab,
			  'MNP STATUS', $nl)" />
    <xsl:apply-templates select="LIST_TERM_CANDIDATES/TERM_CANDIDATE">
      <xsl:sort data-type="number" order="descending" select="NUMBER_OCCURRENCES" />
    </xsl:apply-templates>
  </xsl:template>

  <xsl:template match="TERM_CANDIDATE">
    <xsl:if test="(($filter != 'yes') or (@DISMISSED = 'FALSE')) and (($merge != 'yes') or (@MERGE_TYPE = 'UNIQ') or (@MERGE_TYPE = 'MERGING'))">
      <xsl:value-of select="concat(
			    ./ID, $tab,
			    '', $tab,
			    '', $tab,
			    ./NUMBER_OCCURRENCES, $tab,
			    ./FORM, $tab,
			    ./LEMMA, $tab,
			    ./MORPHOSYNTACTIC_FEATURES/SYNTACTIC_CATEGORY, $tab,
			    key('term-by-id', ./HEAD)/LEMMA, $tab,
			    key('term-by-id', normalize-space(./SYNTACTIC_ANALYSIS/HEAD))/FORM, $tab,
			    key('term-by-id', normalize-space(./SYNTACTIC_ANALYSIS/MODIFIER))/FORM, $tab,
			    LIST_OCCURRENCES/OCCURRENCE[1]/MNP
			    )" />
      <xsl:if test="$corpus">
	<xsl:for-each select="LIST_OCCURRENCES/OCCURRENCE">
	  <xsl:variable name="docid" select="DOC"/>
	  <xsl:variable name="sentid" select="SENTENCE"/>
	  <xsl:value-of select="concat(
				$tab, $docid,
				$tab, $sentid,
				$tab, exsl:node-set($docs)/documentCollection/document[@id = $docid]/sentence[@id = $sentid]
				)"/>
	</xsl:for-each>
      </xsl:if>
      <xsl:value-of select="$nl"/>
    </xsl:if>
  </xsl:template>

</xsl:stylesheet>
