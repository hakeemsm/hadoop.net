using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>General xml utilities.</summary>
	public class XMLUtils
	{
		/// <summary>Transform input xml given a stylesheet.</summary>
		/// <param name="styleSheet">the style-sheet</param>
		/// <param name="xml">input xml data</param>
		/// <param name="out">output</param>
		/// <exception cref="javax.xml.transform.TransformerConfigurationException"/>
		/// <exception cref="javax.xml.transform.TransformerException"/>
		public static void transform(java.io.InputStream styleSheet, java.io.InputStream 
			xml, System.IO.TextWriter @out)
		{
			// Instantiate a TransformerFactory
			javax.xml.transform.TransformerFactory tFactory = javax.xml.transform.TransformerFactory
				.newInstance();
			// Use the TransformerFactory to process the  
			// stylesheet and generate a Transformer
			javax.xml.transform.Transformer transformer = tFactory.newTransformer(new javax.xml.transform.stream.StreamSource
				(styleSheet));
			// Use the Transformer to transform an XML Source 
			// and send the output to a Result object.
			transformer.transform(new javax.xml.transform.stream.StreamSource(xml), new javax.xml.transform.stream.StreamResult
				(@out));
		}
	}
}
