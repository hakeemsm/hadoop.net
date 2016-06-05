using System.IO;
using Javax.Xml.Transform;
using Javax.Xml.Transform.Stream;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>General xml utilities.</summary>
	public class XMLUtils
	{
		/// <summary>Transform input xml given a stylesheet.</summary>
		/// <param name="styleSheet">the style-sheet</param>
		/// <param name="xml">input xml data</param>
		/// <param name="out">output</param>
		/// <exception cref="Javax.Xml.Transform.TransformerConfigurationException"/>
		/// <exception cref="Javax.Xml.Transform.TransformerException"/>
		public static void Transform(InputStream styleSheet, InputStream xml, TextWriter 
			@out)
		{
			// Instantiate a TransformerFactory
			TransformerFactory tFactory = TransformerFactory.NewInstance();
			// Use the TransformerFactory to process the  
			// stylesheet and generate a Transformer
			Transformer transformer = tFactory.NewTransformer(new StreamSource(styleSheet));
			// Use the Transformer to transform an XML Source 
			// and send the output to a Result object.
			transformer.Transform(new StreamSource(xml), new StreamResult(@out));
		}
	}
}
