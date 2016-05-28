using System.IO;
using Javax.Xml.Parsers;
using Javax.Xml.Transform;
using Javax.Xml.Transform.Dom;
using Javax.Xml.Transform.Stream;
using Org.W3c.Dom;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestQueueConfigurationParser
	{
		/// <summary>test xml generation</summary>
		/// <exception cref="Javax.Xml.Parsers.ParserConfigurationException"/>
		/// <exception cref="System.Exception"></exception>
		public virtual void TestQueueConfigurationParser()
		{
			JobQueueInfo info = new JobQueueInfo("root", "rootInfo");
			JobQueueInfo infoChild1 = new JobQueueInfo("child1", "child1Info");
			JobQueueInfo infoChild2 = new JobQueueInfo("child2", "child1Info");
			info.AddChild(infoChild1);
			info.AddChild(infoChild2);
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.NewInstance();
			DocumentBuilder builder = docBuilderFactory.NewDocumentBuilder();
			Document document = builder.NewDocument();
			// test QueueConfigurationParser.getQueueElement 
			Element e = QueueConfigurationParser.GetQueueElement(document, info);
			// transform result to string for check
			DOMSource domSource = new DOMSource(e);
			StringWriter writer = new StringWriter();
			StreamResult result = new StreamResult(writer);
			TransformerFactory tf = TransformerFactory.NewInstance();
			Transformer transformer = tf.NewTransformer();
			transformer.Transform(domSource, result);
			string str = writer.ToString();
			NUnit.Framework.Assert.IsTrue(str.EndsWith("<queue><name>root</name><properties/><state>running</state><queue><name>child1</name><properties/><state>running</state></queue><queue><name>child2</name><properties/><state>running</state></queue></queue>"
				));
		}
	}
}
