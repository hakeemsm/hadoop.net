using System.Collections.Generic;
using System.IO;
using Javax.Xml.Parsers;
using NUnit.Framework;
using Org.Mortbay.Util.Ajax;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Conf
{
	/// <summary>
	/// Basic test case that the ConfServlet can write configuration
	/// to its output in XML and JSON format.
	/// </summary>
	public class TestConfServlet : TestCase
	{
		private const string TestKey = "testconfservlet.key";

		private const string TestVal = "testval";

		private Configuration GetTestConf()
		{
			Configuration testConf = new Configuration();
			testConf.Set(TestKey, TestVal);
			return testConf;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteJson()
		{
			StringWriter sw = new StringWriter();
			ConfServlet.WriteResponse(GetTestConf(), sw, "json");
			string json = sw.ToString();
			bool foundSetting = false;
			object parsed = JSON.Parse(json);
			object[] properties = ((IDictionary<string, object[]>)parsed)["properties"];
			foreach (object o in properties)
			{
				IDictionary<string, object> propertyInfo = (IDictionary<string, object>)o;
				string key = (string)propertyInfo["key"];
				string val = (string)propertyInfo["value"];
				string resource = (string)propertyInfo["resource"];
				System.Console.Error.WriteLine("k: " + key + " v: " + val + " r: " + resource);
				if (TestKey.Equals(key) && TestVal.Equals(val) && "programatically".Equals(resource
					))
				{
					foundSetting = true;
				}
			}
			NUnit.Framework.Assert.IsTrue(foundSetting);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteXml()
		{
			StringWriter sw = new StringWriter();
			ConfServlet.WriteResponse(GetTestConf(), sw, "xml");
			string xml = sw.ToString();
			DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.NewInstance();
			DocumentBuilder builder = docBuilderFactory.NewDocumentBuilder();
			Document doc = builder.Parse(new InputSource(new StringReader(xml)));
			NodeList nameNodes = doc.GetElementsByTagName("name");
			bool foundSetting = false;
			for (int i = 0; i < nameNodes.GetLength(); i++)
			{
				Node nameNode = nameNodes.Item(i);
				string key = nameNode.GetTextContent();
				System.Console.Error.WriteLine("xml key: " + key);
				if (TestKey.Equals(key))
				{
					foundSetting = true;
					Element propertyElem = (Element)nameNode.GetParentNode();
					string val = propertyElem.GetElementsByTagName("value").Item(0).GetTextContent();
					NUnit.Framework.Assert.AreEqual(TestVal, val);
				}
			}
			NUnit.Framework.Assert.IsTrue(foundSetting);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBadFormat()
		{
			StringWriter sw = new StringWriter();
			try
			{
				ConfServlet.WriteResponse(GetTestConf(), sw, "not a format");
				Fail("writeResponse with bad format didn't throw!");
			}
			catch (ConfServlet.BadFormatException)
			{
			}
			// expected
			NUnit.Framework.Assert.AreEqual(string.Empty, sw.ToString());
		}
	}
}
