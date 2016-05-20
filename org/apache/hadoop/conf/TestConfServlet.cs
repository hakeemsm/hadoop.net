using Sharpen;

namespace org.apache.hadoop.conf
{
	/// <summary>
	/// Basic test case that the ConfServlet can write configuration
	/// to its output in XML and JSON format.
	/// </summary>
	public class TestConfServlet : NUnit.Framework.TestCase
	{
		private const string TEST_KEY = "testconfservlet.key";

		private const string TEST_VAL = "testval";

		private org.apache.hadoop.conf.Configuration getTestConf()
		{
			org.apache.hadoop.conf.Configuration testConf = new org.apache.hadoop.conf.Configuration
				();
			testConf.set(TEST_KEY, TEST_VAL);
			return testConf;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWriteJson()
		{
			System.IO.StringWriter sw = new System.IO.StringWriter();
			org.apache.hadoop.conf.ConfServlet.writeResponse(getTestConf(), sw, "json");
			string json = sw.ToString();
			bool foundSetting = false;
			object parsed = org.mortbay.util.ajax.JSON.parse(json);
			object[] properties = ((System.Collections.Generic.IDictionary<string, object[]>)
				parsed)["properties"];
			foreach (object o in properties)
			{
				System.Collections.Generic.IDictionary<string, object> propertyInfo = (System.Collections.Generic.IDictionary
					<string, object>)o;
				string key = (string)propertyInfo["key"];
				string val = (string)propertyInfo["value"];
				string resource = (string)propertyInfo["resource"];
				System.Console.Error.WriteLine("k: " + key + " v: " + val + " r: " + resource);
				if (TEST_KEY.Equals(key) && TEST_VAL.Equals(val) && "programatically".Equals(resource
					))
				{
					foundSetting = true;
				}
			}
			NUnit.Framework.Assert.IsTrue(foundSetting);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWriteXml()
		{
			System.IO.StringWriter sw = new System.IO.StringWriter();
			org.apache.hadoop.conf.ConfServlet.writeResponse(getTestConf(), sw, "xml");
			string xml = sw.ToString();
			javax.xml.parsers.DocumentBuilderFactory docBuilderFactory = javax.xml.parsers.DocumentBuilderFactory
				.newInstance();
			javax.xml.parsers.DocumentBuilder builder = docBuilderFactory.newDocumentBuilder(
				);
			org.w3c.dom.Document doc = builder.parse(new org.xml.sax.InputSource(new java.io.StringReader
				(xml)));
			org.w3c.dom.NodeList nameNodes = doc.getElementsByTagName("name");
			bool foundSetting = false;
			for (int i = 0; i < nameNodes.getLength(); i++)
			{
				org.w3c.dom.Node nameNode = nameNodes.item(i);
				string key = nameNode.getTextContent();
				System.Console.Error.WriteLine("xml key: " + key);
				if (TEST_KEY.Equals(key))
				{
					foundSetting = true;
					org.w3c.dom.Element propertyElem = (org.w3c.dom.Element)nameNode.getParentNode();
					string val = propertyElem.getElementsByTagName("value").item(0).getTextContent();
					NUnit.Framework.Assert.AreEqual(TEST_VAL, val);
				}
			}
			NUnit.Framework.Assert.IsTrue(foundSetting);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testBadFormat()
		{
			System.IO.StringWriter sw = new System.IO.StringWriter();
			try
			{
				org.apache.hadoop.conf.ConfServlet.writeResponse(getTestConf(), sw, "not a format"
					);
				fail("writeResponse with bad format didn't throw!");
			}
			catch (org.apache.hadoop.conf.ConfServlet.BadFormatException)
			{
			}
			// expected
			NUnit.Framework.Assert.AreEqual(string.Empty, sw.ToString());
		}
	}
}
