using Org.W3c.Dom;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	public class WebServicesTestUtils
	{
		public static long GetXmlLong(Element element, string name)
		{
			string val = GetXmlString(element, name);
			return long.Parse(val);
		}

		public static int GetXmlInt(Element element, string name)
		{
			string val = GetXmlString(element, name);
			return System.Convert.ToInt32(val);
		}

		public static bool GetXmlBoolean(Element element, string name)
		{
			string val = GetXmlString(element, name);
			return System.Boolean.Parse(val);
		}

		public static float GetXmlFloat(Element element, string name)
		{
			string val = GetXmlString(element, name);
			return float.ParseFloat(val);
		}

		public static string GetXmlString(Element element, string name)
		{
			NodeList id = element.GetElementsByTagName(name);
			Element line = (Element)id.Item(0);
			if (line == null)
			{
				return null;
			}
			Node first = line.GetFirstChild();
			// handle empty <key></key>
			if (first == null)
			{
				return string.Empty;
			}
			string val = first.GetNodeValue();
			if (val == null)
			{
				return string.Empty;
			}
			return val;
		}

		public static string GetXmlAttrString(Element element, string name)
		{
			Attr at = element.GetAttributeNode(name);
			if (at != null)
			{
				return at.GetValue();
			}
			return null;
		}

		public static void CheckStringMatch(string print, string expected, string got)
		{
			NUnit.Framework.Assert.IsTrue(print + " doesn't match, got: " + got + " expected: "
				 + expected, got.Matches(expected));
		}

		public static void CheckStringContains(string print, string expected, string got)
		{
			NUnit.Framework.Assert.IsTrue(print + " doesn't contain expected string, got: " +
				 got + " expected: " + expected, got.Contains(expected));
		}

		public static void CheckStringEqual(string print, string expected, string got)
		{
			NUnit.Framework.Assert.IsTrue(print + " is not equal, got: " + got + " expected: "
				 + expected, got.Equals(expected));
		}
	}
}
