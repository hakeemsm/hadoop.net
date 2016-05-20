using Sharpen;

namespace org.apache.hadoop.http
{
	public class TestHtmlQuoting
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testNeedsQuoting()
		{
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.http.HtmlQuoting.needsQuoting("abcde>"
				));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.http.HtmlQuoting.needsQuoting("<abcde"
				));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.http.HtmlQuoting.needsQuoting("abc'de"
				));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.http.HtmlQuoting.needsQuoting("abcde\""
				));
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.http.HtmlQuoting.needsQuoting("&"
				));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.http.HtmlQuoting.needsQuoting(string.Empty
				));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.http.HtmlQuoting.needsQuoting("ab\ncdef"
				));
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.http.HtmlQuoting.needsQuoting(null
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testQuoting()
		{
			NUnit.Framework.Assert.AreEqual("ab&lt;cd", org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars
				("ab<cd"));
			NUnit.Framework.Assert.AreEqual("ab&gt;", org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars
				("ab>"));
			NUnit.Framework.Assert.AreEqual("&amp;&amp;&amp;", org.apache.hadoop.http.HtmlQuoting
				.quoteHtmlChars("&&&"));
			NUnit.Framework.Assert.AreEqual(" &apos;\n", org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars
				(" '\n"));
			NUnit.Framework.Assert.AreEqual("&quot;", org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars
				("\""));
			NUnit.Framework.Assert.AreEqual(null, org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars
				(null));
		}

		/// <exception cref="System.Exception"/>
		private void runRoundTrip(string str)
		{
			NUnit.Framework.Assert.AreEqual(str, org.apache.hadoop.http.HtmlQuoting.unquoteHtmlChars
				(org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars(str)));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRoundtrip()
		{
			runRoundTrip(string.Empty);
			runRoundTrip("<>&'\"");
			runRoundTrip("ab>cd<ef&ghi'\"");
			runRoundTrip("A string\n with no quotable chars in it!");
			runRoundTrip(null);
			java.lang.StringBuilder buffer = new java.lang.StringBuilder();
			for (char ch = 0; ch < 127; ++ch)
			{
				buffer.Append(ch);
			}
			runRoundTrip(buffer.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRequestQuoting()
		{
			javax.servlet.http.HttpServletRequest mockReq = org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest
				>();
			org.apache.hadoop.http.HttpServer2.QuotingInputFilter.RequestQuoter quoter = new 
				org.apache.hadoop.http.HttpServer2.QuotingInputFilter.RequestQuoter(mockReq);
			org.mockito.Mockito.doReturn("a<b").when(mockReq).getParameter("x");
			NUnit.Framework.Assert.AreEqual("Test simple param quoting", "a&lt;b", quoter.getParameter
				("x"));
			org.mockito.Mockito.doReturn(null).when(mockReq).getParameter("x");
			NUnit.Framework.Assert.AreEqual("Test that missing parameters dont cause NPE", null
				, quoter.getParameter("x"));
			org.mockito.Mockito.doReturn(new string[] { "a<b", "b" }).when(mockReq).getParameterValues
				("x");
			NUnit.Framework.Assert.assertArrayEquals("Test escaping of an array", new string[
				] { "a&lt;b", "b" }, quoter.getParameterValues("x"));
			org.mockito.Mockito.doReturn(null).when(mockReq).getParameterValues("x");
			NUnit.Framework.Assert.assertArrayEquals("Test that missing parameters dont cause NPE for array"
				, null, quoter.getParameterValues("x"));
		}
	}
}
