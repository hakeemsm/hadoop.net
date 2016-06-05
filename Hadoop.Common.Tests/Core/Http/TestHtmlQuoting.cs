using System.Text;
using Javax.Servlet.Http;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	public class TestHtmlQuoting
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestNeedsQuoting()
		{
			Assert.True(HtmlQuoting.NeedsQuoting("abcde>"));
			Assert.True(HtmlQuoting.NeedsQuoting("<abcde"));
			Assert.True(HtmlQuoting.NeedsQuoting("abc'de"));
			Assert.True(HtmlQuoting.NeedsQuoting("abcde\""));
			Assert.True(HtmlQuoting.NeedsQuoting("&"));
			NUnit.Framework.Assert.IsFalse(HtmlQuoting.NeedsQuoting(string.Empty));
			NUnit.Framework.Assert.IsFalse(HtmlQuoting.NeedsQuoting("ab\ncdef"));
			NUnit.Framework.Assert.IsFalse(HtmlQuoting.NeedsQuoting(null));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestQuoting()
		{
			Assert.Equal("ab&lt;cd", HtmlQuoting.QuoteHtmlChars("ab<cd"));
			Assert.Equal("ab&gt;", HtmlQuoting.QuoteHtmlChars("ab>"));
			Assert.Equal("&amp;&amp;&amp;", HtmlQuoting.QuoteHtmlChars("&&&"
				));
			Assert.Equal(" &apos;\n", HtmlQuoting.QuoteHtmlChars(" '\n"));
			Assert.Equal("&quot;", HtmlQuoting.QuoteHtmlChars("\""));
			Assert.Equal(null, HtmlQuoting.QuoteHtmlChars(null));
		}

		/// <exception cref="System.Exception"/>
		private void RunRoundTrip(string str)
		{
			Assert.Equal(str, HtmlQuoting.UnquoteHtmlChars(HtmlQuoting.QuoteHtmlChars
				(str)));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRoundtrip()
		{
			RunRoundTrip(string.Empty);
			RunRoundTrip("<>&'\"");
			RunRoundTrip("ab>cd<ef&ghi'\"");
			RunRoundTrip("A string\n with no quotable chars in it!");
			RunRoundTrip(null);
			StringBuilder buffer = new StringBuilder();
			for (char ch = 0; ch < 127; ++ch)
			{
				buffer.Append(ch);
			}
			RunRoundTrip(buffer.ToString());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRequestQuoting()
		{
			HttpServletRequest mockReq = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServer2.QuotingInputFilter.RequestQuoter quoter = new HttpServer2.QuotingInputFilter.RequestQuoter
				(mockReq);
			Org.Mockito.Mockito.DoReturn("a<b").When(mockReq).GetParameter("x");
			Assert.Equal("Test simple param quoting", "a&lt;b", quoter.GetParameter
				("x"));
			Org.Mockito.Mockito.DoReturn(null).When(mockReq).GetParameter("x");
			Assert.Equal("Test that missing parameters dont cause NPE", null
				, quoter.GetParameter("x"));
			Org.Mockito.Mockito.DoReturn(new string[] { "a<b", "b" }).When(mockReq).GetParameterValues
				("x");
			Assert.AssertArrayEquals("Test escaping of an array", new string[] { "a&lt;b", "b"
				 }, quoter.GetParameterValues("x"));
			Org.Mockito.Mockito.DoReturn(null).When(mockReq).GetParameterValues("x");
			Assert.AssertArrayEquals("Test that missing parameters dont cause NPE for array", 
				null, quoter.GetParameterValues("x"));
		}
	}
}
