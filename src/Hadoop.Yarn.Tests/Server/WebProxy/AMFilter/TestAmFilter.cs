using System.Collections.Generic;
using System.IO;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Yarn.Server.Webproxy;
using Org.Glassfish.Grizzly.Servlet;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy.Amfilter
{
	/// <summary>Test AmIpFilter.</summary>
	/// <remarks>
	/// Test AmIpFilter. Requests to a no declared hosts should has way through
	/// proxy. Another requests can be filtered with (without) user name.
	/// </remarks>
	public class TestAmFilter
	{
		private string proxyHost = "localhost";

		private string proxyUri = "http://bogus";

		private string doFilterRequest;

		private AmIpServletRequestWrapper servletWrapper;

		private class TestAmIpFilter : AmIpFilter
		{
			private ICollection<string> proxyAddresses = null;

			protected internal override ICollection<string> GetProxyAddresses()
			{
				if (this.proxyAddresses == null)
				{
					this.proxyAddresses = new HashSet<string>();
				}
				this.proxyAddresses.AddItem(this._enclosing.proxyHost);
				return this.proxyAddresses;
			}

			internal TestAmIpFilter(TestAmFilter _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAmFilter _enclosing;
		}

		private class DummyFilterConfig : FilterConfig
		{
			internal readonly IDictionary<string, string> map;

			internal DummyFilterConfig(IDictionary<string, string> map)
			{
				this.map = map;
			}

			public virtual string GetFilterName()
			{
				return "dummy";
			}

			public virtual string GetInitParameter(string arg0)
			{
				return map[arg0];
			}

			public virtual Enumeration<string> GetInitParameterNames()
			{
				return Sharpen.Collections.Enumeration(map.Keys);
			}

			public virtual ServletContext GetServletContext()
			{
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void FilterNullCookies()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(request.GetCookies()).ThenReturn(null);
			Org.Mockito.Mockito.When(request.GetRemoteAddr()).ThenReturn(proxyHost);
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			AtomicBoolean invoked = new AtomicBoolean();
			FilterChain chain = new _FilterChain_104(invoked);
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[AmIpFilter.ProxyHost] = proxyHost;
			@params[AmIpFilter.ProxyUriBase] = proxyUri;
			FilterConfig conf = new TestAmFilter.DummyFilterConfig(@params);
			Filter filter = new TestAmFilter.TestAmIpFilter(this);
			filter.Init(conf);
			filter.DoFilter(request, response, chain);
			NUnit.Framework.Assert.IsTrue(invoked.Get());
			filter.Destroy();
		}

		private sealed class _FilterChain_104 : FilterChain
		{
			public _FilterChain_104(AtomicBoolean invoked)
			{
				this.invoked = invoked;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Servlet.ServletException"/>
			public void DoFilter(ServletRequest servletRequest, ServletResponse servletResponse
				)
			{
				invoked.Set(true);
			}

			private readonly AtomicBoolean invoked;
		}

		/// <summary>Test AmIpFilter</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestFilter()
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[AmIpFilter.ProxyHost] = proxyHost;
			@params[AmIpFilter.ProxyUriBase] = proxyUri;
			FilterConfig config = new TestAmFilter.DummyFilterConfig(@params);
			// dummy filter
			FilterChain chain = new _FilterChain_135(this);
			AmIpFilter testFilter = new AmIpFilter();
			testFilter.Init(config);
			TestAmFilter.HttpServletResponseForTest response = new TestAmFilter.HttpServletResponseForTest
				(this);
			// Test request should implements HttpServletRequest
			ServletRequest failRequest = Org.Mockito.Mockito.Mock<ServletRequest>();
			try
			{
				testFilter.DoFilter(failRequest, response, chain);
				NUnit.Framework.Assert.Fail();
			}
			catch (ServletException e)
			{
				NUnit.Framework.Assert.AreEqual(ProxyUtils.EHttpHttpsOnly, e.Message);
			}
			// request with HttpServletRequest
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(request.GetRemoteAddr()).ThenReturn("redirect");
			Org.Mockito.Mockito.When(request.GetRequestURI()).ThenReturn("/redirect");
			testFilter.DoFilter(request, response, chain);
			// address "redirect" is not in host list
			NUnit.Framework.Assert.AreEqual(302, response.status);
			string redirect = response.GetHeader(ProxyUtils.Location);
			NUnit.Framework.Assert.AreEqual("http://bogus/redirect", redirect);
			// "127.0.0.1" contains in host list. Without cookie
			Org.Mockito.Mockito.When(request.GetRemoteAddr()).ThenReturn("127.0.0.1");
			testFilter.DoFilter(request, response, chain);
			NUnit.Framework.Assert.IsTrue(doFilterRequest.Contains("javax.servlet.http.HttpServletRequest"
				));
			// cookie added
			Cookie[] cookies = new Cookie[1];
			cookies[0] = new Cookie(WebAppProxyServlet.ProxyUserCookieName, "user");
			Org.Mockito.Mockito.When(request.GetCookies()).ThenReturn(cookies);
			testFilter.DoFilter(request, response, chain);
			NUnit.Framework.Assert.AreEqual("org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpServletRequestWrapper"
				, doFilterRequest);
			// request contains principal from cookie
			NUnit.Framework.Assert.AreEqual("user", servletWrapper.GetUserPrincipal().GetName
				());
			NUnit.Framework.Assert.AreEqual("user", servletWrapper.GetRemoteUser());
			NUnit.Framework.Assert.IsFalse(servletWrapper.IsUserInRole(string.Empty));
		}

		private sealed class _FilterChain_135 : FilterChain
		{
			public _FilterChain_135(TestAmFilter _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Servlet.ServletException"/>
			public void DoFilter(ServletRequest servletRequest, ServletResponse servletResponse
				)
			{
				this._enclosing.doFilterRequest = servletRequest.GetType().FullName;
				if (servletRequest is AmIpServletRequestWrapper)
				{
					this._enclosing.servletWrapper = (AmIpServletRequestWrapper)servletRequest;
				}
			}

			private readonly TestAmFilter _enclosing;
		}

		private class HttpServletResponseForTest : HttpServletResponseImpl
		{
			internal string redirectLocation = string.Empty;

			internal int status;

			private string contentType;

			private readonly IDictionary<string, string> headers = new Dictionary<string, string
				>(1);

			private StringWriter body;

			public virtual string GetRedirect()
			{
				return this.redirectLocation;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SendRedirect(string location)
			{
				this.redirectLocation = location;
			}

			public override string EncodeRedirectURL(string url)
			{
				return url;
			}

			public override void SetStatus(int status)
			{
				this.status = status;
			}

			public override void SetContentType(string type)
			{
				this.contentType = type;
			}

			public override void SetHeader(string name, string value)
			{
				this.headers[name] = value;
			}

			public override string GetHeader(string name)
			{
				return this.headers[name];
			}

			/// <exception cref="System.IO.IOException"/>
			public override PrintWriter GetWriter()
			{
				this.body = new StringWriter();
				return new PrintWriter(this.body);
			}

			internal HttpServletResponseForTest(TestAmFilter _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAmFilter _enclosing;
		}
	}
}
