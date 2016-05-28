using Javax.Servlet;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Hadoop.Test;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Servlet
{
	public class TestMDCFilter : HTestCase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Mdc()
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			Org.Mockito.Mockito.When(request.GetUserPrincipal()).ThenReturn(null);
			Org.Mockito.Mockito.When(request.GetMethod()).ThenReturn("METHOD");
			Org.Mockito.Mockito.When(request.GetPathInfo()).ThenReturn("/pathinfo");
			ServletResponse response = Org.Mockito.Mockito.Mock<ServletResponse>();
			AtomicBoolean invoked = new AtomicBoolean();
			FilterChain chain = new _FilterChain_55(invoked);
			MDC.Clear();
			Filter filter = new MDCFilter();
			filter.Init(null);
			filter.DoFilter(request, response, chain);
			NUnit.Framework.Assert.IsTrue(invoked.Get());
			NUnit.Framework.Assert.IsNull(MDC.Get("hostname"));
			NUnit.Framework.Assert.IsNull(MDC.Get("user"));
			NUnit.Framework.Assert.IsNull(MDC.Get("method"));
			NUnit.Framework.Assert.IsNull(MDC.Get("path"));
			Org.Mockito.Mockito.When(request.GetUserPrincipal()).ThenReturn(new _Principal_78
				());
			invoked.Set(false);
			chain = new _FilterChain_86(invoked);
			filter.DoFilter(request, response, chain);
			NUnit.Framework.Assert.IsTrue(invoked.Get());
			HostnameFilter.HostnameTl.Set("HOST");
			invoked.Set(false);
			chain = new _FilterChain_103(invoked);
			filter.DoFilter(request, response, chain);
			NUnit.Framework.Assert.IsTrue(invoked.Get());
			HostnameFilter.HostnameTl.Remove();
			filter.Destroy();
		}

		private sealed class _FilterChain_55 : FilterChain
		{
			public _FilterChain_55(AtomicBoolean invoked)
			{
				this.invoked = invoked;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Servlet.ServletException"/>
			public void DoFilter(ServletRequest servletRequest, ServletResponse servletResponse
				)
			{
				NUnit.Framework.Assert.AreEqual(MDC.Get("hostname"), null);
				NUnit.Framework.Assert.AreEqual(MDC.Get("user"), null);
				NUnit.Framework.Assert.AreEqual(MDC.Get("method"), "METHOD");
				NUnit.Framework.Assert.AreEqual(MDC.Get("path"), "/pathinfo");
				invoked.Set(true);
			}

			private readonly AtomicBoolean invoked;
		}

		private sealed class _Principal_78 : Principal
		{
			public _Principal_78()
			{
			}

			public string GetName()
			{
				return "name";
			}
		}

		private sealed class _FilterChain_86 : FilterChain
		{
			public _FilterChain_86(AtomicBoolean invoked)
			{
				this.invoked = invoked;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Servlet.ServletException"/>
			public void DoFilter(ServletRequest servletRequest, ServletResponse servletResponse
				)
			{
				NUnit.Framework.Assert.AreEqual(MDC.Get("hostname"), null);
				NUnit.Framework.Assert.AreEqual(MDC.Get("user"), "name");
				NUnit.Framework.Assert.AreEqual(MDC.Get("method"), "METHOD");
				NUnit.Framework.Assert.AreEqual(MDC.Get("path"), "/pathinfo");
				invoked.Set(true);
			}

			private readonly AtomicBoolean invoked;
		}

		private sealed class _FilterChain_103 : FilterChain
		{
			public _FilterChain_103(AtomicBoolean invoked)
			{
				this.invoked = invoked;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Servlet.ServletException"/>
			public void DoFilter(ServletRequest servletRequest, ServletResponse servletResponse
				)
			{
				NUnit.Framework.Assert.AreEqual(MDC.Get("hostname"), "HOST");
				NUnit.Framework.Assert.AreEqual(MDC.Get("user"), "name");
				NUnit.Framework.Assert.AreEqual(MDC.Get("method"), "METHOD");
				NUnit.Framework.Assert.AreEqual(MDC.Get("path"), "/pathinfo");
				invoked.Set(true);
			}

			private readonly AtomicBoolean invoked;
		}
	}
}
