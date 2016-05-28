using Javax.Servlet;
using NUnit.Framework;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Servlet
{
	public class TestHostnameFilter : HTestCase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Hostname()
		{
			ServletRequest request = Org.Mockito.Mockito.Mock<ServletRequest>();
			Org.Mockito.Mockito.When(request.GetRemoteAddr()).ThenReturn("localhost");
			ServletResponse response = Org.Mockito.Mockito.Mock<ServletResponse>();
			AtomicBoolean invoked = new AtomicBoolean();
			FilterChain chain = new _FilterChain_49(invoked);
			// Hostname was set to "localhost", but may get resolved automatically to
			// "127.0.0.1" depending on OS.
			Filter filter = new HostnameFilter();
			filter.Init(null);
			NUnit.Framework.Assert.IsNull(HostnameFilter.Get());
			filter.DoFilter(request, response, chain);
			NUnit.Framework.Assert.IsTrue(invoked.Get());
			NUnit.Framework.Assert.IsNull(HostnameFilter.Get());
			filter.Destroy();
		}

		private sealed class _FilterChain_49 : FilterChain
		{
			public _FilterChain_49(AtomicBoolean invoked)
			{
				this.invoked = invoked;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Servlet.ServletException"/>
			public void DoFilter(ServletRequest servletRequest, ServletResponse servletResponse
				)
			{
				NUnit.Framework.Assert.IsTrue(HostnameFilter.Get().Contains("localhost") || HostnameFilter
					.Get().Contains("127.0.0.1"));
				invoked.Set(true);
			}

			private readonly AtomicBoolean invoked;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMissingHostname()
		{
			ServletRequest request = Org.Mockito.Mockito.Mock<ServletRequest>();
			Org.Mockito.Mockito.When(request.GetRemoteAddr()).ThenReturn(null);
			ServletResponse response = Org.Mockito.Mockito.Mock<ServletResponse>();
			AtomicBoolean invoked = new AtomicBoolean();
			FilterChain chain = new _FilterChain_79(invoked);
			Filter filter = new HostnameFilter();
			filter.Init(null);
			NUnit.Framework.Assert.IsNull(HostnameFilter.Get());
			filter.DoFilter(request, response, chain);
			NUnit.Framework.Assert.IsTrue(invoked.Get());
			NUnit.Framework.Assert.IsNull(HostnameFilter.Get());
			filter.Destroy();
		}

		private sealed class _FilterChain_79 : FilterChain
		{
			public _FilterChain_79(AtomicBoolean invoked)
			{
				this.invoked = invoked;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Servlet.ServletException"/>
			public void DoFilter(ServletRequest servletRequest, ServletResponse servletResponse
				)
			{
				NUnit.Framework.Assert.IsTrue(HostnameFilter.Get().Contains("???"));
				invoked.Set(true);
			}

			private readonly AtomicBoolean invoked;
		}
	}
}
