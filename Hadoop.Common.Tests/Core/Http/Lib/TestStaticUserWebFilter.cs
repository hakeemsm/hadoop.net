using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Http.Lib
{
	public class TestStaticUserWebFilter
	{
		private FilterConfig MockConfig(string username)
		{
			FilterConfig mock = Org.Mockito.Mockito.Mock<FilterConfig>();
			Org.Mockito.Mockito.DoReturn(username).When(mock).GetInitParameter(CommonConfigurationKeys
				.HadoopHttpStaticUser);
			return mock;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFilter()
		{
			FilterConfig config = MockConfig("myuser");
			StaticUserWebFilter.StaticUserFilter suf = new StaticUserWebFilter.StaticUserFilter
				();
			suf.Init(config);
			ArgumentCaptor<HttpServletRequestWrapper> wrapperArg = ArgumentCaptor.ForClass<HttpServletRequestWrapper
				>();
			FilterChain chain = Org.Mockito.Mockito.Mock<FilterChain>();
			suf.DoFilter(Org.Mockito.Mockito.Mock<HttpServletRequest>(), Org.Mockito.Mockito.Mock
				<ServletResponse>(), chain);
			Org.Mockito.Mockito.Verify(chain).DoFilter(wrapperArg.Capture(), Org.Mockito.Mockito
				.AnyObject<ServletResponse>());
			HttpServletRequestWrapper wrapper = wrapperArg.GetValue();
			NUnit.Framework.Assert.AreEqual("myuser", wrapper.GetUserPrincipal().GetName());
			NUnit.Framework.Assert.AreEqual("myuser", wrapper.GetRemoteUser());
			suf.Destroy();
		}

		[NUnit.Framework.Test]
		public virtual void TestOldStyleConfiguration()
		{
			Configuration conf = new Configuration();
			conf.Set("dfs.web.ugi", "joe,group1,group2");
			NUnit.Framework.Assert.AreEqual("joe", StaticUserWebFilter.GetUsernameFromConf(conf
				));
		}

		[NUnit.Framework.Test]
		public virtual void TestConfiguration()
		{
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.HadoopHttpStaticUser, "joe");
			NUnit.Framework.Assert.AreEqual("joe", StaticUserWebFilter.GetUsernameFromConf(conf
				));
		}
	}
}
