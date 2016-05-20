using Sharpen;

namespace org.apache.hadoop.http.lib
{
	public class TestStaticUserWebFilter
	{
		private javax.servlet.FilterConfig mockConfig(string username)
		{
			javax.servlet.FilterConfig mock = org.mockito.Mockito.mock<javax.servlet.FilterConfig
				>();
			org.mockito.Mockito.doReturn(username).when(mock).getInitParameter(org.apache.hadoop.fs.CommonConfigurationKeys
				.HADOOP_HTTP_STATIC_USER);
			return mock;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFilter()
		{
			javax.servlet.FilterConfig config = mockConfig("myuser");
			org.apache.hadoop.http.lib.StaticUserWebFilter.StaticUserFilter suf = new org.apache.hadoop.http.lib.StaticUserWebFilter.StaticUserFilter
				();
			suf.init(config);
			org.mockito.ArgumentCaptor<javax.servlet.http.HttpServletRequestWrapper> wrapperArg
				 = org.mockito.ArgumentCaptor.forClass<javax.servlet.http.HttpServletRequestWrapper
				>();
			javax.servlet.FilterChain chain = org.mockito.Mockito.mock<javax.servlet.FilterChain
				>();
			suf.doFilter(org.mockito.Mockito.mock<javax.servlet.http.HttpServletRequest>(), org.mockito.Mockito.mock
				<javax.servlet.ServletResponse>(), chain);
			org.mockito.Mockito.verify(chain).doFilter(wrapperArg.capture(), org.mockito.Mockito
				.anyObject<javax.servlet.ServletResponse>());
			javax.servlet.http.HttpServletRequestWrapper wrapper = wrapperArg.getValue();
			NUnit.Framework.Assert.AreEqual("myuser", wrapper.getUserPrincipal().getName());
			NUnit.Framework.Assert.AreEqual("myuser", wrapper.getRemoteUser());
			suf.destroy();
		}

		[NUnit.Framework.Test]
		public virtual void testOldStyleConfiguration()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set("dfs.web.ugi", "joe,group1,group2");
			NUnit.Framework.Assert.AreEqual("joe", org.apache.hadoop.http.lib.StaticUserWebFilter
				.getUsernameFromConf(conf));
		}

		[NUnit.Framework.Test]
		public virtual void testConfiguration()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER, "joe"
				);
			NUnit.Framework.Assert.AreEqual("joe", org.apache.hadoop.http.lib.StaticUserWebFilter
				.getUsernameFromConf(conf));
		}
	}
}
