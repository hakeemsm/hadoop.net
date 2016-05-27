using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authorize
{
	public class TestProxyServers
	{
		[NUnit.Framework.Test]
		public virtual void TestProxyServer()
		{
			Configuration conf = new Configuration();
			NUnit.Framework.Assert.IsFalse(ProxyServers.IsProxyServer("1.1.1.1"));
			conf.Set(ProxyServers.ConfHadoopProxyservers, "2.2.2.2, 3.3.3.3");
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
			NUnit.Framework.Assert.IsFalse(ProxyServers.IsProxyServer("1.1.1.1"));
			NUnit.Framework.Assert.IsTrue(ProxyServers.IsProxyServer("2.2.2.2"));
			NUnit.Framework.Assert.IsTrue(ProxyServers.IsProxyServer("3.3.3.3"));
		}
	}
}
