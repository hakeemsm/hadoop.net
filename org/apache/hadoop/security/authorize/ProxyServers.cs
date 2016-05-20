using Sharpen;

namespace org.apache.hadoop.security.authorize
{
	public class ProxyServers
	{
		public const string CONF_HADOOP_PROXYSERVERS = "hadoop.proxyservers";

		private static volatile System.Collections.Generic.ICollection<string> proxyServers;

		public static void refresh()
		{
			refresh(new org.apache.hadoop.conf.Configuration());
		}

		public static void refresh(org.apache.hadoop.conf.Configuration conf)
		{
			System.Collections.Generic.ICollection<string> tempServers = new java.util.HashSet
				<string>();
			// trusted proxy servers such as http proxies
			foreach (string host in conf.getTrimmedStrings(CONF_HADOOP_PROXYSERVERS))
			{
				java.net.InetSocketAddress addr = new java.net.InetSocketAddress(host, 0);
				if (!addr.isUnresolved())
				{
					tempServers.add(addr.getAddress().getHostAddress());
				}
			}
			proxyServers = tempServers;
		}

		public static bool isProxyServer(string remoteAddr)
		{
			if (proxyServers == null)
			{
				refresh();
			}
			return proxyServers.contains(remoteAddr);
		}
	}
}
