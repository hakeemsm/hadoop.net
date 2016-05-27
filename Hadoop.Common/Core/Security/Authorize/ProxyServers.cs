using System.Collections.Generic;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authorize
{
	public class ProxyServers
	{
		public const string ConfHadoopProxyservers = "hadoop.proxyservers";

		private static volatile ICollection<string> proxyServers;

		public static void Refresh()
		{
			Refresh(new Configuration());
		}

		public static void Refresh(Configuration conf)
		{
			ICollection<string> tempServers = new HashSet<string>();
			// trusted proxy servers such as http proxies
			foreach (string host in conf.GetTrimmedStrings(ConfHadoopProxyservers))
			{
				IPEndPoint addr = new IPEndPoint(host, 0);
				if (!addr.IsUnresolved())
				{
					tempServers.AddItem(addr.Address.GetHostAddress());
				}
			}
			proxyServers = tempServers;
		}

		public static bool IsProxyServer(string remoteAddr)
		{
			if (proxyServers == null)
			{
				Refresh();
			}
			return proxyServers.Contains(remoteAddr);
		}
	}
}
