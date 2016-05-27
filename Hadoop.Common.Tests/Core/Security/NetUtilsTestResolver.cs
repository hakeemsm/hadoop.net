using System;
using System.Collections.Generic;
using System.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// provides a dummy dns search resolver with a configurable search path
	/// and host mapping
	/// </summary>
	public class NetUtilsTestResolver : SecurityUtil.QualifiedHostResolver
	{
		internal IDictionary<string, IPAddress> resolvedHosts = new Dictionary<string, IPAddress
			>();

		internal IList<string> hostSearches = new List<string>();

		public static NetUtilsTestResolver Install()
		{
			NetUtilsTestResolver resolver = new NetUtilsTestResolver();
			resolver.SetSearchDomains("a.b", "b", "c");
			resolver.AddResolvedHost("host.a.b.", "1.1.1.1");
			resolver.AddResolvedHost("b-host.b.", "2.2.2.2");
			resolver.AddResolvedHost("simple.", "3.3.3.3");
			SecurityUtil.hostResolver = resolver;
			return resolver;
		}

		public virtual void AddResolvedHost(string host, string ip)
		{
			IPAddress addr;
			try
			{
				addr = Sharpen.Extensions.GetAddressByName(ip);
				addr = IPAddress.GetByAddress(host, addr.GetAddressBytes());
			}
			catch (UnknownHostException)
			{
				throw new ArgumentException("not an ip:" + ip);
			}
			resolvedHosts[host] = addr;
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		internal override IPAddress GetInetAddressByName(string host)
		{
			hostSearches.AddItem(host);
			if (!resolvedHosts.Contains(host))
			{
				throw new UnknownHostException(host);
			}
			return resolvedHosts[host];
		}

		internal override IPAddress GetByExactName(string host)
		{
			return base.GetByExactName(host);
		}

		internal override IPAddress GetByNameWithSearch(string host)
		{
			return base.GetByNameWithSearch(host);
		}

		public virtual string[] GetHostSearches()
		{
			return Sharpen.Collections.ToArray(hostSearches, new string[0]);
		}

		public virtual void Reset()
		{
			hostSearches.Clear();
		}
	}
}
