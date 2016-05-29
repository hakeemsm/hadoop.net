using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy.Amfilter
{
	public class AmFilterInitializer : FilterInitializer
	{
		private const string FilterName = "AM_PROXY_FILTER";

		private static readonly string FilterClass = typeof(AmIpFilter).GetCanonicalName(
			);

		public override void InitFilter(FilterContainer container, Configuration conf)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			IList<string> proxies = WebAppUtils.GetProxyHostsAndPortsForAmFilter(conf);
			StringBuilder sb = new StringBuilder();
			foreach (string proxy in proxies)
			{
				sb.Append(proxy.Split(":")[0]).Append(AmIpFilter.ProxyHostsDelimiter);
			}
			sb.Length = sb.Length - 1;
			@params[AmIpFilter.ProxyHosts] = sb.ToString();
			string prefix = WebAppUtils.GetHttpSchemePrefix(conf);
			string proxyBase = GetApplicationWebProxyBase();
			sb = new StringBuilder();
			foreach (string proxy_1 in proxies)
			{
				sb.Append(prefix).Append(proxy_1).Append(proxyBase).Append(AmIpFilter.ProxyHostsDelimiter
					);
			}
			sb.Length = sb.Length - 1;
			@params[AmIpFilter.ProxyUriBases] = sb.ToString();
			container.AddFilter(FilterName, FilterClass, @params);
		}

		[VisibleForTesting]
		protected internal virtual string GetApplicationWebProxyBase()
		{
			return Runtime.Getenv(ApplicationConstants.ApplicationWebProxyBaseEnv);
		}
	}
}
