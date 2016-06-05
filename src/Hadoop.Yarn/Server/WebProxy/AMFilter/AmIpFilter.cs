using System;
using System.Collections.Generic;
using System.Net;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Webproxy;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy.Amfilter
{
	public class AmIpFilter : Filter
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(AmIpFilter));

		[Obsolete]
		public const string ProxyHost = "PROXY_HOST";

		[Obsolete]
		public const string ProxyUriBase = "PROXY_URI_BASE";

		public const string ProxyHosts = "PROXY_HOSTS";

		public const string ProxyHostsDelimiter = ",";

		public const string ProxyUriBases = "PROXY_URI_BASES";

		public const string ProxyUriBasesDelimiter = ",";

		private const long updateInterval = 5 * 60 * 1000;

		private string[] proxyHosts;

		private ICollection<string> proxyAddresses = null;

		private long lastUpdate;

		private IDictionary<string, string> proxyUriBases;

		//update the proxy IP list about every 5 min
		/// <exception cref="Javax.Servlet.ServletException"/>
		public virtual void Init(FilterConfig conf)
		{
			// Maintain for backwards compatibility
			if (conf.GetInitParameter(ProxyHost) != null && conf.GetInitParameter(ProxyUriBase
				) != null)
			{
				proxyHosts = new string[] { conf.GetInitParameter(ProxyHost) };
				proxyUriBases = new Dictionary<string, string>(1);
				proxyUriBases["dummy"] = conf.GetInitParameter(ProxyUriBase);
			}
			else
			{
				proxyHosts = conf.GetInitParameter(ProxyHosts).Split(ProxyHostsDelimiter);
				string[] proxyUriBasesArr = conf.GetInitParameter(ProxyUriBases).Split(ProxyUriBasesDelimiter
					);
				proxyUriBases = new Dictionary<string, string>(proxyUriBasesArr.Length);
				foreach (string proxyUriBase in proxyUriBasesArr)
				{
					try
					{
						Uri url = new Uri(proxyUriBase);
						proxyUriBases[url.GetHost() + ":" + url.Port] = proxyUriBase;
					}
					catch (UriFormatException e)
					{
						Log.Warn("{} does not appear to be a valid URL", proxyUriBase, e);
					}
				}
			}
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		protected internal virtual ICollection<string> GetProxyAddresses()
		{
			long now = Runtime.CurrentTimeMillis();
			lock (this)
			{
				if (proxyAddresses == null || (lastUpdate + updateInterval) >= now)
				{
					proxyAddresses = new HashSet<string>();
					foreach (string proxyHost in proxyHosts)
					{
						try
						{
							foreach (IPAddress add in IPAddress.GetAllByName(proxyHost))
							{
								if (Log.IsDebugEnabled())
								{
									Log.Debug("proxy address is: {}", add.GetHostAddress());
								}
								proxyAddresses.AddItem(add.GetHostAddress());
							}
							lastUpdate = now;
						}
						catch (UnknownHostException e)
						{
							Log.Warn("Could not locate {} - skipping", proxyHost, e);
						}
					}
					if (proxyAddresses.IsEmpty())
					{
						throw new ServletException("Could not locate any of the proxy hosts");
					}
				}
				return proxyAddresses;
			}
		}

		public virtual void Destroy()
		{
		}

		//Empty
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Servlet.ServletException"/>
		public virtual void DoFilter(ServletRequest req, ServletResponse resp, FilterChain
			 chain)
		{
			ProxyUtils.RejectNonHttpRequests(req);
			HttpServletRequest httpReq = (HttpServletRequest)req;
			HttpServletResponse httpResp = (HttpServletResponse)resp;
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Remote address for request is: {}", httpReq.GetRemoteAddr());
			}
			if (!GetProxyAddresses().Contains(httpReq.GetRemoteAddr()))
			{
				string redirectUrl = FindRedirectUrl();
				string target = redirectUrl + httpReq.GetRequestURI();
				ProxyUtils.SendRedirect(httpReq, httpResp, target);
				return;
			}
			string user = null;
			if (httpReq.GetCookies() != null)
			{
				foreach (Cookie c in httpReq.GetCookies())
				{
					if (WebAppProxyServlet.ProxyUserCookieName.Equals(c.GetName()))
					{
						user = c.GetValue();
						break;
					}
				}
			}
			if (user == null)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Could not find " + WebAppProxyServlet.ProxyUserCookieName + " cookie, so user will not be set"
						);
				}
				chain.DoFilter(req, resp);
			}
			else
			{
				AmIpPrincipal principal = new AmIpPrincipal(user);
				ServletRequest requestWrapper = new AmIpServletRequestWrapper(httpReq, principal);
				chain.DoFilter(requestWrapper, resp);
			}
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		protected internal virtual string FindRedirectUrl()
		{
			string addr;
			if (proxyUriBases.Count == 1)
			{
				// external proxy or not RM HA
				addr = proxyUriBases.Values.GetEnumerator().Next();
			}
			else
			{
				// RM HA
				YarnConfiguration conf = new YarnConfiguration();
				string activeRMId = RMHAUtils.FindActiveRMHAId(conf);
				string addressPropertyPrefix = YarnConfiguration.UseHttps(conf) ? YarnConfiguration
					.RmWebappHttpsAddress : YarnConfiguration.RmWebappAddress;
				string host = conf.Get(HAUtil.AddSuffix(addressPropertyPrefix, activeRMId));
				addr = proxyUriBases[host];
			}
			if (addr == null)
			{
				throw new ServletException("Could not determine the proxy server for redirection"
					);
			}
			return addr;
		}
	}
}
