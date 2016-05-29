using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Collect;
using Com.Google.Inject;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Webproxy;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	[System.Serializable]
	public class RMWebAppFilter : GuiceContainer
	{
		private Injector injector;

		private const long serialVersionUID = 1L;

		private static readonly ICollection<string> NonRedirectedUris = Sets.NewHashSet("/conf"
			, "/stacks", "/logLevel", "/logs");

		private string path;

		private const int BasicSleepTime = 5;

		private const int MaxSleepTime = 5 * 60;

		private static readonly Random randnum = new Random();

		[Javax.Inject.Inject]
		public RMWebAppFilter(Injector injector, Configuration conf)
			: base(injector)
		{
			// define a set of URIs which do not need to do redirection
			this.injector = injector;
			IPEndPoint sock = YarnConfiguration.UseHttps(conf) ? conf.GetSocketAddr(YarnConfiguration
				.RmWebappHttpsAddress, YarnConfiguration.DefaultRmWebappHttpsAddress, YarnConfiguration
				.DefaultRmWebappHttpsPort) : conf.GetSocketAddr(YarnConfiguration.RmWebappAddress
				, YarnConfiguration.DefaultRmWebappAddress, YarnConfiguration.DefaultRmWebappPort
				);
			path = sock.GetHostName() + ":" + Sharpen.Extensions.ToString(sock.Port);
			path = YarnConfiguration.UseHttps(conf) ? "https://" + path : "http://" + path;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Servlet.ServletException"/>
		public override void DoFilter(HttpServletRequest request, HttpServletResponse response
			, FilterChain chain)
		{
			response.SetCharacterEncoding("UTF-8");
			string uri = HtmlQuoting.QuoteHtmlChars(request.GetRequestURI());
			if (uri == null)
			{
				uri = "/";
			}
			RMWebApp rmWebApp = injector.GetInstance<RMWebApp>();
			rmWebApp.CheckIfStandbyRM();
			if (rmWebApp.IsStandby() && ShouldRedirect(rmWebApp, uri))
			{
				string redirectPath = rmWebApp.GetRedirectPath();
				if (redirectPath != null && !redirectPath.IsEmpty())
				{
					redirectPath += uri;
					string redirectMsg = "This is standby RM. The redirect url is: " + redirectPath;
					PrintWriter @out = response.GetWriter();
					@out.WriteLine(redirectMsg);
					response.SetHeader("Location", redirectPath);
					response.SetStatus(HttpServletResponse.ScTemporaryRedirect);
					return;
				}
				else
				{
					bool doRetry = true;
					string retryIntervalStr = request.GetParameter(YarnWebParams.NextRefreshInterval);
					int retryInterval = 0;
					if (retryIntervalStr != null)
					{
						try
						{
							retryInterval = System.Convert.ToInt32(retryIntervalStr.Trim());
						}
						catch (FormatException)
						{
							doRetry = false;
						}
					}
					int next = CalculateExponentialTime(retryInterval);
					string redirectUrl = AppendOrReplaceParamter(path + uri, YarnWebParams.NextRefreshInterval
						 + "=" + (retryInterval + 1));
					if (redirectUrl == null || next > MaxSleepTime)
					{
						doRetry = false;
					}
					string redirectMsg = doRetry ? "Can not find any active RM. Will retry in next " 
						+ next + " seconds." : "There is no active RM right now.";
					redirectMsg += "\nHA Zookeeper Connection State: " + rmWebApp.GetHAZookeeperConnectionState
						();
					PrintWriter @out = response.GetWriter();
					@out.WriteLine(redirectMsg);
					if (doRetry)
					{
						response.SetHeader("Refresh", next + ";url=" + redirectUrl);
						response.SetStatus(HttpServletResponse.ScTemporaryRedirect);
					}
				}
				return;
			}
			base.DoFilter(request, response, chain);
		}

		private bool ShouldRedirect(RMWebApp rmWebApp, string uri)
		{
			return !uri.Equals("/" + rmWebApp.WsName() + "/v1/cluster/info") && !uri.Equals("/"
				 + rmWebApp.Name() + "/cluster") && !uri.StartsWith(ProxyUriUtils.ProxyBase) && 
				!NonRedirectedUris.Contains(uri);
		}

		private string AppendOrReplaceParamter(string uri, string newQuery)
		{
			if (uri.Contains(YarnWebParams.NextRefreshInterval + "="))
			{
				return uri.ReplaceAll(YarnWebParams.NextRefreshInterval + "=[^&]+", newQuery);
			}
			try
			{
				URI oldUri = new URI(uri);
				string appendQuery = oldUri.GetQuery();
				if (appendQuery == null)
				{
					appendQuery = newQuery;
				}
				else
				{
					appendQuery += "&" + newQuery;
				}
				URI newUri = new URI(oldUri.GetScheme(), oldUri.GetAuthority(), oldUri.GetPath(), 
					appendQuery, oldUri.GetFragment());
				return newUri.ToString();
			}
			catch (URISyntaxException)
			{
				return null;
			}
		}

		private static int CalculateExponentialTime(int retries)
		{
			long baseTime = BasicSleepTime * (1L << retries);
			return (int)(baseTime * (randnum.NextDouble() + 0.5));
		}
	}
}
