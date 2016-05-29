using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Javax.Servlet.Http;
using Javax.WS.RS.Core;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Hamlet;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Org.Apache.Http;
using Org.Apache.Http.Client.Methods;
using Org.Apache.Http.Client.Params;
using Org.Apache.Http.Client.Utils;
using Org.Apache.Http.Conn.Params;
using Org.Apache.Http.Impl.Client;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy
{
	[System.Serializable]
	public class WebAppProxyServlet : HttpServlet
	{
		private const long serialVersionUID = 1L;

		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Yarn.Server.Webproxy.WebAppProxyServlet
			));

		private static readonly ICollection<string> passThroughHeaders = new HashSet<string
			>(Arrays.AsList("User-Agent", "Accept", "Accept-Encoding", "Accept-Language", "Accept-Charset"
			));

		public const string ProxyUserCookieName = "proxy-user";

		[System.NonSerialized]
		private IList<TrackingUriPlugin> trackingUriPlugins;

		private readonly string rmAppPageUrlBase;

		private readonly string ahsAppPageUrlBase;

		[System.NonSerialized]
		private YarnConfiguration conf;

		private class _ : HamletSpec._
		{
			//Empty
		}

		private class Page : Org.Apache.Hadoop.Yarn.Webapp.Hamlet.Hamlet
		{
			internal Page(PrintWriter @out)
				: base(@out, 0, false)
			{
			}

			public virtual Hamlet.HTML<WebAppProxyServlet._> Html()
			{
				return new Hamlet.HTML<WebAppProxyServlet._>(this, "html", null, EnumSet.Of(HamletImpl.EOpt
					.Endtag));
			}
		}

		/// <summary>Default constructor</summary>
		public WebAppProxyServlet()
			: base()
		{
			conf = new YarnConfiguration();
			this.trackingUriPlugins = conf.GetInstances<TrackingUriPlugin>(YarnConfiguration.
				YarnTrackingUrlGenerator);
			this.rmAppPageUrlBase = StringHelper.Pjoin(WebAppUtils.GetResolvedRMWebAppURLWithScheme
				(conf), "cluster", "app");
			this.ahsAppPageUrlBase = StringHelper.Pjoin(WebAppUtils.GetHttpSchemePrefix(conf)
				 + WebAppUtils.GetAHSWebAppURLWithoutScheme(conf), "applicationhistory", "apps");
		}

		/// <summary>Output 404 with appropriate message.</summary>
		/// <param name="resp">the http response.</param>
		/// <param name="message">the message to include on the page.</param>
		/// <exception cref="System.IO.IOException">on any error.</exception>
		private static void NotFound(HttpServletResponse resp, string message)
		{
			ProxyUtils.NotFound(resp, message);
		}

		/// <summary>Warn the user that the link may not be safe!</summary>
		/// <param name="resp">the http response</param>
		/// <param name="link">the link to point to</param>
		/// <param name="user">the user that owns the link.</param>
		/// <exception cref="System.IO.IOException">on any error.</exception>
		private static void WarnUserPage(HttpServletResponse resp, string link, string user
			, ApplicationId id)
		{
			//Set the cookie when we warn which overrides the query parameter
			//This is so that if a user passes in the approved query parameter without
			//having first visited this page then this page will still be displayed 
			resp.AddCookie(MakeCheckCookie(id, false));
			resp.SetContentType(MimeType.Html);
			WebAppProxyServlet.Page p = new WebAppProxyServlet.Page(resp.GetWriter());
			p.Html().H1("WARNING: The following page may not be safe!").H3().("click ").A(link
				, "here").(" to continue to an Application Master web interface owned by ", user
				).().();
		}

		/// <summary>Download link and have it be the response.</summary>
		/// <param name="req">the http request</param>
		/// <param name="resp">the http response</param>
		/// <param name="link">the link to download</param>
		/// <param name="c">the cookie to set if any</param>
		/// <exception cref="System.IO.IOException">on any error.</exception>
		private static void ProxyLink(HttpServletRequest req, HttpServletResponse resp, URI
			 link, Cookie c, string proxyHost)
		{
			DefaultHttpClient client = new DefaultHttpClient();
			client.GetParams().SetParameter(ClientPNames.CookiePolicy, CookiePolicy.BrowserCompatibility
				).SetBooleanParameter(ClientPNames.AllowCircularRedirects, true);
			// Make sure we send the request from the proxy address in the config
			// since that is what the AM filter checks against. IP aliasing or
			// similar could cause issues otherwise.
			IPAddress localAddress = Sharpen.Extensions.GetAddressByName(proxyHost);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("local InetAddress for proxy host: {}", localAddress);
			}
			client.GetParams().SetParameter(ConnRoutePNames.LocalAddress, localAddress);
			HttpGet httpGet = new HttpGet(link);
			Enumeration<string> names = req.GetHeaderNames();
			while (names.MoveNext())
			{
				string name = names.Current;
				if (passThroughHeaders.Contains(name))
				{
					string value = req.GetHeader(name);
					if (Log.IsDebugEnabled())
					{
						Log.Debug("REQ HEADER: {} : {}", name, value);
					}
					httpGet.SetHeader(name, value);
				}
			}
			string user = req.GetRemoteUser();
			if (user != null && !user.IsEmpty())
			{
				httpGet.SetHeader("Cookie", ProxyUserCookieName + "=" + URLEncoder.Encode(user, "ASCII"
					));
			}
			OutputStream @out = resp.GetOutputStream();
			try
			{
				HttpResponse httpResp = client.Execute(httpGet);
				resp.SetStatus(httpResp.GetStatusLine().GetStatusCode());
				foreach (Header header in httpResp.GetAllHeaders())
				{
					resp.SetHeader(header.GetName(), header.GetValue());
				}
				if (c != null)
				{
					resp.AddCookie(c);
				}
				InputStream @in = httpResp.GetEntity().GetContent();
				if (@in != null)
				{
					IOUtils.CopyBytes(@in, @out, 4096, true);
				}
			}
			finally
			{
				httpGet.ReleaseConnection();
			}
		}

		private static string GetCheckCookieName(ApplicationId id)
		{
			return "checked_" + id;
		}

		private static Cookie MakeCheckCookie(ApplicationId id, bool isSet)
		{
			Cookie c = new Cookie(GetCheckCookieName(id), isSet.ToString());
			c.SetPath(ProxyUriUtils.GetPath(id));
			c.SetMaxAge(60 * 60 * 2);
			//2 hours in seconds
			return c;
		}

		private bool IsSecurityEnabled()
		{
			bool b = (bool)GetServletContext().GetAttribute(WebAppProxy.IsSecurityEnabledAttribute
				);
			return b != null ? b : false;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private AppReportFetcher.FetchedAppReport GetApplicationReport(ApplicationId id)
		{
			return ((AppReportFetcher)GetServletContext().GetAttribute(WebAppProxy.FetcherAttribute
				)).GetApplicationReport(id);
		}

		/// <exception cref="System.IO.IOException"/>
		private string GetProxyHost()
		{
			return ((string)GetServletContext().GetAttribute(WebAppProxy.ProxyHostAttribute));
		}

		/// <exception cref="System.IO.IOException"/>
		protected override void DoGet(HttpServletRequest req, HttpServletResponse resp)
		{
			try
			{
				string userApprovedParamS = req.GetParameter(ProxyUriUtils.ProxyApprovalParam);
				bool userWasWarned = false;
				bool userApproved = Sharpen.Extensions.ValueOf(userApprovedParamS);
				bool securityEnabled = IsSecurityEnabled();
				string remoteUser = req.GetRemoteUser();
				string pathInfo = req.GetPathInfo();
				string[] parts = pathInfo.Split("/", 3);
				if (parts.Length < 2)
				{
					Log.Warn("{} gave an invalid proxy path {}", remoteUser, pathInfo);
					NotFound(resp, "Your path appears to be formatted incorrectly.");
					return;
				}
				//parts[0] is empty because path info always starts with a /
				string appId = parts[1];
				string rest = parts.Length > 2 ? parts[2] : string.Empty;
				ApplicationId id = Apps.ToAppID(appId);
				if (id == null)
				{
					Log.Warn("{} attempting to access {} that is invalid", remoteUser, appId);
					NotFound(resp, appId + " appears to be formatted incorrectly.");
					return;
				}
				if (securityEnabled)
				{
					string cookieName = GetCheckCookieName(id);
					Cookie[] cookies = req.GetCookies();
					if (cookies != null)
					{
						foreach (Cookie c in cookies)
						{
							if (cookieName.Equals(c.GetName()))
							{
								userWasWarned = true;
								userApproved = userApproved || Sharpen.Extensions.ValueOf(c.GetValue());
								break;
							}
						}
					}
				}
				bool checkUser = securityEnabled && (!userWasWarned || !userApproved);
				AppReportFetcher.FetchedAppReport fetchedAppReport = null;
				ApplicationReport applicationReport = null;
				try
				{
					fetchedAppReport = GetApplicationReport(id);
					if (fetchedAppReport != null)
					{
						if (fetchedAppReport.GetAppReportSource() != AppReportFetcher.AppReportSource.Rm 
							&& fetchedAppReport.GetAppReportSource() != AppReportFetcher.AppReportSource.Ahs)
						{
							throw new NotSupportedException("Application report not " + "fetched from RM or history server."
								);
						}
						applicationReport = fetchedAppReport.GetApplicationReport();
					}
				}
				catch (ApplicationNotFoundException)
				{
					applicationReport = null;
				}
				if (applicationReport == null)
				{
					Log.Warn("{} attempting to access {} that was not found", remoteUser, id);
					URI toFetch = ProxyUriUtils.GetUriFromTrackingPlugins(id, this.trackingUriPlugins
						);
					if (toFetch != null)
					{
						ProxyUtils.SendRedirect(req, resp, toFetch.ToString());
						return;
					}
					NotFound(resp, "Application " + appId + " could not be found " + "in RM or history server"
						);
					return;
				}
				string original = applicationReport.GetOriginalTrackingUrl();
				URI trackingUri;
				if (original == null || original.Equals("N/A") || original.Equals(string.Empty))
				{
					if (fetchedAppReport.GetAppReportSource() == AppReportFetcher.AppReportSource.Rm)
					{
						// fallback to ResourceManager's app page if no tracking URI provided
						// and Application Report was fetched from RM
						Log.Debug("Original tracking url is '{}'. Redirecting to RM app page", original ==
							 null ? "NULL" : original);
						ProxyUtils.SendRedirect(req, resp, StringHelper.Pjoin(rmAppPageUrlBase, id.ToString
							()));
					}
					else
					{
						if (fetchedAppReport.GetAppReportSource() == AppReportFetcher.AppReportSource.Ahs)
						{
							// fallback to Application History Server app page if the application
							// report was fetched from AHS
							Log.Debug("Original tracking url is '{}'. Redirecting to AHS app page", original 
								== null ? "NULL" : original);
							ProxyUtils.SendRedirect(req, resp, StringHelper.Pjoin(ahsAppPageUrlBase, id.ToString
								()));
						}
					}
					return;
				}
				else
				{
					if (ProxyUriUtils.GetSchemeFromUrl(original).IsEmpty())
					{
						trackingUri = ProxyUriUtils.GetUriFromAMUrl(WebAppUtils.GetHttpSchemePrefix(conf)
							, original);
					}
					else
					{
						trackingUri = new URI(original);
					}
				}
				string runningUser = applicationReport.GetUser();
				if (checkUser && !runningUser.Equals(remoteUser))
				{
					Log.Info("Asking {} if they want to connect to the " + "app master GUI of {} owned by {}"
						, remoteUser, appId, runningUser);
					WarnUserPage(resp, ProxyUriUtils.GetPathAndQuery(id, rest, req.GetQueryString(), 
						true), runningUser, id);
					return;
				}
				// Append the user-provided path and query parameter to the original
				// tracking url.
				IList<NameValuePair> queryPairs = URLEncodedUtils.Parse(req.GetQueryString(), null
					);
				UriBuilder builder = UriBuilder.FromUri(trackingUri);
				foreach (NameValuePair pair in queryPairs)
				{
					builder.QueryParam(pair.GetName(), pair.GetValue());
				}
				URI toFetch_1 = builder.Path(rest).Build();
				Log.Info("{} is accessing unchecked {}" + " which is the app master GUI of {} owned by {}"
					, remoteUser, toFetch_1, appId, runningUser);
				switch (applicationReport.GetYarnApplicationState())
				{
					case YarnApplicationState.Killed:
					case YarnApplicationState.Finished:
					case YarnApplicationState.Failed:
					{
						ProxyUtils.SendRedirect(req, resp, toFetch_1.ToString());
						return;
					}

					default:
					{
						break;
					}
				}
				// fall out of the switch
				Cookie c_1 = null;
				if (userWasWarned && userApproved)
				{
					c_1 = MakeCheckCookie(id, true);
				}
				ProxyLink(req, resp, toFetch_1, c_1, GetProxyHost());
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
		}

		/// <summary>
		/// This method is used by Java object deserialization, to fill in the
		/// transient
		/// <see cref="trackingUriPlugins"/>
		/// field.
		/// See
		/// <see cref="System.IO.ObjectInputStream.DefaultReadObject()"/>
		/// <p>
		/// <I>Do not remove</I>
		/// <p>
		/// Yarn isn't currently serializing this class, but findbugs
		/// complains in its absence.
		/// </summary>
		/// <param name="input">source</param>
		/// <exception cref="System.IO.IOException">IO failure</exception>
		/// <exception cref="System.TypeLoadException">classloader fun</exception>
		private void ReadObject(ObjectInputStream input)
		{
			input.DefaultReadObject();
			conf = new YarnConfiguration();
			this.trackingUriPlugins = conf.GetInstances<TrackingUriPlugin>(YarnConfiguration.
				YarnTrackingUrlGenerator);
		}
	}
}
