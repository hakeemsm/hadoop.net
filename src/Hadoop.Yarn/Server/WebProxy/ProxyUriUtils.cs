using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Webproxy
{
	public class ProxyUriUtils
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(ProxyUriUtils
			));

		/// <summary>Name of the servlet to use when registering the proxy servlet.</summary>
		public const string ProxyServletName = "proxy";

		/// <summary>Base path where the proxy servlet will handle requests.</summary>
		public const string ProxyBase = "/proxy/";

		/// <summary>Path Specification for the proxy servlet.</summary>
		public const string ProxyPathSpec = ProxyBase + "*";

		/// <summary>Query Parameter indicating that the URI was approved.</summary>
		public const string ProxyApprovalParam = "proxyapproved";

		private static string UriEncode(object o)
		{
			try
			{
				System.Diagnostics.Debug.Assert((o != null), "o canot be null");
				return URLEncoder.Encode(o.ToString(), "UTF-8");
			}
			catch (UnsupportedEncodingException e)
			{
				//This should never happen
				throw new RuntimeException("UTF-8 is not supported by this system?", e);
			}
		}

		/// <summary>Get the proxied path for an application.</summary>
		/// <param name="id">the application id to use.</param>
		/// <returns>the base path to that application through the proxy.</returns>
		public static string GetPath(ApplicationId id)
		{
			if (id == null)
			{
				throw new ArgumentException("Application id cannot be null ");
			}
			return StringHelper.Ujoin(ProxyBase, UriEncode(id));
		}

		/// <summary>Get the proxied path for an application.</summary>
		/// <param name="id">the application id to use.</param>
		/// <param name="path">the rest of the path to the application.</param>
		/// <returns>the base path to that application through the proxy.</returns>
		public static string GetPath(ApplicationId id, string path)
		{
			if (path == null)
			{
				return GetPath(id);
			}
			else
			{
				return StringHelper.Ujoin(GetPath(id), path);
			}
		}

		/// <summary>Get the proxied path for an application</summary>
		/// <param name="id">the id of the application</param>
		/// <param name="path">the path of the application.</param>
		/// <param name="query">the query parameters</param>
		/// <param name="approved">true if the user has approved accessing this app.</param>
		/// <returns>the proxied path for this app.</returns>
		public static string GetPathAndQuery(ApplicationId id, string path, string query, 
			bool approved)
		{
			StringBuilder newp = new StringBuilder();
			newp.Append(GetPath(id, path));
			bool first = AppendQuery(newp, query, true);
			if (approved)
			{
				AppendQuery(newp, ProxyApprovalParam + "=true", first);
			}
			return newp.ToString();
		}

		private static bool AppendQuery(StringBuilder builder, string query, bool first)
		{
			if (query != null && !query.IsEmpty())
			{
				if (first && !query.StartsWith("?"))
				{
					builder.Append('?');
				}
				if (!first && !query.StartsWith("&"))
				{
					builder.Append('&');
				}
				builder.Append(query);
				return false;
			}
			return first;
		}

		/// <summary>Get a proxied URI for the original URI.</summary>
		/// <param name="originalUri">
		/// the original URI to go through the proxy, or null if
		/// a default path "/" can be used.
		/// </param>
		/// <param name="proxyUri">the URI of the proxy itself, scheme, host and port are used.
		/// 	</param>
		/// <param name="id">the id of the application</param>
		/// <returns>the proxied URI</returns>
		public static URI GetProxyUri(URI originalUri, URI proxyUri, ApplicationId id)
		{
			try
			{
				string path = GetPath(id, originalUri == null ? "/" : originalUri.GetPath());
				return new URI(proxyUri.GetScheme(), proxyUri.GetAuthority(), path, originalUri ==
					 null ? null : originalUri.GetQuery(), originalUri == null ? null : originalUri.
					GetFragment());
			}
			catch (URISyntaxException e)
			{
				throw new RuntimeException("Could not proxify " + originalUri, e);
			}
		}

		/// <summary>Create a URI form a no scheme Url, such as is returned by the AM.</summary>
		/// <param name="noSchemeUrl">the URL formate returned by an AM</param>
		/// <returns>a URI with an http scheme</returns>
		/// <exception cref="Sharpen.URISyntaxException">if the url is not formatted correctly.
		/// 	</exception>
		public static URI GetUriFromAMUrl(string scheme, string noSchemeUrl)
		{
			if (GetSchemeFromUrl(noSchemeUrl).IsEmpty())
			{
				/*
				* check is made to make sure if AM reports with scheme then it will be
				* used by default otherwise it will default to the one configured using
				* "yarn.http.policy".
				*/
				return new URI(scheme + noSchemeUrl);
			}
			else
			{
				return new URI(noSchemeUrl);
			}
		}

		/// <summary>
		/// Returns the first valid tracking link, if any, from the given id from the
		/// given list of plug-ins, if any.
		/// </summary>
		/// <param name="id">the id of the application for which the tracking link is desired
		/// 	</param>
		/// <param name="trackingUriPlugins">list of plugins from which to get the tracking link
		/// 	</param>
		/// <returns>the desired link if possible, otherwise null</returns>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public static URI GetUriFromTrackingPlugins(ApplicationId id, IList<TrackingUriPlugin
			> trackingUriPlugins)
		{
			URI toRet = null;
			foreach (TrackingUriPlugin plugin in trackingUriPlugins)
			{
				toRet = plugin.GetTrackingUri(id);
				if (toRet != null)
				{
					return toRet;
				}
			}
			return null;
		}

		/// <summary>
		/// Returns the scheme if present in the url
		/// eg.
		/// </summary>
		/// <remarks>
		/// Returns the scheme if present in the url
		/// eg. "https://issues.apache.org/jira/browse/YARN"
		/// <literal>-&gt;</literal>
		/// "https"
		/// </remarks>
		public static string GetSchemeFromUrl(string url)
		{
			int index = 0;
			if (url != null)
			{
				index = url.IndexOf("://");
			}
			if (index > 0)
			{
				return Sharpen.Runtime.Substring(url, 0, index);
			}
			else
			{
				return string.Empty;
			}
		}
	}
}
