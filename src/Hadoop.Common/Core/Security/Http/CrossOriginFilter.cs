using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.Security.Http
{
	public class CrossOriginFilter : Filter
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(CrossOriginFilter));

		internal const string Origin = "Origin";

		internal const string AccessControlRequestMethod = "Access-Control-Request-Method";

		internal const string AccessControlRequestHeaders = "Access-Control-Request-Headers";

		internal const string AccessControlAllowOrigin = "Access-Control-Allow-Origin";

		internal const string AccessControlAllowCredentials = "Access-Control-Allow-Credentials";

		internal const string AccessControlAllowMethods = "Access-Control-Allow-Methods";

		internal const string AccessControlAllowHeaders = "Access-Control-Allow-Headers";

		internal const string AccessControlMaxAge = "Access-Control-Max-Age";

		public const string AllowedOrigins = "allowed-origins";

		public const string AllowedOriginsDefault = "*";

		public const string AllowedMethods = "allowed-methods";

		public const string AllowedMethodsDefault = "GET,POST,HEAD";

		public const string AllowedHeaders = "allowed-headers";

		public const string AllowedHeadersDefault = "X-Requested-With,Content-Type,Accept,Origin";

		public const string MaxAge = "max-age";

		public const string MaxAgeDefault = "1800";

		private IList<string> allowedMethods = new AList<string>();

		private IList<string> allowedHeaders = new AList<string>();

		private IList<string> allowedOrigins = new AList<string>();

		private bool allowAllOrigins = true;

		private string maxAge;

		// HTTP CORS Request Headers
		// HTTP CORS Response Headers
		// Filter configuration
		/// <exception cref="Javax.Servlet.ServletException"/>
		public virtual void Init(FilterConfig filterConfig)
		{
			InitializeAllowedMethods(filterConfig);
			InitializeAllowedHeaders(filterConfig);
			InitializeAllowedOrigins(filterConfig);
			InitializeMaxAge(filterConfig);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Servlet.ServletException"/>
		public virtual void DoFilter(ServletRequest req, ServletResponse res, FilterChain
			 chain)
		{
			DoCrossFilter((HttpServletRequest)req, (HttpServletResponse)res);
			chain.DoFilter(req, res);
		}

		public virtual void Destroy()
		{
			allowedMethods.Clear();
			allowedHeaders.Clear();
			allowedOrigins.Clear();
		}

		private void DoCrossFilter(HttpServletRequest req, HttpServletResponse res)
		{
			string originsList = EncodeHeader(req.GetHeader(Origin));
			if (!IsCrossOrigin(originsList))
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Header origin is null. Returning");
				}
				return;
			}
			if (!AreOriginsAllowed(originsList))
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Header origins '" + originsList + "' not allowed. Returning");
				}
				return;
			}
			string accessControlRequestMethod = req.GetHeader(AccessControlRequestMethod);
			if (!IsMethodAllowed(accessControlRequestMethod))
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Access control method '" + accessControlRequestMethod + "' not allowed. Returning"
						);
				}
				return;
			}
			string accessControlRequestHeaders = req.GetHeader(AccessControlRequestHeaders);
			if (!AreHeadersAllowed(accessControlRequestHeaders))
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Access control headers '" + accessControlRequestHeaders + "' not allowed. Returning"
						);
				}
				return;
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Completed cross origin filter checks. Populating " + "HttpServletResponse"
					);
			}
			res.SetHeader(AccessControlAllowOrigin, originsList);
			res.SetHeader(AccessControlAllowCredentials, true.ToString());
			res.SetHeader(AccessControlAllowMethods, GetAllowedMethodsHeader());
			res.SetHeader(AccessControlAllowHeaders, GetAllowedHeadersHeader());
			res.SetHeader(AccessControlMaxAge, maxAge);
		}

		[VisibleForTesting]
		internal virtual string GetAllowedHeadersHeader()
		{
			return StringUtils.Join(allowedHeaders, ',');
		}

		[VisibleForTesting]
		internal virtual string GetAllowedMethodsHeader()
		{
			return StringUtils.Join(allowedMethods, ',');
		}

		private void InitializeAllowedMethods(FilterConfig filterConfig)
		{
			string allowedMethodsConfig = filterConfig.GetInitParameter(AllowedMethods);
			if (allowedMethodsConfig == null)
			{
				allowedMethodsConfig = AllowedMethodsDefault;
			}
			Collections.AddAll(allowedMethods, Arrays.AsList(allowedMethodsConfig.Trim
				().Split("\\s*,\\s*")));
			Log.Info("Allowed Methods: " + GetAllowedMethodsHeader());
		}

		private void InitializeAllowedHeaders(FilterConfig filterConfig)
		{
			string allowedHeadersConfig = filterConfig.GetInitParameter(AllowedHeaders);
			if (allowedHeadersConfig == null)
			{
				allowedHeadersConfig = AllowedHeadersDefault;
			}
			Collections.AddAll(allowedHeaders, Arrays.AsList(allowedHeadersConfig.Trim
				().Split("\\s*,\\s*")));
			Log.Info("Allowed Headers: " + GetAllowedHeadersHeader());
		}

		private void InitializeAllowedOrigins(FilterConfig filterConfig)
		{
			string allowedOriginsConfig = filterConfig.GetInitParameter(AllowedOrigins);
			if (allowedOriginsConfig == null)
			{
				allowedOriginsConfig = AllowedOriginsDefault;
			}
			Collections.AddAll(allowedOrigins, Arrays.AsList(allowedOriginsConfig.Trim
				().Split("\\s*,\\s*")));
			allowAllOrigins = allowedOrigins.Contains("*");
			Log.Info("Allowed Origins: " + StringUtils.Join(allowedOrigins, ','));
			Log.Info("Allow All Origins: " + allowAllOrigins);
		}

		private void InitializeMaxAge(FilterConfig filterConfig)
		{
			maxAge = filterConfig.GetInitParameter(MaxAge);
			if (maxAge == null)
			{
				maxAge = MaxAgeDefault;
			}
			Log.Info("Max Age: " + maxAge);
		}

		internal static string EncodeHeader(string header)
		{
			if (header == null)
			{
				return null;
			}
			// Protect against HTTP response splitting vulnerability
			// since value is written as part of the response header
			// Ensure this header only has one header by removing
			// CRs and LFs
			return header.Split("\n|\r")[0].Trim();
		}

		internal static bool IsCrossOrigin(string originsList)
		{
			return originsList != null;
		}

		[VisibleForTesting]
		internal virtual bool AreOriginsAllowed(string originsList)
		{
			if (allowAllOrigins)
			{
				return true;
			}
			string[] origins = originsList.Trim().Split("\\s+");
			foreach (string origin in origins)
			{
				foreach (string allowedOrigin in allowedOrigins)
				{
					if (allowedOrigin.Contains("*"))
					{
						string regex = allowedOrigin.Replace(".", "\\.").Replace("*", ".*");
						Pattern p = Pattern.Compile(regex);
						Matcher m = p.Matcher(origin);
						if (m.Matches())
						{
							return true;
						}
					}
					else
					{
						if (allowedOrigin.Equals(origin))
						{
							return true;
						}
					}
				}
			}
			return false;
		}

		private bool AreHeadersAllowed(string accessControlRequestHeaders)
		{
			if (accessControlRequestHeaders == null)
			{
				return true;
			}
			string[] headers = accessControlRequestHeaders.Trim().Split("\\s*,\\s*");
			return allowedHeaders.ContainsAll(Arrays.AsList(headers));
		}

		private bool IsMethodAllowed(string accessControlRequestMethod)
		{
			if (accessControlRequestMethod == null)
			{
				return true;
			}
			return allowedMethods.Contains(accessControlRequestMethod);
		}
	}
}
