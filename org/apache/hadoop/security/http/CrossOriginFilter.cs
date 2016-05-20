using Sharpen;

namespace org.apache.hadoop.security.http
{
	public class CrossOriginFilter : javax.servlet.Filter
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.http.CrossOriginFilter
			)));

		internal const string ORIGIN = "Origin";

		internal const string ACCESS_CONTROL_REQUEST_METHOD = "Access-Control-Request-Method";

		internal const string ACCESS_CONTROL_REQUEST_HEADERS = "Access-Control-Request-Headers";

		internal const string ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";

		internal const string ACCESS_CONTROL_ALLOW_CREDENTIALS = "Access-Control-Allow-Credentials";

		internal const string ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";

		internal const string ACCESS_CONTROL_ALLOW_HEADERS = "Access-Control-Allow-Headers";

		internal const string ACCESS_CONTROL_MAX_AGE = "Access-Control-Max-Age";

		public const string ALLOWED_ORIGINS = "allowed-origins";

		public const string ALLOWED_ORIGINS_DEFAULT = "*";

		public const string ALLOWED_METHODS = "allowed-methods";

		public const string ALLOWED_METHODS_DEFAULT = "GET,POST,HEAD";

		public const string ALLOWED_HEADERS = "allowed-headers";

		public const string ALLOWED_HEADERS_DEFAULT = "X-Requested-With,Content-Type,Accept,Origin";

		public const string MAX_AGE = "max-age";

		public const string MAX_AGE_DEFAULT = "1800";

		private System.Collections.Generic.IList<string> allowedMethods = new System.Collections.Generic.List
			<string>();

		private System.Collections.Generic.IList<string> allowedHeaders = new System.Collections.Generic.List
			<string>();

		private System.Collections.Generic.IList<string> allowedOrigins = new System.Collections.Generic.List
			<string>();

		private bool allowAllOrigins = true;

		private string maxAge;

		// HTTP CORS Request Headers
		// HTTP CORS Response Headers
		// Filter configuration
		/// <exception cref="javax.servlet.ServletException"/>
		public virtual void init(javax.servlet.FilterConfig filterConfig)
		{
			initializeAllowedMethods(filterConfig);
			initializeAllowedHeaders(filterConfig);
			initializeAllowedOrigins(filterConfig);
			initializeMaxAge(filterConfig);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="javax.servlet.ServletException"/>
		public virtual void doFilter(javax.servlet.ServletRequest req, javax.servlet.ServletResponse
			 res, javax.servlet.FilterChain chain)
		{
			doCrossFilter((javax.servlet.http.HttpServletRequest)req, (javax.servlet.http.HttpServletResponse
				)res);
			chain.doFilter(req, res);
		}

		public virtual void destroy()
		{
			allowedMethods.clear();
			allowedHeaders.clear();
			allowedOrigins.clear();
		}

		private void doCrossFilter(javax.servlet.http.HttpServletRequest req, javax.servlet.http.HttpServletResponse
			 res)
		{
			string originsList = encodeHeader(req.getHeader(ORIGIN));
			if (!isCrossOrigin(originsList))
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Header origin is null. Returning");
				}
				return;
			}
			if (!areOriginsAllowed(originsList))
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Header origins '" + originsList + "' not allowed. Returning");
				}
				return;
			}
			string accessControlRequestMethod = req.getHeader(ACCESS_CONTROL_REQUEST_METHOD);
			if (!isMethodAllowed(accessControlRequestMethod))
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Access control method '" + accessControlRequestMethod + "' not allowed. Returning"
						);
				}
				return;
			}
			string accessControlRequestHeaders = req.getHeader(ACCESS_CONTROL_REQUEST_HEADERS
				);
			if (!areHeadersAllowed(accessControlRequestHeaders))
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Access control headers '" + accessControlRequestHeaders + "' not allowed. Returning"
						);
				}
				return;
			}
			if (LOG.isDebugEnabled())
			{
				LOG.debug("Completed cross origin filter checks. Populating " + "HttpServletResponse"
					);
			}
			res.setHeader(ACCESS_CONTROL_ALLOW_ORIGIN, originsList);
			res.setHeader(ACCESS_CONTROL_ALLOW_CREDENTIALS, true.ToString());
			res.setHeader(ACCESS_CONTROL_ALLOW_METHODS, getAllowedMethodsHeader());
			res.setHeader(ACCESS_CONTROL_ALLOW_HEADERS, getAllowedHeadersHeader());
			res.setHeader(ACCESS_CONTROL_MAX_AGE, maxAge);
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual string getAllowedHeadersHeader()
		{
			return org.apache.commons.lang.StringUtils.join(allowedHeaders, ',');
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual string getAllowedMethodsHeader()
		{
			return org.apache.commons.lang.StringUtils.join(allowedMethods, ',');
		}

		private void initializeAllowedMethods(javax.servlet.FilterConfig filterConfig)
		{
			string allowedMethodsConfig = filterConfig.getInitParameter(ALLOWED_METHODS);
			if (allowedMethodsConfig == null)
			{
				allowedMethodsConfig = ALLOWED_METHODS_DEFAULT;
			}
			Sharpen.Collections.AddAll(allowedMethods, java.util.Arrays.asList(allowedMethodsConfig
				.Trim().split("\\s*,\\s*")));
			LOG.info("Allowed Methods: " + getAllowedMethodsHeader());
		}

		private void initializeAllowedHeaders(javax.servlet.FilterConfig filterConfig)
		{
			string allowedHeadersConfig = filterConfig.getInitParameter(ALLOWED_HEADERS);
			if (allowedHeadersConfig == null)
			{
				allowedHeadersConfig = ALLOWED_HEADERS_DEFAULT;
			}
			Sharpen.Collections.AddAll(allowedHeaders, java.util.Arrays.asList(allowedHeadersConfig
				.Trim().split("\\s*,\\s*")));
			LOG.info("Allowed Headers: " + getAllowedHeadersHeader());
		}

		private void initializeAllowedOrigins(javax.servlet.FilterConfig filterConfig)
		{
			string allowedOriginsConfig = filterConfig.getInitParameter(ALLOWED_ORIGINS);
			if (allowedOriginsConfig == null)
			{
				allowedOriginsConfig = ALLOWED_ORIGINS_DEFAULT;
			}
			Sharpen.Collections.AddAll(allowedOrigins, java.util.Arrays.asList(allowedOriginsConfig
				.Trim().split("\\s*,\\s*")));
			allowAllOrigins = allowedOrigins.contains("*");
			LOG.info("Allowed Origins: " + org.apache.commons.lang.StringUtils.join(allowedOrigins
				, ','));
			LOG.info("Allow All Origins: " + allowAllOrigins);
		}

		private void initializeMaxAge(javax.servlet.FilterConfig filterConfig)
		{
			maxAge = filterConfig.getInitParameter(MAX_AGE);
			if (maxAge == null)
			{
				maxAge = MAX_AGE_DEFAULT;
			}
			LOG.info("Max Age: " + maxAge);
		}

		internal static string encodeHeader(string header)
		{
			if (header == null)
			{
				return null;
			}
			// Protect against HTTP response splitting vulnerability
			// since value is written as part of the response header
			// Ensure this header only has one header by removing
			// CRs and LFs
			return header.split("\n|\r")[0].Trim();
		}

		internal static bool isCrossOrigin(string originsList)
		{
			return originsList != null;
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual bool areOriginsAllowed(string originsList)
		{
			if (allowAllOrigins)
			{
				return true;
			}
			string[] origins = originsList.Trim().split("\\s+");
			foreach (string origin in origins)
			{
				foreach (string allowedOrigin in allowedOrigins)
				{
					if (allowedOrigin.contains("*"))
					{
						string regex = allowedOrigin.Replace(".", "\\.").Replace("*", ".*");
						java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex);
						java.util.regex.Matcher m = p.matcher(origin);
						if (m.matches())
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

		private bool areHeadersAllowed(string accessControlRequestHeaders)
		{
			if (accessControlRequestHeaders == null)
			{
				return true;
			}
			string[] headers = accessControlRequestHeaders.Trim().split("\\s*,\\s*");
			return allowedHeaders.containsAll(java.util.Arrays.asList(headers));
		}

		private bool isMethodAllowed(string accessControlRequestMethod)
		{
			if (accessControlRequestMethod == null)
			{
				return true;
			}
			return allowedMethods.contains(accessControlRequestMethod);
		}
	}
}
