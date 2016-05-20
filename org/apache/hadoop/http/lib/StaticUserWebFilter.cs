using Sharpen;

namespace org.apache.hadoop.http.lib
{
	/// <summary>
	/// Provides a servlet filter that pretends to authenticate a fake user (Dr.Who)
	/// so that the web UI is usable for a secure cluster without authentication.
	/// </summary>
	public class StaticUserWebFilter : org.apache.hadoop.http.FilterInitializer
	{
		internal const string DEPRECATED_UGI_KEY = "dfs.web.ugi";

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.lib.StaticUserWebFilter
			)));

		internal class User : java.security.Principal
		{
			private readonly string name;

			public User(string name)
			{
				this.name = name;
			}

			public virtual string getName()
			{
				return name;
			}

			public override int GetHashCode()
			{
				return name.GetHashCode();
			}

			public override bool Equals(object other)
			{
				if (other == this)
				{
					return true;
				}
				else
				{
					if (other == null || Sharpen.Runtime.getClassForObject(other) != Sharpen.Runtime.getClassForObject
						(this))
					{
						return false;
					}
				}
				return ((org.apache.hadoop.http.lib.StaticUserWebFilter.User)other).name.Equals(name
					);
			}

			public override string ToString()
			{
				return name;
			}
		}

		public class StaticUserFilter : javax.servlet.Filter
		{
			private org.apache.hadoop.http.lib.StaticUserWebFilter.User user;

			private string username;

			public virtual void destroy()
			{
			}

			// NOTHING
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="javax.servlet.ServletException"/>
			public virtual void doFilter(javax.servlet.ServletRequest request, javax.servlet.ServletResponse
				 response, javax.servlet.FilterChain chain)
			{
				javax.servlet.http.HttpServletRequest httpRequest = (javax.servlet.http.HttpServletRequest
					)request;
				// if the user is already authenticated, don't override it
				if (httpRequest.getRemoteUser() != null)
				{
					chain.doFilter(request, response);
				}
				else
				{
					javax.servlet.http.HttpServletRequestWrapper wrapper = new _HttpServletRequestWrapper_99
						(this, httpRequest);
					chain.doFilter(wrapper, response);
				}
			}

			private sealed class _HttpServletRequestWrapper_99 : javax.servlet.http.HttpServletRequestWrapper
			{
				public _HttpServletRequestWrapper_99(StaticUserFilter _enclosing, javax.servlet.http.HttpServletRequest
					 baseArg1)
					: base(baseArg1)
				{
					this._enclosing = _enclosing;
				}

				public override java.security.Principal getUserPrincipal()
				{
					return this._enclosing.user;
				}

				public override string getRemoteUser()
				{
					return this._enclosing.username;
				}

				private readonly StaticUserFilter _enclosing;
			}

			/// <exception cref="javax.servlet.ServletException"/>
			public virtual void init(javax.servlet.FilterConfig conf)
			{
				this.username = conf.getInitParameter(org.apache.hadoop.fs.CommonConfigurationKeys
					.HADOOP_HTTP_STATIC_USER);
				this.user = new org.apache.hadoop.http.lib.StaticUserWebFilter.User(username);
			}
		}

		public override void initFilter(org.apache.hadoop.http.FilterContainer container, 
			org.apache.hadoop.conf.Configuration conf)
		{
			System.Collections.Generic.Dictionary<string, string> options = new System.Collections.Generic.Dictionary
				<string, string>();
			string username = getUsernameFromConf(conf);
			options[org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER] = username;
			container.addFilter("static_user_filter", Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.http.lib.StaticUserWebFilter.StaticUserFilter)).getName(), options
				);
		}

		/// <summary>Retrieve the static username from the configuration.</summary>
		internal static string getUsernameFromConf(org.apache.hadoop.conf.Configuration conf
			)
		{
			string oldStyleUgi = conf.get(DEPRECATED_UGI_KEY);
			if (oldStyleUgi != null)
			{
				// We can't use the normal configuration deprecation mechanism here
				// since we need to split out the username from the configured UGI.
				LOG.warn(DEPRECATED_UGI_KEY + " should not be used. Instead, use " + org.apache.hadoop.fs.CommonConfigurationKeys
					.HADOOP_HTTP_STATIC_USER + ".");
				string[] parts = oldStyleUgi.split(",");
				return parts[0];
			}
			else
			{
				return conf.get(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_HTTP_STATIC_USER
					, org.apache.hadoop.fs.CommonConfigurationKeys.DEFAULT_HADOOP_HTTP_STATIC_USER);
			}
		}
	}
}
