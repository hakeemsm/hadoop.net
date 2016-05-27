using System.Collections.Generic;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Http;
using Sharpen;

namespace Org.Apache.Hadoop.Http.Lib
{
	/// <summary>
	/// Provides a servlet filter that pretends to authenticate a fake user (Dr.Who)
	/// so that the web UI is usable for a secure cluster without authentication.
	/// </summary>
	public class StaticUserWebFilter : FilterInitializer
	{
		internal const string DeprecatedUgiKey = "dfs.web.ugi";

		private static readonly Log Log = LogFactory.GetLog(typeof(StaticUserWebFilter));

		internal class User : Principal
		{
			private readonly string name;

			public User(string name)
			{
				this.name = name;
			}

			public virtual string GetName()
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
					if (other == null || other.GetType() != GetType())
					{
						return false;
					}
				}
				return ((StaticUserWebFilter.User)other).name.Equals(name);
			}

			public override string ToString()
			{
				return name;
			}
		}

		public class StaticUserFilter : Filter
		{
			private StaticUserWebFilter.User user;

			private string username;

			public virtual void Destroy()
			{
			}

			// NOTHING
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Servlet.ServletException"/>
			public virtual void DoFilter(ServletRequest request, ServletResponse response, FilterChain
				 chain)
			{
				HttpServletRequest httpRequest = (HttpServletRequest)request;
				// if the user is already authenticated, don't override it
				if (httpRequest.GetRemoteUser() != null)
				{
					chain.DoFilter(request, response);
				}
				else
				{
					HttpServletRequestWrapper wrapper = new _HttpServletRequestWrapper_99(this, httpRequest
						);
					chain.DoFilter(wrapper, response);
				}
			}

			private sealed class _HttpServletRequestWrapper_99 : HttpServletRequestWrapper
			{
				public _HttpServletRequestWrapper_99(StaticUserFilter _enclosing, HttpServletRequest
					 baseArg1)
					: base(baseArg1)
				{
					this._enclosing = _enclosing;
				}

				public override Principal GetUserPrincipal()
				{
					return this._enclosing.user;
				}

				public override string GetRemoteUser()
				{
					return this._enclosing.username;
				}

				private readonly StaticUserFilter _enclosing;
			}

			/// <exception cref="Javax.Servlet.ServletException"/>
			public virtual void Init(FilterConfig conf)
			{
				this.username = conf.GetInitParameter(CommonConfigurationKeys.HadoopHttpStaticUser
					);
				this.user = new StaticUserWebFilter.User(username);
			}
		}

		public override void InitFilter(FilterContainer container, Configuration conf)
		{
			Dictionary<string, string> options = new Dictionary<string, string>();
			string username = GetUsernameFromConf(conf);
			options[CommonConfigurationKeys.HadoopHttpStaticUser] = username;
			container.AddFilter("static_user_filter", typeof(StaticUserWebFilter.StaticUserFilter
				).FullName, options);
		}

		/// <summary>Retrieve the static username from the configuration.</summary>
		internal static string GetUsernameFromConf(Configuration conf)
		{
			string oldStyleUgi = conf.Get(DeprecatedUgiKey);
			if (oldStyleUgi != null)
			{
				// We can't use the normal configuration deprecation mechanism here
				// since we need to split out the username from the configured UGI.
				Log.Warn(DeprecatedUgiKey + " should not be used. Instead, use " + CommonConfigurationKeys
					.HadoopHttpStaticUser + ".");
				string[] parts = oldStyleUgi.Split(",");
				return parts[0];
			}
			else
			{
				return conf.Get(CommonConfigurationKeys.HadoopHttpStaticUser, CommonConfigurationKeys
					.DefaultHadoopHttpStaticUser);
			}
		}
	}
}
