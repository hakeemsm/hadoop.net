using System;
using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Com.Sun.Jersey.Api.Container.Filter;
using Com.Sun.Jersey.Api.Core;
using Com.Sun.Jersey.Core.Util;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Com.Sun.Jersey.Spi.Container.Servlet;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	/// <seealso cref="WebApps">for a usage example</seealso>
	public abstract class WebApp : ServletModule
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Yarn.Webapp.WebApp
			));

		public enum HTTP
		{
			Get,
			Post,
			Head,
			Put,
			Delete
		}

		private volatile string name;

		private volatile IList<string> servePathSpecs = new AList<string>();

		private volatile string redirectPath;

		private volatile string wsName;

		private volatile Configuration conf;

		private volatile HttpServer2 httpServer;

		private volatile GuiceFilter guiceFilter;

		private readonly Org.Apache.Hadoop.Yarn.Webapp.Router router = new Org.Apache.Hadoop.Yarn.Webapp.Router
			();

		internal const int RPath = 0;

		internal const int RController = 1;

		internal const int RAction = 2;

		internal const int RParams = 3;

		internal static readonly Splitter pathSplitter = Splitter.On('/').TrimResults().OmitEmptyStrings
			();

		// path to redirect to
		// index for the parsed route result
		internal virtual void SetHttpServer(HttpServer2 server)
		{
			httpServer = Preconditions.CheckNotNull(server, "http server");
		}

		[Provides]
		public virtual HttpServer2 HttpServer()
		{
			return httpServer;
		}

		/// <summary>Get the address the http server is bound to</summary>
		/// <returns>InetSocketAddress</returns>
		public virtual IPEndPoint GetListenerAddress()
		{
			return Preconditions.CheckNotNull(httpServer, "httpServer").GetConnectorAddress(0
				);
		}

		public virtual int Port()
		{
			IPEndPoint addr = Preconditions.CheckNotNull(httpServer, "httpServer").GetConnectorAddress
				(0);
			return addr == null ? -1 : addr.Port;
		}

		public virtual void Stop()
		{
			try
			{
				Preconditions.CheckNotNull(httpServer, "httpServer").Stop();
				Preconditions.CheckNotNull(guiceFilter, "guiceFilter").Destroy();
			}
			catch (Exception e)
			{
				throw new WebAppException(e);
			}
		}

		public virtual void JoinThread()
		{
			try
			{
				Preconditions.CheckNotNull(httpServer, "httpServer").Join();
			}
			catch (Exception e)
			{
				Log.Info("interrupted", e);
			}
		}

		internal virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		[Provides]
		public virtual Configuration Conf()
		{
			return conf;
		}

		[Provides]
		internal virtual Org.Apache.Hadoop.Yarn.Webapp.Router Router()
		{
			return router;
		}

		[Provides]
		internal virtual Org.Apache.Hadoop.Yarn.Webapp.WebApp WebApp()
		{
			return this;
		}

		internal virtual void SetName(string name)
		{
			this.name = name;
		}

		public virtual string Name()
		{
			return this.name;
		}

		public virtual string WsName()
		{
			return this.wsName;
		}

		internal virtual void AddServePathSpec(string path)
		{
			this.servePathSpecs.AddItem(path);
		}

		public virtual string[] GetServePathSpecs()
		{
			return Sharpen.Collections.ToArray(this.servePathSpecs, new string[this.servePathSpecs
				.Count]);
		}

		/// <summary>Set a path to redirect the user to if they just go to "/".</summary>
		/// <remarks>
		/// Set a path to redirect the user to if they just go to "/". For
		/// instance "/" goes to "/yarn/apps". This allows the filters to
		/// more easily differentiate the different webapps.
		/// </remarks>
		/// <param name="path">the path to redirect to</param>
		internal virtual void SetRedirectPath(string path)
		{
			this.redirectPath = path;
		}

		internal virtual void SetWebServices(string name)
		{
			this.wsName = name;
		}

		public virtual string GetRedirectPath()
		{
			return this.redirectPath;
		}

		internal virtual void SetHostClass(Type cls)
		{
			router.SetHostClass(cls);
		}

		internal virtual void SetGuiceFilter(GuiceFilter instance)
		{
			guiceFilter = instance;
		}

		protected override void ConfigureServlets()
		{
			Setup();
			Serve("/", "/__stop").With(typeof(Dispatcher));
			foreach (string path in this.servePathSpecs)
			{
				Serve(path).With(typeof(Dispatcher));
			}
			ConfigureWebAppServlets();
		}

		protected internal virtual void ConfigureWebAppServlets()
		{
			// Add in the web services filters/serves if app has them.
			// Using Jersey/guice integration module. If user has web services
			// they must have also bound a default one in their webapp code.
			if (this.wsName != null)
			{
				// There seems to be an issue with the guice/jersey integration
				// where we have to list the stuff we don't want it to serve
				// through the guicecontainer. In this case its everything except
				// the the web services api prefix. We can't just change the filter
				// from /* below - that doesn't work.
				string regex = "(?!/" + this.wsName + ")";
				ServeRegex(regex).With(typeof(DefaultWrapperServlet));
				IDictionary<string, string> @params = new Dictionary<string, string>();
				@params[ResourceConfig.FeatureImplicitViewables] = "true";
				@params[ServletContainer.FeatureFilterForwardOn404] = "true";
				@params[FeaturesAndProperties.FeatureXmlrootelementProcessing] = "true";
				@params[ResourceConfig.PropertyContainerRequestFilters] = typeof(GZIPContentEncodingFilter
					).FullName;
				@params[ResourceConfig.PropertyContainerResponseFilters] = typeof(GZIPContentEncodingFilter
					).FullName;
				Filter("/*").Through(GetWebAppFilterClass(), @params);
			}
		}

		protected internal virtual Type GetWebAppFilterClass()
		{
			return typeof(GuiceContainer);
		}

		/// <summary>Setup of a webapp serving route.</summary>
		/// <param name="method">the http method for the route</param>
		/// <param name="pathSpec">the path spec in the form of /controller/action/:args etc.
		/// 	</param>
		/// <param name="cls">the controller class</param>
		/// <param name="action">the controller method</param>
		public virtual void Route(WebApp.HTTP method, string pathSpec, Type cls, string action
			)
		{
			IList<string> res = ParseRoute(pathSpec);
			router.Add(method, res[RPath], cls, action, res.SubList(RParams, res.Count));
		}

		public virtual void Route(string pathSpec, Type cls, string action)
		{
			Route(WebApp.HTTP.Get, pathSpec, cls, action);
		}

		public virtual void Route(string pathSpec, Type cls)
		{
			IList<string> res = ParseRoute(pathSpec);
			router.Add(WebApp.HTTP.Get, res[RPath], cls, res[RAction], res.SubList(RParams, res
				.Count));
		}

		/// <summary>
		/// /controller/action/:args =&gt; [/controller/action, controller, action, args]
		/// /controller/:args =&gt; [/controller, controller, index, args]
		/// </summary>
		internal static IList<string> ParseRoute(string pathSpec)
		{
			IList<string> result = Lists.NewArrayList();
			result.AddItem(GetPrefix(Preconditions.CheckNotNull(pathSpec, "pathSpec")));
			IEnumerable<string> parts = pathSplitter.Split(pathSpec);
			string controller = null;
			string action = null;
			foreach (string s in parts)
			{
				if (controller == null)
				{
					if (s[0] == ':')
					{
						controller = "default";
						result.AddItem(controller);
						action = "index";
						result.AddItem(action);
					}
					else
					{
						controller = s;
					}
				}
				else
				{
					if (action == null)
					{
						if (s[0] == ':')
						{
							action = "index";
							result.AddItem(action);
						}
						else
						{
							action = s;
						}
					}
				}
				result.AddItem(s);
			}
			if (controller == null)
			{
				result.AddItem("default");
			}
			if (action == null)
			{
				result.AddItem("index");
			}
			return result;
		}

		internal static string GetPrefix(string pathSpec)
		{
			int start = 0;
			while (CharMatcher.Whitespace.Matches(pathSpec[start]))
			{
				++start;
			}
			if (pathSpec[start] != '/')
			{
				throw new WebAppException("Path spec syntax error: " + pathSpec);
			}
			int ci = pathSpec.IndexOf(':');
			if (ci == -1)
			{
				ci = pathSpec.Length;
			}
			if (ci == 1)
			{
				return "/";
			}
			char c;
			do
			{
				c = pathSpec[--ci];
			}
			while (c == '/' || CharMatcher.Whitespace.Matches(c));
			return Sharpen.Runtime.Substring(pathSpec, start, ci + 1);
		}

		public abstract void Setup();
	}
}
