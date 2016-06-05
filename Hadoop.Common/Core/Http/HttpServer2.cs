using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Sun.Jersey.Spi.Container.Servlet;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Util;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Jmx;
using Org.Apache.Hadoop.Log;
using Org.Apache.Hadoop.Metrics;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Security.Authentication.Util;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Ssl;
using Org.Apache.Hadoop.Util;
using Org.Mortbay.IO;
using Org.Mortbay.Jetty;
using Org.Mortbay.Jetty.Handler;
using Org.Mortbay.Jetty.Nio;
using Org.Mortbay.Jetty.Security;
using Org.Mortbay.Jetty.Servlet;
using Org.Mortbay.Jetty.Webapp;
using Org.Mortbay.Thread;
using Org.Mortbay.Util;


namespace Org.Apache.Hadoop.Http
{
	/// <summary>Create a Jetty embedded server to answer http requests.</summary>
	/// <remarks>
	/// Create a Jetty embedded server to answer http requests. The primary goal is
	/// to serve up status information for the server. There are three contexts:
	/// "/logs/" -&gt; points to the log directory "/static/" -&gt; points to common static
	/// files (src/webapps/static) "/" -&gt; the jsp server code from
	/// (src/webapps/<name>)
	/// This class is a fork of the old HttpServer. HttpServer exists for
	/// compatibility reasons. See HBASE-10336 for more details.
	/// </remarks>
	public sealed class HttpServer2 : FilterContainer
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Http.HttpServer2
			));

		internal const string FilterInitializerProperty = "hadoop.http.filter.initializers";

		public const string HttpMaxThreads = "hadoop.http.max.threads";

		public const string ConfContextAttribute = "hadoop.conf";

		public const string AdminsAcl = "admins.acl";

		public const string SpnegoFilter = "SpnegoFilter";

		public const string NoCacheFilter = "NoCacheFilter";

		public const string BindAddress = "bind.address";

		private readonly AccessControlList adminsAcl;

		protected internal readonly Server webServer;

		private readonly IList<Connector> listeners = Lists.NewArrayList();

		protected internal readonly WebAppContext webAppContext;

		protected internal readonly bool findPort;

		protected internal readonly IDictionary<Context, bool> defaultContexts = new Dictionary
			<Context, bool>();

		protected internal readonly IList<string> filterNames = new AList<string>();

		internal const string StateDescriptionAlive = " - alive";

		internal const string StateDescriptionNotLive = " - not live";

		private readonly SignerSecretProvider secretProvider;

		/// <summary>Class to construct instances of HTTP server with specific options.</summary>
		public class Builder
		{
			private AList<URI> endpoints = Lists.NewArrayList();

			private string name;

			private Configuration conf;

			private string[] pathSpecs;

			private AccessControlList adminsAcl;

			private bool securityEnabled = false;

			private string usernameConfKey;

			private string keytabConfKey;

			private bool needsClientAuth;

			private string trustStore;

			private string trustStorePassword;

			private string trustStoreType;

			private string keyStore;

			private string keyStorePassword;

			private string keyStoreType;

			private string keyPassword;

			private bool findPort;

			private string hostName;

			private bool disallowFallbackToRandomSignerSecretProvider;

			private string authFilterConfigurationPrefix = "hadoop.http.authentication.";

			// The ServletContext attribute where the daemon Configuration
			// gets stored.
			// The -keypass option in keytool
			public virtual HttpServer2.Builder SetName(string name)
			{
				this.name = name;
				return this;
			}

			/// <summary>Add an endpoint that the HTTP server should listen to.</summary>
			/// <param name="endpoint">
			/// the endpoint of that the HTTP server should listen to. The
			/// scheme specifies the protocol (i.e. HTTP / HTTPS), the host
			/// specifies the binding address, and the port specifies the
			/// listening port. Unspecified or zero port means that the server
			/// can listen to any port.
			/// </param>
			public virtual HttpServer2.Builder AddEndpoint(URI endpoint)
			{
				endpoints.AddItem(endpoint);
				return this;
			}

			/// <summary>Set the hostname of the http server.</summary>
			/// <remarks>
			/// Set the hostname of the http server. The host name is used to resolve the
			/// _HOST field in Kerberos principals. The hostname of the first listener
			/// will be used if the name is unspecified.
			/// </remarks>
			public virtual HttpServer2.Builder HostName(string hostName)
			{
				this.hostName = hostName;
				return this;
			}

			public virtual HttpServer2.Builder TrustStore(string location, string password, string
				 type)
			{
				this.trustStore = location;
				this.trustStorePassword = password;
				this.trustStoreType = type;
				return this;
			}

			public virtual HttpServer2.Builder KeyStore(string location, string password, string
				 type)
			{
				this.keyStore = location;
				this.keyStorePassword = password;
				this.keyStoreType = type;
				return this;
			}

			public virtual HttpServer2.Builder KeyPassword(string password)
			{
				this.keyPassword = password;
				return this;
			}

			/// <summary>
			/// Specify whether the server should authorize the client in SSL
			/// connections.
			/// </summary>
			public virtual HttpServer2.Builder NeedsClientAuth(bool value)
			{
				this.needsClientAuth = value;
				return this;
			}

			public virtual HttpServer2.Builder SetFindPort(bool findPort)
			{
				this.findPort = findPort;
				return this;
			}

			public virtual HttpServer2.Builder SetConf(Configuration conf)
			{
				this.conf = conf;
				return this;
			}

			public virtual HttpServer2.Builder SetPathSpec(string[] pathSpec)
			{
				this.pathSpecs = pathSpec;
				return this;
			}

			public virtual HttpServer2.Builder SetACL(AccessControlList acl)
			{
				this.adminsAcl = acl;
				return this;
			}

			public virtual HttpServer2.Builder SetSecurityEnabled(bool securityEnabled)
			{
				this.securityEnabled = securityEnabled;
				return this;
			}

			public virtual HttpServer2.Builder SetUsernameConfKey(string usernameConfKey)
			{
				this.usernameConfKey = usernameConfKey;
				return this;
			}

			public virtual HttpServer2.Builder SetKeytabConfKey(string keytabConfKey)
			{
				this.keytabConfKey = keytabConfKey;
				return this;
			}

			public virtual HttpServer2.Builder DisallowFallbackToRandomSingerSecretProvider(bool
				 value)
			{
				this.disallowFallbackToRandomSignerSecretProvider = value;
				return this;
			}

			public virtual HttpServer2.Builder AuthFilterConfigurationPrefix(string value)
			{
				this.authFilterConfigurationPrefix = value;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual HttpServer2 Build()
			{
				Preconditions.CheckNotNull(name, "name is not set");
				Preconditions.CheckState(!endpoints.IsEmpty(), "No endpoints specified");
				if (hostName == null)
				{
					hostName = endpoints[0].GetHost();
				}
				if (this.conf == null)
				{
					conf = new Configuration();
				}
				HttpServer2 server = new HttpServer2(this);
				if (this.securityEnabled)
				{
					server.InitSpnego(conf, hostName, usernameConfKey, keytabConfKey);
				}
				foreach (URI ep in endpoints)
				{
					Connector listener;
					string scheme = ep.GetScheme();
					if ("http".Equals(scheme))
					{
						listener = HttpServer2.CreateDefaultChannelConnector();
					}
					else
					{
						if ("https".Equals(scheme))
						{
							SslSocketConnector c = new SslSocketConnectorSecure();
							c.SetHeaderBufferSize(1024 * 64);
							c.SetNeedClientAuth(needsClientAuth);
							c.SetKeyPassword(keyPassword);
							if (keyStore != null)
							{
								c.SetKeystore(keyStore);
								c.SetKeystoreType(keyStoreType);
								c.SetPassword(keyStorePassword);
							}
							if (trustStore != null)
							{
								c.SetTruststore(trustStore);
								c.SetTruststoreType(trustStoreType);
								c.SetTrustPassword(trustStorePassword);
							}
							listener = c;
						}
						else
						{
							throw new HadoopIllegalArgumentException("unknown scheme for endpoint:" + ep);
						}
					}
					listener.SetHost(ep.GetHost());
					listener.SetPort(ep.GetPort() == -1 ? 0 : ep.GetPort());
					server.AddListener(listener);
				}
				server.LoadListeners();
				return server;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private HttpServer2(HttpServer2.Builder b)
		{
			string appDir = GetWebAppsPath(b.name);
			this.webServer = new Server();
			this.adminsAcl = b.adminsAcl;
			this.webAppContext = CreateWebAppContext(b.name, b.conf, adminsAcl, appDir);
			try
			{
				this.secretProvider = ConstructSecretProvider(b, webAppContext.GetServletContext(
					));
				this.webAppContext.GetServletContext().SetAttribute(AuthenticationFilter.SignerSecretProviderAttribute
					, secretProvider);
			}
			catch (IOException e)
			{
				throw;
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
			this.findPort = b.findPort;
			InitializeWebServer(b.name, b.hostName, b.conf, b.pathSpecs);
		}

		/// <exception cref="System.IO.IOException"/>
		private void InitializeWebServer(string name, string hostName, Configuration conf
			, string[] pathSpecs)
		{
			Preconditions.CheckNotNull(webAppContext);
			int maxThreads = conf.GetInt(HttpMaxThreads, -1);
			// If HTTP_MAX_THREADS is not configured, QueueThreadPool() will use the
			// default value (currently 250).
			QueuedThreadPool threadPool = maxThreads == -1 ? new QueuedThreadPool() : new QueuedThreadPool
				(maxThreads);
			threadPool.SetDaemon(true);
			webServer.SetThreadPool(threadPool);
			SessionManager sm = webAppContext.GetSessionHandler().GetSessionManager();
			if (sm is AbstractSessionManager)
			{
				AbstractSessionManager asm = (AbstractSessionManager)sm;
				asm.SetHttpOnly(true);
				asm.SetSecureCookies(true);
			}
			ContextHandlerCollection contexts = new ContextHandlerCollection();
			RequestLog requestLog = HttpRequestLog.GetRequestLog(name);
			if (requestLog != null)
			{
				RequestLogHandler requestLogHandler = new RequestLogHandler();
				requestLogHandler.SetRequestLog(requestLog);
				HandlerCollection handlers = new HandlerCollection();
				handlers.SetHandlers(new Org.Mortbay.Jetty.Handler[] { contexts, requestLogHandler
					 });
				webServer.SetHandler(handlers);
			}
			else
			{
				webServer.SetHandler(contexts);
			}
			string appDir = GetWebAppsPath(name);
			webServer.AddHandler(webAppContext);
			AddDefaultApps(contexts, appDir, conf);
			AddGlobalFilter("safety", typeof(HttpServer2.QuotingInputFilter).FullName, null);
			FilterInitializer[] initializers = GetFilterInitializers(conf);
			if (initializers != null)
			{
				conf = new Configuration(conf);
				conf.Set(BindAddress, hostName);
				foreach (FilterInitializer c in initializers)
				{
					c.InitFilter(this, conf);
				}
			}
			AddDefaultServlets();
			if (pathSpecs != null)
			{
				foreach (string path in pathSpecs)
				{
					Log.Info("adding path spec: " + path);
					AddFilterPathMapping(path, webAppContext);
				}
			}
		}

		private void AddListener(Connector connector)
		{
			listeners.AddItem(connector);
		}

		private static WebAppContext CreateWebAppContext(string name, Configuration conf, 
			AccessControlList adminsAcl, string appDir)
		{
			WebAppContext ctx = new WebAppContext();
			ctx.SetDefaultsDescriptor(null);
			ServletHolder holder = new ServletHolder(new DefaultServlet());
			IDictionary<string, string> @params = ImmutableMap.Builder<string, string>().Put(
				"acceptRanges", "true").Put("dirAllowed", "false").Put("gzip", "true").Put("useFileMappedBuffer"
				, "true").Build();
			holder.SetInitParameters(@params);
			ctx.SetWelcomeFiles(new string[] { "index.html" });
			ctx.AddServlet(holder, "/");
			ctx.SetDisplayName(name);
			ctx.SetContextPath("/");
			ctx.SetWar(appDir + "/" + name);
			ctx.GetServletContext().SetAttribute(ConfContextAttribute, conf);
			ctx.GetServletContext().SetAttribute(AdminsAcl, adminsAcl);
			AddNoCacheFilter(ctx);
			return ctx;
		}

		/// <exception cref="System.Exception"/>
		private static SignerSecretProvider ConstructSecretProvider(HttpServer2.Builder b
			, ServletContext ctx)
		{
			Configuration conf = b.conf;
			Properties config = GetFilterProperties(conf, b.authFilterConfigurationPrefix);
			return AuthenticationFilter.ConstructSecretProvider(ctx, config, b.disallowFallbackToRandomSignerSecretProvider
				);
		}

		private static Properties GetFilterProperties(Configuration conf, string prefix)
		{
			Properties prop = new Properties();
			IDictionary<string, string> filterConfig = AuthenticationFilterInitializer.GetFilterConfigMap
				(conf, prefix);
			prop.PutAll(filterConfig);
			return prop;
		}

		private static void AddNoCacheFilter(WebAppContext ctxt)
		{
			DefineFilter(ctxt, NoCacheFilter, typeof(NoCacheFilter).FullName, Collections
				.EmptyMap<string, string>(), new string[] { "/*" });
		}

		private class SelectChannelConnectorWithSafeStartup : SelectChannelConnector
		{
			public SelectChannelConnectorWithSafeStartup()
				: base()
			{
			}

			/* Override the broken isRunning() method (JETTY-1316). This bug is present
			* in 6.1.26. For the versions wihout this bug, it adds insignificant
			* overhead.
			*/
			public override bool IsRunning()
			{
				if (base.IsRunning())
				{
					return true;
				}
				// We might be hitting JETTY-1316. If the internal state changed from
				// STARTING to STARTED in the middle of the check, the above call may
				// return false.  Check it one more time.
				Log.Warn("HttpServer Acceptor: isRunning is false. Rechecking.");
				try
				{
					Thread.Sleep(10);
				}
				catch (Exception)
				{
					// Mark this thread as interrupted. Someone up in the call chain
					// might care.
					Thread.CurrentThread().Interrupt();
				}
				bool runState = base.IsRunning();
				Log.Warn("HttpServer Acceptor: isRunning is " + runState);
				return runState;
			}
		}

		[InterfaceAudience.Private]
		public static Connector CreateDefaultChannelConnector()
		{
			SelectChannelConnector ret = new HttpServer2.SelectChannelConnectorWithSafeStartup
				();
			ret.SetLowResourceMaxIdleTime(10000);
			ret.SetAcceptQueueSize(128);
			ret.SetResolveNames(false);
			ret.SetUseDirectBuffers(false);
			if (Shell.Windows)
			{
				// result of setting the SO_REUSEADDR flag is different on Windows
				// http://msdn.microsoft.com/en-us/library/ms740621(v=vs.85).aspx
				// without this 2 NN's can start on the same machine and listen on
				// the same port with indeterminate routing of incoming requests to them
				ret.SetReuseAddress(false);
			}
			ret.SetHeaderBufferSize(1024 * 64);
			return ret;
		}

		/// <summary>Get an array of FilterConfiguration specified in the conf</summary>
		private static FilterInitializer[] GetFilterInitializers(Configuration conf)
		{
			if (conf == null)
			{
				return null;
			}
			Type[] classes = conf.GetClasses(FilterInitializerProperty);
			if (classes == null)
			{
				return null;
			}
			FilterInitializer[] initializers = new FilterInitializer[classes.Length];
			for (int i = 0; i < classes.Length; i++)
			{
				initializers[i] = (FilterInitializer)ReflectionUtils.NewInstance(classes[i], conf
					);
			}
			return initializers;
		}

		/// <summary>Add default apps.</summary>
		/// <param name="appDir">The application directory</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal void AddDefaultApps(ContextHandlerCollection parent, string appDir
			, Configuration conf)
		{
			// set up the context for "/logs/" if "hadoop.log.dir" property is defined.
			string logDir = Runtime.GetProperty("hadoop.log.dir");
			if (logDir != null)
			{
				Context logContext = new Context(parent, "/logs");
				logContext.SetResourceBase(logDir);
				logContext.AddServlet(typeof(AdminAuthorizedServlet), "/*");
				if (conf.GetBoolean(CommonConfigurationKeys.HadoopJettyLogsServeAliases, CommonConfigurationKeys
					.DefaultHadoopJettyLogsServeAliases))
				{
					IDictionary<string, string> @params = logContext.GetInitParams();
					@params["org.mortbay.jetty.servlet.Default.aliases"] = "true";
				}
				logContext.SetDisplayName("logs");
				SetContextAttributes(logContext, conf);
				AddNoCacheFilter(webAppContext);
				defaultContexts[logContext] = true;
			}
			// set up the context for "/static/*"
			Context staticContext = new Context(parent, "/static");
			staticContext.SetResourceBase(appDir + "/static");
			staticContext.AddServlet(typeof(DefaultServlet), "/*");
			staticContext.SetDisplayName("static");
			SetContextAttributes(staticContext, conf);
			defaultContexts[staticContext] = true;
		}

		private void SetContextAttributes(Context context, Configuration conf)
		{
			context.GetServletContext().SetAttribute(ConfContextAttribute, conf);
			context.GetServletContext().SetAttribute(AdminsAcl, adminsAcl);
		}

		/// <summary>Add default servlets.</summary>
		protected internal void AddDefaultServlets()
		{
			// set up default servlets
			AddServlet("stacks", "/stacks", typeof(HttpServer2.StackServlet));
			AddServlet("logLevel", "/logLevel", typeof(LogLevel.Servlet));
			AddServlet("metrics", "/metrics", typeof(MetricsServlet));
			AddServlet("jmx", "/jmx", typeof(JMXJsonServlet));
			AddServlet("conf", "/conf", typeof(ConfServlet));
		}

		public void AddContext(Context ctxt, bool isFiltered)
		{
			webServer.AddHandler(ctxt);
			AddNoCacheFilter(webAppContext);
			defaultContexts[ctxt] = isFiltered;
		}

		/// <summary>Set a value in the webapp context.</summary>
		/// <remarks>
		/// Set a value in the webapp context. These values are available to the jsp
		/// pages as "application.getAttribute(name)".
		/// </remarks>
		/// <param name="name">The name of the attribute</param>
		/// <param name="value">The value of the attribute</param>
		public void SetAttribute(string name, object value)
		{
			webAppContext.SetAttribute(name, value);
		}

		/// <summary>Add a Jersey resource package.</summary>
		/// <param name="packageName">The Java package name containing the Jersey resource.</param>
		/// <param name="pathSpec">The path spec for the servlet</param>
		public void AddJerseyResourcePackage(string packageName, string pathSpec)
		{
			Log.Info("addJerseyResourcePackage: packageName=" + packageName + ", pathSpec=" +
				 pathSpec);
			ServletHolder sh = new ServletHolder(typeof(ServletContainer));
			sh.SetInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig"
				);
			sh.SetInitParameter("com.sun.jersey.config.property.packages", packageName);
			webAppContext.AddServlet(sh, pathSpec);
		}

		/// <summary>Add a servlet in the server.</summary>
		/// <param name="name">The name of the servlet (can be passed as null)</param>
		/// <param name="pathSpec">The path spec for the servlet</param>
		/// <param name="clazz">The servlet class</param>
		public void AddServlet(string name, string pathSpec, Type clazz)
		{
			AddInternalServlet(name, pathSpec, clazz, false);
			AddFilterPathMapping(pathSpec, webAppContext);
		}

		/// <summary>Add an internal servlet in the server.</summary>
		/// <remarks>
		/// Add an internal servlet in the server.
		/// Note: This method is to be used for adding servlets that facilitate
		/// internal communication and not for user facing functionality. For
		/// servlets added using this method, filters are not enabled.
		/// </remarks>
		/// <param name="name">The name of the servlet (can be passed as null)</param>
		/// <param name="pathSpec">The path spec for the servlet</param>
		/// <param name="clazz">The servlet class</param>
		public void AddInternalServlet(string name, string pathSpec, Type clazz)
		{
			AddInternalServlet(name, pathSpec, clazz, false);
		}

		/// <summary>
		/// Add an internal servlet in the server, specifying whether or not to
		/// protect with Kerberos authentication.
		/// </summary>
		/// <remarks>
		/// Add an internal servlet in the server, specifying whether or not to
		/// protect with Kerberos authentication.
		/// Note: This method is to be used for adding servlets that facilitate
		/// internal communication and not for user facing functionality. For
		/// +   * servlets added using this method, filters (except internal Kerberos
		/// filters) are not enabled.
		/// </remarks>
		/// <param name="name">The name of the servlet (can be passed as null)</param>
		/// <param name="pathSpec">The path spec for the servlet</param>
		/// <param name="clazz">The servlet class</param>
		/// <param name="requireAuth">Require Kerberos authenticate to access servlet</param>
		public void AddInternalServlet(string name, string pathSpec, Type clazz, bool requireAuth
			)
		{
			ServletHolder holder = new ServletHolder(clazz);
			if (name != null)
			{
				holder.SetName(name);
			}
			webAppContext.AddServlet(holder, pathSpec);
			if (requireAuth && UserGroupInformation.IsSecurityEnabled())
			{
				Log.Info("Adding Kerberos (SPNEGO) filter to " + name);
				ServletHandler handler = webAppContext.GetServletHandler();
				FilterMapping fmap = new FilterMapping();
				fmap.SetPathSpec(pathSpec);
				fmap.SetFilterName(SpnegoFilter);
				fmap.SetDispatches(Org.Mortbay.Jetty.Handler.All);
				handler.AddFilterMapping(fmap);
			}
		}

		public void AddFilter(string name, string classname, IDictionary<string, string> 
			parameters)
		{
			FilterHolder filterHolder = GetFilterHolder(name, classname, parameters);
			string[] UserFacingUrls = new string[] { "*.html", "*.jsp" };
			FilterMapping fmap = GetFilterMapping(name, UserFacingUrls);
			DefineFilter(webAppContext, filterHolder, fmap);
			Log.Info("Added filter " + name + " (class=" + classname + ") to context " + webAppContext
				.GetDisplayName());
			string[] AllUrls = new string[] { "/*" };
			fmap = GetFilterMapping(name, AllUrls);
			foreach (KeyValuePair<Context, bool> e in defaultContexts)
			{
				if (e.Value)
				{
					Context ctx = e.Key;
					DefineFilter(ctx, filterHolder, fmap);
					Log.Info("Added filter " + name + " (class=" + classname + ") to context " + ctx.
						GetDisplayName());
				}
			}
			filterNames.AddItem(name);
		}

		public void AddGlobalFilter(string name, string classname, IDictionary<string, string
			> parameters)
		{
			string[] AllUrls = new string[] { "/*" };
			FilterHolder filterHolder = GetFilterHolder(name, classname, parameters);
			FilterMapping fmap = GetFilterMapping(name, AllUrls);
			DefineFilter(webAppContext, filterHolder, fmap);
			foreach (Context ctx in defaultContexts.Keys)
			{
				DefineFilter(ctx, filterHolder, fmap);
			}
			Log.Info("Added global filter '" + name + "' (class=" + classname + ")");
		}

		/// <summary>Define a filter for a context and set up default url mappings.</summary>
		public static void DefineFilter(Context ctx, string name, string classname, IDictionary
			<string, string> parameters, string[] urls)
		{
			FilterHolder filterHolder = GetFilterHolder(name, classname, parameters);
			FilterMapping fmap = GetFilterMapping(name, urls);
			DefineFilter(ctx, filterHolder, fmap);
		}

		/// <summary>Define a filter for a context and set up default url mappings.</summary>
		private static void DefineFilter(Context ctx, FilterHolder holder, FilterMapping 
			fmap)
		{
			ServletHandler handler = ctx.GetServletHandler();
			handler.AddFilter(holder, fmap);
		}

		private static FilterMapping GetFilterMapping(string name, string[] urls)
		{
			FilterMapping fmap = new FilterMapping();
			fmap.SetPathSpecs(urls);
			fmap.SetDispatches(Org.Mortbay.Jetty.Handler.All);
			fmap.SetFilterName(name);
			return fmap;
		}

		private static FilterHolder GetFilterHolder(string name, string classname, IDictionary
			<string, string> parameters)
		{
			FilterHolder holder = new FilterHolder();
			holder.SetName(name);
			holder.SetClassName(classname);
			holder.SetInitParameters(parameters);
			return holder;
		}

		/// <summary>Add the path spec to the filter path mapping.</summary>
		/// <param name="pathSpec">The path spec</param>
		/// <param name="webAppCtx">The WebApplicationContext to add to</param>
		protected internal void AddFilterPathMapping(string pathSpec, Context webAppCtx)
		{
			ServletHandler handler = webAppCtx.GetServletHandler();
			foreach (string name in filterNames)
			{
				FilterMapping fmap = new FilterMapping();
				fmap.SetPathSpec(pathSpec);
				fmap.SetFilterName(name);
				fmap.SetDispatches(Org.Mortbay.Jetty.Handler.All);
				handler.AddFilterMapping(fmap);
			}
		}

		/// <summary>Get the value in the webapp context.</summary>
		/// <param name="name">The name of the attribute</param>
		/// <returns>The value of the attribute</returns>
		public object GetAttribute(string name)
		{
			return webAppContext.GetAttribute(name);
		}

		public WebAppContext GetWebAppContext()
		{
			return this.webAppContext;
		}

		/// <summary>Get the pathname to the webapps files.</summary>
		/// <param name="appName">eg "secondary" or "datanode"</param>
		/// <returns>the pathname as a URL</returns>
		/// <exception cref="System.IO.FileNotFoundException">if 'webapps' directory cannot be found on CLASSPATH.
		/// 	</exception>
		protected internal string GetWebAppsPath(string appName)
		{
			Uri url = GetType().GetClassLoader().GetResource("webapps/" + appName);
			if (url == null)
			{
				throw new FileNotFoundException("webapps/" + appName + " not found in CLASSPATH");
			}
			string urlString = url.ToString();
			return Runtime.Substring(urlString, 0, urlString.LastIndexOf('/'));
		}

		/// <summary>Get the port that the server is on</summary>
		/// <returns>the port</returns>
		[Obsolete]
		public int GetPort()
		{
			return webServer.GetConnectors()[0].GetLocalPort();
		}

		/// <summary>Get the address that corresponds to a particular connector.</summary>
		/// <returns>
		/// the corresponding address for the connector, or null if there's no
		/// such connector or the connector is not bounded.
		/// </returns>
		public IPEndPoint GetConnectorAddress(int index)
		{
			Preconditions.CheckArgument(index >= 0);
			if (index > webServer.GetConnectors().Length)
			{
				return null;
			}
			Connector c = webServer.GetConnectors()[index];
			if (c.GetLocalPort() == -1)
			{
				// The connector is not bounded
				return null;
			}
			return new IPEndPoint(c.GetHost(), c.GetLocalPort());
		}

		/// <summary>Set the min, max number of worker threads (simultaneous connections).</summary>
		public void SetThreads(int min, int max)
		{
			QueuedThreadPool pool = (QueuedThreadPool)webServer.GetThreadPool();
			pool.SetMinThreads(min);
			pool.SetMaxThreads(max);
		}

		/// <exception cref="System.IO.IOException"/>
		private void InitSpnego(Configuration conf, string hostName, string usernameConfKey
			, string keytabConfKey)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			string principalInConf = conf.Get(usernameConfKey);
			if (principalInConf != null && !principalInConf.IsEmpty())
			{
				@params["kerberos.principal"] = SecurityUtil.GetServerPrincipal(principalInConf, 
					hostName);
			}
			string httpKeytab = conf.Get(keytabConfKey);
			if (httpKeytab != null && !httpKeytab.IsEmpty())
			{
				@params["kerberos.keytab"] = httpKeytab;
			}
			@params[AuthenticationFilter.AuthType] = "kerberos";
			DefineFilter(webAppContext, SpnegoFilter, typeof(AuthenticationFilter).FullName, 
				@params, null);
		}

		/// <summary>Start the server.</summary>
		/// <remarks>Start the server. Does not wait for the server to start.</remarks>
		/// <exception cref="System.IO.IOException"/>
		public void Start()
		{
			try
			{
				try
				{
					OpenListeners();
					webServer.Start();
				}
				catch (IOException ex)
				{
					Log.Info("HttpServer.start() threw a non Bind IOException", ex);
					throw;
				}
				catch (MultiException ex)
				{
					Log.Info("HttpServer.start() threw a MultiException", ex);
					throw;
				}
				// Make sure there is no handler failures.
				Org.Mortbay.Jetty.Handler[] handlers = webServer.GetHandlers();
				foreach (Org.Mortbay.Jetty.Handler handler in handlers)
				{
					if (handler.IsFailed())
					{
						throw new IOException("Problem in starting http server. Server handlers failed");
					}
				}
				// Make sure there are no errors initializing the context.
				Exception unavailableException = webAppContext.GetUnavailableException();
				if (unavailableException != null)
				{
					// Have to stop the webserver, or else its non-daemon threads
					// will hang forever.
					webServer.Stop();
					throw new IOException("Unable to initialize WebAppContext", unavailableException);
				}
			}
			catch (IOException e)
			{
				throw;
			}
			catch (Exception e)
			{
				throw (IOException)Extensions.InitCause(new ThreadInterruptedException("Interrupted while starting HTTP server"
					), e);
			}
			catch (Exception e)
			{
				throw new IOException("Problem starting http server", e);
			}
		}

		private void LoadListeners()
		{
			foreach (Connector c in listeners)
			{
				webServer.AddConnector(c);
			}
		}

		/// <summary>Open the main listener for the server</summary>
		/// <exception cref="System.Exception"/>
		internal void OpenListeners()
		{
			foreach (Connector listener in listeners)
			{
				if (listener.GetLocalPort() != -1)
				{
					// This listener is either started externally or has been bound
					continue;
				}
				int port = listener.GetPort();
				while (true)
				{
					// jetty has a bug where you can't reopen a listener that previously
					// failed to open w/o issuing a close first, even if the port is changed
					try
					{
						listener.Close();
						listener.Open();
						Log.Info("Jetty bound to port " + listener.GetLocalPort());
						break;
					}
					catch (BindException ex)
					{
						if (port == 0 || !findPort)
						{
							BindException be = new BindException("Port in use: " + listener.GetHost() + ":" +
								 listener.GetPort());
							Extensions.InitCause(be, ex);
							throw be;
						}
					}
					// try the next port number
					listener.SetPort(++port);
					Thread.Sleep(100);
				}
			}
		}

		/// <summary>stop the server</summary>
		/// <exception cref="System.Exception"/>
		public void Stop()
		{
			MultiException exception = null;
			foreach (Connector c in listeners)
			{
				try
				{
					c.Close();
				}
				catch (Exception e)
				{
					Log.Error("Error while stopping listener for webapp" + webAppContext.GetDisplayName
						(), e);
					exception = AddMultiException(exception, e);
				}
			}
			try
			{
				// explicitly destroy the secrete provider
				secretProvider.Destroy();
				// clear & stop webAppContext attributes to avoid memory leaks.
				webAppContext.ClearAttributes();
				webAppContext.Stop();
			}
			catch (Exception e)
			{
				Log.Error("Error while stopping web app context for webapp " + webAppContext.GetDisplayName
					(), e);
				exception = AddMultiException(exception, e);
			}
			try
			{
				webServer.Stop();
			}
			catch (Exception e)
			{
				Log.Error("Error while stopping web server for webapp " + webAppContext.GetDisplayName
					(), e);
				exception = AddMultiException(exception, e);
			}
			if (exception != null)
			{
				exception.IfExceptionThrow();
			}
		}

		private MultiException AddMultiException(MultiException exception, Exception e)
		{
			if (exception == null)
			{
				exception = new MultiException();
			}
			exception.Add(e);
			return exception;
		}

		/// <exception cref="System.Exception"/>
		public void Join()
		{
			webServer.Join();
		}

		/// <summary>Test for the availability of the web server</summary>
		/// <returns>true if the web server is started, false otherwise</returns>
		public bool IsAlive()
		{
			return webServer != null && webServer.IsStarted();
		}

		public override string ToString()
		{
			Preconditions.CheckState(!listeners.IsEmpty());
			StringBuilder sb = new StringBuilder("HttpServer (").Append(IsAlive() ? StateDescriptionAlive
				 : StateDescriptionNotLive).Append("), listening at:");
			foreach (Connector l in listeners)
			{
				sb.Append(l.GetHost()).Append(":").Append(l.GetPort()).Append("/,");
			}
			return sb.ToString();
		}

		/// <summary>Checks the user has privileges to access to instrumentation servlets.</summary>
		/// <remarks>
		/// Checks the user has privileges to access to instrumentation servlets.
		/// <p/>
		/// If <code>hadoop.security.instrumentation.requires.admin</code> is set to FALSE
		/// (default value) it always returns TRUE.
		/// <p/>
		/// If <code>hadoop.security.instrumentation.requires.admin</code> is set to TRUE
		/// it will check that if the current user is in the admin ACLS. If the user is
		/// in the admin ACLs it returns TRUE, otherwise it returns FALSE.
		/// </remarks>
		/// <param name="servletContext">the servlet context.</param>
		/// <param name="request">the servlet request.</param>
		/// <param name="response">the servlet response.</param>
		/// <returns>TRUE/FALSE based on the logic decribed above.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static bool IsInstrumentationAccessAllowed(ServletContext servletContext, 
			HttpServletRequest request, HttpServletResponse response)
		{
			Configuration conf = (Configuration)servletContext.GetAttribute(ConfContextAttribute
				);
			bool access = true;
			bool adminAccess = conf.GetBoolean(CommonConfigurationKeys.HadoopSecurityInstrumentationRequiresAdmin
				, false);
			if (adminAccess)
			{
				access = HasAdministratorAccess(servletContext, request, response);
			}
			return access;
		}

		/// <summary>
		/// Does the user sending the HttpServletRequest has the administrator ACLs? If
		/// it isn't the case, response will be modified to send an error to the user.
		/// </summary>
		/// <param name="response">used to send the error response if user does not have admin access.
		/// 	</param>
		/// <returns>true if admin-authorized, false otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		public static bool HasAdministratorAccess(ServletContext servletContext, HttpServletRequest
			 request, HttpServletResponse response)
		{
			Configuration conf = (Configuration)servletContext.GetAttribute(ConfContextAttribute
				);
			// If there is no authorization, anybody has administrator access.
			if (!conf.GetBoolean(CommonConfigurationKeys.HadoopSecurityAuthorization, false))
			{
				return true;
			}
			string remoteUser = request.GetRemoteUser();
			if (remoteUser == null)
			{
				response.SendError(HttpServletResponse.ScForbidden, "Unauthenticated users are not "
					 + "authorized to access this page.");
				return false;
			}
			if (servletContext.GetAttribute(AdminsAcl) != null && !UserHasAdministratorAccess
				(servletContext, remoteUser))
			{
				response.SendError(HttpServletResponse.ScForbidden, "User " + remoteUser + " is unauthorized to access this page."
					);
				return false;
			}
			return true;
		}

		/// <summary>
		/// Get the admin ACLs from the given ServletContext and check if the given
		/// user is in the ACL.
		/// </summary>
		/// <param name="servletContext">the context containing the admin ACL.</param>
		/// <param name="remoteUser">the remote user to check for.</param>
		/// <returns>
		/// true if the user is present in the ACL, false if no ACL is set or
		/// the user is not present
		/// </returns>
		public static bool UserHasAdministratorAccess(ServletContext servletContext, string
			 remoteUser)
		{
			AccessControlList adminsAcl = (AccessControlList)servletContext.GetAttribute(AdminsAcl
				);
			UserGroupInformation remoteUserUGI = UserGroupInformation.CreateRemoteUser(remoteUser
				);
			return adminsAcl != null && adminsAcl.IsUserAllowed(remoteUserUGI);
		}

		/// <summary>
		/// A very simple servlet to serve up a text representation of the current
		/// stack traces.
		/// </summary>
		/// <remarks>
		/// A very simple servlet to serve up a text representation of the current
		/// stack traces. It both returns the stacks to the caller and logs them.
		/// Currently the stack traces are done sequentially rather than exactly the
		/// same data.
		/// </remarks>
		[System.Serializable]
		public class StackServlet : HttpServlet
		{
			private const long serialVersionUID = -6284183679759467039L;

			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest request, HttpServletResponse response
				)
			{
				if (!HttpServer2.IsInstrumentationAccessAllowed(GetServletContext(), request, response
					))
				{
					return;
				}
				response.SetContentType("text/plain; charset=UTF-8");
				using (TextWriter @out = new TextWriter(response.GetOutputStream(), false, "UTF-8"
					))
				{
					ReflectionUtils.PrintThreadInfo(@out, string.Empty);
				}
				ReflectionUtils.LogThreadInfo(Log, "jsp requested", 1);
			}
		}

		/// <summary>
		/// A Servlet input filter that quotes all HTML active characters in the
		/// parameter names and values.
		/// </summary>
		/// <remarks>
		/// A Servlet input filter that quotes all HTML active characters in the
		/// parameter names and values. The goal is to quote the characters to make
		/// all of the servlets resistant to cross-site scripting attacks.
		/// </remarks>
		public class QuotingInputFilter : Filter
		{
			private FilterConfig config;

			public class RequestQuoter : HttpServletRequestWrapper
			{
				private readonly HttpServletRequest rawRequest;

				public RequestQuoter(HttpServletRequest rawRequest)
					: base(rawRequest)
				{
					this.rawRequest = rawRequest;
				}

				/// <summary>Return the set of parameter names, quoting each name.</summary>
				public override IEnumeration GetParameterNames()
				{
					return new _Enumeration_1139(this);
				}

				private sealed class _Enumeration_1139 : Enumeration<string>
				{
					public _Enumeration_1139()
					{
						this.rawIterator = this._enclosing.rawRequest.GetParameterNames();
					}

					private Enumeration<string> rawIterator;

					public bool MoveNext()
					{
						return this.rawIterator.MoveNext();
					}

					public string Current
					{
						get
						{
							return HtmlQuoting.QuoteHtmlChars(this.rawIterator.Current);
						}
					}
				}

				/// <summary>Unquote the name and quote the value.</summary>
				public override string GetParameter(string name)
				{
					return HtmlQuoting.QuoteHtmlChars(rawRequest.GetParameter(HtmlQuoting.UnquoteHtmlChars
						(name)));
				}

				public override string[] GetParameterValues(string name)
				{
					string unquoteName = HtmlQuoting.UnquoteHtmlChars(name);
					string[] unquoteValue = rawRequest.GetParameterValues(unquoteName);
					if (unquoteValue == null)
					{
						return null;
					}
					string[] result = new string[unquoteValue.Length];
					for (int i = 0; i < result.Length; ++i)
					{
						result[i] = HtmlQuoting.QuoteHtmlChars(unquoteValue[i]);
					}
					return result;
				}

				public override IDictionary GetParameterMap()
				{
					IDictionary<string, string[]> result = new Dictionary<string, string[]>();
					IDictionary<string, string[]> raw = rawRequest.GetParameterMap();
					foreach (KeyValuePair<string, string[]> item in raw)
					{
						string[] rawValue = item.Value;
						string[] cookedValue = new string[rawValue.Length];
						for (int i = 0; i < rawValue.Length; ++i)
						{
							cookedValue[i] = HtmlQuoting.QuoteHtmlChars(rawValue[i]);
						}
						result[HtmlQuoting.QuoteHtmlChars(item.Key)] = cookedValue;
					}
					return result;
				}

				/// <summary>
				/// Quote the url so that users specifying the HOST HTTP header
				/// can't inject attacks.
				/// </summary>
				public override StringBuilder GetRequestURL()
				{
					string url = rawRequest.GetRequestURL().ToString();
					return new StringBuilder(HtmlQuoting.QuoteHtmlChars(url));
				}

				/// <summary>
				/// Quote the server name so that users specifying the HOST HTTP header
				/// can't inject attacks.
				/// </summary>
				public override string GetServerName()
				{
					return HtmlQuoting.QuoteHtmlChars(rawRequest.GetServerName());
				}
			}

			/// <exception cref="Javax.Servlet.ServletException"/>
			public virtual void Init(FilterConfig config)
			{
				this.config = config;
			}

			public virtual void Destroy()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Servlet.ServletException"/>
			public virtual void DoFilter(ServletRequest request, ServletResponse response, FilterChain
				 chain)
			{
				HttpServletRequestWrapper quoted = new HttpServer2.QuotingInputFilter.RequestQuoter
					((HttpServletRequest)request);
				HttpServletResponse httpResponse = (HttpServletResponse)response;
				string mime = InferMimeType(request);
				if (mime == null)
				{
					httpResponse.SetContentType("text/plain; charset=utf-8");
				}
				else
				{
					if (mime.StartsWith("text/html"))
					{
						// HTML with unspecified encoding, we want to
						// force HTML with utf-8 encoding
						// This is to avoid the following security issue:
						// http://openmya.hacker.jp/hasegawa/security/utf7cs.html
						httpResponse.SetContentType("text/html; charset=utf-8");
					}
					else
					{
						if (mime.StartsWith("application/xml"))
						{
							httpResponse.SetContentType("text/xml; charset=utf-8");
						}
					}
				}
				chain.DoFilter(quoted, httpResponse);
			}

			/// <summary>
			/// Infer the mime type for the response based on the extension of the request
			/// URI.
			/// </summary>
			/// <remarks>
			/// Infer the mime type for the response based on the extension of the request
			/// URI. Returns null if unknown.
			/// </remarks>
			private string InferMimeType(ServletRequest request)
			{
				string path = ((HttpServletRequest)request).GetRequestURI();
				ContextHandler.SContext sContext = (ContextHandler.SContext)config.GetServletContext
					();
				MimeTypes mimes = sContext.GetContextHandler().GetMimeTypes();
				Buffer mimeBuffer = mimes.GetMimeByExtension(path);
				return (mimeBuffer == null) ? null : mimeBuffer.ToString();
			}
		}
	}
}
