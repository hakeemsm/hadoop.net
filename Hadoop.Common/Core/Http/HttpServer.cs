using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Sun.Jersey.Spi.Container.Servlet;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Jmx;
using Org.Apache.Hadoop.Log;
using Org.Apache.Hadoop.Metrics;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Server;
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
using Sharpen;

namespace Org.Apache.Hadoop.Http
{
	/// <summary>Create a Jetty embedded server to answer http requests.</summary>
	/// <remarks>
	/// Create a Jetty embedded server to answer http requests. The primary goal
	/// is to serve up status information for the server.
	/// There are three contexts:
	/// "/logs/" -&gt; points to the log directory
	/// "/static/" -&gt; points to common static files (src/webapps/static)
	/// "/" -&gt; the jsp server code from (src/webapps/<name>)
	/// This class comes from the HttpServer in branch-2.2. HDFS and YARN have
	/// moved to use HttpServer2. This class exists for compatibility reasons, it
	/// stays in hadoop-common to allow HBase working with both Hadoop 2.2
	/// and Hadoop 2.3. See HBASE-10336 for more details.
	/// </remarks>
	public class HttpServer : FilterContainer
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Http.HttpServer
			));

		internal const string FilterInitializerProperty = "hadoop.http.filter.initializers";

		internal const string HttpMaxThreads = "hadoop.http.max.threads";

		public const string ConfContextAttribute = "hadoop.conf";

		public const string AdminsAcl = "admins.acl";

		public const string SpnegoFilter = "SpnegoFilter";

		public const string NoCacheFilter = "NoCacheFilter";

		public const string BindAddress = "bind.address";

		private AccessControlList adminsAcl;

		private SSLFactory sslFactory;

		protected internal readonly Server webServer;

		protected internal readonly Connector listener;

		protected internal readonly WebAppContext webAppContext;

		protected internal readonly bool findPort;

		protected internal readonly IDictionary<Context, bool> defaultContexts = new Dictionary
			<Context, bool>();

		protected internal readonly IList<string> filterNames = new AList<string>();

		private const int MaxRetries = 10;

		internal const string StateDescriptionAlive = " - alive";

		internal const string StateDescriptionNotLive = " - not live";

		private readonly bool listenerStartedExternally;

		/// <summary>Same as this(name, bindAddress, port, findPort, null);</summary>
		/// <exception cref="System.IO.IOException"/>
		public HttpServer(string name, string bindAddress, int port, bool findPort)
			: this(name, bindAddress, port, findPort, new Configuration())
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public HttpServer(string name, string bindAddress, int port, bool findPort, Configuration
			 conf, Connector connector)
			: this(name, bindAddress, port, findPort, conf, null, connector, null)
		{
		}

		/// <summary>Create a status server on the given port.</summary>
		/// <remarks>
		/// Create a status server on the given port. Allows you to specify the
		/// path specifications that this server will be serving so that they will be
		/// added to the filters properly.
		/// </remarks>
		/// <param name="name">The name of the server</param>
		/// <param name="bindAddress">The address for this server</param>
		/// <param name="port">The port to use on the server</param>
		/// <param name="findPort">
		/// whether the server should start at the given port and
		/// increment by 1 until it finds a free port.
		/// </param>
		/// <param name="conf">Configuration</param>
		/// <param name="pathSpecs">
		/// Path specifications that this httpserver will be serving.
		/// These will be added to any filters.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public HttpServer(string name, string bindAddress, int port, bool findPort, Configuration
			 conf, string[] pathSpecs)
			: this(name, bindAddress, port, findPort, conf, null, null, pathSpecs)
		{
		}

		/// <summary>Create a status server on the given port.</summary>
		/// <remarks>
		/// Create a status server on the given port.
		/// The jsp scripts are taken from src/webapps/<name>.
		/// </remarks>
		/// <param name="name">The name of the server</param>
		/// <param name="port">The port to use on the server</param>
		/// <param name="findPort">
		/// whether the server should start at the given port and
		/// increment by 1 until it finds a free port.
		/// </param>
		/// <param name="conf">Configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public HttpServer(string name, string bindAddress, int port, bool findPort, Configuration
			 conf)
			: this(name, bindAddress, port, findPort, conf, null, null, null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public HttpServer(string name, string bindAddress, int port, bool findPort, Configuration
			 conf, AccessControlList adminsAcl)
			: this(name, bindAddress, port, findPort, conf, adminsAcl, null, null)
		{
		}

		/// <summary>Create a status server on the given port.</summary>
		/// <remarks>
		/// Create a status server on the given port.
		/// The jsp scripts are taken from src/webapps/<name>.
		/// </remarks>
		/// <param name="name">The name of the server</param>
		/// <param name="bindAddress">The address for this server</param>
		/// <param name="port">The port to use on the server</param>
		/// <param name="findPort">
		/// whether the server should start at the given port and
		/// increment by 1 until it finds a free port.
		/// </param>
		/// <param name="conf">Configuration</param>
		/// <param name="adminsAcl">
		/// 
		/// <see cref="Org.Apache.Hadoop.Security.Authorize.AccessControlList"/>
		/// of the admins
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public HttpServer(string name, string bindAddress, int port, bool findPort, Configuration
			 conf, AccessControlList adminsAcl, Connector connector)
			: this(name, bindAddress, port, findPort, conf, adminsAcl, connector, null)
		{
		}

		/// <summary>Create a status server on the given port.</summary>
		/// <remarks>
		/// Create a status server on the given port.
		/// The jsp scripts are taken from src/webapps/<name>.
		/// </remarks>
		/// <param name="name">The name of the server</param>
		/// <param name="bindAddress">The address for this server</param>
		/// <param name="port">The port to use on the server</param>
		/// <param name="findPort">
		/// whether the server should start at the given port and
		/// increment by 1 until it finds a free port.
		/// </param>
		/// <param name="conf">Configuration</param>
		/// <param name="adminsAcl">
		/// 
		/// <see cref="Org.Apache.Hadoop.Security.Authorize.AccessControlList"/>
		/// of the admins
		/// </param>
		/// <param name="connector">A jetty connection listener</param>
		/// <param name="pathSpecs">
		/// Path specifications that this httpserver will be serving.
		/// These will be added to any filters.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public HttpServer(string name, string bindAddress, int port, bool findPort, Configuration
			 conf, AccessControlList adminsAcl, Connector connector, string[] pathSpecs)
		{
			// The ServletContext attribute where the daemon Configuration
			// gets stored.
			webServer = new Server();
			this.findPort = findPort;
			this.adminsAcl = adminsAcl;
			if (connector == null)
			{
				listenerStartedExternally = false;
				if (conf.GetBoolean(CommonConfigurationKeysPublic.HadoopSslEnabledKey, CommonConfigurationKeysPublic
					.HadoopSslEnabledDefault))
				{
					sslFactory = new SSLFactory(SSLFactory.Mode.Server, conf);
					try
					{
						sslFactory.Init();
					}
					catch (GeneralSecurityException ex)
					{
						throw new IOException(ex);
					}
					SslSocketConnector sslListener = new _SslSocketConnector_232(this);
					listener = sslListener;
				}
				else
				{
					listener = CreateBaseListener(conf);
				}
				listener.SetHost(bindAddress);
				listener.SetPort(port);
			}
			else
			{
				listenerStartedExternally = true;
				listener = connector;
			}
			webServer.AddConnector(listener);
			int maxThreads = conf.GetInt(HttpMaxThreads, -1);
			// If HTTP_MAX_THREADS is not configured, QueueThreadPool() will use the
			// default value (currently 250).
			QueuedThreadPool threadPool = maxThreads == -1 ? new QueuedThreadPool() : new QueuedThreadPool
				(maxThreads);
			threadPool.SetDaemon(true);
			webServer.SetThreadPool(threadPool);
			string appDir = GetWebAppsPath(name);
			ContextHandlerCollection contexts = new ContextHandlerCollection();
			webServer.SetHandler(contexts);
			webAppContext = new WebAppContext();
			webAppContext.SetDisplayName(name);
			webAppContext.SetContextPath("/");
			webAppContext.SetWar(appDir + "/" + name);
			webAppContext.GetServletContext().SetAttribute(ConfContextAttribute, conf);
			webAppContext.GetServletContext().SetAttribute(AdminsAcl, adminsAcl);
			AddNoCacheFilter(webAppContext);
			webServer.AddHandler(webAppContext);
			AddDefaultApps(contexts, appDir, conf);
			AddGlobalFilter("safety", typeof(HttpServer.QuotingInputFilter).FullName, null);
			FilterInitializer[] initializers = GetFilterInitializers(conf);
			if (initializers != null)
			{
				conf = new Configuration(conf);
				conf.Set(BindAddress, bindAddress);
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

		private sealed class _SslSocketConnector_232 : SslSocketConnector
		{
			public _SslSocketConnector_232(HttpServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			protected override SSLServerSocketFactory CreateFactory()
			{
				return this._enclosing.sslFactory.CreateSSLServerSocketFactory();
			}

			private readonly HttpServer _enclosing;
		}

		private void AddNoCacheFilter(WebAppContext ctxt)
		{
			DefineFilter(ctxt, NoCacheFilter, typeof(NoCacheFilter).FullName, Sharpen.Collections
				.EmptyMap, new string[] { "/*" });
		}

		/// <summary>
		/// Create a required listener for the Jetty instance listening on the port
		/// provided.
		/// </summary>
		/// <remarks>
		/// Create a required listener for the Jetty instance listening on the port
		/// provided. This wrapper and all subclasses must create at least one
		/// listener.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual Connector CreateBaseListener(Configuration conf)
		{
			return Org.Apache.Hadoop.Http.HttpServer.CreateDefaultChannelConnector();
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
					Sharpen.Thread.Sleep(10);
				}
				catch (Exception)
				{
					// Mark this thread as interrupted. Someone up in the call chain
					// might care.
					Sharpen.Thread.CurrentThread().Interrupt();
				}
				bool runState = base.IsRunning();
				Log.Warn("HttpServer Acceptor: isRunning is " + runState);
				return runState;
			}
		}

		[InterfaceAudience.Private]
		public static Connector CreateDefaultChannelConnector()
		{
			SelectChannelConnector ret = new HttpServer.SelectChannelConnectorWithSafeStartup
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
		protected internal virtual void AddDefaultApps(ContextHandlerCollection parent, string
			 appDir, Configuration conf)
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
					logContext.GetInitParams()["org.mortbay.jetty.servlet.Default.aliases"] = "true";
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
		protected internal virtual void AddDefaultServlets()
		{
			// set up default servlets
			AddServlet("stacks", "/stacks", typeof(HttpServer.StackServlet));
			AddServlet("logLevel", "/logLevel", typeof(LogLevel.Servlet));
			AddServlet("metrics", "/metrics", typeof(MetricsServlet));
			AddServlet("jmx", "/jmx", typeof(JMXJsonServlet));
			AddServlet("conf", "/conf", typeof(ConfServlet));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void AddContext(Context ctxt, bool isFiltered)
		{
			webServer.AddHandler(ctxt);
			AddNoCacheFilter(webAppContext);
			defaultContexts[ctxt] = isFiltered;
		}

		/// <summary>Add a context</summary>
		/// <param name="pathSpec">The path spec for the context</param>
		/// <param name="dir">The directory containing the context</param>
		/// <param name="isFiltered">if true, the servlet is added to the filter path mapping
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void AddContext(string pathSpec, string dir, bool isFiltered
			)
		{
			if (0 == webServer.GetHandlers().Length)
			{
				throw new RuntimeException("Couldn't find handler");
			}
			WebAppContext webAppCtx = new WebAppContext();
			webAppCtx.SetContextPath(pathSpec);
			webAppCtx.SetWar(dir);
			AddContext(webAppCtx, true);
		}

		/// <summary>Set a value in the webapp context.</summary>
		/// <remarks>
		/// Set a value in the webapp context. These values are available to the jsp
		/// pages as "application.getAttribute(name)".
		/// </remarks>
		/// <param name="name">The name of the attribute</param>
		/// <param name="value">The value of the attribute</param>
		public virtual void SetAttribute(string name, object value)
		{
			webAppContext.SetAttribute(name, value);
		}

		/// <summary>Add a Jersey resource package.</summary>
		/// <param name="packageName">The Java package name containing the Jersey resource.</param>
		/// <param name="pathSpec">The path spec for the servlet</param>
		public virtual void AddJerseyResourcePackage(string packageName, string pathSpec)
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
		public virtual void AddServlet(string name, string pathSpec, Type clazz)
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
		public virtual void AddInternalServlet(string name, string pathSpec, Type clazz)
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
		public virtual void AddInternalServlet(string name, string pathSpec, Type clazz, 
			bool requireAuth)
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

		public virtual void AddFilter(string name, string classname, IDictionary<string, 
			string> parameters)
		{
			string[] UserFacingUrls = new string[] { "*.html", "*.jsp" };
			DefineFilter(webAppContext, name, classname, parameters, UserFacingUrls);
			Log.Info("Added filter " + name + " (class=" + classname + ") to context " + webAppContext
				.GetDisplayName());
			string[] AllUrls = new string[] { "/*" };
			foreach (KeyValuePair<Context, bool> e in defaultContexts)
			{
				if (e.Value)
				{
					Context ctx = e.Key;
					DefineFilter(ctx, name, classname, parameters, AllUrls);
					Log.Info("Added filter " + name + " (class=" + classname + ") to context " + ctx.
						GetDisplayName());
				}
			}
			filterNames.AddItem(name);
		}

		public virtual void AddGlobalFilter(string name, string classname, IDictionary<string
			, string> parameters)
		{
			string[] AllUrls = new string[] { "/*" };
			DefineFilter(webAppContext, name, classname, parameters, AllUrls);
			foreach (Context ctx in defaultContexts.Keys)
			{
				DefineFilter(ctx, name, classname, parameters, AllUrls);
			}
			Log.Info("Added global filter '" + name + "' (class=" + classname + ")");
		}

		/// <summary>Define a filter for a context and set up default url mappings.</summary>
		public virtual void DefineFilter(Context ctx, string name, string classname, IDictionary
			<string, string> parameters, string[] urls)
		{
			FilterHolder holder = new FilterHolder();
			holder.SetName(name);
			holder.SetClassName(classname);
			holder.SetInitParameters(parameters);
			FilterMapping fmap = new FilterMapping();
			fmap.SetPathSpecs(urls);
			fmap.SetDispatches(Org.Mortbay.Jetty.Handler.All);
			fmap.SetFilterName(name);
			ServletHandler handler = ctx.GetServletHandler();
			handler.AddFilter(holder, fmap);
		}

		/// <summary>Add the path spec to the filter path mapping.</summary>
		/// <param name="pathSpec">The path spec</param>
		/// <param name="webAppCtx">The WebApplicationContext to add to</param>
		protected internal virtual void AddFilterPathMapping(string pathSpec, Context webAppCtx
			)
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
		public virtual object GetAttribute(string name)
		{
			return webAppContext.GetAttribute(name);
		}

		public virtual WebAppContext GetWebAppContext()
		{
			return this.webAppContext;
		}

		/// <summary>Get the pathname to the webapps files.</summary>
		/// <param name="appName">eg "secondary" or "datanode"</param>
		/// <returns>the pathname as a URL</returns>
		/// <exception cref="System.IO.FileNotFoundException">if 'webapps' directory cannot be found on CLASSPATH.
		/// 	</exception>
		protected internal virtual string GetWebAppsPath(string appName)
		{
			Uri url = GetType().GetClassLoader().GetResource("webapps/" + appName);
			if (url == null)
			{
				throw new FileNotFoundException("webapps/" + appName + " not found in CLASSPATH");
			}
			string urlString = url.ToString();
			return Sharpen.Runtime.Substring(urlString, 0, urlString.LastIndexOf('/'));
		}

		/// <summary>Get the port that the server is on</summary>
		/// <returns>the port</returns>
		public virtual int GetPort()
		{
			return webServer.GetConnectors()[0].GetLocalPort();
		}

		/// <summary>Set the min, max number of worker threads (simultaneous connections).</summary>
		public virtual void SetThreads(int min, int max)
		{
			QueuedThreadPool pool = (QueuedThreadPool)webServer.GetThreadPool();
			pool.SetMinThreads(min);
			pool.SetMaxThreads(max);
		}

		/// <summary>Configure an ssl listener on the server.</summary>
		/// <param name="addr">address to listen on</param>
		/// <param name="keystore">location of the keystore</param>
		/// <param name="storPass">password for the keystore</param>
		/// <param name="keyPass">password for the key</param>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use AddSslListener(System.Net.IPEndPoint, Org.Apache.Hadoop.Conf.Configuration, bool)"
			)]
		public virtual void AddSslListener(IPEndPoint addr, string keystore, string storPass
			, string keyPass)
		{
			if (webServer.IsStarted())
			{
				throw new IOException("Failed to add ssl listener");
			}
			SslSocketConnector sslListener = new SslSocketConnector();
			sslListener.SetHost(addr.GetHostName());
			sslListener.SetPort(addr.Port);
			sslListener.SetKeystore(keystore);
			sslListener.SetPassword(storPass);
			sslListener.SetKeyPassword(keyPass);
			webServer.AddConnector(sslListener);
		}

		/// <summary>Configure an ssl listener on the server.</summary>
		/// <param name="addr">address to listen on</param>
		/// <param name="sslConf">conf to retrieve ssl options</param>
		/// <param name="needCertsAuth">whether x509 certificate authentication is required</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AddSslListener(IPEndPoint addr, Configuration sslConf, bool needCertsAuth
			)
		{
			if (webServer.IsStarted())
			{
				throw new IOException("Failed to add ssl listener");
			}
			if (needCertsAuth)
			{
				// setting up SSL truststore for authenticating clients
				Runtime.SetProperty("javax.net.ssl.trustStore", sslConf.Get("ssl.server.truststore.location"
					, string.Empty));
				Runtime.SetProperty("javax.net.ssl.trustStorePassword", sslConf.Get("ssl.server.truststore.password"
					, string.Empty));
				Runtime.SetProperty("javax.net.ssl.trustStoreType", sslConf.Get("ssl.server.truststore.type"
					, "jks"));
			}
			SslSocketConnector sslListener = new SslSocketConnector();
			sslListener.SetHost(addr.GetHostName());
			sslListener.SetPort(addr.Port);
			sslListener.SetKeystore(sslConf.Get("ssl.server.keystore.location"));
			sslListener.SetPassword(sslConf.Get("ssl.server.keystore.password", string.Empty)
				);
			sslListener.SetKeyPassword(sslConf.Get("ssl.server.keystore.keypassword", string.Empty
				));
			sslListener.SetKeystoreType(sslConf.Get("ssl.server.keystore.type", "jks"));
			sslListener.SetNeedClientAuth(needCertsAuth);
			webServer.AddConnector(sslListener);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void InitSpnego(Configuration conf, string usernameConfKey
			, string keytabConfKey)
		{
			IDictionary<string, string> @params = new Dictionary<string, string>();
			string principalInConf = conf.Get(usernameConfKey);
			if (principalInConf != null && !principalInConf.IsEmpty())
			{
				@params["kerberos.principal"] = SecurityUtil.GetServerPrincipal(principalInConf, 
					listener.GetHost());
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
		public virtual void Start()
		{
			try
			{
				try
				{
					OpenListener();
					Log.Info("Jetty bound to port " + listener.GetLocalPort());
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
				for (int i = 0; i < handlers.Length; i++)
				{
					if (handlers[i].IsFailed())
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
				throw new IOException("Problem starting http server", e);
			}
		}

		/// <summary>Open the main listener for the server</summary>
		/// <exception cref="System.Exception"/>
		internal virtual void OpenListener()
		{
			if (listener.GetLocalPort() != -1)
			{
				// it's already bound
				return;
			}
			if (listenerStartedExternally)
			{
				// Expect that listener was started securely
				throw new Exception("Expected webserver's listener to be started " + "previously but wasn't"
					);
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
					break;
				}
				catch (BindException ex)
				{
					if (port == 0 || !findPort)
					{
						BindException be = new BindException("Port in use: " + listener.GetHost() + ":" +
							 listener.GetPort());
						Sharpen.Extensions.InitCause(be, ex);
						throw be;
					}
				}
				// try the next port number
				listener.SetPort(++port);
				Sharpen.Thread.Sleep(100);
			}
		}

		/// <summary>Return the bind address of the listener.</summary>
		/// <returns>InetSocketAddress of the listener</returns>
		public virtual IPEndPoint GetListenerAddress()
		{
			int port = listener.GetLocalPort();
			if (port == -1)
			{
				// not bound, return requested port
				port = listener.GetPort();
			}
			return new IPEndPoint(listener.GetHost(), port);
		}

		/// <summary>stop the server</summary>
		/// <exception cref="System.Exception"/>
		public virtual void Stop()
		{
			MultiException exception = null;
			try
			{
				listener.Close();
			}
			catch (Exception e)
			{
				Log.Error("Error while stopping listener for webapp" + webAppContext.GetDisplayName
					(), e);
				exception = AddMultiException(exception, e);
			}
			try
			{
				if (sslFactory != null)
				{
					sslFactory.Destroy();
				}
			}
			catch (Exception e)
			{
				Log.Error("Error while destroying the SSLFactory" + webAppContext.GetDisplayName(
					), e);
				exception = AddMultiException(exception, e);
			}
			try
			{
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
		public virtual void Join()
		{
			webServer.Join();
		}

		/// <summary>Test for the availability of the web server</summary>
		/// <returns>true if the web server is started, false otherwise</returns>
		public virtual bool IsAlive()
		{
			return webServer != null && webServer.IsStarted();
		}

		/// <summary>Return the host and port of the HttpServer, if live</summary>
		/// <returns>the classname and any HTTP URL</returns>
		public override string ToString()
		{
			return listener != null ? ("HttpServer at http://" + listener.GetHost() + ":" + listener
				.GetLocalPort() + "/" + (IsAlive() ? StateDescriptionAlive : StateDescriptionNotLive
				)) : "Inactive HttpServer";
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
		/// <param name="servletContext"/>
		/// <param name="request"/>
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
				response.SendError(HttpServletResponse.ScUnauthorized, "Unauthenticated users are not "
					 + "authorized to access this page.");
				return false;
			}
			if (servletContext.GetAttribute(AdminsAcl) != null && !UserHasAdministratorAccess
				(servletContext, remoteUser))
			{
				response.SendError(HttpServletResponse.ScUnauthorized, "User " + remoteUser + " is unauthorized to access this page."
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
				if (!HttpServer.IsInstrumentationAccessAllowed(GetServletContext(), request, response
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
					return new _Enumeration_1019(this);
				}

				private sealed class _Enumeration_1019 : Enumeration<string>
				{
					public _Enumeration_1019()
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
				HttpServletRequestWrapper quoted = new HttpServer.QuotingInputFilter.RequestQuoter
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
