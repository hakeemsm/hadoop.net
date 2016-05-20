using Sharpen;

namespace org.apache.hadoop.http
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
	public class HttpServer : org.apache.hadoop.http.FilterContainer
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.HttpServer
			)));

		internal const string FILTER_INITIALIZER_PROPERTY = "hadoop.http.filter.initializers";

		internal const string HTTP_MAX_THREADS = "hadoop.http.max.threads";

		public const string CONF_CONTEXT_ATTRIBUTE = "hadoop.conf";

		public const string ADMINS_ACL = "admins.acl";

		public const string SPNEGO_FILTER = "SpnegoFilter";

		public const string NO_CACHE_FILTER = "NoCacheFilter";

		public const string BIND_ADDRESS = "bind.address";

		private org.apache.hadoop.security.authorize.AccessControlList adminsAcl;

		private org.apache.hadoop.security.ssl.SSLFactory sslFactory;

		protected internal readonly org.mortbay.jetty.Server webServer;

		protected internal readonly org.mortbay.jetty.Connector listener;

		protected internal readonly org.mortbay.jetty.webapp.WebAppContext webAppContext;

		protected internal readonly bool findPort;

		protected internal readonly System.Collections.Generic.IDictionary<org.mortbay.jetty.servlet.Context
			, bool> defaultContexts = new System.Collections.Generic.Dictionary<org.mortbay.jetty.servlet.Context
			, bool>();

		protected internal readonly System.Collections.Generic.IList<string> filterNames = 
			new System.Collections.Generic.List<string>();

		private const int MAX_RETRIES = 10;

		internal const string STATE_DESCRIPTION_ALIVE = " - alive";

		internal const string STATE_DESCRIPTION_NOT_LIVE = " - not live";

		private readonly bool listenerStartedExternally;

		/// <summary>Same as this(name, bindAddress, port, findPort, null);</summary>
		/// <exception cref="System.IO.IOException"/>
		public HttpServer(string name, string bindAddress, int port, bool findPort)
			: this(name, bindAddress, port, findPort, new org.apache.hadoop.conf.Configuration
				())
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public HttpServer(string name, string bindAddress, int port, bool findPort, org.apache.hadoop.conf.Configuration
			 conf, org.mortbay.jetty.Connector connector)
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
		public HttpServer(string name, string bindAddress, int port, bool findPort, org.apache.hadoop.conf.Configuration
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
		public HttpServer(string name, string bindAddress, int port, bool findPort, org.apache.hadoop.conf.Configuration
			 conf)
			: this(name, bindAddress, port, findPort, conf, null, null, null)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public HttpServer(string name, string bindAddress, int port, bool findPort, org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.security.authorize.AccessControlList adminsAcl)
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
		/// <see cref="org.apache.hadoop.security.authorize.AccessControlList"/>
		/// of the admins
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public HttpServer(string name, string bindAddress, int port, bool findPort, org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.security.authorize.AccessControlList adminsAcl, org.mortbay.jetty.Connector
			 connector)
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
		/// <see cref="org.apache.hadoop.security.authorize.AccessControlList"/>
		/// of the admins
		/// </param>
		/// <param name="connector">A jetty connection listener</param>
		/// <param name="pathSpecs">
		/// Path specifications that this httpserver will be serving.
		/// These will be added to any filters.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		public HttpServer(string name, string bindAddress, int port, bool findPort, org.apache.hadoop.conf.Configuration
			 conf, org.apache.hadoop.security.authorize.AccessControlList adminsAcl, org.mortbay.jetty.Connector
			 connector, string[] pathSpecs)
		{
			// The ServletContext attribute where the daemon Configuration
			// gets stored.
			webServer = new org.mortbay.jetty.Server();
			this.findPort = findPort;
			this.adminsAcl = adminsAcl;
			if (connector == null)
			{
				listenerStartedExternally = false;
				if (conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_KEY
					, org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SSL_ENABLED_DEFAULT))
				{
					sslFactory = new org.apache.hadoop.security.ssl.SSLFactory(org.apache.hadoop.security.ssl.SSLFactory.Mode
						.SERVER, conf);
					try
					{
						sslFactory.init();
					}
					catch (java.security.GeneralSecurityException ex)
					{
						throw new System.IO.IOException(ex);
					}
					org.mortbay.jetty.security.SslSocketConnector sslListener = new _SslSocketConnector_232
						(this);
					listener = sslListener;
				}
				else
				{
					listener = createBaseListener(conf);
				}
				listener.setHost(bindAddress);
				listener.setPort(port);
			}
			else
			{
				listenerStartedExternally = true;
				listener = connector;
			}
			webServer.addConnector(listener);
			int maxThreads = conf.getInt(HTTP_MAX_THREADS, -1);
			// If HTTP_MAX_THREADS is not configured, QueueThreadPool() will use the
			// default value (currently 250).
			org.mortbay.thread.QueuedThreadPool threadPool = maxThreads == -1 ? new org.mortbay.thread.QueuedThreadPool
				() : new org.mortbay.thread.QueuedThreadPool(maxThreads);
			threadPool.setDaemon(true);
			webServer.setThreadPool(threadPool);
			string appDir = getWebAppsPath(name);
			org.mortbay.jetty.handler.ContextHandlerCollection contexts = new org.mortbay.jetty.handler.ContextHandlerCollection
				();
			webServer.setHandler(contexts);
			webAppContext = new org.mortbay.jetty.webapp.WebAppContext();
			webAppContext.setDisplayName(name);
			webAppContext.setContextPath("/");
			webAppContext.setWar(appDir + "/" + name);
			webAppContext.getServletContext().setAttribute(CONF_CONTEXT_ATTRIBUTE, conf);
			webAppContext.getServletContext().setAttribute(ADMINS_ACL, adminsAcl);
			addNoCacheFilter(webAppContext);
			webServer.addHandler(webAppContext);
			addDefaultApps(contexts, appDir, conf);
			addGlobalFilter("safety", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.HttpServer.QuotingInputFilter
				)).getName(), null);
			org.apache.hadoop.http.FilterInitializer[] initializers = getFilterInitializers(conf
				);
			if (initializers != null)
			{
				conf = new org.apache.hadoop.conf.Configuration(conf);
				conf.set(BIND_ADDRESS, bindAddress);
				foreach (org.apache.hadoop.http.FilterInitializer c in initializers)
				{
					c.initFilter(this, conf);
				}
			}
			addDefaultServlets();
			if (pathSpecs != null)
			{
				foreach (string path in pathSpecs)
				{
					LOG.info("adding path spec: " + path);
					addFilterPathMapping(path, webAppContext);
				}
			}
		}

		private sealed class _SslSocketConnector_232 : org.mortbay.jetty.security.SslSocketConnector
		{
			public _SslSocketConnector_232(HttpServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			protected override javax.net.ssl.SSLServerSocketFactory createFactory()
			{
				return this._enclosing.sslFactory.createSSLServerSocketFactory();
			}

			private readonly HttpServer _enclosing;
		}

		private void addNoCacheFilter(org.mortbay.jetty.webapp.WebAppContext ctxt)
		{
			defineFilter(ctxt, NO_CACHE_FILTER, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.NoCacheFilter
				)).getName(), java.util.Collections.EMPTY_MAP, new string[] { "/*" });
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
		public virtual org.mortbay.jetty.Connector createBaseListener(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return org.apache.hadoop.http.HttpServer.createDefaultChannelConnector();
		}

		private class SelectChannelConnectorWithSafeStartup : org.mortbay.jetty.nio.SelectChannelConnector
		{
			public SelectChannelConnectorWithSafeStartup()
				: base()
			{
			}

			/* Override the broken isRunning() method (JETTY-1316). This bug is present
			* in 6.1.26. For the versions wihout this bug, it adds insignificant
			* overhead.
			*/
			public override bool isRunning()
			{
				if (base.isRunning())
				{
					return true;
				}
				// We might be hitting JETTY-1316. If the internal state changed from
				// STARTING to STARTED in the middle of the check, the above call may
				// return false.  Check it one more time.
				LOG.warn("HttpServer Acceptor: isRunning is false. Rechecking.");
				try
				{
					java.lang.Thread.sleep(10);
				}
				catch (System.Exception)
				{
					// Mark this thread as interrupted. Someone up in the call chain
					// might care.
					java.lang.Thread.currentThread().interrupt();
				}
				bool runState = base.isRunning();
				LOG.warn("HttpServer Acceptor: isRunning is " + runState);
				return runState;
			}
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static org.mortbay.jetty.Connector createDefaultChannelConnector()
		{
			org.mortbay.jetty.nio.SelectChannelConnector ret = new org.apache.hadoop.http.HttpServer.SelectChannelConnectorWithSafeStartup
				();
			ret.setLowResourceMaxIdleTime(10000);
			ret.setAcceptQueueSize(128);
			ret.setResolveNames(false);
			ret.setUseDirectBuffers(false);
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				// result of setting the SO_REUSEADDR flag is different on Windows
				// http://msdn.microsoft.com/en-us/library/ms740621(v=vs.85).aspx
				// without this 2 NN's can start on the same machine and listen on 
				// the same port with indeterminate routing of incoming requests to them
				ret.setReuseAddress(false);
			}
			ret.setHeaderBufferSize(1024 * 64);
			return ret;
		}

		/// <summary>Get an array of FilterConfiguration specified in the conf</summary>
		private static org.apache.hadoop.http.FilterInitializer[] getFilterInitializers(org.apache.hadoop.conf.Configuration
			 conf)
		{
			if (conf == null)
			{
				return null;
			}
			java.lang.Class[] classes = conf.getClasses(FILTER_INITIALIZER_PROPERTY);
			if (classes == null)
			{
				return null;
			}
			org.apache.hadoop.http.FilterInitializer[] initializers = new org.apache.hadoop.http.FilterInitializer
				[classes.Length];
			for (int i = 0; i < classes.Length; i++)
			{
				initializers[i] = (org.apache.hadoop.http.FilterInitializer)org.apache.hadoop.util.ReflectionUtils
					.newInstance(classes[i], conf);
			}
			return initializers;
		}

		/// <summary>Add default apps.</summary>
		/// <param name="appDir">The application directory</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void addDefaultApps(org.mortbay.jetty.handler.ContextHandlerCollection
			 parent, string appDir, org.apache.hadoop.conf.Configuration conf)
		{
			// set up the context for "/logs/" if "hadoop.log.dir" property is defined. 
			string logDir = Sharpen.Runtime.getProperty("hadoop.log.dir");
			if (logDir != null)
			{
				org.mortbay.jetty.servlet.Context logContext = new org.mortbay.jetty.servlet.Context
					(parent, "/logs");
				logContext.setResourceBase(logDir);
				logContext.addServlet(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.AdminAuthorizedServlet
					)), "/*");
				if (conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_JETTY_LOGS_SERVE_ALIASES
					, org.apache.hadoop.fs.CommonConfigurationKeys.DEFAULT_HADOOP_JETTY_LOGS_SERVE_ALIASES
					))
				{
					logContext.getInitParams()["org.mortbay.jetty.servlet.Default.aliases"] = "true";
				}
				logContext.setDisplayName("logs");
				setContextAttributes(logContext, conf);
				addNoCacheFilter(webAppContext);
				defaultContexts[logContext] = true;
			}
			// set up the context for "/static/*"
			org.mortbay.jetty.servlet.Context staticContext = new org.mortbay.jetty.servlet.Context
				(parent, "/static");
			staticContext.setResourceBase(appDir + "/static");
			staticContext.addServlet(Sharpen.Runtime.getClassForType(typeof(org.mortbay.jetty.servlet.DefaultServlet
				)), "/*");
			staticContext.setDisplayName("static");
			setContextAttributes(staticContext, conf);
			defaultContexts[staticContext] = true;
		}

		private void setContextAttributes(org.mortbay.jetty.servlet.Context context, org.apache.hadoop.conf.Configuration
			 conf)
		{
			context.getServletContext().setAttribute(CONF_CONTEXT_ATTRIBUTE, conf);
			context.getServletContext().setAttribute(ADMINS_ACL, adminsAcl);
		}

		/// <summary>Add default servlets.</summary>
		protected internal virtual void addDefaultServlets()
		{
			// set up default servlets
			addServlet("stacks", "/stacks", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.HttpServer.StackServlet
				)));
			addServlet("logLevel", "/logLevel", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.log.LogLevel.Servlet
				)));
			addServlet("metrics", "/metrics", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics.MetricsServlet
				)));
			addServlet("jmx", "/jmx", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.jmx.JMXJsonServlet
				)));
			addServlet("conf", "/conf", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.ConfServlet
				)));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void addContext(org.mortbay.jetty.servlet.Context ctxt, bool isFiltered
			)
		{
			webServer.addHandler(ctxt);
			addNoCacheFilter(webAppContext);
			defaultContexts[ctxt] = isFiltered;
		}

		/// <summary>Add a context</summary>
		/// <param name="pathSpec">The path spec for the context</param>
		/// <param name="dir">The directory containing the context</param>
		/// <param name="isFiltered">if true, the servlet is added to the filter path mapping
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void addContext(string pathSpec, string dir, bool isFiltered
			)
		{
			if (0 == webServer.getHandlers().Length)
			{
				throw new System.Exception("Couldn't find handler");
			}
			org.mortbay.jetty.webapp.WebAppContext webAppCtx = new org.mortbay.jetty.webapp.WebAppContext
				();
			webAppCtx.setContextPath(pathSpec);
			webAppCtx.setWar(dir);
			addContext(webAppCtx, true);
		}

		/// <summary>Set a value in the webapp context.</summary>
		/// <remarks>
		/// Set a value in the webapp context. These values are available to the jsp
		/// pages as "application.getAttribute(name)".
		/// </remarks>
		/// <param name="name">The name of the attribute</param>
		/// <param name="value">The value of the attribute</param>
		public virtual void setAttribute(string name, object value)
		{
			webAppContext.setAttribute(name, value);
		}

		/// <summary>Add a Jersey resource package.</summary>
		/// <param name="packageName">The Java package name containing the Jersey resource.</param>
		/// <param name="pathSpec">The path spec for the servlet</param>
		public virtual void addJerseyResourcePackage(string packageName, string pathSpec)
		{
			LOG.info("addJerseyResourcePackage: packageName=" + packageName + ", pathSpec=" +
				 pathSpec);
			org.mortbay.jetty.servlet.ServletHolder sh = new org.mortbay.jetty.servlet.ServletHolder
				(Sharpen.Runtime.getClassForType(typeof(com.sun.jersey.spi.container.servlet.ServletContainer
				)));
			sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig"
				);
			sh.setInitParameter("com.sun.jersey.config.property.packages", packageName);
			webAppContext.addServlet(sh, pathSpec);
		}

		/// <summary>Add a servlet in the server.</summary>
		/// <param name="name">The name of the servlet (can be passed as null)</param>
		/// <param name="pathSpec">The path spec for the servlet</param>
		/// <param name="clazz">The servlet class</param>
		public virtual void addServlet(string name, string pathSpec, java.lang.Class clazz
			)
		{
			addInternalServlet(name, pathSpec, clazz, false);
			addFilterPathMapping(pathSpec, webAppContext);
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
		public virtual void addInternalServlet(string name, string pathSpec, java.lang.Class
			 clazz)
		{
			addInternalServlet(name, pathSpec, clazz, false);
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
		public virtual void addInternalServlet(string name, string pathSpec, java.lang.Class
			 clazz, bool requireAuth)
		{
			org.mortbay.jetty.servlet.ServletHolder holder = new org.mortbay.jetty.servlet.ServletHolder
				(clazz);
			if (name != null)
			{
				holder.setName(name);
			}
			webAppContext.addServlet(holder, pathSpec);
			if (requireAuth && org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled
				())
			{
				LOG.info("Adding Kerberos (SPNEGO) filter to " + name);
				org.mortbay.jetty.servlet.ServletHandler handler = webAppContext.getServletHandler
					();
				org.mortbay.jetty.servlet.FilterMapping fmap = new org.mortbay.jetty.servlet.FilterMapping
					();
				fmap.setPathSpec(pathSpec);
				fmap.setFilterName(SPNEGO_FILTER);
				fmap.setDispatches(org.mortbay.jetty.Handler.ALL);
				handler.addFilterMapping(fmap);
			}
		}

		public virtual void addFilter(string name, string classname, System.Collections.Generic.IDictionary
			<string, string> parameters)
		{
			string[] USER_FACING_URLS = new string[] { "*.html", "*.jsp" };
			defineFilter(webAppContext, name, classname, parameters, USER_FACING_URLS);
			LOG.info("Added filter " + name + " (class=" + classname + ") to context " + webAppContext
				.getDisplayName());
			string[] ALL_URLS = new string[] { "/*" };
			foreach (System.Collections.Generic.KeyValuePair<org.mortbay.jetty.servlet.Context
				, bool> e in defaultContexts)
			{
				if (e.Value)
				{
					org.mortbay.jetty.servlet.Context ctx = e.Key;
					defineFilter(ctx, name, classname, parameters, ALL_URLS);
					LOG.info("Added filter " + name + " (class=" + classname + ") to context " + ctx.
						getDisplayName());
				}
			}
			filterNames.add(name);
		}

		public virtual void addGlobalFilter(string name, string classname, System.Collections.Generic.IDictionary
			<string, string> parameters)
		{
			string[] ALL_URLS = new string[] { "/*" };
			defineFilter(webAppContext, name, classname, parameters, ALL_URLS);
			foreach (org.mortbay.jetty.servlet.Context ctx in defaultContexts.Keys)
			{
				defineFilter(ctx, name, classname, parameters, ALL_URLS);
			}
			LOG.info("Added global filter '" + name + "' (class=" + classname + ")");
		}

		/// <summary>Define a filter for a context and set up default url mappings.</summary>
		public virtual void defineFilter(org.mortbay.jetty.servlet.Context ctx, string name
			, string classname, System.Collections.Generic.IDictionary<string, string> parameters
			, string[] urls)
		{
			org.mortbay.jetty.servlet.FilterHolder holder = new org.mortbay.jetty.servlet.FilterHolder
				();
			holder.setName(name);
			holder.setClassName(classname);
			holder.setInitParameters(parameters);
			org.mortbay.jetty.servlet.FilterMapping fmap = new org.mortbay.jetty.servlet.FilterMapping
				();
			fmap.setPathSpecs(urls);
			fmap.setDispatches(org.mortbay.jetty.Handler.ALL);
			fmap.setFilterName(name);
			org.mortbay.jetty.servlet.ServletHandler handler = ctx.getServletHandler();
			handler.addFilter(holder, fmap);
		}

		/// <summary>Add the path spec to the filter path mapping.</summary>
		/// <param name="pathSpec">The path spec</param>
		/// <param name="webAppCtx">The WebApplicationContext to add to</param>
		protected internal virtual void addFilterPathMapping(string pathSpec, org.mortbay.jetty.servlet.Context
			 webAppCtx)
		{
			org.mortbay.jetty.servlet.ServletHandler handler = webAppCtx.getServletHandler();
			foreach (string name in filterNames)
			{
				org.mortbay.jetty.servlet.FilterMapping fmap = new org.mortbay.jetty.servlet.FilterMapping
					();
				fmap.setPathSpec(pathSpec);
				fmap.setFilterName(name);
				fmap.setDispatches(org.mortbay.jetty.Handler.ALL);
				handler.addFilterMapping(fmap);
			}
		}

		/// <summary>Get the value in the webapp context.</summary>
		/// <param name="name">The name of the attribute</param>
		/// <returns>The value of the attribute</returns>
		public virtual object getAttribute(string name)
		{
			return webAppContext.getAttribute(name);
		}

		public virtual org.mortbay.jetty.webapp.WebAppContext getWebAppContext()
		{
			return this.webAppContext;
		}

		/// <summary>Get the pathname to the webapps files.</summary>
		/// <param name="appName">eg "secondary" or "datanode"</param>
		/// <returns>the pathname as a URL</returns>
		/// <exception cref="java.io.FileNotFoundException">if 'webapps' directory cannot be found on CLASSPATH.
		/// 	</exception>
		protected internal virtual string getWebAppsPath(string appName)
		{
			java.net.URL url = Sharpen.Runtime.getClassForObject(this).getClassLoader().getResource
				("webapps/" + appName);
			if (url == null)
			{
				throw new java.io.FileNotFoundException("webapps/" + appName + " not found in CLASSPATH"
					);
			}
			string urlString = url.ToString();
			return Sharpen.Runtime.substring(urlString, 0, urlString.LastIndexOf('/'));
		}

		/// <summary>Get the port that the server is on</summary>
		/// <returns>the port</returns>
		public virtual int getPort()
		{
			return webServer.getConnectors()[0].getLocalPort();
		}

		/// <summary>Set the min, max number of worker threads (simultaneous connections).</summary>
		public virtual void setThreads(int min, int max)
		{
			org.mortbay.thread.QueuedThreadPool pool = (org.mortbay.thread.QueuedThreadPool)webServer
				.getThreadPool();
			pool.setMinThreads(min);
			pool.setMaxThreads(max);
		}

		/// <summary>Configure an ssl listener on the server.</summary>
		/// <param name="addr">address to listen on</param>
		/// <param name="keystore">location of the keystore</param>
		/// <param name="storPass">password for the keystore</param>
		/// <param name="keyPass">password for the key</param>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use addSslListener(java.net.InetSocketAddress, org.apache.hadoop.conf.Configuration, bool)"
			)]
		public virtual void addSslListener(java.net.InetSocketAddress addr, string keystore
			, string storPass, string keyPass)
		{
			if (webServer.isStarted())
			{
				throw new System.IO.IOException("Failed to add ssl listener");
			}
			org.mortbay.jetty.security.SslSocketConnector sslListener = new org.mortbay.jetty.security.SslSocketConnector
				();
			sslListener.setHost(addr.getHostName());
			sslListener.setPort(addr.getPort());
			sslListener.setKeystore(keystore);
			sslListener.setPassword(storPass);
			sslListener.setKeyPassword(keyPass);
			webServer.addConnector(sslListener);
		}

		/// <summary>Configure an ssl listener on the server.</summary>
		/// <param name="addr">address to listen on</param>
		/// <param name="sslConf">conf to retrieve ssl options</param>
		/// <param name="needCertsAuth">whether x509 certificate authentication is required</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void addSslListener(java.net.InetSocketAddress addr, org.apache.hadoop.conf.Configuration
			 sslConf, bool needCertsAuth)
		{
			if (webServer.isStarted())
			{
				throw new System.IO.IOException("Failed to add ssl listener");
			}
			if (needCertsAuth)
			{
				// setting up SSL truststore for authenticating clients
				Sharpen.Runtime.setProperty("javax.net.ssl.trustStore", sslConf.get("ssl.server.truststore.location"
					, string.Empty));
				Sharpen.Runtime.setProperty("javax.net.ssl.trustStorePassword", sslConf.get("ssl.server.truststore.password"
					, string.Empty));
				Sharpen.Runtime.setProperty("javax.net.ssl.trustStoreType", sslConf.get("ssl.server.truststore.type"
					, "jks"));
			}
			org.mortbay.jetty.security.SslSocketConnector sslListener = new org.mortbay.jetty.security.SslSocketConnector
				();
			sslListener.setHost(addr.getHostName());
			sslListener.setPort(addr.getPort());
			sslListener.setKeystore(sslConf.get("ssl.server.keystore.location"));
			sslListener.setPassword(sslConf.get("ssl.server.keystore.password", string.Empty)
				);
			sslListener.setKeyPassword(sslConf.get("ssl.server.keystore.keypassword", string.Empty
				));
			sslListener.setKeystoreType(sslConf.get("ssl.server.keystore.type", "jks"));
			sslListener.setNeedClientAuth(needCertsAuth);
			webServer.addConnector(sslListener);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void initSpnego(org.apache.hadoop.conf.Configuration conf
			, string usernameConfKey, string keytabConfKey)
		{
			System.Collections.Generic.IDictionary<string, string> @params = new System.Collections.Generic.Dictionary
				<string, string>();
			string principalInConf = conf.get(usernameConfKey);
			if (principalInConf != null && !principalInConf.isEmpty())
			{
				@params["kerberos.principal"] = org.apache.hadoop.security.SecurityUtil.getServerPrincipal
					(principalInConf, listener.getHost());
			}
			string httpKeytab = conf.get(keytabConfKey);
			if (httpKeytab != null && !httpKeytab.isEmpty())
			{
				@params["kerberos.keytab"] = httpKeytab;
			}
			@params[org.apache.hadoop.security.authentication.server.AuthenticationFilter.AUTH_TYPE
				] = "kerberos";
			defineFilter(webAppContext, SPNEGO_FILTER, Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.security.authentication.server.AuthenticationFilter)).getName(
				), @params, null);
		}

		/// <summary>Start the server.</summary>
		/// <remarks>Start the server. Does not wait for the server to start.</remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void start()
		{
			try
			{
				try
				{
					openListener();
					LOG.info("Jetty bound to port " + listener.getLocalPort());
					webServer.start();
				}
				catch (System.IO.IOException ex)
				{
					LOG.info("HttpServer.start() threw a non Bind IOException", ex);
					throw;
				}
				catch (org.mortbay.util.MultiException ex)
				{
					LOG.info("HttpServer.start() threw a MultiException", ex);
					throw;
				}
				// Make sure there is no handler failures.
				org.mortbay.jetty.Handler[] handlers = webServer.getHandlers();
				for (int i = 0; i < handlers.Length; i++)
				{
					if (handlers[i].isFailed())
					{
						throw new System.IO.IOException("Problem in starting http server. Server handlers failed"
							);
					}
				}
				// Make sure there are no errors initializing the context.
				System.Exception unavailableException = webAppContext.getUnavailableException();
				if (unavailableException != null)
				{
					// Have to stop the webserver, or else its non-daemon threads
					// will hang forever.
					webServer.stop();
					throw new System.IO.IOException("Unable to initialize WebAppContext", unavailableException
						);
				}
			}
			catch (System.IO.IOException e)
			{
				throw;
			}
			catch (System.Exception e)
			{
				throw new System.IO.IOException("Problem starting http server", e);
			}
		}

		/// <summary>Open the main listener for the server</summary>
		/// <exception cref="System.Exception"/>
		internal virtual void openListener()
		{
			if (listener.getLocalPort() != -1)
			{
				// it's already bound
				return;
			}
			if (listenerStartedExternally)
			{
				// Expect that listener was started securely
				throw new System.Exception("Expected webserver's listener to be started " + "previously but wasn't"
					);
			}
			int port = listener.getPort();
			while (true)
			{
				// jetty has a bug where you can't reopen a listener that previously
				// failed to open w/o issuing a close first, even if the port is changed
				try
				{
					listener.close();
					listener.open();
					break;
				}
				catch (java.net.BindException ex)
				{
					if (port == 0 || !findPort)
					{
						java.net.BindException be = new java.net.BindException("Port in use: " + listener
							.getHost() + ":" + listener.getPort());
						be.initCause(ex);
						throw be;
					}
				}
				// try the next port number
				listener.setPort(++port);
				java.lang.Thread.sleep(100);
			}
		}

		/// <summary>Return the bind address of the listener.</summary>
		/// <returns>InetSocketAddress of the listener</returns>
		public virtual java.net.InetSocketAddress getListenerAddress()
		{
			int port = listener.getLocalPort();
			if (port == -1)
			{
				// not bound, return requested port
				port = listener.getPort();
			}
			return new java.net.InetSocketAddress(listener.getHost(), port);
		}

		/// <summary>stop the server</summary>
		/// <exception cref="System.Exception"/>
		public virtual void stop()
		{
			org.mortbay.util.MultiException exception = null;
			try
			{
				listener.close();
			}
			catch (System.Exception e)
			{
				LOG.error("Error while stopping listener for webapp" + webAppContext.getDisplayName
					(), e);
				exception = addMultiException(exception, e);
			}
			try
			{
				if (sslFactory != null)
				{
					sslFactory.destroy();
				}
			}
			catch (System.Exception e)
			{
				LOG.error("Error while destroying the SSLFactory" + webAppContext.getDisplayName(
					), e);
				exception = addMultiException(exception, e);
			}
			try
			{
				// clear & stop webAppContext attributes to avoid memory leaks.
				webAppContext.clearAttributes();
				webAppContext.stop();
			}
			catch (System.Exception e)
			{
				LOG.error("Error while stopping web app context for webapp " + webAppContext.getDisplayName
					(), e);
				exception = addMultiException(exception, e);
			}
			try
			{
				webServer.stop();
			}
			catch (System.Exception e)
			{
				LOG.error("Error while stopping web server for webapp " + webAppContext.getDisplayName
					(), e);
				exception = addMultiException(exception, e);
			}
			if (exception != null)
			{
				exception.ifExceptionThrow();
			}
		}

		private org.mortbay.util.MultiException addMultiException(org.mortbay.util.MultiException
			 exception, System.Exception e)
		{
			if (exception == null)
			{
				exception = new org.mortbay.util.MultiException();
			}
			exception.add(e);
			return exception;
		}

		/// <exception cref="System.Exception"/>
		public virtual void join()
		{
			webServer.join();
		}

		/// <summary>Test for the availability of the web server</summary>
		/// <returns>true if the web server is started, false otherwise</returns>
		public virtual bool isAlive()
		{
			return webServer != null && webServer.isStarted();
		}

		/// <summary>Return the host and port of the HttpServer, if live</summary>
		/// <returns>the classname and any HTTP URL</returns>
		public override string ToString()
		{
			return listener != null ? ("HttpServer at http://" + listener.getHost() + ":" + listener
				.getLocalPort() + "/" + (isAlive() ? STATE_DESCRIPTION_ALIVE : STATE_DESCRIPTION_NOT_LIVE
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
		public static bool isInstrumentationAccessAllowed(javax.servlet.ServletContext servletContext
			, javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response)
		{
			org.apache.hadoop.conf.Configuration conf = (org.apache.hadoop.conf.Configuration
				)servletContext.getAttribute(CONF_CONTEXT_ATTRIBUTE);
			bool access = true;
			bool adminAccess = conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN
				, false);
			if (adminAccess)
			{
				access = hasAdministratorAccess(servletContext, request, response);
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
		public static bool hasAdministratorAccess(javax.servlet.ServletContext servletContext
			, javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response)
		{
			org.apache.hadoop.conf.Configuration conf = (org.apache.hadoop.conf.Configuration
				)servletContext.getAttribute(CONF_CONTEXT_ATTRIBUTE);
			// If there is no authorization, anybody has administrator access.
			if (!conf.getBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION
				, false))
			{
				return true;
			}
			string remoteUser = request.getRemoteUser();
			if (remoteUser == null)
			{
				response.sendError(javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED, "Unauthenticated users are not "
					 + "authorized to access this page.");
				return false;
			}
			if (servletContext.getAttribute(ADMINS_ACL) != null && !userHasAdministratorAccess
				(servletContext, remoteUser))
			{
				response.sendError(javax.servlet.http.HttpServletResponse.SC_UNAUTHORIZED, "User "
					 + remoteUser + " is unauthorized to access this page.");
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
		public static bool userHasAdministratorAccess(javax.servlet.ServletContext servletContext
			, string remoteUser)
		{
			org.apache.hadoop.security.authorize.AccessControlList adminsAcl = (org.apache.hadoop.security.authorize.AccessControlList
				)servletContext.getAttribute(ADMINS_ACL);
			org.apache.hadoop.security.UserGroupInformation remoteUserUGI = org.apache.hadoop.security.UserGroupInformation
				.createRemoteUser(remoteUser);
			return adminsAcl != null && adminsAcl.isUserAllowed(remoteUserUGI);
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
		public class StackServlet : javax.servlet.http.HttpServlet
		{
			private const long serialVersionUID = -6284183679759467039L;

			/// <exception cref="javax.servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void doGet(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
				 response)
			{
				if (!org.apache.hadoop.http.HttpServer.isInstrumentationAccessAllowed(getServletContext
					(), request, response))
				{
					return;
				}
				response.setContentType("text/plain; charset=UTF-8");
				using (System.IO.TextWriter @out = new System.IO.TextWriter(response.getOutputStream
					(), false, "UTF-8"))
				{
					org.apache.hadoop.util.ReflectionUtils.printThreadInfo(@out, string.Empty);
				}
				org.apache.hadoop.util.ReflectionUtils.logThreadInfo(LOG, "jsp requested", 1);
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
		public class QuotingInputFilter : javax.servlet.Filter
		{
			private javax.servlet.FilterConfig config;

			public class RequestQuoter : javax.servlet.http.HttpServletRequestWrapper
			{
				private readonly javax.servlet.http.HttpServletRequest rawRequest;

				public RequestQuoter(javax.servlet.http.HttpServletRequest rawRequest)
					: base(rawRequest)
				{
					this.rawRequest = rawRequest;
				}

				/// <summary>Return the set of parameter names, quoting each name.</summary>
				public override System.Collections.IEnumerator getParameterNames()
				{
					return new _Enumeration_1019(this);
				}

				private sealed class _Enumeration_1019 : java.util.Enumeration<string>
				{
					public _Enumeration_1019()
					{
						this.rawIterator = this._enclosing.rawRequest.getParameterNames();
					}

					private java.util.Enumeration<string> rawIterator;

					public bool MoveNext()
					{
						return this.rawIterator.MoveNext();
					}

					public string Current
					{
						get
						{
							return org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars(this.rawIterator.Current
								);
						}
					}
				}

				/// <summary>Unquote the name and quote the value.</summary>
				public override string getParameter(string name)
				{
					return org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars(rawRequest.getParameter(
						org.apache.hadoop.http.HtmlQuoting.unquoteHtmlChars(name)));
				}

				public override string[] getParameterValues(string name)
				{
					string unquoteName = org.apache.hadoop.http.HtmlQuoting.unquoteHtmlChars(name);
					string[] unquoteValue = rawRequest.getParameterValues(unquoteName);
					if (unquoteValue == null)
					{
						return null;
					}
					string[] result = new string[unquoteValue.Length];
					for (int i = 0; i < result.Length; ++i)
					{
						result[i] = org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars(unquoteValue[i]);
					}
					return result;
				}

				public override System.Collections.IDictionary getParameterMap()
				{
					System.Collections.Generic.IDictionary<string, string[]> result = new System.Collections.Generic.Dictionary
						<string, string[]>();
					System.Collections.Generic.IDictionary<string, string[]> raw = rawRequest.getParameterMap
						();
					foreach (System.Collections.Generic.KeyValuePair<string, string[]> item in raw)
					{
						string[] rawValue = item.Value;
						string[] cookedValue = new string[rawValue.Length];
						for (int i = 0; i < rawValue.Length; ++i)
						{
							cookedValue[i] = org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars(rawValue[i]);
						}
						result[org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars(item.Key)] = cookedValue;
					}
					return result;
				}

				/// <summary>
				/// Quote the url so that users specifying the HOST HTTP header
				/// can't inject attacks.
				/// </summary>
				public override System.Text.StringBuilder getRequestURL()
				{
					string url = rawRequest.getRequestURL().ToString();
					return new System.Text.StringBuilder(org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars
						(url));
				}

				/// <summary>
				/// Quote the server name so that users specifying the HOST HTTP header
				/// can't inject attacks.
				/// </summary>
				public override string getServerName()
				{
					return org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars(rawRequest.getServerName
						());
				}
			}

			/// <exception cref="javax.servlet.ServletException"/>
			public virtual void init(javax.servlet.FilterConfig config)
			{
				this.config = config;
			}

			public virtual void destroy()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="javax.servlet.ServletException"/>
			public virtual void doFilter(javax.servlet.ServletRequest request, javax.servlet.ServletResponse
				 response, javax.servlet.FilterChain chain)
			{
				javax.servlet.http.HttpServletRequestWrapper quoted = new org.apache.hadoop.http.HttpServer.QuotingInputFilter.RequestQuoter
					((javax.servlet.http.HttpServletRequest)request);
				javax.servlet.http.HttpServletResponse httpResponse = (javax.servlet.http.HttpServletResponse
					)response;
				string mime = inferMimeType(request);
				if (mime == null)
				{
					httpResponse.setContentType("text/plain; charset=utf-8");
				}
				else
				{
					if (mime.StartsWith("text/html"))
					{
						// HTML with unspecified encoding, we want to
						// force HTML with utf-8 encoding
						// This is to avoid the following security issue:
						// http://openmya.hacker.jp/hasegawa/security/utf7cs.html
						httpResponse.setContentType("text/html; charset=utf-8");
					}
					else
					{
						if (mime.StartsWith("application/xml"))
						{
							httpResponse.setContentType("text/xml; charset=utf-8");
						}
					}
				}
				chain.doFilter(quoted, httpResponse);
			}

			/// <summary>
			/// Infer the mime type for the response based on the extension of the request
			/// URI.
			/// </summary>
			/// <remarks>
			/// Infer the mime type for the response based on the extension of the request
			/// URI. Returns null if unknown.
			/// </remarks>
			private string inferMimeType(javax.servlet.ServletRequest request)
			{
				string path = ((javax.servlet.http.HttpServletRequest)request).getRequestURI();
				org.mortbay.jetty.handler.ContextHandler.SContext sContext = (org.mortbay.jetty.handler.ContextHandler.SContext
					)config.getServletContext();
				org.mortbay.jetty.MimeTypes mimes = sContext.getContextHandler().getMimeTypes();
				org.mortbay.io.Buffer mimeBuffer = mimes.getMimeByExtension(path);
				return (mimeBuffer == null) ? null : mimeBuffer.ToString();
			}
		}
	}
}
