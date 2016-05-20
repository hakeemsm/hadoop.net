using Sharpen;

namespace org.apache.hadoop.http
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
	public sealed class HttpServer2 : org.apache.hadoop.http.FilterContainer
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.HttpServer2
			)));

		internal const string FILTER_INITIALIZER_PROPERTY = "hadoop.http.filter.initializers";

		public const string HTTP_MAX_THREADS = "hadoop.http.max.threads";

		public const string CONF_CONTEXT_ATTRIBUTE = "hadoop.conf";

		public const string ADMINS_ACL = "admins.acl";

		public const string SPNEGO_FILTER = "SpnegoFilter";

		public const string NO_CACHE_FILTER = "NoCacheFilter";

		public const string BIND_ADDRESS = "bind.address";

		private readonly org.apache.hadoop.security.authorize.AccessControlList adminsAcl;

		protected internal readonly org.mortbay.jetty.Server webServer;

		private readonly System.Collections.Generic.IList<org.mortbay.jetty.Connector> listeners
			 = com.google.common.collect.Lists.newArrayList();

		protected internal readonly org.mortbay.jetty.webapp.WebAppContext webAppContext;

		protected internal readonly bool findPort;

		protected internal readonly System.Collections.Generic.IDictionary<org.mortbay.jetty.servlet.Context
			, bool> defaultContexts = new System.Collections.Generic.Dictionary<org.mortbay.jetty.servlet.Context
			, bool>();

		protected internal readonly System.Collections.Generic.IList<string> filterNames = 
			new System.Collections.Generic.List<string>();

		internal const string STATE_DESCRIPTION_ALIVE = " - alive";

		internal const string STATE_DESCRIPTION_NOT_LIVE = " - not live";

		private readonly org.apache.hadoop.security.authentication.util.SignerSecretProvider
			 secretProvider;

		/// <summary>Class to construct instances of HTTP server with specific options.</summary>
		public class Builder
		{
			private System.Collections.Generic.List<java.net.URI> endpoints = com.google.common.collect.Lists
				.newArrayList();

			private string name;

			private org.apache.hadoop.conf.Configuration conf;

			private string[] pathSpecs;

			private org.apache.hadoop.security.authorize.AccessControlList adminsAcl;

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
			public virtual org.apache.hadoop.http.HttpServer2.Builder setName(string name)
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
			public virtual org.apache.hadoop.http.HttpServer2.Builder addEndpoint(java.net.URI
				 endpoint)
			{
				endpoints.add(endpoint);
				return this;
			}

			/// <summary>Set the hostname of the http server.</summary>
			/// <remarks>
			/// Set the hostname of the http server. The host name is used to resolve the
			/// _HOST field in Kerberos principals. The hostname of the first listener
			/// will be used if the name is unspecified.
			/// </remarks>
			public virtual org.apache.hadoop.http.HttpServer2.Builder hostName(string hostName
				)
			{
				this.hostName = hostName;
				return this;
			}

			public virtual org.apache.hadoop.http.HttpServer2.Builder trustStore(string location
				, string password, string type)
			{
				this.trustStore = location;
				this.trustStorePassword = password;
				this.trustStoreType = type;
				return this;
			}

			public virtual org.apache.hadoop.http.HttpServer2.Builder keyStore(string location
				, string password, string type)
			{
				this.keyStore = location;
				this.keyStorePassword = password;
				this.keyStoreType = type;
				return this;
			}

			public virtual org.apache.hadoop.http.HttpServer2.Builder keyPassword(string password
				)
			{
				this.keyPassword = password;
				return this;
			}

			/// <summary>
			/// Specify whether the server should authorize the client in SSL
			/// connections.
			/// </summary>
			public virtual org.apache.hadoop.http.HttpServer2.Builder needsClientAuth(bool value
				)
			{
				this.needsClientAuth = value;
				return this;
			}

			public virtual org.apache.hadoop.http.HttpServer2.Builder setFindPort(bool findPort
				)
			{
				this.findPort = findPort;
				return this;
			}

			public virtual org.apache.hadoop.http.HttpServer2.Builder setConf(org.apache.hadoop.conf.Configuration
				 conf)
			{
				this.conf = conf;
				return this;
			}

			public virtual org.apache.hadoop.http.HttpServer2.Builder setPathSpec(string[] pathSpec
				)
			{
				this.pathSpecs = pathSpec;
				return this;
			}

			public virtual org.apache.hadoop.http.HttpServer2.Builder setACL(org.apache.hadoop.security.authorize.AccessControlList
				 acl)
			{
				this.adminsAcl = acl;
				return this;
			}

			public virtual org.apache.hadoop.http.HttpServer2.Builder setSecurityEnabled(bool
				 securityEnabled)
			{
				this.securityEnabled = securityEnabled;
				return this;
			}

			public virtual org.apache.hadoop.http.HttpServer2.Builder setUsernameConfKey(string
				 usernameConfKey)
			{
				this.usernameConfKey = usernameConfKey;
				return this;
			}

			public virtual org.apache.hadoop.http.HttpServer2.Builder setKeytabConfKey(string
				 keytabConfKey)
			{
				this.keytabConfKey = keytabConfKey;
				return this;
			}

			public virtual org.apache.hadoop.http.HttpServer2.Builder disallowFallbackToRandomSingerSecretProvider
				(bool value)
			{
				this.disallowFallbackToRandomSignerSecretProvider = value;
				return this;
			}

			public virtual org.apache.hadoop.http.HttpServer2.Builder authFilterConfigurationPrefix
				(string value)
			{
				this.authFilterConfigurationPrefix = value;
				return this;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.http.HttpServer2 build()
			{
				com.google.common.@base.Preconditions.checkNotNull(name, "name is not set");
				com.google.common.@base.Preconditions.checkState(!endpoints.isEmpty(), "No endpoints specified"
					);
				if (hostName == null)
				{
					hostName = endpoints[0].getHost();
				}
				if (this.conf == null)
				{
					conf = new org.apache.hadoop.conf.Configuration();
				}
				org.apache.hadoop.http.HttpServer2 server = new org.apache.hadoop.http.HttpServer2
					(this);
				if (this.securityEnabled)
				{
					server.initSpnego(conf, hostName, usernameConfKey, keytabConfKey);
				}
				foreach (java.net.URI ep in endpoints)
				{
					org.mortbay.jetty.Connector listener;
					string scheme = ep.getScheme();
					if ("http".Equals(scheme))
					{
						listener = org.apache.hadoop.http.HttpServer2.createDefaultChannelConnector();
					}
					else
					{
						if ("https".Equals(scheme))
						{
							org.mortbay.jetty.security.SslSocketConnector c = new org.apache.hadoop.security.ssl.SslSocketConnectorSecure
								();
							c.setHeaderBufferSize(1024 * 64);
							c.setNeedClientAuth(needsClientAuth);
							c.setKeyPassword(keyPassword);
							if (keyStore != null)
							{
								c.setKeystore(keyStore);
								c.setKeystoreType(keyStoreType);
								c.setPassword(keyStorePassword);
							}
							if (trustStore != null)
							{
								c.setTruststore(trustStore);
								c.setTruststoreType(trustStoreType);
								c.setTrustPassword(trustStorePassword);
							}
							listener = c;
						}
						else
						{
							throw new org.apache.hadoop.HadoopIllegalArgumentException("unknown scheme for endpoint:"
								 + ep);
						}
					}
					listener.setHost(ep.getHost());
					listener.setPort(ep.getPort() == -1 ? 0 : ep.getPort());
					server.addListener(listener);
				}
				server.loadListeners();
				return server;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private HttpServer2(org.apache.hadoop.http.HttpServer2.Builder b)
		{
			string appDir = getWebAppsPath(b.name);
			this.webServer = new org.mortbay.jetty.Server();
			this.adminsAcl = b.adminsAcl;
			this.webAppContext = createWebAppContext(b.name, b.conf, adminsAcl, appDir);
			try
			{
				this.secretProvider = constructSecretProvider(b, webAppContext.getServletContext(
					));
				this.webAppContext.getServletContext().setAttribute(org.apache.hadoop.security.authentication.server.AuthenticationFilter
					.SIGNER_SECRET_PROVIDER_ATTRIBUTE, secretProvider);
			}
			catch (System.IO.IOException e)
			{
				throw;
			}
			catch (System.Exception e)
			{
				throw new System.IO.IOException(e);
			}
			this.findPort = b.findPort;
			initializeWebServer(b.name, b.hostName, b.conf, b.pathSpecs);
		}

		/// <exception cref="System.IO.IOException"/>
		private void initializeWebServer(string name, string hostName, org.apache.hadoop.conf.Configuration
			 conf, string[] pathSpecs)
		{
			com.google.common.@base.Preconditions.checkNotNull(webAppContext);
			int maxThreads = conf.getInt(HTTP_MAX_THREADS, -1);
			// If HTTP_MAX_THREADS is not configured, QueueThreadPool() will use the
			// default value (currently 250).
			org.mortbay.thread.QueuedThreadPool threadPool = maxThreads == -1 ? new org.mortbay.thread.QueuedThreadPool
				() : new org.mortbay.thread.QueuedThreadPool(maxThreads);
			threadPool.setDaemon(true);
			webServer.setThreadPool(threadPool);
			org.mortbay.jetty.SessionManager sm = webAppContext.getSessionHandler().getSessionManager
				();
			if (sm is org.mortbay.jetty.servlet.AbstractSessionManager)
			{
				org.mortbay.jetty.servlet.AbstractSessionManager asm = (org.mortbay.jetty.servlet.AbstractSessionManager
					)sm;
				asm.setHttpOnly(true);
				asm.setSecureCookies(true);
			}
			org.mortbay.jetty.handler.ContextHandlerCollection contexts = new org.mortbay.jetty.handler.ContextHandlerCollection
				();
			org.mortbay.jetty.RequestLog requestLog = org.apache.hadoop.http.HttpRequestLog.getRequestLog
				(name);
			if (requestLog != null)
			{
				org.mortbay.jetty.handler.RequestLogHandler requestLogHandler = new org.mortbay.jetty.handler.RequestLogHandler
					();
				requestLogHandler.setRequestLog(requestLog);
				org.mortbay.jetty.handler.HandlerCollection handlers = new org.mortbay.jetty.handler.HandlerCollection
					();
				handlers.setHandlers(new org.mortbay.jetty.Handler[] { contexts, requestLogHandler
					 });
				webServer.setHandler(handlers);
			}
			else
			{
				webServer.setHandler(contexts);
			}
			string appDir = getWebAppsPath(name);
			webServer.addHandler(webAppContext);
			addDefaultApps(contexts, appDir, conf);
			addGlobalFilter("safety", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.HttpServer2.QuotingInputFilter
				)).getName(), null);
			org.apache.hadoop.http.FilterInitializer[] initializers = getFilterInitializers(conf
				);
			if (initializers != null)
			{
				conf = new org.apache.hadoop.conf.Configuration(conf);
				conf.set(BIND_ADDRESS, hostName);
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

		private void addListener(org.mortbay.jetty.Connector connector)
		{
			listeners.add(connector);
		}

		private static org.mortbay.jetty.webapp.WebAppContext createWebAppContext(string 
			name, org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.security.authorize.AccessControlList
			 adminsAcl, string appDir)
		{
			org.mortbay.jetty.webapp.WebAppContext ctx = new org.mortbay.jetty.webapp.WebAppContext
				();
			ctx.setDefaultsDescriptor(null);
			org.mortbay.jetty.servlet.ServletHolder holder = new org.mortbay.jetty.servlet.ServletHolder
				(new org.mortbay.jetty.servlet.DefaultServlet());
			System.Collections.Generic.IDictionary<string, string> @params = com.google.common.collect.ImmutableMap
				.builder<string, string>().put("acceptRanges", "true").put("dirAllowed", "false"
				).put("gzip", "true").put("useFileMappedBuffer", "true").build();
			holder.setInitParameters(@params);
			ctx.setWelcomeFiles(new string[] { "index.html" });
			ctx.addServlet(holder, "/");
			ctx.setDisplayName(name);
			ctx.setContextPath("/");
			ctx.setWar(appDir + "/" + name);
			ctx.getServletContext().setAttribute(CONF_CONTEXT_ATTRIBUTE, conf);
			ctx.getServletContext().setAttribute(ADMINS_ACL, adminsAcl);
			addNoCacheFilter(ctx);
			return ctx;
		}

		/// <exception cref="System.Exception"/>
		private static org.apache.hadoop.security.authentication.util.SignerSecretProvider
			 constructSecretProvider(org.apache.hadoop.http.HttpServer2.Builder b, javax.servlet.ServletContext
			 ctx)
		{
			org.apache.hadoop.conf.Configuration conf = b.conf;
			java.util.Properties config = getFilterProperties(conf, b.authFilterConfigurationPrefix
				);
			return org.apache.hadoop.security.authentication.server.AuthenticationFilter.constructSecretProvider
				(ctx, config, b.disallowFallbackToRandomSignerSecretProvider);
		}

		private static java.util.Properties getFilterProperties(org.apache.hadoop.conf.Configuration
			 conf, string prefix)
		{
			java.util.Properties prop = new java.util.Properties();
			System.Collections.Generic.IDictionary<string, string> filterConfig = org.apache.hadoop.security.AuthenticationFilterInitializer
				.getFilterConfigMap(conf, prefix);
			prop.putAll(filterConfig);
			return prop;
		}

		private static void addNoCacheFilter(org.mortbay.jetty.webapp.WebAppContext ctxt)
		{
			defineFilter(ctxt, NO_CACHE_FILTER, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.NoCacheFilter
				)).getName(), java.util.Collections.emptyMap<string, string>(), new string[] { "/*"
				 });
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
			org.mortbay.jetty.nio.SelectChannelConnector ret = new org.apache.hadoop.http.HttpServer2.SelectChannelConnectorWithSafeStartup
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
		protected internal void addDefaultApps(org.mortbay.jetty.handler.ContextHandlerCollection
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
					System.Collections.Generic.IDictionary<string, string> @params = logContext.getInitParams
						();
					@params["org.mortbay.jetty.servlet.Default.aliases"] = "true";
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
		protected internal void addDefaultServlets()
		{
			// set up default servlets
			addServlet("stacks", "/stacks", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.HttpServer2.StackServlet
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

		public void addContext(org.mortbay.jetty.servlet.Context ctxt, bool isFiltered)
		{
			webServer.addHandler(ctxt);
			addNoCacheFilter(webAppContext);
			defaultContexts[ctxt] = isFiltered;
		}

		/// <summary>Set a value in the webapp context.</summary>
		/// <remarks>
		/// Set a value in the webapp context. These values are available to the jsp
		/// pages as "application.getAttribute(name)".
		/// </remarks>
		/// <param name="name">The name of the attribute</param>
		/// <param name="value">The value of the attribute</param>
		public void setAttribute(string name, object value)
		{
			webAppContext.setAttribute(name, value);
		}

		/// <summary>Add a Jersey resource package.</summary>
		/// <param name="packageName">The Java package name containing the Jersey resource.</param>
		/// <param name="pathSpec">The path spec for the servlet</param>
		public void addJerseyResourcePackage(string packageName, string pathSpec)
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
		public void addServlet(string name, string pathSpec, java.lang.Class clazz)
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
		public void addInternalServlet(string name, string pathSpec, java.lang.Class clazz
			)
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
		public void addInternalServlet(string name, string pathSpec, java.lang.Class clazz
			, bool requireAuth)
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

		public void addFilter(string name, string classname, System.Collections.Generic.IDictionary
			<string, string> parameters)
		{
			org.mortbay.jetty.servlet.FilterHolder filterHolder = getFilterHolder(name, classname
				, parameters);
			string[] USER_FACING_URLS = new string[] { "*.html", "*.jsp" };
			org.mortbay.jetty.servlet.FilterMapping fmap = getFilterMapping(name, USER_FACING_URLS
				);
			defineFilter(webAppContext, filterHolder, fmap);
			LOG.info("Added filter " + name + " (class=" + classname + ") to context " + webAppContext
				.getDisplayName());
			string[] ALL_URLS = new string[] { "/*" };
			fmap = getFilterMapping(name, ALL_URLS);
			foreach (System.Collections.Generic.KeyValuePair<org.mortbay.jetty.servlet.Context
				, bool> e in defaultContexts)
			{
				if (e.Value)
				{
					org.mortbay.jetty.servlet.Context ctx = e.Key;
					defineFilter(ctx, filterHolder, fmap);
					LOG.info("Added filter " + name + " (class=" + classname + ") to context " + ctx.
						getDisplayName());
				}
			}
			filterNames.add(name);
		}

		public void addGlobalFilter(string name, string classname, System.Collections.Generic.IDictionary
			<string, string> parameters)
		{
			string[] ALL_URLS = new string[] { "/*" };
			org.mortbay.jetty.servlet.FilterHolder filterHolder = getFilterHolder(name, classname
				, parameters);
			org.mortbay.jetty.servlet.FilterMapping fmap = getFilterMapping(name, ALL_URLS);
			defineFilter(webAppContext, filterHolder, fmap);
			foreach (org.mortbay.jetty.servlet.Context ctx in defaultContexts.Keys)
			{
				defineFilter(ctx, filterHolder, fmap);
			}
			LOG.info("Added global filter '" + name + "' (class=" + classname + ")");
		}

		/// <summary>Define a filter for a context and set up default url mappings.</summary>
		public static void defineFilter(org.mortbay.jetty.servlet.Context ctx, string name
			, string classname, System.Collections.Generic.IDictionary<string, string> parameters
			, string[] urls)
		{
			org.mortbay.jetty.servlet.FilterHolder filterHolder = getFilterHolder(name, classname
				, parameters);
			org.mortbay.jetty.servlet.FilterMapping fmap = getFilterMapping(name, urls);
			defineFilter(ctx, filterHolder, fmap);
		}

		/// <summary>Define a filter for a context and set up default url mappings.</summary>
		private static void defineFilter(org.mortbay.jetty.servlet.Context ctx, org.mortbay.jetty.servlet.FilterHolder
			 holder, org.mortbay.jetty.servlet.FilterMapping fmap)
		{
			org.mortbay.jetty.servlet.ServletHandler handler = ctx.getServletHandler();
			handler.addFilter(holder, fmap);
		}

		private static org.mortbay.jetty.servlet.FilterMapping getFilterMapping(string name
			, string[] urls)
		{
			org.mortbay.jetty.servlet.FilterMapping fmap = new org.mortbay.jetty.servlet.FilterMapping
				();
			fmap.setPathSpecs(urls);
			fmap.setDispatches(org.mortbay.jetty.Handler.ALL);
			fmap.setFilterName(name);
			return fmap;
		}

		private static org.mortbay.jetty.servlet.FilterHolder getFilterHolder(string name
			, string classname, System.Collections.Generic.IDictionary<string, string> parameters
			)
		{
			org.mortbay.jetty.servlet.FilterHolder holder = new org.mortbay.jetty.servlet.FilterHolder
				();
			holder.setName(name);
			holder.setClassName(classname);
			holder.setInitParameters(parameters);
			return holder;
		}

		/// <summary>Add the path spec to the filter path mapping.</summary>
		/// <param name="pathSpec">The path spec</param>
		/// <param name="webAppCtx">The WebApplicationContext to add to</param>
		protected internal void addFilterPathMapping(string pathSpec, org.mortbay.jetty.servlet.Context
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
		public object getAttribute(string name)
		{
			return webAppContext.getAttribute(name);
		}

		public org.mortbay.jetty.webapp.WebAppContext getWebAppContext()
		{
			return this.webAppContext;
		}

		/// <summary>Get the pathname to the webapps files.</summary>
		/// <param name="appName">eg "secondary" or "datanode"</param>
		/// <returns>the pathname as a URL</returns>
		/// <exception cref="java.io.FileNotFoundException">if 'webapps' directory cannot be found on CLASSPATH.
		/// 	</exception>
		protected internal string getWebAppsPath(string appName)
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
		[System.Obsolete]
		public int getPort()
		{
			return webServer.getConnectors()[0].getLocalPort();
		}

		/// <summary>Get the address that corresponds to a particular connector.</summary>
		/// <returns>
		/// the corresponding address for the connector, or null if there's no
		/// such connector or the connector is not bounded.
		/// </returns>
		public java.net.InetSocketAddress getConnectorAddress(int index)
		{
			com.google.common.@base.Preconditions.checkArgument(index >= 0);
			if (index > webServer.getConnectors().Length)
			{
				return null;
			}
			org.mortbay.jetty.Connector c = webServer.getConnectors()[index];
			if (c.getLocalPort() == -1)
			{
				// The connector is not bounded
				return null;
			}
			return new java.net.InetSocketAddress(c.getHost(), c.getLocalPort());
		}

		/// <summary>Set the min, max number of worker threads (simultaneous connections).</summary>
		public void setThreads(int min, int max)
		{
			org.mortbay.thread.QueuedThreadPool pool = (org.mortbay.thread.QueuedThreadPool)webServer
				.getThreadPool();
			pool.setMinThreads(min);
			pool.setMaxThreads(max);
		}

		/// <exception cref="System.IO.IOException"/>
		private void initSpnego(org.apache.hadoop.conf.Configuration conf, string hostName
			, string usernameConfKey, string keytabConfKey)
		{
			System.Collections.Generic.IDictionary<string, string> @params = new System.Collections.Generic.Dictionary
				<string, string>();
			string principalInConf = conf.get(usernameConfKey);
			if (principalInConf != null && !principalInConf.isEmpty())
			{
				@params["kerberos.principal"] = org.apache.hadoop.security.SecurityUtil.getServerPrincipal
					(principalInConf, hostName);
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
		public void start()
		{
			try
			{
				try
				{
					openListeners();
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
				foreach (org.mortbay.jetty.Handler handler in handlers)
				{
					if (handler.isFailed())
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
				throw (System.IO.IOException)new java.io.InterruptedIOException("Interrupted while starting HTTP server"
					).initCause(e);
			}
			catch (System.Exception e)
			{
				throw new System.IO.IOException("Problem starting http server", e);
			}
		}

		private void loadListeners()
		{
			foreach (org.mortbay.jetty.Connector c in listeners)
			{
				webServer.addConnector(c);
			}
		}

		/// <summary>Open the main listener for the server</summary>
		/// <exception cref="System.Exception"/>
		internal void openListeners()
		{
			foreach (org.mortbay.jetty.Connector listener in listeners)
			{
				if (listener.getLocalPort() != -1)
				{
					// This listener is either started externally or has been bound
					continue;
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
						LOG.info("Jetty bound to port " + listener.getLocalPort());
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
		}

		/// <summary>stop the server</summary>
		/// <exception cref="System.Exception"/>
		public void stop()
		{
			org.mortbay.util.MultiException exception = null;
			foreach (org.mortbay.jetty.Connector c in listeners)
			{
				try
				{
					c.close();
				}
				catch (System.Exception e)
				{
					LOG.error("Error while stopping listener for webapp" + webAppContext.getDisplayName
						(), e);
					exception = addMultiException(exception, e);
				}
			}
			try
			{
				// explicitly destroy the secrete provider
				secretProvider.destroy();
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
		public void join()
		{
			webServer.join();
		}

		/// <summary>Test for the availability of the web server</summary>
		/// <returns>true if the web server is started, false otherwise</returns>
		public bool isAlive()
		{
			return webServer != null && webServer.isStarted();
		}

		public override string ToString()
		{
			com.google.common.@base.Preconditions.checkState(!listeners.isEmpty());
			java.lang.StringBuilder sb = new java.lang.StringBuilder("HttpServer (").Append(isAlive
				() ? STATE_DESCRIPTION_ALIVE : STATE_DESCRIPTION_NOT_LIVE).Append("), listening at:"
				);
			foreach (org.mortbay.jetty.Connector l in listeners)
			{
				sb.Append(l.getHost()).Append(":").Append(l.getPort()).Append("/,");
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
				response.sendError(javax.servlet.http.HttpServletResponse.SC_FORBIDDEN, "Unauthenticated users are not "
					 + "authorized to access this page.");
				return false;
			}
			if (servletContext.getAttribute(ADMINS_ACL) != null && !userHasAdministratorAccess
				(servletContext, remoteUser))
			{
				response.sendError(javax.servlet.http.HttpServletResponse.SC_FORBIDDEN, "User " +
					 remoteUser + " is unauthorized to access this page.");
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
				if (!org.apache.hadoop.http.HttpServer2.isInstrumentationAccessAllowed(getServletContext
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
					return new _Enumeration_1139(this);
				}

				private sealed class _Enumeration_1139 : java.util.Enumeration<string>
				{
					public _Enumeration_1139()
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
				javax.servlet.http.HttpServletRequestWrapper quoted = new org.apache.hadoop.http.HttpServer2.QuotingInputFilter.RequestQuoter
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
