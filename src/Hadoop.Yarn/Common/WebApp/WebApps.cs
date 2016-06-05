using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	/// <summary>Helpers to create an embedded webapp.</summary>
	/// <remarks>
	/// Helpers to create an embedded webapp.
	/// <b>Quick start:</b>
	/// <pre>
	/// WebApp wa = WebApps.$for(myApp).start();</pre>
	/// Starts a webapp with default routes binds to 0.0.0.0 (all network interfaces)
	/// on an ephemeral port, which can be obtained with:<pre>
	/// int port = wa.port();</pre>
	/// <b>With more options:</b>
	/// <pre>
	/// WebApp wa = WebApps.$for(myApp).at(address, port).
	/// with(configuration).
	/// start(new WebApp() {
	/// &#064;Override public void setup() {
	/// route("/foo/action", FooController.class);
	/// route("/foo/:id", FooController.class, "show");
	/// }
	/// });</pre>
	/// </remarks>
	public class WebApps
	{
		internal static readonly Logger Log = LoggerFactory.GetLogger(typeof(WebApps));

		public class Builder<T>
		{
			internal class ServletStruct
			{
				public Type clazz;

				public string name;

				public string spec;
			}

			internal readonly string name;

			internal readonly string wsName;

			internal readonly Type api;

			internal readonly T application;

			internal string bindAddress = "0.0.0.0";

			internal int port = 0;

			internal bool findPort = false;

			internal Configuration conf;

			internal HttpConfig.Policy httpPolicy = null;

			internal bool devMode = false;

			private string spnegoPrincipalKey;

			private string spnegoKeytabKey;

			private readonly HashSet<WebApps.Builder.ServletStruct> servlets = new HashSet<WebApps.Builder.ServletStruct
				>();

			private readonly Dictionary<string, object> attributes = new Dictionary<string, object
				>();

			internal Builder(string name, Type api, T application, string wsName)
			{
				this.name = name;
				this.api = api;
				this.application = application;
				this.wsName = wsName;
			}

			internal Builder(string name, Type api, T application)
				: this(name, api, application, null)
			{
			}

			public virtual WebApps.Builder<T> At(string bindAddress)
			{
				string[] parts = StringUtils.Split(bindAddress, ':');
				if (parts.Length == 2)
				{
					int port = System.Convert.ToInt32(parts[1]);
					return At(parts[0], port, port == 0);
				}
				return At(bindAddress, 0, true);
			}

			public virtual WebApps.Builder<T> At(int port)
			{
				return At("0.0.0.0", port, port == 0);
			}

			public virtual WebApps.Builder<T> At(string address, int port, bool findPort)
			{
				this.bindAddress = Preconditions.CheckNotNull(address, "bind address");
				this.port = port;
				this.findPort = findPort;
				return this;
			}

			public virtual WebApps.Builder<T> WithAttribute(string key, object value)
			{
				attributes[key] = value;
				return this;
			}

			public virtual WebApps.Builder<T> WithServlet(string name, string pathSpec, Type 
				servlet)
			{
				WebApps.Builder.ServletStruct @struct = new WebApps.Builder.ServletStruct();
				@struct.clazz = servlet;
				@struct.name = name;
				@struct.spec = pathSpec;
				servlets.AddItem(@struct);
				return this;
			}

			public virtual WebApps.Builder<T> With(Configuration conf)
			{
				this.conf = conf;
				return this;
			}

			public virtual WebApps.Builder<T> WithHttpPolicy(Configuration conf, HttpConfig.Policy
				 httpPolicy)
			{
				this.conf = conf;
				this.httpPolicy = httpPolicy;
				return this;
			}

			public virtual WebApps.Builder<T> WithHttpSpnegoPrincipalKey(string spnegoPrincipalKey
				)
			{
				this.spnegoPrincipalKey = spnegoPrincipalKey;
				return this;
			}

			public virtual WebApps.Builder<T> WithHttpSpnegoKeytabKey(string spnegoKeytabKey)
			{
				this.spnegoKeytabKey = spnegoKeytabKey;
				return this;
			}

			public virtual WebApps.Builder<T> InDevMode()
			{
				devMode = true;
				return this;
			}

			public virtual WebApp Build(WebApp webapp)
			{
				if (webapp == null)
				{
					webapp = new _WebApp_171();
				}
				// Defaults should be fine in usual cases
				webapp.SetName(name);
				webapp.SetWebServices(wsName);
				string basePath = "/" + name;
				webapp.SetRedirectPath(basePath);
				IList<string> pathList = new AList<string>();
				if (basePath.Equals("/"))
				{
					webapp.AddServePathSpec("/*");
					pathList.AddItem("/*");
				}
				else
				{
					webapp.AddServePathSpec(basePath);
					webapp.AddServePathSpec(basePath + "/*");
					pathList.AddItem(basePath + "/*");
				}
				if (wsName != null && !wsName.Equals(basePath))
				{
					if (wsName.Equals("/"))
					{
						webapp.AddServePathSpec("/*");
						pathList.AddItem("/*");
					}
					else
					{
						webapp.AddServePathSpec("/" + wsName);
						webapp.AddServePathSpec("/" + wsName + "/*");
						pathList.AddItem("/" + wsName + "/*");
					}
				}
				if (conf == null)
				{
					conf = new Configuration();
				}
				try
				{
					if (application != null)
					{
						webapp.SetHostClass(application.GetType());
					}
					else
					{
						string cls = InferHostClass();
						Log.Debug("setting webapp host class to {}", cls);
						webapp.SetHostClass(Sharpen.Runtime.GetType(cls));
					}
					if (devMode)
					{
						if (port > 0)
						{
							try
							{
								new Uri("http://localhost:" + port + "/__stop").GetContent();
								Log.Info("stopping existing webapp instance");
								Sharpen.Thread.Sleep(100);
							}
							catch (ConnectException e)
							{
								Log.Info("no existing webapp instance found: {}", e.ToString());
							}
							catch (Exception e)
							{
								// should not be fatal
								Log.Warn("error stopping existing instance: {}", e.ToString());
							}
						}
						else
						{
							Log.Error("dev mode does NOT work with ephemeral port!");
							System.Environment.Exit(1);
						}
					}
					string httpScheme;
					if (this.httpPolicy == null)
					{
						httpScheme = WebAppUtils.GetHttpSchemePrefix(conf);
					}
					else
					{
						httpScheme = (httpPolicy == HttpConfig.Policy.HttpsOnly) ? WebAppUtils.HttpsPrefix
							 : WebAppUtils.HttpPrefix;
					}
					HttpServer2.Builder builder = new HttpServer2.Builder().SetName(name).AddEndpoint
						(URI.Create(httpScheme + bindAddress + ":" + port)).SetConf(conf).SetFindPort(findPort
						).SetACL(new AccessControlList(conf.Get(YarnConfiguration.YarnAdminAcl, YarnConfiguration
						.DefaultYarnAdminAcl))).SetPathSpec(Sharpen.Collections.ToArray(pathList, new string
						[0]));
					bool hasSpnegoConf = spnegoPrincipalKey != null && conf.Get(spnegoPrincipalKey) !=
						 null && spnegoKeytabKey != null && conf.Get(spnegoKeytabKey) != null;
					if (hasSpnegoConf)
					{
						builder.SetUsernameConfKey(spnegoPrincipalKey).SetKeytabConfKey(spnegoKeytabKey).
							SetSecurityEnabled(UserGroupInformation.IsSecurityEnabled());
					}
					if (httpScheme.Equals(WebAppUtils.HttpsPrefix))
					{
						WebAppUtils.LoadSslConfiguration(builder);
					}
					HttpServer2 server = builder.Build();
					foreach (WebApps.Builder.ServletStruct @struct in servlets)
					{
						server.AddServlet(@struct.name, @struct.spec, @struct.clazz);
					}
					foreach (KeyValuePair<string, object> entry in attributes)
					{
						server.SetAttribute(entry.Key, entry.Value);
					}
					HttpServer2.DefineFilter(server.GetWebAppContext(), "guice", typeof(GuiceFilter).
						FullName, null, new string[] { "/*" });
					webapp.SetConf(conf);
					webapp.SetHttpServer(server);
				}
				catch (TypeLoadException e)
				{
					throw new WebAppException("Error starting http server", e);
				}
				catch (IOException e)
				{
					throw new WebAppException("Error starting http server", e);
				}
				Injector injector = Guice.CreateInjector(webapp, new _AbstractModule_280(this));
				Log.Info("Registered webapp guice modules");
				// save a guice filter instance for webapp stop (mostly for unit tests)
				webapp.SetGuiceFilter(injector.GetInstance<GuiceFilter>());
				if (devMode)
				{
					injector.GetInstance<Dispatcher>().SetDevMode(devMode);
					Log.Info("in dev mode!");
				}
				return webapp;
			}

			private sealed class _WebApp_171 : WebApp
			{
				public _WebApp_171()
				{
				}

				public override void Setup()
				{
				}
			}

			private sealed class _AbstractModule_280 : AbstractModule
			{
				public _AbstractModule_280(Builder<T> _enclosing)
				{
					this._enclosing = _enclosing;
				}

				protected override void Configure()
				{
					if (this._enclosing.api != null)
					{
						this.Bind(this._enclosing.api).ToInstance(this._enclosing.application);
					}
				}

				private readonly Builder<T> _enclosing;
			}

			public virtual WebApp Start()
			{
				return Start(null);
			}

			public virtual WebApp Start(WebApp webapp)
			{
				WebApp webApp = Build(webapp);
				HttpServer2 httpServer = webApp.HttpServer();
				try
				{
					httpServer.Start();
					Log.Info("Web app " + name + " started at " + httpServer.GetConnectorAddress(0).Port
						);
				}
				catch (IOException e)
				{
					throw new WebAppException("Error starting http server", e);
				}
				return webApp;
			}

			private string InferHostClass()
			{
				string thisClass = this.GetType().FullName;
				Exception t = new Exception();
				foreach (StackTraceElement e in t.GetStackTrace())
				{
					if (e.GetClassName().Equals(thisClass))
					{
						continue;
					}
					return e.GetClassName();
				}
				Log.Warn("could not infer host class from", t);
				return thisClass;
			}
		}

		/// <summary>Create a new webapp builder.</summary>
		/// <seealso cref="WebApps">for a complete example</seealso>
		/// <?/>
		/// <param name="prefix">of the webapp</param>
		/// <param name="api">the api class for the application</param>
		/// <param name="app">the application instance</param>
		/// <param name="wsPrefix">the prefix for the webservice api for this app</param>
		/// <returns>a webapp builder</returns>
		public static WebApps.Builder<T> $for<T>(string prefix, T app, string wsPrefix)
		{
			System.Type api = typeof(T);
			return new WebApps.Builder<T>(prefix, api, app, wsPrefix);
		}

		/// <summary>Create a new webapp builder.</summary>
		/// <seealso cref="WebApps">for a complete example</seealso>
		/// <?/>
		/// <param name="prefix">of the webapp</param>
		/// <param name="api">the api class for the application</param>
		/// <param name="app">the application instance</param>
		/// <returns>a webapp builder</returns>
		public static WebApps.Builder<T> $for<T>(string prefix, T app)
		{
			System.Type api = typeof(T);
			return new WebApps.Builder<T>(prefix, api, app);
		}

		// Short cut mostly for tests/demos
		public static WebApps.Builder<T> $for<T>(string prefix, T app)
		{
			return $for(prefix, (Type)app.GetType(), app);
		}

		// Ditto
		public static WebApps.Builder<T> $for<T>(T app)
		{
			return $for(string.Empty, app);
		}

		public static WebApps.Builder<T> $for<T>(string prefix)
		{
			return $for(prefix, null, null);
		}
	}
}
