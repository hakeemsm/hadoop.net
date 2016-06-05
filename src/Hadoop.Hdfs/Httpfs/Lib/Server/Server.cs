using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Lib.Util;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Server
{
	/// <summary>
	/// A Server class provides standard configuration, logging and
	/// <see cref="Service"/>
	/// lifecyle management.
	/// <p>
	/// A Server normally has a home directory, a configuration directory, a temp
	/// directory and logs directory.
	/// <p>
	/// The Server configuration is loaded from 2 overlapped files,
	/// <code>#SERVER#-default.xml</code> and <code>#SERVER#-site.xml</code>. The
	/// default file is loaded from the classpath, the site file is laoded from the
	/// configuration directory.
	/// <p>
	/// The Server collects all configuration properties prefixed with
	/// <code>#SERVER#</code>. The property names are then trimmed from the
	/// <code>#SERVER#</code> prefix.
	/// <p>
	/// The Server log configuration is loaded from the
	/// <code>#SERVICE#-log4j.properties</code> file in the configuration directory.
	/// <p>
	/// The lifecycle of server is defined in by
	/// <see cref="Status"/>
	/// enum.
	/// When a server is create, its status is UNDEF, when being initialized it is
	/// BOOTING, once initialization is complete by default transitions to NORMAL.
	/// The <code>#SERVER#.startup.status</code> configuration property can be used
	/// to specify a different startup status (NORMAL, ADMIN or HALTED).
	/// <p>
	/// Services classes are defined in the <code>#SERVER#.services</code> and
	/// <code>#SERVER#.services.ext</code> properties. They are loaded in order
	/// (services first, then services.ext).
	/// <p>
	/// Before initializing the services, they are traversed and duplicate service
	/// interface are removed from the service list. The last service using a given
	/// interface wins (this enables a simple override mechanism).
	/// <p>
	/// After the services have been resoloved by interface de-duplication they are
	/// initialized in order. Once all services are initialized they are
	/// post-initialized (this enables late/conditional service bindings).
	/// </summary>
	public class Server
	{
		private Logger log;

		/// <summary>Server property name that defines the service classes.</summary>
		public const string ConfServices = "services";

		/// <summary>Server property name that defines the service extension classes.</summary>
		public const string ConfServicesExt = "services.ext";

		/// <summary>Server property name that defines server startup status.</summary>
		public const string ConfStartupStatus = "startup.status";

		/// <summary>Enumeration that defines the server status.</summary>
		[System.Serializable]
		public sealed class Status
		{
			public static readonly Server.Status Undef = new Server.Status(false, false);

			public static readonly Server.Status Booting = new Server.Status(false, true);

			public static readonly Server.Status Halted = new Server.Status(true, true);

			public static readonly Server.Status Admin = new Server.Status(true, true);

			public static readonly Server.Status Normal = new Server.Status(true, true);

			public static readonly Server.Status ShuttingDown = new Server.Status(false, true
				);

			public static readonly Server.Status Shutdown = new Server.Status(false, false);

			private bool settable;

			private bool operational;

			/// <summary>Status constructor.</summary>
			/// <param name="settable">indicates if the status is settable.</param>
			/// <param name="operational">
			/// indicates if the server is operational
			/// when in this status.
			/// </param>
			private Status(bool settable, bool operational)
			{
				this.settable = settable;
				this.operational = operational;
			}

			/// <summary>Returns if this server status is operational.</summary>
			/// <returns>if this server status is operational.</returns>
			public bool IsOperational()
			{
				return Server.Status.operational;
			}
		}

		/// <summary>
		/// Name of the log4j configuration file the Server will load from the
		/// classpath if the <code>#SERVER#-log4j.properties</code> is not defined
		/// in the server configuration directory.
		/// </summary>
		public const string DefaultLog4jProperties = "default-log4j.properties";

		private Server.Status status;

		private string name;

		private string homeDir;

		private string configDir;

		private string logDir;

		private string tempDir;

		private Configuration config;

		private IDictionary<Type, Service> services = new LinkedHashMap<Type, Service>();

		/// <summary>Creates a server instance.</summary>
		/// <remarks>
		/// Creates a server instance.
		/// <p>
		/// The config, log and temp directories are all under the specified home directory.
		/// </remarks>
		/// <param name="name">server name.</param>
		/// <param name="homeDir">server home directory.</param>
		public Server(string name, string homeDir)
			: this(name, homeDir, null)
		{
		}

		/// <summary>Creates a server instance.</summary>
		/// <param name="name">server name.</param>
		/// <param name="homeDir">server home directory.</param>
		/// <param name="configDir">config directory.</param>
		/// <param name="logDir">log directory.</param>
		/// <param name="tempDir">temp directory.</param>
		public Server(string name, string homeDir, string configDir, string logDir, string
			 tempDir)
			: this(name, homeDir, configDir, logDir, tempDir, null)
		{
		}

		/// <summary>Creates a server instance.</summary>
		/// <remarks>
		/// Creates a server instance.
		/// <p>
		/// The config, log and temp directories are all under the specified home directory.
		/// <p>
		/// It uses the provided configuration instead loading it from the config dir.
		/// </remarks>
		/// <param name="name">server name.</param>
		/// <param name="homeDir">server home directory.</param>
		/// <param name="config">server configuration.</param>
		public Server(string name, string homeDir, Configuration config)
			: this(name, homeDir, homeDir + "/conf", homeDir + "/log", homeDir + "/temp", config
				)
		{
		}

		/// <summary>Creates a server instance.</summary>
		/// <remarks>
		/// Creates a server instance.
		/// <p>
		/// It uses the provided configuration instead loading it from the config dir.
		/// </remarks>
		/// <param name="name">server name.</param>
		/// <param name="homeDir">server home directory.</param>
		/// <param name="configDir">config directory.</param>
		/// <param name="logDir">log directory.</param>
		/// <param name="tempDir">temp directory.</param>
		/// <param name="config">server configuration.</param>
		public Server(string name, string homeDir, string configDir, string logDir, string
			 tempDir, Configuration config)
		{
			this.name = StringUtils.ToLowerCase(Check.NotEmpty(name, "name").Trim());
			this.homeDir = Check.NotEmpty(homeDir, "homeDir");
			this.configDir = Check.NotEmpty(configDir, "configDir");
			this.logDir = Check.NotEmpty(logDir, "logDir");
			this.tempDir = Check.NotEmpty(tempDir, "tempDir");
			CheckAbsolutePath(homeDir, "homeDir");
			CheckAbsolutePath(configDir, "configDir");
			CheckAbsolutePath(logDir, "logDir");
			CheckAbsolutePath(tempDir, "tempDir");
			if (config != null)
			{
				this.config = new Configuration(false);
				ConfigurationUtils.Copy(config, this.config);
			}
			status = Server.Status.Undef;
		}

		/// <summary>Validates that the specified value is an absolute path (starts with '/').
		/// 	</summary>
		/// <param name="value">value to verify it is an absolute path.</param>
		/// <param name="name">
		/// name to use in the exception if the value is not an absolute
		/// path.
		/// </param>
		/// <returns>the value.</returns>
		/// <exception cref="System.ArgumentException">
		/// thrown if the value is not an absolute
		/// path.
		/// </exception>
		private string CheckAbsolutePath(string value, string name)
		{
			if (!new FilePath(value).IsAbsolute())
			{
				throw new ArgumentException(MessageFormat.Format("[{0}] must be an absolute path [{1}]"
					, name, value));
			}
			return value;
		}

		/// <summary>Returns the current server status.</summary>
		/// <returns>the current server status.</returns>
		public virtual Server.Status GetStatus()
		{
			return status;
		}

		/// <summary>Sets a new server status.</summary>
		/// <remarks>
		/// Sets a new server status.
		/// <p>
		/// The status must be settable.
		/// <p>
		/// All services will be notified o the status change via the
		/// <see cref="Service.ServerStatusChange(Status, Status)"/>
		/// method. If a service
		/// throws an exception during the notification, the server will be destroyed.
		/// </remarks>
		/// <param name="status">status to set.</param>
		/// <exception cref="ServerException">
		/// thrown if the service has been destroy because of
		/// a failed notification to a service.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServerException"/>
		public virtual void SetStatus(Server.Status status)
		{
			Check.NotNull(status, "status");
			if (status.settable)
			{
				if (status != this.status)
				{
					Server.Status oldStatus = this.status;
					this.status = status;
					foreach (Service service in services.Values)
					{
						try
						{
							service.ServerStatusChange(oldStatus, status);
						}
						catch (Exception ex)
						{
							log.Error("Service [{}] exception during status change to [{}] -server shutting down-,  {}"
								, new object[] { service.GetInterface().Name, status, ex.Message, ex });
							Destroy();
							throw new ServerException(ServerException.ERROR.S11, service.GetInterface().Name, 
								status, ex.Message, ex);
						}
					}
				}
			}
			else
			{
				throw new ArgumentException("Status [" + status + " is not settable");
			}
		}

		/// <summary>Verifies the server is operational.</summary>
		/// <exception cref="System.InvalidOperationException">thrown if the server is not operational.
		/// 	</exception>
		protected internal virtual void EnsureOperational()
		{
			if (!GetStatus().IsOperational())
			{
				throw new InvalidOperationException("Server is not running");
			}
		}

		/// <summary>
		/// Convenience method that returns a resource as inputstream from the
		/// classpath.
		/// </summary>
		/// <remarks>
		/// Convenience method that returns a resource as inputstream from the
		/// classpath.
		/// <p>
		/// It first attempts to use the Thread's context classloader and if not
		/// set it uses the <code>ClassUtils</code> classloader.
		/// </remarks>
		/// <param name="name">resource to retrieve.</param>
		/// <returns>
		/// inputstream with the resource, NULL if the resource does not
		/// exist.
		/// </returns>
		internal static InputStream GetResource(string name)
		{
			Check.NotEmpty(name, "name");
			ClassLoader cl = Sharpen.Thread.CurrentThread().GetContextClassLoader();
			if (cl == null)
			{
				cl = typeof(Org.Apache.Hadoop.Lib.Server.Server).GetClassLoader();
			}
			return cl.GetResourceAsStream(name);
		}

		/// <summary>Initializes the Server.</summary>
		/// <remarks>
		/// Initializes the Server.
		/// <p>
		/// The initialization steps are:
		/// <ul>
		/// <li>It verifies the service home and temp directories exist</li>
		/// <li>Loads the Server <code>#SERVER#-default.xml</code>
		/// configuration file from the classpath</li>
		/// <li>Initializes log4j logging. If the
		/// <code>#SERVER#-log4j.properties</code> file does not exist in the config
		/// directory it load <code>default-log4j.properties</code> from the classpath
		/// </li>
		/// <li>Loads the <code>#SERVER#-site.xml</code> file from the server config
		/// directory and merges it with the default configuration.</li>
		/// <li>Loads the services</li>
		/// <li>Initializes the services</li>
		/// <li>Post-initializes the services</li>
		/// <li>Sets the server startup status</li>
		/// </ul>
		/// </remarks>
		/// <exception cref="ServerException">thrown if the server could not be initialized.</exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServerException"/>
		public virtual void Init()
		{
			if (status != Server.Status.Undef)
			{
				throw new InvalidOperationException("Server already initialized");
			}
			status = Server.Status.Booting;
			VerifyDir(homeDir);
			VerifyDir(tempDir);
			Properties serverInfo = new Properties();
			try
			{
				InputStream @is = GetResource(name + ".properties");
				serverInfo.Load(@is);
				@is.Close();
			}
			catch (IOException)
			{
				throw new RuntimeException("Could not load server information file: " + name + ".properties"
					);
			}
			InitLog();
			log.Info("++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			log.Info("Server [{}] starting", name);
			log.Info("  Built information:");
			log.Info("    Version           : {}", serverInfo.GetProperty(name + ".version", 
				"undef"));
			log.Info("    Source Repository : {}", serverInfo.GetProperty(name + ".source.repository"
				, "undef"));
			log.Info("    Source Revision   : {}", serverInfo.GetProperty(name + ".source.revision"
				, "undef"));
			log.Info("    Built by          : {}", serverInfo.GetProperty(name + ".build.username"
				, "undef"));
			log.Info("    Built timestamp   : {}", serverInfo.GetProperty(name + ".build.timestamp"
				, "undef"));
			log.Info("  Runtime information:");
			log.Info("    Home   dir: {}", homeDir);
			log.Info("    Config dir: {}", (config == null) ? configDir : "-");
			log.Info("    Log    dir: {}", logDir);
			log.Info("    Temp   dir: {}", tempDir);
			InitConfig();
			log.Debug("Loading services");
			IList<Service> list = LoadServices();
			try
			{
				log.Debug("Initializing services");
				InitServices(list);
				log.Info("Services initialized");
			}
			catch (ServerException ex)
			{
				log.Error("Services initialization failure, destroying initialized services");
				DestroyServices();
				throw;
			}
			Server.Status status = Server.Status.ValueOf(GetConfig().Get(GetPrefixedName(ConfStartupStatus
				), Server.Status.Normal.ToString()));
			SetStatus(status);
			log.Info("Server [{}] started!, status [{}]", name, status);
		}

		/// <summary>Verifies the specified directory exists.</summary>
		/// <param name="dir">directory to verify it exists.</param>
		/// <exception cref="ServerException">
		/// thrown if the directory does not exist or it the
		/// path it is not a directory.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServerException"/>
		private void VerifyDir(string dir)
		{
			FilePath file = new FilePath(dir);
			if (!file.Exists())
			{
				throw new ServerException(ServerException.ERROR.S01, dir);
			}
			if (!file.IsDirectory())
			{
				throw new ServerException(ServerException.ERROR.S02, dir);
			}
		}

		/// <summary>Initializes Log4j logging.</summary>
		/// <exception cref="ServerException">thrown if Log4j could not be initialized.</exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServerException"/>
		protected internal virtual void InitLog()
		{
			VerifyDir(logDir);
			LogManager.ResetConfiguration();
			FilePath log4jFile = new FilePath(configDir, name + "-log4j.properties");
			if (log4jFile.Exists())
			{
				PropertyConfigurator.ConfigureAndWatch(log4jFile.ToString(), 10 * 1000);
				//every 10 secs
				log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Lib.Server.Server));
			}
			else
			{
				Properties props = new Properties();
				try
				{
					InputStream @is = GetResource(DefaultLog4jProperties);
					try
					{
						props.Load(@is);
					}
					finally
					{
						@is.Close();
					}
				}
				catch (IOException ex)
				{
					throw new ServerException(ServerException.ERROR.S03, DefaultLog4jProperties, ex.Message
						, ex);
				}
				PropertyConfigurator.Configure(props);
				log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Lib.Server.Server));
				log.Warn("Log4j [{}] configuration file not found, using default configuration from classpath"
					, log4jFile);
			}
		}

		/// <summary>Loads and inializes the server configuration.</summary>
		/// <exception cref="ServerException">thrown if the configuration could not be loaded/initialized.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServerException"/>
		protected internal virtual void InitConfig()
		{
			VerifyDir(configDir);
			FilePath file = new FilePath(configDir);
			Configuration defaultConf;
			string defaultConfig = name + "-default.xml";
			ClassLoader classLoader = Sharpen.Thread.CurrentThread().GetContextClassLoader();
			InputStream inputStream = classLoader.GetResourceAsStream(defaultConfig);
			if (inputStream == null)
			{
				log.Warn("Default configuration file not available in classpath [{}]", defaultConfig
					);
				defaultConf = new Configuration(false);
			}
			else
			{
				try
				{
					defaultConf = new Configuration(false);
					ConfigurationUtils.Load(defaultConf, inputStream);
				}
				catch (Exception ex)
				{
					throw new ServerException(ServerException.ERROR.S03, defaultConfig, ex.Message, ex
						);
				}
			}
			if (config == null)
			{
				Configuration siteConf;
				FilePath siteFile = new FilePath(file, name + "-site.xml");
				if (!siteFile.Exists())
				{
					log.Warn("Site configuration file [{}] not found in config directory", siteFile);
					siteConf = new Configuration(false);
				}
				else
				{
					if (!siteFile.IsFile())
					{
						throw new ServerException(ServerException.ERROR.S05, siteFile.GetAbsolutePath());
					}
					try
					{
						log.Debug("Loading site configuration from [{}]", siteFile);
						inputStream = new FileInputStream(siteFile);
						siteConf = new Configuration(false);
						ConfigurationUtils.Load(siteConf, inputStream);
					}
					catch (IOException ex)
					{
						throw new ServerException(ServerException.ERROR.S06, siteFile, ex.Message, ex);
					}
				}
				config = new Configuration(false);
				ConfigurationUtils.Copy(siteConf, config);
			}
			ConfigurationUtils.InjectDefaults(defaultConf, config);
			foreach (string name in Runtime.GetProperties().StringPropertyNames())
			{
				string value = Runtime.GetProperty(name);
				if (name.StartsWith(GetPrefix() + "."))
				{
					config.Set(name, value);
					if (name.EndsWith(".password") || name.EndsWith(".secret"))
					{
						value = "*MASKED*";
					}
					log.Info("System property sets  {}: {}", name, value);
				}
			}
			log.Debug("Loaded Configuration:");
			log.Debug("------------------------------------------------------");
			foreach (KeyValuePair<string, string> entry in config)
			{
				string name_1 = entry.Key;
				string value = config.Get(entry.Key);
				if (name_1.EndsWith(".password") || name_1.EndsWith(".secret"))
				{
					value = "*MASKED*";
				}
				log.Debug("  {}: {}", entry.Key, value);
			}
			log.Debug("------------------------------------------------------");
		}

		/// <summary>Loads the specified services.</summary>
		/// <param name="classes">services classes to load.</param>
		/// <param name="list">
		/// list of loaded service in order of appearance in the
		/// configuration.
		/// </param>
		/// <exception cref="ServerException">thrown if a service class could not be loaded.</exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServerException"/>
		private void LoadServices(Type[] classes, IList<Service> list)
		{
			foreach (Type klass in classes)
			{
				try
				{
					Service service = (Service)System.Activator.CreateInstance(klass);
					log.Debug("Loading service [{}] implementation [{}]", service.GetInterface(), service
						.GetType());
					if (!service.GetInterface().IsInstanceOfType(service))
					{
						throw new ServerException(ServerException.ERROR.S04, klass, service.GetInterface(
							).FullName);
					}
					list.AddItem(service);
				}
				catch (ServerException ex)
				{
					throw;
				}
				catch (Exception ex)
				{
					throw new ServerException(ServerException.ERROR.S07, klass, ex.Message, ex);
				}
			}
		}

		/// <summary>
		/// Loads services defined in <code>services</code> and
		/// <code>services.ext</code> and de-dups them.
		/// </summary>
		/// <returns>List of final services to initialize.</returns>
		/// <exception cref="ServerException">throw if the services could not be loaded.</exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServerException"/>
		protected internal virtual IList<Service> LoadServices()
		{
			try
			{
				IDictionary<Type, Service> map = new LinkedHashMap<Type, Service>();
				Type[] classes = GetConfig().GetClasses(GetPrefixedName(ConfServices));
				Type[] classesExt = GetConfig().GetClasses(GetPrefixedName(ConfServicesExt));
				IList<Service> list = new AList<Service>();
				LoadServices(classes, list);
				LoadServices(classesExt, list);
				//removing duplicate services, strategy: last one wins
				foreach (Service service in list)
				{
					if (map.Contains(service.GetInterface()))
					{
						log.Debug("Replacing service [{}] implementation [{}]", service.GetInterface(), service
							.GetType());
					}
					map[service.GetInterface()] = service;
				}
				list = new AList<Service>();
				foreach (KeyValuePair<Type, Service> entry in map)
				{
					list.AddItem(entry.Value);
				}
				return list;
			}
			catch (RuntimeException ex)
			{
				throw new ServerException(ServerException.ERROR.S08, ex.Message, ex);
			}
		}

		/// <summary>Initializes the list of services.</summary>
		/// <param name="services">
		/// services to initialized, it must be a de-dupped list of
		/// services.
		/// </param>
		/// <exception cref="ServerException">thrown if the services could not be initialized.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServerException"/>
		protected internal virtual void InitServices(IList<Service> services)
		{
			foreach (Service service in services)
			{
				log.Debug("Initializing service [{}]", service.GetInterface());
				CheckServiceDependencies(service);
				service.Init(this);
				this.services[service.GetInterface()] = service;
			}
			foreach (Service service_1 in services)
			{
				service_1.PostInit();
			}
		}

		/// <summary>Checks if all service dependencies of a service are available.</summary>
		/// <param name="service">service to check if all its dependencies are available.</param>
		/// <exception cref="ServerException">thrown if a service dependency is missing.</exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServerException"/>
		protected internal virtual void CheckServiceDependencies(Service service)
		{
			if (service.GetServiceDependencies() != null)
			{
				foreach (Type dependency in service.GetServiceDependencies())
				{
					if (services[dependency] == null)
					{
						throw new ServerException(ServerException.ERROR.S10, service.GetType(), dependency
							);
					}
				}
			}
		}

		/// <summary>Destroys the server services.</summary>
		protected internal virtual void DestroyServices()
		{
			IList<Service> list = new AList<Service>(services.Values);
			Sharpen.Collections.Reverse(list);
			foreach (Service service in list)
			{
				try
				{
					log.Debug("Destroying service [{}]", service.GetInterface());
					service.Destroy();
				}
				catch (Exception ex)
				{
					log.Error("Could not destroy service [{}], {}", new object[] { service.GetInterface
						(), ex.Message, ex });
				}
			}
			log.Info("Services destroyed");
		}

		/// <summary>Destroys the server.</summary>
		/// <remarks>
		/// Destroys the server.
		/// <p>
		/// All services are destroyed in reverse order of initialization, then the
		/// Log4j framework is shutdown.
		/// </remarks>
		public virtual void Destroy()
		{
			EnsureOperational();
			DestroyServices();
			log.Info("Server [{}] shutdown!", name);
			log.Info("======================================================");
			if (!bool.GetBoolean("test.circus"))
			{
				LogManager.Shutdown();
			}
			status = Server.Status.Shutdown;
		}

		/// <summary>Returns the name of the server.</summary>
		/// <returns>the server name.</returns>
		public virtual string GetName()
		{
			return name;
		}

		/// <summary>Returns the server prefix for server configuration properties.</summary>
		/// <remarks>
		/// Returns the server prefix for server configuration properties.
		/// <p>
		/// By default it is the server name.
		/// </remarks>
		/// <returns>the prefix for server configuration properties.</returns>
		public virtual string GetPrefix()
		{
			return GetName();
		}

		/// <summary>Returns the prefixed name of a server property.</summary>
		/// <param name="name">of the property.</param>
		/// <returns>prefixed name of the property.</returns>
		public virtual string GetPrefixedName(string name)
		{
			return GetPrefix() + "." + Check.NotEmpty(name, "name");
		}

		/// <summary>Returns the server home dir.</summary>
		/// <returns>the server home dir.</returns>
		public virtual string GetHomeDir()
		{
			return homeDir;
		}

		/// <summary>Returns the server config dir.</summary>
		/// <returns>the server config dir.</returns>
		public virtual string GetConfigDir()
		{
			return configDir;
		}

		/// <summary>Returns the server log dir.</summary>
		/// <returns>the server log dir.</returns>
		public virtual string GetLogDir()
		{
			return logDir;
		}

		/// <summary>Returns the server temp dir.</summary>
		/// <returns>the server temp dir.</returns>
		public virtual string GetTempDir()
		{
			return tempDir;
		}

		/// <summary>Returns the server configuration.</summary>
		/// <returns>the server configuration.</returns>
		public virtual Configuration GetConfig()
		{
			return config;
		}

		/// <summary>
		/// Returns the
		/// <see cref="Service"/>
		/// associated to the specified interface.
		/// </summary>
		/// <param name="serviceKlass">service interface.</param>
		/// <returns>the service implementation.</returns>
		public virtual T Get<T>()
		{
			System.Type serviceKlass = typeof(T);
			EnsureOperational();
			Check.NotNull(serviceKlass, "serviceKlass");
			return (T)services[serviceKlass];
		}

		/// <summary>Adds a service programmatically.</summary>
		/// <remarks>
		/// Adds a service programmatically.
		/// <p>
		/// If a service with the same interface exists, it will be destroyed and
		/// removed before the given one is initialized and added.
		/// <p>
		/// If an exception is thrown the server is destroyed.
		/// </remarks>
		/// <param name="klass">service class to add.</param>
		/// <exception cref="ServerException">
		/// throw if the service could not initialized/added
		/// to the server.
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServerException"/>
		public virtual void SetService(Type klass)
		{
			EnsureOperational();
			Check.NotNull(klass, "serviceKlass");
			if (GetStatus() == Server.Status.ShuttingDown)
			{
				throw new InvalidOperationException("Server shutting down");
			}
			try
			{
				Service newService = System.Activator.CreateInstance(klass);
				Service oldService = services[newService.GetInterface()];
				if (oldService != null)
				{
					try
					{
						oldService.Destroy();
					}
					catch (Exception ex)
					{
						log.Error("Could not destroy service [{}], {}", new object[] { oldService.GetInterface
							(), ex.Message, ex });
					}
				}
				newService.Init(this);
				services[newService.GetInterface()] = newService;
			}
			catch (Exception ex)
			{
				log.Error("Could not set service [{}] programmatically -server shutting down-, {}"
					, klass, ex);
				Destroy();
				throw new ServerException(ServerException.ERROR.S09, klass, ex.Message, ex);
			}
		}
	}
}
