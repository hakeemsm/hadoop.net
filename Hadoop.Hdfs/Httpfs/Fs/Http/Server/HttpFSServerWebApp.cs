using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Lib.Service;
using Org.Apache.Hadoop.Lib.Servlet;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Server
{
	/// <summary>
	/// Bootstrap class that manages the initialization and destruction of the
	/// HttpFSServer server, it is a <code>javax.servlet.ServletContextListener
	/// </code> implementation that is wired in HttpFSServer's WAR
	/// <code>WEB-INF/web.xml</code>.
	/// </summary>
	/// <remarks>
	/// Bootstrap class that manages the initialization and destruction of the
	/// HttpFSServer server, it is a <code>javax.servlet.ServletContextListener
	/// </code> implementation that is wired in HttpFSServer's WAR
	/// <code>WEB-INF/web.xml</code>.
	/// <p>
	/// It provides acces to the server context via the singleton
	/// <see cref="Get()"/>
	/// .
	/// <p>
	/// All the configuration is loaded from configuration properties prefixed
	/// with <code>httpfs.</code>.
	/// </remarks>
	public class HttpFSServerWebApp : ServerWebApp
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.FS.Http.Server.HttpFSServerWebApp
			));

		/// <summary>Server name and prefix for all configuration properties.</summary>
		public const string Name = "httpfs";

		/// <summary>Configuration property that defines HttpFSServer admin group.</summary>
		public const string ConfAdminGroup = "admin.group";

		private static Org.Apache.Hadoop.FS.Http.Server.HttpFSServerWebApp Server;

		private string adminGroup;

		/// <summary>Default constructor.</summary>
		/// <exception cref="System.IO.IOException">
		/// thrown if the home/conf/log/temp directory paths
		/// could not be resolved.
		/// </exception>
		public HttpFSServerWebApp()
			: base(Name)
		{
		}

		/// <summary>Constructor used for testing purposes.</summary>
		public HttpFSServerWebApp(string homeDir, string configDir, string logDir, string
			 tempDir, Configuration config)
			: base(Name, homeDir, configDir, logDir, tempDir, config)
		{
		}

		/// <summary>Constructor used for testing purposes.</summary>
		public HttpFSServerWebApp(string homeDir, Configuration config)
			: base(Name, homeDir, config)
		{
		}

		/// <summary>
		/// Initializes the HttpFSServer server, loads configuration and required
		/// services.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServerException">
		/// thrown if HttpFSServer server could not be
		/// initialized.
		/// </exception>
		public override void Init()
		{
			if (Server != null)
			{
				throw new RuntimeException("HttpFSServer server already initialized");
			}
			Server = this;
			base.Init();
			adminGroup = GetConfig().Get(GetPrefixedName(ConfAdminGroup), "admin");
			Log.Info("Connects to Namenode [{}]", Get().Get<FileSystemAccess>().GetFileSystemConfiguration
				().Get(CommonConfigurationKeysPublic.FsDefaultNameKey));
		}

		/// <summary>Shutdowns all running services.</summary>
		public override void Destroy()
		{
			Server = null;
			base.Destroy();
		}

		/// <summary>
		/// Returns HttpFSServer server singleton, configuration and services are
		/// accessible through it.
		/// </summary>
		/// <returns>the HttpFSServer server singleton.</returns>
		public static Org.Apache.Hadoop.FS.Http.Server.HttpFSServerWebApp Get()
		{
			return Server;
		}

		/// <summary>Returns HttpFSServer admin group.</summary>
		/// <returns>httpfs admin group.</returns>
		public virtual string GetAdminGroup()
		{
			return adminGroup;
		}
	}
}
