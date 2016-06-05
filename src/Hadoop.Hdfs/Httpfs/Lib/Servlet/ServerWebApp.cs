using System;
using System.Net;
using Com.Google.Common.Annotations;
using Javax.Servlet;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Lib.Server;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Servlet
{
	/// <summary>
	/// <see cref="Org.Apache.Hadoop.Lib.Server.Server"/>
	/// subclass that implements <code>ServletContextListener</code>
	/// and uses its lifecycle to start and stop the server.
	/// </summary>
	public abstract class ServerWebApp : Org.Apache.Hadoop.Lib.Server.Server, ServletContextListener
	{
		private const string HomeDir = ".home.dir";

		private const string ConfigDir = ".config.dir";

		private const string LogDir = ".log.dir";

		private const string TempDir = ".temp.dir";

		private const string HttpHostname = ".http.hostname";

		private const string HttpPort = ".http.port";

		public const string SslEnabled = ".ssl.enabled";

		private static ThreadLocal<string> HomeDirTl = new ThreadLocal<string>();

		private IPEndPoint authority;

		/// <summary>Method for testing purposes.</summary>
		public static void SetHomeDirForCurrentThread(string homeDir)
		{
			HomeDirTl.Set(homeDir);
		}

		/// <summary>Constructor for testing purposes.</summary>
		protected internal ServerWebApp(string name, string homeDir, string configDir, string
			 logDir, string tempDir, Configuration config)
			: base(name, homeDir, configDir, logDir, tempDir, config)
		{
		}

		/// <summary>Constructor for testing purposes.</summary>
		protected internal ServerWebApp(string name, string homeDir, Configuration config
			)
			: base(name, homeDir, config)
		{
		}

		/// <summary>Constructor.</summary>
		/// <remarks>
		/// Constructor. Subclasses must have a default constructor specifying
		/// the server name.
		/// <p>
		/// The server name is used to resolve the Java System properties that define
		/// the server home, config, log and temp directories.
		/// <p>
		/// The home directory is looked in the Java System property
		/// <code>#SERVER_NAME#.home.dir</code>.
		/// <p>
		/// The config directory is looked in the Java System property
		/// <code>#SERVER_NAME#.config.dir</code>, if not defined it resolves to
		/// the <code>#SERVER_HOME_DIR#/conf</code> directory.
		/// <p>
		/// The log directory is looked in the Java System property
		/// <code>#SERVER_NAME#.log.dir</code>, if not defined it resolves to
		/// the <code>#SERVER_HOME_DIR#/log</code> directory.
		/// <p>
		/// The temp directory is looked in the Java System property
		/// <code>#SERVER_NAME#.temp.dir</code>, if not defined it resolves to
		/// the <code>#SERVER_HOME_DIR#/temp</code> directory.
		/// </remarks>
		/// <param name="name">server name.</param>
		public ServerWebApp(string name)
			: base(name, GetHomeDir(name), GetDir(name, ConfigDir, GetHomeDir(name) + "/conf"
				), GetDir(name, LogDir, GetHomeDir(name) + "/log"), GetDir(name, TempDir, GetHomeDir
				(name) + "/temp"), null)
		{
		}

		/// <summary>Returns the server home directory.</summary>
		/// <remarks>
		/// Returns the server home directory.
		/// <p>
		/// It is looked up in the Java System property
		/// <code>#SERVER_NAME#.home.dir</code>.
		/// </remarks>
		/// <param name="name">the server home directory.</param>
		/// <returns>the server home directory.</returns>
		internal static string GetHomeDir(string name)
		{
			string homeDir = HomeDirTl.Get();
			if (homeDir == null)
			{
				string sysProp = name + HomeDir;
				homeDir = Runtime.GetProperty(sysProp);
				if (homeDir == null)
				{
					throw new ArgumentException(MessageFormat.Format("System property [{0}] not defined"
						, sysProp));
				}
			}
			return homeDir;
		}

		/// <summary>
		/// Convenience method that looks for Java System property defining a
		/// diretory and if not present defaults to the specified directory.
		/// </summary>
		/// <param name="name">server name, used as prefix of the Java System property.</param>
		/// <param name="dirType">dir type, use as postfix of the Java System property.</param>
		/// <param name="defaultDir">
		/// the default directory to return if the Java System
		/// property <code>name + dirType</code> is not defined.
		/// </param>
		/// <returns>
		/// the directory defined in the Java System property or the
		/// the default directory if the Java System property is not defined.
		/// </returns>
		internal static string GetDir(string name, string dirType, string defaultDir)
		{
			string sysProp = name + dirType;
			return Runtime.GetProperty(sysProp, defaultDir);
		}

		/// <summary>
		/// Initializes the <code>ServletContextListener</code> which initializes
		/// the Server.
		/// </summary>
		/// <param name="event">servelt context event.</param>
		public virtual void ContextInitialized(ServletContextEvent @event)
		{
			try
			{
				Init();
			}
			catch (ServerException ex)
			{
				@event.GetServletContext().Log("ERROR: " + ex.Message);
				throw new RuntimeException(ex);
			}
		}

		/// <summary>Resolves the host and port InetSocketAddress the web server is listening to.
		/// 	</summary>
		/// <remarks>
		/// Resolves the host and port InetSocketAddress the web server is listening to.
		/// <p>
		/// This implementation looks for the following 2 properties:
		/// <ul>
		/// <li>#SERVER_NAME#.http.hostname</li>
		/// <li>#SERVER_NAME#.http.port</li>
		/// </ul>
		/// </remarks>
		/// <returns>the host and port InetSocketAddress the web server is listening to.</returns>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServerException">thrown if any of the above 2 properties is not defined.
		/// 	</exception>
		protected internal virtual IPEndPoint ResolveAuthority()
		{
			string hostnameKey = GetName() + HttpHostname;
			string portKey = GetName() + HttpPort;
			string host = Runtime.GetProperty(hostnameKey);
			string port = Runtime.GetProperty(portKey);
			if (host == null)
			{
				throw new ServerException(ServerException.ERROR.S13, hostnameKey);
			}
			if (port == null)
			{
				throw new ServerException(ServerException.ERROR.S13, portKey);
			}
			try
			{
				IPAddress add = Sharpen.Extensions.GetAddressByName(host);
				int portNum = System.Convert.ToInt32(port);
				return new IPEndPoint(add, portNum);
			}
			catch (UnknownHostException ex)
			{
				throw new ServerException(ServerException.ERROR.S14, ex.ToString(), ex);
			}
		}

		/// <summary>
		/// Destroys the <code>ServletContextListener</code> which destroys
		/// the Server.
		/// </summary>
		/// <param name="event">servelt context event.</param>
		public virtual void ContextDestroyed(ServletContextEvent @event)
		{
			Destroy();
		}

		/// <summary>Returns the hostname:port InetSocketAddress the webserver is listening to.
		/// 	</summary>
		/// <returns>the hostname:port InetSocketAddress the webserver is listening to.</returns>
		/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServerException"/>
		public virtual IPEndPoint GetAuthority()
		{
			lock (this)
			{
				if (authority == null)
				{
					authority = ResolveAuthority();
				}
			}
			return authority;
		}

		/// <summary>Sets an alternate hostname:port InetSocketAddress to use.</summary>
		/// <remarks>
		/// Sets an alternate hostname:port InetSocketAddress to use.
		/// <p>
		/// For testing purposes.
		/// </remarks>
		/// <param name="authority">alterante authority.</param>
		[VisibleForTesting]
		public virtual void SetAuthority(IPEndPoint authority)
		{
			this.authority = authority;
		}

		public virtual bool IsSslEnabled()
		{
			return Sharpen.Extensions.ValueOf(Runtime.GetProperty(GetName() + SslEnabled, "false"
				));
		}
	}
}
