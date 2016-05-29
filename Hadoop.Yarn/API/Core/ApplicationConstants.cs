using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api
{
	/// <summary>
	/// This is the API for the applications comprising of constants that YARN sets
	/// up for the applications and the containers.
	/// </summary>
	/// <remarks>
	/// This is the API for the applications comprising of constants that YARN sets
	/// up for the applications and the containers.
	/// TODO: Investigate the semantics and security of each cross-boundary refs.
	/// </remarks>
	public abstract class ApplicationConstants
	{
		/// <summary>The environment variable for APP_SUBMIT_TIME.</summary>
		/// <remarks>
		/// The environment variable for APP_SUBMIT_TIME. Set in AppMaster environment
		/// only
		/// </remarks>
		public const string AppSubmitTimeEnv = "APP_SUBMIT_TIME_ENV";

		/// <summary>The cache file into which container token is written</summary>
		public const string ContainerTokenFileEnvName = UserGroupInformation.HadoopTokenFileLocation;

		/// <summary>The environmental variable for APPLICATION_WEB_PROXY_BASE.</summary>
		/// <remarks>
		/// The environmental variable for APPLICATION_WEB_PROXY_BASE. Set in
		/// ApplicationMaster's environment only. This states that for all non-relative
		/// web URLs in the app masters web UI what base should they have.
		/// </remarks>
		public const string ApplicationWebProxyBaseEnv = "APPLICATION_WEB_PROXY_BASE";

		/// <summary>The temporary environmental variable for container log directory.</summary>
		/// <remarks>
		/// The temporary environmental variable for container log directory. This
		/// should be replaced by real container log directory on container launch.
		/// </remarks>
		public const string LogDirExpansionVar = "<LOG_DIR>";

		/// <summary>
		/// This constant is used to construct class path and it will be replaced with
		/// real class path separator(':' for Linux and ';' for Windows) by
		/// NodeManager on container launch.
		/// </summary>
		/// <remarks>
		/// This constant is used to construct class path and it will be replaced with
		/// real class path separator(':' for Linux and ';' for Windows) by
		/// NodeManager on container launch. User has to use this constant to construct
		/// class path if user wants cross-platform practice i.e. submit an application
		/// from a Windows client to a Linux/Unix server or vice versa.
		/// </remarks>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public const string ClassPathSeparator = "<CPS>";

		/// <summary>
		/// The following two constants are used to expand parameter and it will be
		/// replaced with real parameter expansion marker ('%' for Windows and '$' for
		/// Linux) by NodeManager on container launch.
		/// </summary>
		/// <remarks>
		/// The following two constants are used to expand parameter and it will be
		/// replaced with real parameter expansion marker ('%' for Windows and '$' for
		/// Linux) by NodeManager on container launch. For example: {{VAR}} will be
		/// replaced as $VAR on Linux, and %VAR% on Windows. User has to use this
		/// constant to construct class path if user wants cross-platform practice i.e.
		/// submit an application from a Windows client to a Linux/Unix server or vice
		/// versa.
		/// </remarks>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public const string ParameterExpansionLeft = "{{";

		/// <summary>
		/// User has to use this constant to construct class path if user wants
		/// cross-platform practice i.e.
		/// </summary>
		/// <remarks>
		/// User has to use this constant to construct class path if user wants
		/// cross-platform practice i.e. submit an application from a Windows client to
		/// a Linux/Unix server or vice versa.
		/// </remarks>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public const string ParameterExpansionRight = "}}";

		public const string Stderr = "stderr";

		public const string Stdout = "stdout";

		/// <summary>The environment variable for MAX_APP_ATTEMPTS.</summary>
		/// <remarks>
		/// The environment variable for MAX_APP_ATTEMPTS. Set in AppMaster environment
		/// only
		/// </remarks>
		public const string MaxAppAttemptsEnv = "MAX_APP_ATTEMPTS";

		/// <summary>Environment for Applications.</summary>
		/// <remarks>
		/// Environment for Applications.
		/// Some of the environment variables for applications are <em>final</em>
		/// i.e. they cannot be modified by the applications.
		/// </remarks>
		[System.Serializable]
		public sealed class Environment
		{
			/// <summary>
			/// $USER
			/// Final, non-modifiable.
			/// </summary>
			public static readonly ApplicationConstants.Environment User = new ApplicationConstants.Environment
				("USER");

			/// <summary>
			/// $LOGNAME
			/// Final, non-modifiable.
			/// </summary>
			public static readonly ApplicationConstants.Environment Logname = new ApplicationConstants.Environment
				("LOGNAME");

			/// <summary>
			/// $HOME
			/// Final, non-modifiable.
			/// </summary>
			public static readonly ApplicationConstants.Environment Home = new ApplicationConstants.Environment
				("HOME");

			/// <summary>
			/// $PWD
			/// Final, non-modifiable.
			/// </summary>
			public static readonly ApplicationConstants.Environment Pwd = new ApplicationConstants.Environment
				("PWD");

			/// <summary>$PATH</summary>
			public static readonly ApplicationConstants.Environment Path = new ApplicationConstants.Environment
				("PATH");

			/// <summary>$SHELL</summary>
			public static readonly ApplicationConstants.Environment Shell = new ApplicationConstants.Environment
				("SHELL");

			/// <summary>$JAVA_HOME</summary>
			public static readonly ApplicationConstants.Environment JavaHome = new ApplicationConstants.Environment
				("JAVA_HOME");

			/// <summary>$CLASSPATH</summary>
			public static readonly ApplicationConstants.Environment Classpath = new ApplicationConstants.Environment
				("CLASSPATH");

			/// <summary>$APP_CLASSPATH</summary>
			public static readonly ApplicationConstants.Environment AppClasspath = new ApplicationConstants.Environment
				("APP_CLASSPATH");

			/// <summary>$HADOOP_CLASSPATH.</summary>
			public static readonly ApplicationConstants.Environment HadoopClasspath = new ApplicationConstants.Environment
				("HADOOP_CLASSPATH");

			/// <summary>$LD_LIBRARY_PATH</summary>
			public static readonly ApplicationConstants.Environment LdLibraryPath = new ApplicationConstants.Environment
				("LD_LIBRARY_PATH");

			/// <summary>
			/// $HADOOP_CONF_DIR
			/// Final, non-modifiable.
			/// </summary>
			public static readonly ApplicationConstants.Environment HadoopConfDir = new ApplicationConstants.Environment
				("HADOOP_CONF_DIR");

			/// <summary>$HADOOP_COMMON_HOME</summary>
			public static readonly ApplicationConstants.Environment HadoopCommonHome = new ApplicationConstants.Environment
				("HADOOP_COMMON_HOME");

			/// <summary>$HADOOP_HDFS_HOME</summary>
			public static readonly ApplicationConstants.Environment HadoopHdfsHome = new ApplicationConstants.Environment
				("HADOOP_HDFS_HOME");

			/// <summary>$MALLOC_ARENA_MAX</summary>
			public static readonly ApplicationConstants.Environment MallocArenaMax = new ApplicationConstants.Environment
				("MALLOC_ARENA_MAX");

			/// <summary>$HADOOP_YARN_HOME</summary>
			public static readonly ApplicationConstants.Environment HadoopYarnHome = new ApplicationConstants.Environment
				("HADOOP_YARN_HOME");

			/// <summary>
			/// $CLASSPATH_PREPEND_DISTCACHE
			/// Private, Windows specific
			/// </summary>
			[InterfaceAudience.Private]
			public static readonly ApplicationConstants.Environment ClasspathPrependDistcache
				 = new ApplicationConstants.Environment("CLASSPATH_PREPEND_DISTCACHE");

			/// <summary>
			/// $CONTAINER_ID
			/// Final, exported by NodeManager and non-modifiable by users.
			/// </summary>
			public static readonly ApplicationConstants.Environment ContainerId = new ApplicationConstants.Environment
				("CONTAINER_ID");

			/// <summary>
			/// $NM_HOST
			/// Final, exported by NodeManager and non-modifiable by users.
			/// </summary>
			public static readonly ApplicationConstants.Environment NmHost = new ApplicationConstants.Environment
				("NM_HOST");

			/// <summary>
			/// $NM_HTTP_PORT
			/// Final, exported by NodeManager and non-modifiable by users.
			/// </summary>
			public static readonly ApplicationConstants.Environment NmHttpPort = new ApplicationConstants.Environment
				("NM_HTTP_PORT");

			/// <summary>
			/// $NM_PORT
			/// Final, exported by NodeManager and non-modifiable by users.
			/// </summary>
			public static readonly ApplicationConstants.Environment NmPort = new ApplicationConstants.Environment
				("NM_PORT");

			/// <summary>
			/// $LOCAL_DIRS
			/// Final, exported by NodeManager and non-modifiable by users.
			/// </summary>
			public static readonly ApplicationConstants.Environment LocalDirs = new ApplicationConstants.Environment
				("LOCAL_DIRS");

			/// <summary>
			/// $LOG_DIRS
			/// Final, exported by NodeManager and non-modifiable by users.
			/// </summary>
			/// <remarks>
			/// $LOG_DIRS
			/// Final, exported by NodeManager and non-modifiable by users.
			/// Comma separate list of directories that the container should use for
			/// logging.
			/// </remarks>
			public static readonly ApplicationConstants.Environment LogDirs = new ApplicationConstants.Environment
				("LOG_DIRS");

			private readonly string variable;

			private Environment(string variable)
			{
				this.variable = variable;
			}

			public string Key()
			{
				return ApplicationConstants.Environment.variable;
			}

			public override string ToString()
			{
				return ApplicationConstants.Environment.variable;
			}

			/// <summary>
			/// Expand the environment variable based on client OS environment variable
			/// expansion syntax (e.g.
			/// </summary>
			/// <remarks>
			/// Expand the environment variable based on client OS environment variable
			/// expansion syntax (e.g. $VAR for Linux and %VAR% for Windows).
			/// <p>
			/// Note: Use $$() method for cross-platform practice i.e. submit an
			/// application from a Windows client to a Linux/Unix server or vice versa.
			/// </p>
			/// </remarks>
			public string $()
			{
				if (Shell.Windows)
				{
					return "%" + ApplicationConstants.Environment.variable + "%";
				}
				else
				{
					return "$" + ApplicationConstants.Environment.variable;
				}
			}

			/// <summary>Expand the environment variable in platform-agnostic syntax.</summary>
			/// <remarks>
			/// Expand the environment variable in platform-agnostic syntax. The
			/// parameter expansion marker "{{VAR}}" will be replaced with real parameter
			/// expansion marker ('%' for Windows and '$' for Linux) by NodeManager on
			/// container launch. For example: {{VAR}} will be replaced as $VAR on Linux,
			/// and %VAR% on Windows.
			/// </remarks>
			[InterfaceAudience.Public]
			[InterfaceStability.Unstable]
			public string $$()
			{
				return ParameterExpansionLeft + ApplicationConstants.Environment.variable + ParameterExpansionRight;
			}
		}
	}

	public static class ApplicationConstantsConstants
	{
	}
}
