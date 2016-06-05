using Org.Apache.Hadoop.Http;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Jobhistory
{
	/// <summary>
	/// Stores Job History configuration keys that can be set by administrators of
	/// the Job History server.
	/// </summary>
	public class JHAdminConfig
	{
		/// <summary>The prefix to all Job History configuration properties.</summary>
		public const string MrHistoryPrefix = "mapreduce.jobhistory.";

		/// <summary>host:port address for History Server API.</summary>
		public const string MrHistoryAddress = MrHistoryPrefix + "address";

		public const int DefaultMrHistoryPort = 10020;

		public const string DefaultMrHistoryAddress = "0.0.0.0:" + DefaultMrHistoryPort;

		public const string MrHistoryBindHost = MrHistoryPrefix + "bind-host";

		/// <summary>The address of the History server admin interface.</summary>
		public const string JhsAdminAddress = MrHistoryPrefix + "admin.address";

		public const int DefaultJhsAdminPort = 10033;

		public const string DefaultJhsAdminAddress = "0.0.0.0:" + DefaultJhsAdminPort;

		/// <summary>ACL of who can be admin of Job history server.</summary>
		public const string JhsAdminAcl = MrHistoryPrefix + "admin.acl";

		public const string DefaultJhsAdminAcl = "*";

		/// <summary>If history cleaning should be enabled or not.</summary>
		public const string MrHistoryCleanerEnable = MrHistoryPrefix + "cleaner.enable";

		/// <summary>Run the History Cleaner every X ms.</summary>
		public const string MrHistoryCleanerIntervalMs = MrHistoryPrefix + "cleaner.interval-ms";

		public const long DefaultMrHistoryCleanerIntervalMs = 1 * 24 * 60 * 60 * 1000l;

		/// <summary>The number of threads to handle client API requests.</summary>
		public const string MrHistoryClientThreadCount = MrHistoryPrefix + "client.thread-count";

		public const int DefaultMrHistoryClientThreadCount = 10;

		/// <summary>Size of the date string cache.</summary>
		/// <remarks>
		/// Size of the date string cache. Effects the number of directories
		/// which will be scanned to find a job.
		/// </remarks>
		public const string MrHistoryDatestringCacheSize = MrHistoryPrefix + "datestring.cache.size";

		public const int DefaultMrHistoryDatestringCacheSize = 200000;

		/// <summary>Path where history files should be stored for DONE jobs.</summary>
		public const string MrHistoryDoneDir = MrHistoryPrefix + "done-dir";

		/// <summary>
		/// Maximum time the History server will wait for the FileSystem for History
		/// files to become available.
		/// </summary>
		/// <remarks>
		/// Maximum time the History server will wait for the FileSystem for History
		/// files to become available. Default value is -1, forever.
		/// </remarks>
		public const string MrHistoryMaxStartWaitTime = MrHistoryPrefix + "maximum-start-wait-time-millis";

		public const long DefaultMrHistoryMaxStartWaitTime = -1;

		/// <summary>
		/// Path where history files should be stored after a job finished and before
		/// they are pulled into the job history server.
		/// </summary>
		public const string MrHistoryIntermediateDoneDir = MrHistoryPrefix + "intermediate-done-dir";

		/// <summary>Size of the job list cache.</summary>
		public const string MrHistoryJoblistCacheSize = MrHistoryPrefix + "joblist.cache.size";

		public const int DefaultMrHistoryJoblistCacheSize = 20000;

		/// <summary>The location of the Kerberos keytab file.</summary>
		public const string MrHistoryKeytab = MrHistoryPrefix + "keytab";

		/// <summary>Size of the loaded job cache.</summary>
		public const string MrHistoryLoadedJobCacheSize = MrHistoryPrefix + "loadedjobs.cache.size";

		public const int DefaultMrHistoryLoadedJobCacheSize = 5;

		/// <summary>
		/// The maximum age of a job history file before it is deleted from the history
		/// server.
		/// </summary>
		public const string MrHistoryMaxAgeMs = MrHistoryPrefix + "max-age-ms";

		public const long DefaultMrHistoryMaxAge = 7 * 24 * 60 * 60 * 1000L;

		/// <summary>
		/// Scan for history files to more from intermediate done dir to done dir
		/// every X ms.
		/// </summary>
		public const string MrHistoryMoveIntervalMs = MrHistoryPrefix + "move.interval-ms";

		public const long DefaultMrHistoryMoveIntervalMs = 3 * 60 * 1000l;

		/// <summary>The number of threads used to move files.</summary>
		public const string MrHistoryMoveThreadCount = MrHistoryPrefix + "move.thread-count";

		public const int DefaultMrHistoryMoveThreadCount = 3;

		/// <summary>The Kerberos principal for the history server.</summary>
		public const string MrHistoryPrincipal = MrHistoryPrefix + "principal";

		/// <summary>To enable https in MR history server</summary>
		public const string MrHsHttpPolicy = MrHistoryPrefix + "http.policy";

		public static string DefaultMrHsHttpPolicy = HttpConfig.Policy.HttpOnly.ToString(
			);

		/// <summary>The address the history server webapp is on.</summary>
		public const string MrHistoryWebappAddress = MrHistoryPrefix + "webapp.address";

		public const int DefaultMrHistoryWebappPort = 19888;

		public const string DefaultMrHistoryWebappAddress = "0.0.0.0:" + DefaultMrHistoryWebappPort;

		/// <summary>The https address the history server webapp is on.</summary>
		public const string MrHistoryWebappHttpsAddress = MrHistoryPrefix + "webapp.https.address";

		public const int DefaultMrHistoryWebappHttpsPort = 19890;

		public const string DefaultMrHistoryWebappHttpsAddress = "0.0.0.0:" + DefaultMrHistoryWebappHttpsPort;

		/// <summary>The kerberos principal to be used for spnego filter for history server</summary>
		public const string MrWebappSpnegoUserNameKey = MrHistoryPrefix + "webapp.spnego-principal";

		/// <summary>The kerberos keytab to be used for spnego filter for history server</summary>
		public const string MrWebappSpnegoKeytabFileKey = MrHistoryPrefix + "webapp.spnego-keytab-file";

		public const string MrHsSecurityServiceAuthorization = "security.mrhs.client.protocol.acl";

		public const string MrHsSecurityServiceAuthorizationAdminRefresh = "security.mrhs.admin.refresh.protocol.acl";

		/// <summary>The HistoryStorage class to use to cache history data.</summary>
		public const string MrHistoryStorage = MrHistoryPrefix + "store.class";

		/// <summary>
		/// Enable the history server to store server state and recover server state
		/// upon startup.
		/// </summary>
		public const string MrHsRecoveryEnable = MrHistoryPrefix + "recovery.enable";

		public const bool DefaultMrHsRecoveryEnable = false;

		/// <summary>The HistoryServerStateStoreService class to store and recover server state
		/// 	</summary>
		public const string MrHsStateStore = MrHistoryPrefix + "recovery.store.class";

		/// <summary>
		/// The URI where server state will be stored when
		/// HistoryServerFileSystemStateStoreService is configured as the state store
		/// </summary>
		public const string MrHsFsStateStoreUri = MrHistoryPrefix + "recovery.store.fs.uri";

		/// <summary>
		/// The local path where server state will be stored when
		/// HistoryServerLeveldbStateStoreService is configured as the state store
		/// </summary>
		public const string MrHsLeveldbStateStorePath = MrHistoryPrefix + "recovery.store.leveldb.path";

		/// <summary>Whether to use fixed ports with the minicluster.</summary>
		public const string MrHistoryMiniclusterFixedPorts = MrHistoryPrefix + "minicluster.fixed.ports";

		/// <summary>
		/// Default is false to be able to run tests concurrently without port
		/// conflicts.
		/// </summary>
		public static bool DefaultMrHistoryMiniclusterFixedPorts = false;
		//1 day
		//1 week
		//3 minutes
		/*
		* HS Service Authorization
		*/
	}
}
