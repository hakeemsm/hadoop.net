using System;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>Place holder for cluster level configuration keys.</summary>
	/// <remarks>
	/// Place holder for cluster level configuration keys.
	/// The keys should have "mapreduce.cluster." as the prefix.
	/// </remarks>
	public abstract class MRConfig
	{
		public const string TempDir = "mapreduce.cluster.temp.dir";

		public const string LocalDir = "mapreduce.cluster.local.dir";

		public const string MapmemoryMb = "mapreduce.cluster.mapmemory.mb";

		public const string ReducememoryMb = "mapreduce.cluster.reducememory.mb";

		public const string MrAclsEnabled = "mapreduce.cluster.acls.enabled";

		public const string MrAdmins = "mapreduce.cluster.administrators";

		[Obsolete]
		public const string MrSupergroup = "mapreduce.cluster.permissions.supergroup";

		public const string DelegationKeyUpdateIntervalKey = "mapreduce.cluster.delegation.key.update-interval";

		public const long DelegationKeyUpdateIntervalDefault = 24 * 60 * 60 * 1000;

		public const string DelegationTokenRenewIntervalKey = "mapreduce.cluster.delegation.token.renew-interval";

		public const long DelegationTokenRenewIntervalDefault = 24 * 60 * 60 * 1000;

		public const string DelegationTokenMaxLifetimeKey = "mapreduce.cluster.delegation.token.max-lifetime";

		public const long DelegationTokenMaxLifetimeDefault = 7 * 24 * 60 * 60 * 1000;

		public const string ResourceCalculatorProcessTree = "mapreduce.job.process-tree.class";

		public const string StaticResolutions = "mapreduce.job.net.static.resolutions";

		public const string MasterAddress = "mapreduce.jobtracker.address";

		public const string MasterUserName = "mapreduce.jobtracker.kerberos.principal";

		public const string FrameworkName = "mapreduce.framework.name";

		public const string ClassicFrameworkName = "classic";

		public const string YarnFrameworkName = "yarn";

		public const string LocalFrameworkName = "local";

		public const string TaskLocalOutputClass = "mapreduce.task.local.output.class";

		public const string ProgressStatusLenLimitKey = "mapreduce.task.max.status.length";

		public const int ProgressStatusLenLimitDefault = 512;

		public const int MaxBlockLocationsDefault = 10;

		public const string MaxBlockLocationsKey = "mapreduce.job.max.split.locations";

		public const string ShuffleSslEnabledKey = "mapreduce.shuffle.ssl.enabled";

		public const bool ShuffleSslEnabledDefault = false;

		public const string ShuffleConsumerPlugin = "mapreduce.job.reduce.shuffle.consumer.plugin.class";

		/// <summary>Configuration key to enable/disable IFile readahead.</summary>
		public const string MapredIfileReadahead = "mapreduce.ifile.readahead";

		public const bool DefaultMapredIfileReadahead = true;

		/// <summary>Configuration key to set the IFile readahead length in bytes.</summary>
		public const string MapredIfileReadaheadBytes = "mapreduce.ifile.readahead.bytes";

		public const int DefaultMapredIfileReadaheadBytes = 4 * 1024 * 1024;

		/// <summary>
		/// Whether users are explicitly trying to control resource monitoring
		/// configuration for the MiniMRCluster.
		/// </summary>
		/// <remarks>
		/// Whether users are explicitly trying to control resource monitoring
		/// configuration for the MiniMRCluster. Disabled by default.
		/// </remarks>
		public const string MapreduceMiniclusterControlResourceMonitoring = "mapreduce.minicluster.control-resource-monitoring";

		public const bool DefaultMapreduceMiniclusterControlResourceMonitoring = false;

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public const string MapreduceAppSubmissionCrossPlatform = "mapreduce.app-submission.cross-platform";

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public const bool DefaultMapreduceAppSubmissionCrossPlatform = false;
		// Cluster-level configuration parameters
		//Delegation token related keys
		// 1 day
		// 1 day
		// 7 days
	}

	public static class MRConfigConstants
	{
	}
}
