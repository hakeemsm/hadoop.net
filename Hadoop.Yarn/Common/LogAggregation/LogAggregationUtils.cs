using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Logaggregation
{
	public class LogAggregationUtils
	{
		public const string TmpFileSuffix = ".tmp";

		/// <summary>Constructs the full filename for an application's log file per node.</summary>
		/// <param name="remoteRootLogDir"/>
		/// <param name="appId"/>
		/// <param name="user"/>
		/// <param name="nodeId"/>
		/// <param name="suffix"/>
		/// <returns>the remote log file.</returns>
		public static Path GetRemoteNodeLogFileForApp(Path remoteRootLogDir, ApplicationId
			 appId, string user, NodeId nodeId, string suffix)
		{
			return new Path(GetRemoteAppLogDir(remoteRootLogDir, appId, user, suffix), GetNodeString
				(nodeId));
		}

		/// <summary>Gets the remote app log dir.</summary>
		/// <param name="remoteRootLogDir"/>
		/// <param name="appId"/>
		/// <param name="user"/>
		/// <param name="suffix"/>
		/// <returns>the remote application specific log dir.</returns>
		public static Path GetRemoteAppLogDir(Path remoteRootLogDir, ApplicationId appId, 
			string user, string suffix)
		{
			return new Path(GetRemoteLogSuffixedDir(remoteRootLogDir, user, suffix), appId.ToString
				());
		}

		/// <summary>Gets the remote suffixed log dir for the user.</summary>
		/// <param name="remoteRootLogDir"/>
		/// <param name="user"/>
		/// <param name="suffix"/>
		/// <returns>the remote suffixed log dir.</returns>
		public static Path GetRemoteLogSuffixedDir(Path remoteRootLogDir, string user, string
			 suffix)
		{
			if (suffix == null || suffix.IsEmpty())
			{
				return GetRemoteLogUserDir(remoteRootLogDir, user);
			}
			// TODO Maybe support suffix to be more than a single file.
			return new Path(GetRemoteLogUserDir(remoteRootLogDir, user), suffix);
		}

		// TODO Add a utility method to list available log files. Ignore the
		// temporary ones.
		/// <summary>Gets the remote log user dir.</summary>
		/// <param name="remoteRootLogDir"/>
		/// <param name="user"/>
		/// <returns>the remote per user log dir.</returns>
		public static Path GetRemoteLogUserDir(Path remoteRootLogDir, string user)
		{
			return new Path(remoteRootLogDir, user);
		}

		/// <summary>Returns the suffix component of the log dir.</summary>
		/// <param name="conf"/>
		/// <returns>the suffix which will be appended to the user log dir.</returns>
		public static string GetRemoteNodeLogDirSuffix(Configuration conf)
		{
			return conf.Get(YarnConfiguration.NmRemoteAppLogDirSuffix, YarnConfiguration.DefaultNmRemoteAppLogDirSuffix
				);
		}

		/// <summary>Converts a nodeId to a form used in the app log file name.</summary>
		/// <param name="nodeId"/>
		/// <returns>the node string to be used to construct the file name.</returns>
		[VisibleForTesting]
		public static string GetNodeString(NodeId nodeId)
		{
			return nodeId.ToString().Replace(":", "_");
		}

		[VisibleForTesting]
		public static string GetNodeString(string nodeId)
		{
			return nodeId.ToString().Replace(":", "_");
		}
	}
}
