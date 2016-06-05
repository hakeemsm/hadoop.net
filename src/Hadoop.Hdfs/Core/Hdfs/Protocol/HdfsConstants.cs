using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Some handy constants</summary>
	public class HdfsConstants
	{
		protected internal HdfsConstants()
		{
		}

		/// <summary>HDFS Protocol Names:</summary>
		public const string ClientNamenodeProtocolName = "org.apache.hadoop.hdfs.protocol.ClientProtocol";

		public const string ClientDatanodeProtocolName = "org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol";

		public const int MinBlocksForWrite = 1;

		public const long QuotaDontSet = long.MaxValue;

		public const long QuotaReset = -1L;

		public const long LeaseSoftlimitPeriod = 60 * 1000;

		public const long LeaseHardlimitPeriod = 60 * LeaseSoftlimitPeriod;

		public const long LeaseRecoverPeriod = 10 * 1000;

		public const int MaxPathLength = 8000;

		public const int MaxPathDepth = 1000;

		public const int DefaultDataSocketSize = 128 * 1024;

		public static readonly int IoFileBufferSize = new HdfsConfiguration().GetInt(DFSConfigKeys
			.IoFileBufferSizeKey, DFSConfigKeys.IoFileBufferSizeDefault);

		public static readonly int SmallBufferSize = Math.Min(IoFileBufferSize / 2, 512);

		public const int BytesInInteger = int.Size / byte.Size;

		public enum SafeModeAction
		{
			SafemodeLeave,
			SafemodeEnter,
			SafemodeGet
		}

		[System.Serializable]
		public sealed class RollingUpgradeAction
		{
			public static readonly HdfsConstants.RollingUpgradeAction Query = new HdfsConstants.RollingUpgradeAction
				();

			public static readonly HdfsConstants.RollingUpgradeAction Prepare = new HdfsConstants.RollingUpgradeAction
				();

			public static readonly HdfsConstants.RollingUpgradeAction Finalize = new HdfsConstants.RollingUpgradeAction
				();

			private static readonly IDictionary<string, HdfsConstants.RollingUpgradeAction> Map
				 = new Dictionary<string, HdfsConstants.RollingUpgradeAction>();

			static RollingUpgradeAction()
			{
				/* Hidden constructor */
				// Long that indicates "leave current quota unchanged"
				//
				// Timeouts, constants
				//
				// in ms
				// We need to limit the length and depth of a path in the filesystem.
				// HADOOP-438
				// Currently we set the maximum length to 8k characters and the maximum depth
				// to 1k.
				// TODO should be conf injected?
				// Used for writing header etc.
				// SafeMode actions
				HdfsConstants.RollingUpgradeAction.Map[string.Empty] = HdfsConstants.RollingUpgradeAction
					.Query;
				foreach (HdfsConstants.RollingUpgradeAction a in Values())
				{
					HdfsConstants.RollingUpgradeAction.Map[a.ToString()] = a;
				}
			}

			/// <summary>Covert the given String to a RollingUpgradeAction.</summary>
			public static HdfsConstants.RollingUpgradeAction FromString(string s)
			{
				return HdfsConstants.RollingUpgradeAction.Map[StringUtils.ToUpperCase(s)];
			}
		}

		public enum DatanodeReportType
		{
			All,
			Live,
			Dead,
			Decommissioning
		}

		public const long InvalidTxid = -12345;

		public const long ReservedGenerationStampsV1 = 1024L * 1024 * 1024 * 1024;

		/// <summary>URI Scheme for hdfs://namenode/ URIs.</summary>
		public const string HdfsUriScheme = "hdfs";

		/// <summary>
		/// A prefix put before the namenode URI inside the "service" field
		/// of a delgation token, indicating that the URI is a logical (HA)
		/// URI.
		/// </summary>
		public const string HaDtServicePrefix = "ha-";

		/// <summary>Path components that are reserved in HDFS.</summary>
		/// <remarks>
		/// Path components that are reserved in HDFS.
		/// <p>
		/// .reserved is only reserved under root ("/").
		/// </remarks>
		public static readonly string[] ReservedPathComponents = new string[] { HdfsConstants
			.DotSnapshotDir, FSDirectory.DotReservedString };

		/// <summary>Current layout version for NameNode.</summary>
		/// <remarks>
		/// Current layout version for NameNode.
		/// Please see
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNodeLayoutVersion.Feature"/
		/// 	>
		/// on adding new layout version.
		/// </remarks>
		public static readonly int NamenodeLayoutVersion = NameNodeLayoutVersion.CurrentLayoutVersion;

		/// <summary>Current layout version for DataNode.</summary>
		/// <remarks>
		/// Current layout version for DataNode.
		/// Please see
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.DataNodeLayoutVersion.Feature"/
		/// 	>
		/// on adding new layout version.
		/// </remarks>
		public static readonly int DatanodeLayoutVersion = DataNodeLayoutVersion.CurrentLayoutVersion;

		/// <summary>A special path component contained in the path for a snapshot file/dir</summary>
		public const string DotSnapshotDir = ".snapshot";

		public static readonly byte[] DotSnapshotDirBytes = DFSUtil.String2Bytes(DotSnapshotDir
			);

		public const string SeparatorDotSnapshotDir = Path.Separator + DotSnapshotDir;

		public const string SeparatorDotSnapshotDirSeparator = Path.Separator + DotSnapshotDir
			 + Path.Separator;

		public const string MemoryStoragePolicyName = "LAZY_PERSIST";

		public const string AllssdStoragePolicyName = "ALL_SSD";

		public const string OnessdStoragePolicyName = "ONE_SSD";

		public const string HotStoragePolicyName = "HOT";

		public const string WarmStoragePolicyName = "WARM";

		public const string ColdStoragePolicyName = "COLD";

		public const byte MemoryStoragePolicyId = 15;

		public const byte AllssdStoragePolicyId = 12;

		public const byte OnessdStoragePolicyId = 10;

		public const byte HotStoragePolicyId = 7;

		public const byte WarmStoragePolicyId = 5;

		public const byte ColdStoragePolicyId = 2;
		// type of the datanode report
		// An invalid transaction ID that will never be seen in a real namesystem.
		// Number of generation stamps reserved for legacy blocks.
	}
}
