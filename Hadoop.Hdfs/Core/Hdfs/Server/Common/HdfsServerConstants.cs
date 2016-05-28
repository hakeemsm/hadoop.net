using System;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Common
{
	/// <summary>Some handy internal HDFS constants</summary>
	public sealed class HdfsServerConstants
	{
		private HdfsServerConstants()
		{
		}

		/// <summary>Type of the node</summary>
		public enum NodeType
		{
			NameNode,
			DataNode,
			JournalNode
		}

		/// <summary>Startup options for rolling upgrade.</summary>
		[System.Serializable]
		public sealed class RollingUpgradeStartupOption
		{
			public static readonly HdfsServerConstants.RollingUpgradeStartupOption Rollback = 
				new HdfsServerConstants.RollingUpgradeStartupOption();

			public static readonly HdfsServerConstants.RollingUpgradeStartupOption Downgrade = 
				new HdfsServerConstants.RollingUpgradeStartupOption();

			public static readonly HdfsServerConstants.RollingUpgradeStartupOption Started = 
				new HdfsServerConstants.RollingUpgradeStartupOption();

			/* Hidden constructor */
			public string GetOptionString()
			{
				return HdfsServerConstants.StartupOption.Rollingupgrade.GetName() + " " + StringUtils
					.ToLowerCase(Name());
			}

			public bool Matches(HdfsServerConstants.StartupOption option)
			{
				return option == HdfsServerConstants.StartupOption.Rollingupgrade && option.GetRollingUpgradeStartupOption
					() == this;
			}

			private static readonly HdfsServerConstants.RollingUpgradeStartupOption[] Values = 
				Values();

			internal static HdfsServerConstants.RollingUpgradeStartupOption FromString(string
				 s)
			{
				foreach (HdfsServerConstants.RollingUpgradeStartupOption opt in HdfsServerConstants.RollingUpgradeStartupOption
					.Values)
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(opt.ToString(), s))
					{
						return opt;
					}
				}
				throw new ArgumentException("Failed to convert \"" + s + "\" to " + typeof(HdfsServerConstants.RollingUpgradeStartupOption
					).Name);
			}

			public static string GetAllOptionString()
			{
				StringBuilder b = new StringBuilder("<");
				foreach (HdfsServerConstants.RollingUpgradeStartupOption opt in HdfsServerConstants.RollingUpgradeStartupOption
					.Values)
				{
					b.Append(StringUtils.ToLowerCase(opt.ToString())).Append("|");
				}
				Sharpen.Runtime.SetCharAt(b, b.Length - 1, '>');
				return b.ToString();
			}
		}

		/// <summary>Startup options</summary>
		[System.Serializable]
		public sealed class StartupOption
		{
			public static readonly HdfsServerConstants.StartupOption Format = new HdfsServerConstants.StartupOption
				("-format");

			public static readonly HdfsServerConstants.StartupOption Clusterid = new HdfsServerConstants.StartupOption
				("-clusterid");

			public static readonly HdfsServerConstants.StartupOption Genclusterid = new HdfsServerConstants.StartupOption
				("-genclusterid");

			public static readonly HdfsServerConstants.StartupOption Regular = new HdfsServerConstants.StartupOption
				("-regular");

			public static readonly HdfsServerConstants.StartupOption Backup = new HdfsServerConstants.StartupOption
				("-backup");

			public static readonly HdfsServerConstants.StartupOption Checkpoint = new HdfsServerConstants.StartupOption
				("-checkpoint");

			public static readonly HdfsServerConstants.StartupOption Upgrade = new HdfsServerConstants.StartupOption
				("-upgrade");

			public static readonly HdfsServerConstants.StartupOption Rollback = new HdfsServerConstants.StartupOption
				("-rollback");

			public static readonly HdfsServerConstants.StartupOption Finalize = new HdfsServerConstants.StartupOption
				("-finalize");

			public static readonly HdfsServerConstants.StartupOption Rollingupgrade = new HdfsServerConstants.StartupOption
				("-rollingUpgrade");

			public static readonly HdfsServerConstants.StartupOption Import = new HdfsServerConstants.StartupOption
				("-importCheckpoint");

			public static readonly HdfsServerConstants.StartupOption Bootstrapstandby = new HdfsServerConstants.StartupOption
				("-bootstrapStandby");

			public static readonly HdfsServerConstants.StartupOption Initializesharededits = 
				new HdfsServerConstants.StartupOption("-initializeSharedEdits");

			public static readonly HdfsServerConstants.StartupOption Recover = new HdfsServerConstants.StartupOption
				("-recover");

			public static readonly HdfsServerConstants.StartupOption Force = new HdfsServerConstants.StartupOption
				("-force");

			public static readonly HdfsServerConstants.StartupOption Noninteractive = new HdfsServerConstants.StartupOption
				("-nonInteractive");

			public static readonly HdfsServerConstants.StartupOption Renamereserved = new HdfsServerConstants.StartupOption
				("-renameReserved");

			public static readonly HdfsServerConstants.StartupOption Metadataversion = new HdfsServerConstants.StartupOption
				("-metadataVersion");

			public static readonly HdfsServerConstants.StartupOption Upgradeonly = new HdfsServerConstants.StartupOption
				("-upgradeOnly");

			public static readonly HdfsServerConstants.StartupOption Hotswap = new HdfsServerConstants.StartupOption
				("-hotswap");

			private static readonly Sharpen.Pattern EnumWithRollingUpgradeOption = Sharpen.Pattern
				.Compile("(\\w+)\\((\\w+)\\)");

			private readonly string name;

			private string clusterId = null;

			private HdfsServerConstants.RollingUpgradeStartupOption rollingUpgradeStartupOption;

			private bool isForceFormat = false;

			private bool isInteractiveFormat = true;

			private int force = 0;

			private StartupOption(string arg)
			{
				// The -hotswap constant should not be used as a startup option, it is
				// only used for StorageDirectory.analyzeStorage() in hot swap drive scenario.
				// TODO refactor StorageDirectory.analyzeStorage() so that we can do away with
				// this in StartupOption.
				// Used only with format and upgrade options
				// Used only by rolling upgrade
				// Used only with format option
				// Used only with recovery option
				this.name = arg;
			}

			public string GetName()
			{
				return HdfsServerConstants.StartupOption.name;
			}

			public HdfsServerConstants.NamenodeRole ToNodeRole()
			{
				switch (this)
				{
					case HdfsServerConstants.StartupOption.Backup:
					{
						return HdfsServerConstants.NamenodeRole.Backup;
					}

					case HdfsServerConstants.StartupOption.Checkpoint:
					{
						return HdfsServerConstants.NamenodeRole.Checkpoint;
					}

					default:
					{
						return HdfsServerConstants.NamenodeRole.Namenode;
					}
				}
			}

			public void SetClusterId(string cid)
			{
				HdfsServerConstants.StartupOption.clusterId = cid;
			}

			public string GetClusterId()
			{
				return HdfsServerConstants.StartupOption.clusterId;
			}

			public void SetRollingUpgradeStartupOption(string opt)
			{
				Preconditions.CheckState(this == HdfsServerConstants.StartupOption.Rollingupgrade
					);
				HdfsServerConstants.StartupOption.rollingUpgradeStartupOption = HdfsServerConstants.RollingUpgradeStartupOption
					.FromString(opt);
			}

			public HdfsServerConstants.RollingUpgradeStartupOption GetRollingUpgradeStartupOption
				()
			{
				Preconditions.CheckState(this == HdfsServerConstants.StartupOption.Rollingupgrade
					);
				return HdfsServerConstants.StartupOption.rollingUpgradeStartupOption;
			}

			public MetaRecoveryContext CreateRecoveryContext()
			{
				if (!HdfsServerConstants.StartupOption.name.Equals(HdfsServerConstants.StartupOption
					.Recover.name))
				{
					return null;
				}
				return new MetaRecoveryContext(HdfsServerConstants.StartupOption.force);
			}

			public void SetForce(int force)
			{
				this.force = force;
			}

			public int GetForce()
			{
				return this.force;
			}

			public bool GetForceFormat()
			{
				return HdfsServerConstants.StartupOption.isForceFormat;
			}

			public void SetForceFormat(bool force)
			{
				HdfsServerConstants.StartupOption.isForceFormat = force;
			}

			public bool GetInteractiveFormat()
			{
				return HdfsServerConstants.StartupOption.isInteractiveFormat;
			}

			public void SetInteractiveFormat(bool interactive)
			{
				HdfsServerConstants.StartupOption.isInteractiveFormat = interactive;
			}

			public override string ToString()
			{
				if (this == HdfsServerConstants.StartupOption.Rollingupgrade)
				{
					return new StringBuilder(base.ToString()).Append("(").Append(GetRollingUpgradeStartupOption
						()).Append(")").ToString();
				}
				return base.ToString();
			}

			public static HdfsServerConstants.StartupOption GetEnum(string value)
			{
				Matcher matcher = HdfsServerConstants.StartupOption.EnumWithRollingUpgradeOption.
					Matcher(value);
				if (matcher.Matches())
				{
					HdfsServerConstants.StartupOption option = HdfsServerConstants.StartupOption.ValueOf
						(matcher.Group(1));
					option.SetRollingUpgradeStartupOption(matcher.Group(2));
					return option;
				}
				else
				{
					return HdfsServerConstants.StartupOption.ValueOf(value);
				}
			}
		}

		public const int ReadTimeout = 60 * 1000;

		public const int ReadTimeoutExtension = 5 * 1000;

		public const int WriteTimeout = 8 * 60 * 1000;

		public const int WriteTimeoutExtension = 5 * 1000;

		/// <summary>Defines the NameNode role.</summary>
		[System.Serializable]
		public sealed class NamenodeRole
		{
			public static readonly HdfsServerConstants.NamenodeRole Namenode = new HdfsServerConstants.NamenodeRole
				("NameNode");

			public static readonly HdfsServerConstants.NamenodeRole Backup = new HdfsServerConstants.NamenodeRole
				("Backup Node");

			public static readonly HdfsServerConstants.NamenodeRole Checkpoint = new HdfsServerConstants.NamenodeRole
				("Checkpoint Node");

			private string description = null;

			private NamenodeRole(string arg)
			{
				// Timeouts for communicating with DataNode for streaming writes/reads
				//for write pipeline
				this.description = arg;
			}

			public override string ToString()
			{
				return HdfsServerConstants.NamenodeRole.description;
			}
		}

		/// <summary>Block replica states, which it can go through while being constructed.</summary>
		[System.Serializable]
		public sealed class ReplicaState
		{
			/// <summary>Replica is finalized.</summary>
			/// <remarks>Replica is finalized. The state when replica is not modified.</remarks>
			public static readonly HdfsServerConstants.ReplicaState Finalized = new HdfsServerConstants.ReplicaState
				(0);

			/// <summary>Replica is being written to.</summary>
			public static readonly HdfsServerConstants.ReplicaState Rbw = new HdfsServerConstants.ReplicaState
				(1);

			/// <summary>Replica is waiting to be recovered.</summary>
			public static readonly HdfsServerConstants.ReplicaState Rwr = new HdfsServerConstants.ReplicaState
				(2);

			/// <summary>Replica is under recovery.</summary>
			public static readonly HdfsServerConstants.ReplicaState Rur = new HdfsServerConstants.ReplicaState
				(3);

			/// <summary>Temporary replica: created for replication and relocation only.</summary>
			public static readonly HdfsServerConstants.ReplicaState Temporary = new HdfsServerConstants.ReplicaState
				(4);

			private static readonly HdfsServerConstants.ReplicaState[] cachedValues = HdfsServerConstants.ReplicaState
				.Values();

			private readonly int value;

			private ReplicaState(int v)
			{
				HdfsServerConstants.ReplicaState.value = v;
			}

			public int GetValue()
			{
				return HdfsServerConstants.ReplicaState.value;
			}

			public static HdfsServerConstants.ReplicaState GetState(int v)
			{
				return HdfsServerConstants.ReplicaState.cachedValues[v];
			}

			/// <summary>Read from in</summary>
			/// <exception cref="System.IO.IOException"/>
			public static HdfsServerConstants.ReplicaState Read(DataInput @in)
			{
				return HdfsServerConstants.ReplicaState.cachedValues[@in.ReadByte()];
			}

			/// <summary>Write to out</summary>
			/// <exception cref="System.IO.IOException"/>
			public void Write(DataOutput @out)
			{
				@out.WriteByte(Ordinal());
			}
		}

		/// <summary>States, which a block can go through while it is under construction.</summary>
		public enum BlockUCState
		{
			Complete,
			UnderConstruction,
			UnderRecovery,
			Committed
		}

		public const string NamenodeLeaseHolder = "HDFS_NameNode";

		public const long NamenodeLeaseRecheckInterval = 2000;

		public const string CryptoXattrEncryptionZone = "raw.hdfs.crypto.encryption.zone";

		public const string CryptoXattrFileEncryptionInfo = "raw.hdfs.crypto.file.encryption.info";

		public const string SecurityXattrUnreadableBySuperuser = "security.hdfs.unreadable.by.superuser";
	}
}
