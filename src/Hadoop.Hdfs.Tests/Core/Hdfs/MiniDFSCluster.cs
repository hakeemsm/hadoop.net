using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class creates a single-process DFS cluster for junit testing.</summary>
	/// <remarks>
	/// This class creates a single-process DFS cluster for junit testing.
	/// The data directories for non-simulated DFS are under the testing directory.
	/// For simulated data nodes, no underlying fs storage is used.
	/// </remarks>
	public class MiniDFSCluster
	{
		private const string NameserviceIdPrefix = "nameserviceId";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.MiniDFSCluster
			));

		/// <summary>
		/// System property to set the data dir:
		/// <value/>
		/// 
		/// </summary>
		public const string PropTestBuildData = "test.build.data";

		/// <summary>
		/// Configuration option to set the data dir:
		/// <value/>
		/// 
		/// </summary>
		public const string HdfsMinidfsBasedir = "hdfs.minidfs.basedir";

		public const string DfsNamenodeSafemodeExtensionTestingKey = DFSConfigKeys.DfsNamenodeSafemodeExtensionKey
			 + ".testing";

		private const int DefaultStoragesPerDatanode = 2;

		static MiniDFSCluster()
		{
			// Changing this default may break some tests that assume it is 2.
			DefaultMetricsSystem.SetMiniClusterMode(true);
		}

		public virtual int GetStoragesPerDatanode()
		{
			return storagesPerDatanode;
		}

		/// <summary>Class to construct instances of MiniDFSClusters with specific options.</summary>
		public class Builder
		{
			private int nameNodePort = 0;

			private int nameNodeHttpPort = 0;

			private readonly Configuration conf;

			private int numDataNodes = 1;

			private StorageType[][] storageTypes = null;

			private StorageType[] storageTypes1D = null;

			private int storagesPerDatanode = DefaultStoragesPerDatanode;

			private bool format = true;

			private bool manageNameDfsDirs = true;

			private bool manageNameDfsSharedDirs = true;

			private bool enableManagedDfsDirsRedundancy = true;

			private bool manageDataDfsDirs = true;

			private HdfsServerConstants.StartupOption option = null;

			private HdfsServerConstants.StartupOption dnOption = null;

			private string[] racks = null;

			private string[] hosts = null;

			private long[] simulatedCapacities = null;

			private long[][] storageCapacities = null;

			private long[] storageCapacities1D = null;

			private string clusterId = null;

			private bool waitSafeMode = true;

			private bool setupHostsFile = false;

			private MiniDFSNNTopology nnTopology = null;

			private bool checkExitOnShutdown = true;

			private bool checkDataNodeAddrConfig = false;

			private bool checkDataNodeHostConfig = false;

			private Configuration[] dnConfOverlays;

			private bool skipFsyncForTesting = true;

			public Builder(Configuration conf)
			{
				this.conf = conf;
			}

			/// <summary>Default: 0</summary>
			public virtual MiniDFSCluster.Builder NameNodePort(int val)
			{
				this.nameNodePort = val;
				return this;
			}

			/// <summary>Default: 0</summary>
			public virtual MiniDFSCluster.Builder NameNodeHttpPort(int val)
			{
				this.nameNodeHttpPort = val;
				return this;
			}

			/// <summary>Default: 1</summary>
			public virtual MiniDFSCluster.Builder NumDataNodes(int val)
			{
				this.numDataNodes = val;
				return this;
			}

			/// <summary>Default: DEFAULT_STORAGES_PER_DATANODE</summary>
			public virtual MiniDFSCluster.Builder StoragesPerDatanode(int numStorages)
			{
				this.storagesPerDatanode = numStorages;
				return this;
			}

			/// <summary>Set the same storage type configuration for each datanode.</summary>
			/// <remarks>
			/// Set the same storage type configuration for each datanode.
			/// If storageTypes is uninitialized or passed null then
			/// StorageType.DEFAULT is used.
			/// </remarks>
			public virtual MiniDFSCluster.Builder StorageTypes(StorageType[] types)
			{
				this.storageTypes1D = types;
				return this;
			}

			/// <summary>Set custom storage type configuration for each datanode.</summary>
			/// <remarks>
			/// Set custom storage type configuration for each datanode.
			/// If storageTypes is uninitialized or passed null then
			/// StorageType.DEFAULT is used.
			/// </remarks>
			public virtual MiniDFSCluster.Builder StorageTypes(StorageType[][] types)
			{
				this.storageTypes = types;
				return this;
			}

			/// <summary>Set the same storage capacity configuration for each datanode.</summary>
			/// <remarks>
			/// Set the same storage capacity configuration for each datanode.
			/// If storageTypes is uninitialized or passed null then
			/// StorageType.DEFAULT is used.
			/// </remarks>
			public virtual MiniDFSCluster.Builder StorageCapacities(long[] capacities)
			{
				this.storageCapacities1D = capacities;
				return this;
			}

			/// <summary>Set custom storage capacity configuration for each datanode.</summary>
			/// <remarks>
			/// Set custom storage capacity configuration for each datanode.
			/// If storageCapacities is uninitialized or passed null then
			/// capacity is limited by available disk space.
			/// </remarks>
			public virtual MiniDFSCluster.Builder StorageCapacities(long[][] capacities)
			{
				this.storageCapacities = capacities;
				return this;
			}

			/// <summary>Default: true</summary>
			public virtual MiniDFSCluster.Builder Format(bool val)
			{
				this.format = val;
				return this;
			}

			/// <summary>Default: true</summary>
			public virtual MiniDFSCluster.Builder ManageNameDfsDirs(bool val)
			{
				this.manageNameDfsDirs = val;
				return this;
			}

			/// <summary>Default: true</summary>
			public virtual MiniDFSCluster.Builder ManageNameDfsSharedDirs(bool val)
			{
				this.manageNameDfsSharedDirs = val;
				return this;
			}

			/// <summary>Default: true</summary>
			public virtual MiniDFSCluster.Builder EnableManagedDfsDirsRedundancy(bool val)
			{
				this.enableManagedDfsDirsRedundancy = val;
				return this;
			}

			/// <summary>Default: true</summary>
			public virtual MiniDFSCluster.Builder ManageDataDfsDirs(bool val)
			{
				this.manageDataDfsDirs = val;
				return this;
			}

			/// <summary>Default: null</summary>
			public virtual MiniDFSCluster.Builder StartupOption(HdfsServerConstants.StartupOption
				 val)
			{
				this.option = val;
				return this;
			}

			/// <summary>Default: null</summary>
			public virtual MiniDFSCluster.Builder DnStartupOption(HdfsServerConstants.StartupOption
				 val)
			{
				this.dnOption = val;
				return this;
			}

			/// <summary>Default: null</summary>
			public virtual MiniDFSCluster.Builder Racks(string[] val)
			{
				this.racks = val;
				return this;
			}

			/// <summary>Default: null</summary>
			public virtual MiniDFSCluster.Builder Hosts(string[] val)
			{
				this.hosts = val;
				return this;
			}

			/// <summary>
			/// Use SimulatedFSDataset and limit the capacity of each DN per
			/// the values passed in val.
			/// </summary>
			/// <remarks>
			/// Use SimulatedFSDataset and limit the capacity of each DN per
			/// the values passed in val.
			/// For limiting the capacity of volumes with real storage, see
			/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl.FsVolumeImpl.SetCapacityForTesting(long)
			/// 	"/>
			/// Default: null
			/// </remarks>
			public virtual MiniDFSCluster.Builder SimulatedCapacities(long[] val)
			{
				this.simulatedCapacities = val;
				return this;
			}

			/// <summary>Default: true</summary>
			public virtual MiniDFSCluster.Builder WaitSafeMode(bool val)
			{
				this.waitSafeMode = val;
				return this;
			}

			/// <summary>Default: true</summary>
			public virtual MiniDFSCluster.Builder CheckExitOnShutdown(bool val)
			{
				this.checkExitOnShutdown = val;
				return this;
			}

			/// <summary>Default: false</summary>
			public virtual MiniDFSCluster.Builder CheckDataNodeAddrConfig(bool val)
			{
				this.checkDataNodeAddrConfig = val;
				return this;
			}

			/// <summary>Default: false</summary>
			public virtual MiniDFSCluster.Builder CheckDataNodeHostConfig(bool val)
			{
				this.checkDataNodeHostConfig = val;
				return this;
			}

			/// <summary>Default: null</summary>
			public virtual MiniDFSCluster.Builder ClusterId(string cid)
			{
				this.clusterId = cid;
				return this;
			}

			/// <summary>
			/// Default: false
			/// When true the hosts file/include file for the cluster is setup
			/// </summary>
			public virtual MiniDFSCluster.Builder SetupHostsFile(bool val)
			{
				this.setupHostsFile = val;
				return this;
			}

			/// <summary>Default: a single namenode.</summary>
			/// <remarks>
			/// Default: a single namenode.
			/// See
			/// <see cref="MiniDFSNNTopology.SimpleFederatedTopology(int)"/>
			/// to set up
			/// federated nameservices
			/// </remarks>
			public virtual MiniDFSCluster.Builder NnTopology(MiniDFSNNTopology topology)
			{
				this.nnTopology = topology;
				return this;
			}

			/// <summary>
			/// Default: null
			/// An array of
			/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
			/// objects that will overlay the
			/// global MiniDFSCluster Configuration for the corresponding DataNode.
			/// Useful for setting specific per-DataNode configuration parameters.
			/// </summary>
			public virtual MiniDFSCluster.Builder DataNodeConfOverlays(Configuration[] dnConfOverlays
				)
			{
				this.dnConfOverlays = dnConfOverlays;
				return this;
			}

			/// <summary>
			/// Default: true
			/// When true, we skip fsync() calls for speed improvements.
			/// </summary>
			public virtual MiniDFSCluster.Builder SkipFsyncForTesting(bool val)
			{
				this.skipFsyncForTesting = val;
				return this;
			}

			/// <summary>Construct the actual MiniDFSCluster</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual MiniDFSCluster Build()
			{
				return new MiniDFSCluster(this);
			}
		}

		/// <summary>Used by builder to create and return an instance of MiniDFSCluster</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal MiniDFSCluster(MiniDFSCluster.Builder builder)
		{
			if (builder.nnTopology == null)
			{
				// If no topology is specified, build a single NN. 
				builder.nnTopology = MiniDFSNNTopology.SimpleSingleNN(builder.nameNodePort, builder
					.nameNodeHttpPort);
			}
			System.Diagnostics.Debug.Assert(builder.storageTypes == null || builder.storageTypes
				.Length == builder.numDataNodes);
			int numNameNodes = builder.nnTopology.CountNameNodes();
			Log.Info("starting cluster: numNameNodes=" + numNameNodes + ", numDataNodes=" + builder
				.numDataNodes);
			nameNodes = new MiniDFSCluster.NameNodeInfo[numNameNodes];
			this.storagesPerDatanode = builder.storagesPerDatanode;
			// Duplicate the storageType setting for each DN.
			if (builder.storageTypes == null && builder.storageTypes1D != null)
			{
				System.Diagnostics.Debug.Assert(builder.storageTypes1D.Length == storagesPerDatanode
					);
				//builder.storageTypes = new StorageType[builder.numDataNodes][storagesPerDatanode];
				//HM: line above replaced by one below since CS doesnt take 2nd dim for array
				builder.storageTypes = new StorageType[builder.numDataNodes][];
				for (int i = 0; i < builder.numDataNodes; ++i)
				{
					builder.storageTypes[i] = builder.storageTypes1D;
				}
			}
			// Duplicate the storageCapacity setting for each DN.
			if (builder.storageCapacities == null && builder.storageCapacities1D != null)
			{
				System.Diagnostics.Debug.Assert(builder.storageCapacities1D.Length == storagesPerDatanode
					);
				//builder.storageCapacities = new long[builder.numDataNodes][storagesPerDatanode];
				//HM: line above replaced by one below since CS doesnt take 2nd dim for array
				builder.storageCapacities = new long[builder.numDataNodes][];
				for (int i = 0; i < builder.numDataNodes; ++i)
				{
					builder.storageCapacities[i] = builder.storageCapacities1D;
				}
			}
			InitMiniDFSCluster(builder.conf, builder.numDataNodes, builder.storageTypes, builder
				.format, builder.manageNameDfsDirs, builder.manageNameDfsSharedDirs, builder.enableManagedDfsDirsRedundancy
				, builder.manageDataDfsDirs, builder.option, builder.dnOption, builder.racks, builder
				.hosts, builder.storageCapacities, builder.simulatedCapacities, builder.clusterId
				, builder.waitSafeMode, builder.setupHostsFile, builder.nnTopology, builder.checkExitOnShutdown
				, builder.checkDataNodeAddrConfig, builder.checkDataNodeHostConfig, builder.dnConfOverlays
				, builder.skipFsyncForTesting);
		}

		public class DataNodeProperties
		{
			internal readonly DataNode datanode;

			internal readonly Configuration conf;

			internal string[] dnArgs;

			internal readonly SecureDataNodeStarter.SecureResources secureResources;

			internal readonly int ipcPort;

			internal DataNodeProperties(MiniDFSCluster _enclosing, DataNode node, Configuration
				 conf, string[] args, SecureDataNodeStarter.SecureResources secureResources, int
				 ipcPort)
			{
				this._enclosing = _enclosing;
				this.datanode = node;
				this.conf = conf;
				this.dnArgs = args;
				this.secureResources = secureResources;
				this.ipcPort = ipcPort;
			}

			public virtual void SetDnArgs(params string[] args)
			{
				this.dnArgs = args;
			}

			private readonly MiniDFSCluster _enclosing;
		}

		private Configuration conf;

		private MiniDFSCluster.NameNodeInfo[] nameNodes;

		protected internal int numDataNodes;

		protected internal readonly AList<MiniDFSCluster.DataNodeProperties> dataNodes = 
			new AList<MiniDFSCluster.DataNodeProperties>();

		private FilePath base_dir;

		private FilePath data_dir;

		private bool waitSafeMode = true;

		private bool federation;

		private bool checkExitOnShutdown = true;

		protected internal readonly int storagesPerDatanode;

		/// <summary>A unique instance identifier for the cluster.</summary>
		/// <remarks>
		/// A unique instance identifier for the cluster. This
		/// is used to disambiguate HA filesystems in the case where
		/// multiple MiniDFSClusters are used in the same test suite.
		/// </remarks>
		private int instanceId;

		private static int instanceCount = 0;

		/// <summary>Stores the information related to a namenode in the cluster</summary>
		public class NameNodeInfo
		{
			internal readonly NameNode nameNode;

			internal readonly Configuration conf;

			internal readonly string nameserviceId;

			internal readonly string nnId;

			internal HdfsServerConstants.StartupOption startOpt;

			internal NameNodeInfo(NameNode nn, string nameserviceId, string nnId, HdfsServerConstants.StartupOption
				 startOpt, Configuration conf)
			{
				this.nameNode = nn;
				this.nameserviceId = nameserviceId;
				this.nnId = nnId;
				this.startOpt = startOpt;
				this.conf = conf;
			}

			public virtual void SetStartOpt(HdfsServerConstants.StartupOption startOpt)
			{
				this.startOpt = startOpt;
			}
		}

		/// <summary>
		/// This null constructor is used only when wishing to start a data node cluster
		/// without a name node (ie when the name node is started elsewhere).
		/// </summary>
		public MiniDFSCluster()
		{
			nameNodes = new MiniDFSCluster.NameNodeInfo[0];
			// No namenode in the cluster
			storagesPerDatanode = DefaultStoragesPerDatanode;
			lock (typeof(MiniDFSCluster))
			{
				instanceId = instanceCount++;
			}
		}

		/// <summary>Modify the config and start up the servers with the given operation.</summary>
		/// <remarks>
		/// Modify the config and start up the servers with the given operation.
		/// Servers will be started on free ports.
		/// <p>
		/// The caller must manage the creation of NameNode and DataNode directories
		/// and have already set
		/// <see cref="DFSConfigKeys.DfsNamenodeNameDirKey"/>
		/// and
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// in the given conf.
		/// </remarks>
		/// <param name="conf">
		/// the base configuration to use in starting the servers.  This
		/// will be modified as necessary.
		/// </param>
		/// <param name="numDataNodes">Number of DataNodes to start; may be zero</param>
		/// <param name="nameNodeOperation">
		/// the operation with which to start the servers.  If null
		/// or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public MiniDFSCluster(Configuration conf, int numDataNodes, HdfsServerConstants.StartupOption
			 nameNodeOperation)
			: this(0, conf, numDataNodes, false, false, false, nameNodeOperation, null, null, 
				null)
		{
		}

		/// <summary>Modify the config and start up the servers.</summary>
		/// <remarks>
		/// Modify the config and start up the servers.  The rpc and info ports for
		/// servers are guaranteed to use free ports.
		/// <p>
		/// NameNode and DataNode directory creation and configuration will be
		/// managed by this class.
		/// </remarks>
		/// <param name="conf">
		/// the base configuration to use in starting the servers.  This
		/// will be modified as necessary.
		/// </param>
		/// <param name="numDataNodes">Number of DataNodes to start; may be zero</param>
		/// <param name="format">if true, format the NameNode and DataNodes before starting up
		/// 	</param>
		/// <param name="racks">array of strings indicating the rack that each DataNode is on
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public MiniDFSCluster(Configuration conf, int numDataNodes, bool format, string[]
			 racks)
			: this(0, conf, numDataNodes, format, true, true, null, racks, null, null)
		{
		}

		/// <summary>Modify the config and start up the servers.</summary>
		/// <remarks>
		/// Modify the config and start up the servers.  The rpc and info ports for
		/// servers are guaranteed to use free ports.
		/// <p>
		/// NameNode and DataNode directory creation and configuration will be
		/// managed by this class.
		/// </remarks>
		/// <param name="conf">
		/// the base configuration to use in starting the servers.  This
		/// will be modified as necessary.
		/// </param>
		/// <param name="numDataNodes">Number of DataNodes to start; may be zero</param>
		/// <param name="format">if true, format the NameNode and DataNodes before starting up
		/// 	</param>
		/// <param name="racks">array of strings indicating the rack that each DataNode is on
		/// 	</param>
		/// <param name="hosts">array of strings indicating the hostname for each DataNode</param>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public MiniDFSCluster(Configuration conf, int numDataNodes, bool format, string[]
			 racks, string[] hosts)
			: this(0, conf, numDataNodes, format, true, true, null, racks, hosts, null)
		{
		}

		/// <summary>
		/// NOTE: if possible, the other constructors that don't have nameNode port
		/// parameter should be used as they will ensure that the servers use free
		/// ports.
		/// </summary>
		/// <remarks>
		/// NOTE: if possible, the other constructors that don't have nameNode port
		/// parameter should be used as they will ensure that the servers use free
		/// ports.
		/// <p>
		/// Modify the config and start up the servers.
		/// </remarks>
		/// <param name="nameNodePort">
		/// suggestion for which rpc port to use.  caller should
		/// use getNameNodePort() to get the actual port used.
		/// </param>
		/// <param name="conf">
		/// the base configuration to use in starting the servers.  This
		/// will be modified as necessary.
		/// </param>
		/// <param name="numDataNodes">Number of DataNodes to start; may be zero</param>
		/// <param name="format">
		/// if true, format the NameNode and DataNodes before starting
		/// up
		/// </param>
		/// <param name="manageDfsDirs">
		/// if true, the data directories for servers will be
		/// created and
		/// <see cref="DFSConfigKeys.DfsNamenodeNameDirKey"/>
		/// and
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// will be set in
		/// the conf
		/// </param>
		/// <param name="operation">
		/// the operation with which to start the servers.  If null
		/// or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
		/// </param>
		/// <param name="racks">array of strings indicating the rack that each DataNode is on
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public MiniDFSCluster(int nameNodePort, Configuration conf, int numDataNodes, bool
			 format, bool manageDfsDirs, HdfsServerConstants.StartupOption operation, string
			[] racks)
			: this(nameNodePort, conf, numDataNodes, format, manageDfsDirs, manageDfsDirs, operation
				, racks, null, null)
		{
		}

		/// <summary>
		/// NOTE: if possible, the other constructors that don't have nameNode port
		/// parameter should be used as they will ensure that the servers use free ports.
		/// </summary>
		/// <remarks>
		/// NOTE: if possible, the other constructors that don't have nameNode port
		/// parameter should be used as they will ensure that the servers use free ports.
		/// <p>
		/// Modify the config and start up the servers.
		/// </remarks>
		/// <param name="nameNodePort">
		/// suggestion for which rpc port to use.  caller should
		/// use getNameNodePort() to get the actual port used.
		/// </param>
		/// <param name="conf">
		/// the base configuration to use in starting the servers.  This
		/// will be modified as necessary.
		/// </param>
		/// <param name="numDataNodes">Number of DataNodes to start; may be zero</param>
		/// <param name="format">if true, format the NameNode and DataNodes before starting up
		/// 	</param>
		/// <param name="manageDfsDirs">
		/// if true, the data directories for servers will be
		/// created and
		/// <see cref="DFSConfigKeys.DfsNamenodeNameDirKey"/>
		/// and
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// will be set in
		/// the conf
		/// </param>
		/// <param name="operation">
		/// the operation with which to start the servers.  If null
		/// or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
		/// </param>
		/// <param name="racks">array of strings indicating the rack that each DataNode is on
		/// 	</param>
		/// <param name="simulatedCapacities">array of capacities of the simulated data nodes
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public MiniDFSCluster(int nameNodePort, Configuration conf, int numDataNodes, bool
			 format, bool manageDfsDirs, HdfsServerConstants.StartupOption operation, string
			[] racks, long[] simulatedCapacities)
			: this(nameNodePort, conf, numDataNodes, format, manageDfsDirs, manageDfsDirs, operation
				, racks, null, simulatedCapacities)
		{
		}

		/// <summary>
		/// NOTE: if possible, the other constructors that don't have nameNode port
		/// parameter should be used as they will ensure that the servers use free ports.
		/// </summary>
		/// <remarks>
		/// NOTE: if possible, the other constructors that don't have nameNode port
		/// parameter should be used as they will ensure that the servers use free ports.
		/// <p>
		/// Modify the config and start up the servers.
		/// </remarks>
		/// <param name="nameNodePort">
		/// suggestion for which rpc port to use.  caller should
		/// use getNameNodePort() to get the actual port used.
		/// </param>
		/// <param name="conf">
		/// the base configuration to use in starting the servers.  This
		/// will be modified as necessary.
		/// </param>
		/// <param name="numDataNodes">Number of DataNodes to start; may be zero</param>
		/// <param name="format">if true, format the NameNode and DataNodes before starting up
		/// 	</param>
		/// <param name="manageNameDfsDirs">
		/// if true, the data directories for servers will be
		/// created and
		/// <see cref="DFSConfigKeys.DfsNamenodeNameDirKey"/>
		/// and
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// will be set in
		/// the conf
		/// </param>
		/// <param name="manageDataDfsDirs">
		/// if true, the data directories for datanodes will
		/// be created and
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// set to same in the conf
		/// </param>
		/// <param name="operation">
		/// the operation with which to start the servers.  If null
		/// or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
		/// </param>
		/// <param name="racks">array of strings indicating the rack that each DataNode is on
		/// 	</param>
		/// <param name="hosts">array of strings indicating the hostnames of each DataNode</param>
		/// <param name="simulatedCapacities">array of capacities of the simulated data nodes
		/// 	</param>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public MiniDFSCluster(int nameNodePort, Configuration conf, int numDataNodes, bool
			 format, bool manageNameDfsDirs, bool manageDataDfsDirs, HdfsServerConstants.StartupOption
			 operation, string[] racks, string[] hosts, long[] simulatedCapacities)
		{
			// in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
			// in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
			// in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
			// in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
			// in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
			// in 22 to be removed in 24. Use MiniDFSCluster.Builder instead
			this.nameNodes = new MiniDFSCluster.NameNodeInfo[1];
			// Single namenode in the cluster
			this.storagesPerDatanode = DefaultStoragesPerDatanode;
			InitMiniDFSCluster(conf, numDataNodes, null, format, manageNameDfsDirs, true, manageDataDfsDirs
				, manageDataDfsDirs, operation, null, racks, hosts, null, simulatedCapacities, null
				, true, false, MiniDFSNNTopology.SimpleSingleNN(nameNodePort, 0), true, false, false
				, null, true);
		}

		/// <exception cref="System.IO.IOException"/>
		private void InitMiniDFSCluster(Configuration conf, int numDataNodes, StorageType
			[][] storageTypes, bool format, bool manageNameDfsDirs, bool manageNameDfsSharedDirs
			, bool enableManagedDfsDirsRedundancy, bool manageDataDfsDirs, HdfsServerConstants.StartupOption
			 startOpt, HdfsServerConstants.StartupOption dnStartOpt, string[] racks, string[]
			 hosts, long[][] storageCapacities, long[] simulatedCapacities, string clusterId
			, bool waitSafeMode, bool setupHostsFile, MiniDFSNNTopology nnTopology, bool checkExitOnShutdown
			, bool checkDataNodeAddrConfig, bool checkDataNodeHostConfig, Configuration[] dnConfOverlays
			, bool skipFsyncForTesting)
		{
			bool success = false;
			try
			{
				ExitUtil.DisableSystemExit();
				// Re-enable symlinks for tests, see HADOOP-10020 and HADOOP-10052
				FileSystem.EnableSymlinks();
				lock (typeof(MiniDFSCluster))
				{
					instanceId = instanceCount++;
				}
				this.conf = conf;
				base_dir = new FilePath(DetermineDfsBaseDir());
				data_dir = new FilePath(base_dir, "data");
				this.waitSafeMode = waitSafeMode;
				this.checkExitOnShutdown = checkExitOnShutdown;
				int replication = conf.GetInt(DFSConfigKeys.DfsReplicationKey, 3);
				conf.SetInt(DFSConfigKeys.DfsReplicationKey, Math.Min(replication, numDataNodes));
				int safemodeExtension = conf.GetInt(DfsNamenodeSafemodeExtensionTestingKey, 0);
				conf.SetInt(DFSConfigKeys.DfsNamenodeSafemodeExtensionKey, safemodeExtension);
				conf.SetInt(DFSConfigKeys.DfsNamenodeDecommissionIntervalKey, 3);
				// 3 second
				conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
					typeof(StaticMapping), typeof(DNSToSwitchMapping));
				// In an HA cluster, in order for the StandbyNode to perform checkpoints,
				// it needs to know the HTTP port of the Active. So, if ephemeral ports
				// are chosen, disable checkpoints for the test.
				if (!nnTopology.AllHttpPortsSpecified() && nnTopology.IsHA())
				{
					Log.Info("MiniDFSCluster disabling checkpointing in the Standby node " + "since no HTTP ports have been specified."
						);
					conf.SetBoolean(DFSConfigKeys.DfsHaStandbyCheckpointsKey, false);
				}
				if (!nnTopology.AllIpcPortsSpecified() && nnTopology.IsHA())
				{
					Log.Info("MiniDFSCluster disabling log-roll triggering in the " + "Standby node since no IPC ports have been specified."
						);
					conf.SetInt(DFSConfigKeys.DfsHaLogrollPeriodKey, -1);
				}
				EditLogFileOutputStream.SetShouldSkipFsyncForTesting(skipFsyncForTesting);
				federation = nnTopology.IsFederated();
				try
				{
					CreateNameNodesAndSetConf(nnTopology, manageNameDfsDirs, manageNameDfsSharedDirs, 
						enableManagedDfsDirsRedundancy, format, startOpt, clusterId, conf);
				}
				catch (IOException ioe)
				{
					Log.Error("IOE creating namenodes. Permissions dump:\n" + CreatePermissionsDiagnosisString
						(data_dir), ioe);
					throw;
				}
				if (format)
				{
					if (data_dir.Exists() && !FileUtil.FullyDelete(data_dir))
					{
						throw new IOException("Cannot remove data directory: " + data_dir + CreatePermissionsDiagnosisString
							(data_dir));
					}
				}
				if (startOpt == HdfsServerConstants.StartupOption.Recover)
				{
					return;
				}
				// Start the DataNodes
				StartDataNodes(conf, numDataNodes, storageTypes, manageDataDfsDirs, dnStartOpt !=
					 null ? dnStartOpt : startOpt, racks, hosts, storageCapacities, simulatedCapacities
					, setupHostsFile, checkDataNodeAddrConfig, checkDataNodeHostConfig, dnConfOverlays
					);
				WaitClusterUp();
				//make sure ProxyUsers uses the latest conf
				ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
				success = true;
			}
			finally
			{
				if (!success)
				{
					Shutdown();
				}
			}
		}

		/// <returns>
		/// a debug string which can help diagnose an error of why
		/// a given directory might have a permissions error in the context
		/// of a test case
		/// </returns>
		private string CreatePermissionsDiagnosisString(FilePath path)
		{
			StringBuilder sb = new StringBuilder();
			while (path != null)
			{
				sb.Append("path '" + path + "': ").Append("\n");
				sb.Append("\tabsolute:").Append(path.GetAbsolutePath()).Append("\n");
				sb.Append("\tpermissions: ");
				sb.Append(path.IsDirectory() ? "d" : "-");
				sb.Append(FileUtil.CanRead(path) ? "r" : "-");
				sb.Append(FileUtil.CanWrite(path) ? "w" : "-");
				sb.Append(FileUtil.CanExecute(path) ? "x" : "-");
				sb.Append("\n");
				path = path.GetParentFile();
			}
			return sb.ToString();
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateNameNodesAndSetConf(MiniDFSNNTopology nnTopology, bool manageNameDfsDirs
			, bool manageNameDfsSharedDirs, bool enableManagedDfsDirsRedundancy, bool format
			, HdfsServerConstants.StartupOption operation, string clusterId, Configuration conf
			)
		{
			Preconditions.CheckArgument(nnTopology.CountNameNodes() > 0, "empty NN topology: no namenodes specified!"
				);
			if (!federation && nnTopology.CountNameNodes() == 1)
			{
				MiniDFSNNTopology.NNConf onlyNN = nnTopology.GetOnlyNameNode();
				// we only had one NN, set DEFAULT_NAME for it. If not explicitly
				// specified initially, the port will be 0 to make NN bind to any
				// available port. It will be set to the right address after
				// NN is started.
				conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, "hdfs://127.0.0.1:" + onlyNN
					.GetIpcPort());
			}
			IList<string> allNsIds = Lists.NewArrayList();
			foreach (MiniDFSNNTopology.NSConf nameservice in nnTopology.GetNameservices())
			{
				if (nameservice.GetId() != null)
				{
					allNsIds.AddItem(nameservice.GetId());
				}
			}
			if (!allNsIds.IsEmpty())
			{
				conf.Set(DFSConfigKeys.DfsNameservices, Joiner.On(",").Join(allNsIds));
			}
			int nnCounter = 0;
			foreach (MiniDFSNNTopology.NSConf nameservice_1 in nnTopology.GetNameservices())
			{
				string nsId = nameservice_1.GetId();
				string lastDefaultFileSystem = null;
				Preconditions.CheckArgument(!federation || nsId != null, "if there is more than one NS, they must have names"
					);
				// First set up the configuration which all of the NNs
				// need to have - have to do this a priori before starting
				// *any* of the NNs, so they know to come up in standby.
				IList<string> nnIds = Lists.NewArrayList();
				// Iterate over the NNs in this nameservice
				foreach (MiniDFSNNTopology.NNConf nn in nameservice_1.GetNNs())
				{
					nnIds.AddItem(nn.GetNnId());
					InitNameNodeAddress(conf, nameservice_1.GetId(), nn);
				}
				// If HA is enabled on this nameservice, enumerate all the namenodes
				// in the configuration. Also need to set a shared edits dir
				if (nnIds.Count > 1)
				{
					conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, nameservice_1
						.GetId()), Joiner.On(",").Join(nnIds));
					if (manageNameDfsSharedDirs)
					{
						URI sharedEditsUri = GetSharedEditsDir(nnCounter, nnCounter + nnIds.Count - 1);
						conf.Set(DFSConfigKeys.DfsNamenodeSharedEditsDirKey, sharedEditsUri.ToString());
						// Clean out the shared edits dir completely, including all subdirectories.
						FileUtil.FullyDelete(new FilePath(sharedEditsUri));
					}
				}
				// Now format first NN and copy the storage directory from that node to the others.
				int i = 0;
				ICollection<URI> prevNNDirs = null;
				int nnCounterForFormat = nnCounter;
				foreach (MiniDFSNNTopology.NNConf nn_1 in nameservice_1.GetNNs())
				{
					InitNameNodeConf(conf, nsId, nn_1.GetNnId(), manageNameDfsDirs, enableManagedDfsDirsRedundancy
						, nnCounterForFormat);
					ICollection<URI> namespaceDirs = FSNamesystem.GetNamespaceDirs(conf);
					if (format)
					{
						foreach (URI nameDirUri in namespaceDirs)
						{
							FilePath nameDir = new FilePath(nameDirUri);
							if (nameDir.Exists() && !FileUtil.FullyDelete(nameDir))
							{
								throw new IOException("Could not fully delete " + nameDir);
							}
						}
						ICollection<URI> checkpointDirs = Org.Apache.Hadoop.Hdfs.Server.Common.Util.StringCollectionAsURIs
							(conf.GetTrimmedStringCollection(DFSConfigKeys.DfsNamenodeCheckpointDirKey));
						foreach (URI checkpointDirUri in checkpointDirs)
						{
							FilePath checkpointDir = new FilePath(checkpointDirUri);
							if (checkpointDir.Exists() && !FileUtil.FullyDelete(checkpointDir))
							{
								throw new IOException("Could not fully delete " + checkpointDir);
							}
						}
					}
					bool formatThisOne = format;
					if (format && i++ > 0)
					{
						// Don't format the second NN in an HA setup - that
						// would result in it having a different clusterID,
						// block pool ID, etc. Instead, copy the name dirs
						// from the first one.
						formatThisOne = false;
						System.Diagnostics.Debug.Assert((null != prevNNDirs));
						CopyNameDirs(prevNNDirs, namespaceDirs, conf);
					}
					nnCounterForFormat++;
					if (formatThisOne)
					{
						// Allow overriding clusterID for specific NNs to test
						// misconfiguration.
						if (nn_1.GetClusterId() == null)
						{
							HdfsServerConstants.StartupOption.Format.SetClusterId(clusterId);
						}
						else
						{
							HdfsServerConstants.StartupOption.Format.SetClusterId(nn_1.GetClusterId());
						}
						DFSTestUtil.FormatNameNode(conf);
					}
					prevNNDirs = namespaceDirs;
				}
				// Start all Namenodes
				foreach (MiniDFSNNTopology.NNConf nn_2 in nameservice_1.GetNNs())
				{
					InitNameNodeConf(conf, nsId, nn_2.GetNnId(), manageNameDfsDirs, enableManagedDfsDirsRedundancy
						, nnCounter);
					CreateNameNode(nnCounter, conf, numDataNodes, false, operation, clusterId, nsId, 
						nn_2.GetNnId());
					// Record the last namenode uri
					if (nameNodes[nnCounter] != null && nameNodes[nnCounter].conf != null)
					{
						lastDefaultFileSystem = nameNodes[nnCounter].conf.Get(CommonConfigurationKeysPublic
							.FsDefaultNameKey);
					}
					nnCounter++;
				}
				if (!federation && lastDefaultFileSystem != null)
				{
					// Set the default file system to the actual bind address of NN.
					conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, lastDefaultFileSystem);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual URI GetSharedEditsDir(int minNN, int maxNN)
		{
			return FormatSharedEditsDir(base_dir, minNN, maxNN);
		}

		/// <exception cref="System.IO.IOException"/>
		public static URI FormatSharedEditsDir(FilePath baseDir, int minNN, int maxNN)
		{
			return Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI(new FilePath(baseDir, 
				"shared-edits-" + minNN + "-through-" + maxNN));
		}

		public virtual MiniDFSCluster.NameNodeInfo[] GetNameNodeInfos()
		{
			return this.nameNodes;
		}

		/// <exception cref="System.IO.IOException"/>
		private void InitNameNodeConf(Configuration conf, string nameserviceId, string nnId
			, bool manageNameDfsDirs, bool enableManagedDfsDirsRedundancy, int nnIndex)
		{
			if (nameserviceId != null)
			{
				conf.Set(DFSConfigKeys.DfsNameserviceId, nameserviceId);
			}
			if (nnId != null)
			{
				conf.Set(DFSConfigKeys.DfsHaNamenodeIdKey, nnId);
			}
			if (manageNameDfsDirs)
			{
				if (enableManagedDfsDirsRedundancy)
				{
					conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
						(new FilePath(base_dir, "name" + (2 * nnIndex + 1))) + "," + Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
						(new FilePath(base_dir, "name" + (2 * nnIndex + 2))));
					conf.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
						(new FilePath(base_dir, "namesecondary" + (2 * nnIndex + 1))) + "," + Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
						(new FilePath(base_dir, "namesecondary" + (2 * nnIndex + 2))));
				}
				else
				{
					conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
						(new FilePath(base_dir, "name" + (2 * nnIndex + 1))).ToString());
					conf.Set(DFSConfigKeys.DfsNamenodeCheckpointDirKey, Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
						(new FilePath(base_dir, "namesecondary" + (2 * nnIndex + 1))).ToString());
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void CopyNameDirs(ICollection<URI> srcDirs, ICollection<URI> dstDirs
			, Configuration dstConf)
		{
			URI srcDir = Lists.NewArrayList(srcDirs)[0];
			FileSystem dstFS = FileSystem.GetLocal(dstConf).GetRaw();
			foreach (URI dstDir in dstDirs)
			{
				Preconditions.CheckArgument(!dstDir.Equals(srcDir), "src and dst are the same: " 
					+ dstDir);
				FilePath dstDirF = new FilePath(dstDir);
				if (dstDirF.Exists())
				{
					if (!FileUtil.FullyDelete(dstDirF))
					{
						throw new IOException("Unable to delete: " + dstDirF);
					}
				}
				Log.Info("Copying namedir from primary node dir " + srcDir + " to " + dstDir);
				FileUtil.Copy(new FilePath(srcDir), dstFS, new Path(dstDir), false, dstConf);
			}
		}

		/// <summary>Initialize the address and port for this NameNode.</summary>
		/// <remarks>
		/// Initialize the address and port for this NameNode. In the
		/// non-federated case, the nameservice and namenode ID may be
		/// null.
		/// </remarks>
		private static void InitNameNodeAddress(Configuration conf, string nameserviceId, 
			MiniDFSNNTopology.NNConf nnConf)
		{
			// Set NN-specific specific key
			string key = DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeHttpAddressKey, nameserviceId
				, nnConf.GetNnId());
			conf.Set(key, "127.0.0.1:" + nnConf.GetHttpPort());
			key = DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, nameserviceId
				, nnConf.GetNnId());
			conf.Set(key, "127.0.0.1:" + nnConf.GetIpcPort());
		}

		private static string[] CreateArgs(HdfsServerConstants.StartupOption operation)
		{
			if (operation == HdfsServerConstants.StartupOption.Rollingupgrade)
			{
				return new string[] { operation.GetName(), operation.GetRollingUpgradeStartupOption
					().ToString() };
			}
			string[] args = (operation == null || operation == HdfsServerConstants.StartupOption
				.Format || operation == HdfsServerConstants.StartupOption.Regular) ? new string[
				] {  } : new string[] { operation.GetName() };
			return args;
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateNameNode(int nnIndex, Configuration conf, int numDataNodes, bool
			 format, HdfsServerConstants.StartupOption operation, string clusterId, string nameserviceId
			, string nnId)
		{
			// Format and clean out DataNode directories
			if (format)
			{
				DFSTestUtil.FormatNameNode(conf);
			}
			if (operation == HdfsServerConstants.StartupOption.Upgrade)
			{
				operation.SetClusterId(clusterId);
			}
			// Start the NameNode after saving the default file system.
			string originalDefaultFs = conf.Get(CommonConfigurationKeysPublic.FsDefaultNameKey
				);
			string[] args = CreateArgs(operation);
			NameNode nn = NameNode.CreateNameNode(args, conf);
			if (operation == HdfsServerConstants.StartupOption.Recover)
			{
				return;
			}
			// After the NN has started, set back the bound ports into
			// the conf
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, nameserviceId
				, nnId), nn.GetNameNodeAddressHostPortString());
			if (nn.GetHttpAddress() != null)
			{
				conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeHttpAddressKey, nameserviceId
					, nnId), NetUtils.GetHostPortString(nn.GetHttpAddress()));
			}
			if (nn.GetHttpsAddress() != null)
			{
				conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeHttpsAddressKey, nameserviceId
					, nnId), NetUtils.GetHostPortString(nn.GetHttpsAddress()));
			}
			DFSUtil.SetGenericConf(conf, nameserviceId, nnId, DFSConfigKeys.DfsNamenodeHttpAddressKey
				);
			nameNodes[nnIndex] = new MiniDFSCluster.NameNodeInfo(nn, nameserviceId, nnId, operation
				, new Configuration(conf));
			// Restore the default fs name
			if (originalDefaultFs == null)
			{
				conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, string.Empty);
			}
			else
			{
				conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, originalDefaultFs);
			}
		}

		/// <returns>URI of the namenode from a single namenode MiniDFSCluster</returns>
		public virtual URI GetURI()
		{
			CheckSingleNameNode();
			return GetURI(0);
		}

		/// <returns>URI of the given namenode in MiniDFSCluster</returns>
		public virtual URI GetURI(int nnIndex)
		{
			string hostPort = nameNodes[nnIndex].nameNode.GetNameNodeAddressHostPortString();
			URI uri = null;
			try
			{
				uri = new URI("hdfs://" + hostPort);
			}
			catch (URISyntaxException e)
			{
				NameNode.Log.Warn("unexpected URISyntaxException: " + e);
			}
			return uri;
		}

		public virtual int GetInstanceId()
		{
			return instanceId;
		}

		/// <returns>Configuration of for the given namenode</returns>
		public virtual Configuration GetConfiguration(int nnIndex)
		{
			return nameNodes[nnIndex].conf;
		}

		/// <summary>wait for the given namenode to get out of safemode.</summary>
		public virtual void WaitNameNodeUp(int nnIndex)
		{
			while (!IsNameNodeUp(nnIndex))
			{
				try
				{
					Log.Warn("Waiting for namenode at " + nnIndex + " to start...");
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
			}
		}

		/// <summary>wait for the cluster to get out of safemode.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void WaitClusterUp()
		{
			int i = 0;
			if (numDataNodes > 0)
			{
				while (!IsClusterUp())
				{
					try
					{
						Log.Warn("Waiting for the Mini HDFS Cluster to start...");
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
					}
					if (++i > 10)
					{
						string msg = "Timed out waiting for Mini HDFS Cluster to start";
						Log.Error(msg);
						throw new IOException(msg);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual string MakeDataNodeDirs(int dnIndex, StorageType[] storageTypes)
		{
			StringBuilder sb = new StringBuilder();
			System.Diagnostics.Debug.Assert(storageTypes == null || storageTypes.Length == storagesPerDatanode
				);
			for (int j = 0; j < storagesPerDatanode; ++j)
			{
				FilePath dir = GetInstanceStorageDir(dnIndex, j);
				dir.Mkdirs();
				if (!dir.IsDirectory())
				{
					throw new IOException("Mkdirs failed to create directory for DataNode " + dir);
				}
				sb.Append((j > 0 ? "," : string.Empty) + "[" + (storageTypes == null ? StorageType
					.Default : storageTypes[j]) + "]" + Org.Apache.Hadoop.Hdfs.Server.Common.Util.FileAsURI
					(dir));
			}
			return sb.ToString();
		}

		/// <summary>Modify the config and start up additional DataNodes.</summary>
		/// <remarks>
		/// Modify the config and start up additional DataNodes.  The info port for
		/// DataNodes is guaranteed to use a free port.
		/// Data nodes can run with the name node in the mini cluster or
		/// a real name node. For example, running with a real name node is useful
		/// when running simulated data nodes with a real name node.
		/// If minicluster's name node is null assume that the conf has been
		/// set with the right address:port of the name node.
		/// </remarks>
		/// <param name="conf">
		/// the base configuration to use in starting the DataNodes.  This
		/// will be modified as necessary.
		/// </param>
		/// <param name="numDataNodes">Number of DataNodes to start; may be zero</param>
		/// <param name="manageDfsDirs">
		/// if true, the data directories for DataNodes will be
		/// created and
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// will be set
		/// in the conf
		/// </param>
		/// <param name="operation">
		/// the operation with which to start the DataNodes.  If null
		/// or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
		/// </param>
		/// <param name="racks">array of strings indicating the rack that each DataNode is on
		/// 	</param>
		/// <param name="hosts">array of strings indicating the hostnames for each DataNode</param>
		/// <param name="simulatedCapacities">array of capacities of the simulated data nodes
		/// 	</param>
		/// <exception cref="System.InvalidOperationException">if NameNode has been shutdown</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void StartDataNodes(Configuration conf, int numDataNodes, bool manageDfsDirs
			, HdfsServerConstants.StartupOption operation, string[] racks, string[] hosts, long
			[] simulatedCapacities)
		{
			lock (this)
			{
				StartDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, hosts, simulatedCapacities
					, false);
			}
		}

		/// <summary>Modify the config and start up additional DataNodes.</summary>
		/// <remarks>
		/// Modify the config and start up additional DataNodes.  The info port for
		/// DataNodes is guaranteed to use a free port.
		/// Data nodes can run with the name node in the mini cluster or
		/// a real name node. For example, running with a real name node is useful
		/// when running simulated data nodes with a real name node.
		/// If minicluster's name node is null assume that the conf has been
		/// set with the right address:port of the name node.
		/// </remarks>
		/// <param name="conf">
		/// the base configuration to use in starting the DataNodes.  This
		/// will be modified as necessary.
		/// </param>
		/// <param name="numDataNodes">Number of DataNodes to start; may be zero</param>
		/// <param name="manageDfsDirs">
		/// if true, the data directories for DataNodes will be
		/// created and
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// will be
		/// set in the conf
		/// </param>
		/// <param name="operation">
		/// the operation with which to start the DataNodes.  If null
		/// or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
		/// </param>
		/// <param name="racks">array of strings indicating the rack that each DataNode is on
		/// 	</param>
		/// <param name="hosts">array of strings indicating the hostnames for each DataNode</param>
		/// <param name="simulatedCapacities">array of capacities of the simulated data nodes
		/// 	</param>
		/// <param name="setupHostsFile">add new nodes to dfs hosts files</param>
		/// <exception cref="System.InvalidOperationException">if NameNode has been shutdown</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void StartDataNodes(Configuration conf, int numDataNodes, bool manageDfsDirs
			, HdfsServerConstants.StartupOption operation, string[] racks, string[] hosts, long
			[] simulatedCapacities, bool setupHostsFile)
		{
			lock (this)
			{
				StartDataNodes(conf, numDataNodes, null, manageDfsDirs, operation, racks, hosts, 
					null, simulatedCapacities, setupHostsFile, false, false, null);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartDataNodes(Configuration conf, int numDataNodes, bool manageDfsDirs
			, HdfsServerConstants.StartupOption operation, string[] racks, string[] hosts, long
			[] simulatedCapacities, bool setupHostsFile, bool checkDataNodeAddrConfig)
		{
			lock (this)
			{
				StartDataNodes(conf, numDataNodes, null, manageDfsDirs, operation, racks, hosts, 
					null, simulatedCapacities, setupHostsFile, checkDataNodeAddrConfig, false, null);
			}
		}

		/// <summary>Modify the config and start up additional DataNodes.</summary>
		/// <remarks>
		/// Modify the config and start up additional DataNodes.  The info port for
		/// DataNodes is guaranteed to use a free port.
		/// Data nodes can run with the name node in the mini cluster or
		/// a real name node. For example, running with a real name node is useful
		/// when running simulated data nodes with a real name node.
		/// If minicluster's name node is null assume that the conf has been
		/// set with the right address:port of the name node.
		/// </remarks>
		/// <param name="conf">
		/// the base configuration to use in starting the DataNodes.  This
		/// will be modified as necessary.
		/// </param>
		/// <param name="numDataNodes">Number of DataNodes to start; may be zero</param>
		/// <param name="manageDfsDirs">
		/// if true, the data directories for DataNodes will be
		/// created and
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// will be
		/// set in the conf
		/// </param>
		/// <param name="operation">
		/// the operation with which to start the DataNodes.  If null
		/// or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
		/// </param>
		/// <param name="racks">array of strings indicating the rack that each DataNode is on
		/// 	</param>
		/// <param name="hosts">array of strings indicating the hostnames for each DataNode</param>
		/// <param name="simulatedCapacities">array of capacities of the simulated data nodes
		/// 	</param>
		/// <param name="setupHostsFile">add new nodes to dfs hosts files</param>
		/// <param name="checkDataNodeAddrConfig">if true, only set DataNode port addresses if not already set in config
		/// 	</param>
		/// <param name="checkDataNodeHostConfig">if true, only set DataNode hostname key if not already set in config
		/// 	</param>
		/// <param name="dnConfOverlays">
		/// An array of
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// objects that will overlay the
		/// global MiniDFSCluster Configuration for the corresponding DataNode.
		/// </param>
		/// <exception cref="System.InvalidOperationException">if NameNode has been shutdown</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void StartDataNodes(Configuration conf, int numDataNodes, StorageType
			[][] storageTypes, bool manageDfsDirs, HdfsServerConstants.StartupOption operation
			, string[] racks, string[] hosts, long[][] storageCapacities, long[] simulatedCapacities
			, bool setupHostsFile, bool checkDataNodeAddrConfig, bool checkDataNodeHostConfig
			, Configuration[] dnConfOverlays)
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(storageCapacities == null || simulatedCapacities 
					== null);
				System.Diagnostics.Debug.Assert(storageTypes == null || storageTypes.Length == numDataNodes
					);
				System.Diagnostics.Debug.Assert(storageCapacities == null || storageCapacities.Length
					 == numDataNodes);
				if (operation == HdfsServerConstants.StartupOption.Recover)
				{
					return;
				}
				if (checkDataNodeHostConfig)
				{
					conf.SetIfUnset(DFSConfigKeys.DfsDatanodeHostNameKey, "127.0.0.1");
				}
				else
				{
					conf.Set(DFSConfigKeys.DfsDatanodeHostNameKey, "127.0.0.1");
				}
				int curDatanodesNum = dataNodes.Count;
				int curDatanodesNumSaved = curDatanodesNum;
				// for mincluster's the default initialDelay for BRs is 0
				if (conf.Get(DFSConfigKeys.DfsBlockreportInitialDelayKey) == null)
				{
					conf.SetLong(DFSConfigKeys.DfsBlockreportInitialDelayKey, 0);
				}
				// If minicluster's name node is null assume that the conf has been
				// set with the right address:port of the name node.
				//
				if (racks != null && numDataNodes > racks.Length)
				{
					throw new ArgumentException("The length of racks [" + racks.Length + "] is less than the number of datanodes ["
						 + numDataNodes + "].");
				}
				if (hosts != null && numDataNodes > hosts.Length)
				{
					throw new ArgumentException("The length of hosts [" + hosts.Length + "] is less than the number of datanodes ["
						 + numDataNodes + "].");
				}
				//Generate some hostnames if required
				if (racks != null && hosts == null)
				{
					hosts = new string[numDataNodes];
					for (int i = curDatanodesNum; i < curDatanodesNum + numDataNodes; i++)
					{
						hosts[i - curDatanodesNum] = "host" + i + ".foo.com";
					}
				}
				if (simulatedCapacities != null && numDataNodes > simulatedCapacities.Length)
				{
					throw new ArgumentException("The length of simulatedCapacities [" + simulatedCapacities
						.Length + "] is less than the number of datanodes [" + numDataNodes + "].");
				}
				if (dnConfOverlays != null && numDataNodes > dnConfOverlays.Length)
				{
					throw new ArgumentException("The length of dnConfOverlays [" + dnConfOverlays.Length
						 + "] is less than the number of datanodes [" + numDataNodes + "].");
				}
				string[] dnArgs = (operation == null || operation != HdfsServerConstants.StartupOption
					.Rollback) ? null : new string[] { operation.GetName() };
				DataNode[] dns = new DataNode[numDataNodes];
				for (int i_1 = curDatanodesNum; i_1 < curDatanodesNum + numDataNodes; i_1++)
				{
					Configuration dnConf = new HdfsConfiguration(conf);
					if (dnConfOverlays != null)
					{
						dnConf.AddResource(dnConfOverlays[i_1]);
					}
					// Set up datanode address
					SetupDatanodeAddress(dnConf, setupHostsFile, checkDataNodeAddrConfig);
					if (manageDfsDirs)
					{
						string dirs = MakeDataNodeDirs(i_1, storageTypes == null ? null : storageTypes[i_1
							 - curDatanodesNum]);
						dnConf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, dirs);
						conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, dirs);
					}
					if (simulatedCapacities != null)
					{
						SimulatedFSDataset.SetFactory(dnConf);
						dnConf.SetLong(SimulatedFSDataset.ConfigPropertyCapacity, simulatedCapacities[i_1
							 - curDatanodesNum]);
					}
					Log.Info("Starting DataNode " + i_1 + " with " + DFSConfigKeys.DfsDatanodeDataDirKey
						 + ": " + dnConf.Get(DFSConfigKeys.DfsDatanodeDataDirKey));
					if (hosts != null)
					{
						dnConf.Set(DFSConfigKeys.DfsDatanodeHostNameKey, hosts[i_1 - curDatanodesNum]);
						Log.Info("Starting DataNode " + i_1 + " with hostname set to: " + dnConf.Get(DFSConfigKeys
							.DfsDatanodeHostNameKey));
					}
					if (racks != null)
					{
						string name = hosts[i_1 - curDatanodesNum];
						Log.Info("Adding node with hostname : " + name + " to rack " + racks[i_1 - curDatanodesNum
							]);
						StaticMapping.AddNodeToRack(name, racks[i_1 - curDatanodesNum]);
					}
					Configuration newconf = new HdfsConfiguration(dnConf);
					// save config
					if (hosts != null)
					{
						NetUtils.AddStaticResolution(hosts[i_1 - curDatanodesNum], "localhost");
					}
					SecureDataNodeStarter.SecureResources secureResources = null;
					if (UserGroupInformation.IsSecurityEnabled() && conf.Get(DFSConfigKeys.DfsDataTransferProtectionKey
						) == null)
					{
						try
						{
							secureResources = SecureDataNodeStarter.GetSecureResources(dnConf);
						}
						catch (Exception ex)
						{
							Sharpen.Runtime.PrintStackTrace(ex);
						}
					}
					int maxRetriesOnSasl = conf.GetInt(CommonConfigurationKeys.IpcClientConnectMaxRetriesOnSaslKey
						, CommonConfigurationKeys.IpcClientConnectMaxRetriesOnSaslDefault);
					int numRetries = 0;
					DataNode dn = null;
					while (true)
					{
						try
						{
							dn = DataNode.InstantiateDataNode(dnArgs, dnConf, secureResources);
							break;
						}
						catch (IOException e)
						{
							// Work around issue testing security where rapidly starting multiple
							// DataNodes using the same principal gets rejected by the KDC as a
							// replay attack.
							if (UserGroupInformation.IsSecurityEnabled() && numRetries < maxRetriesOnSasl)
							{
								try
								{
									Sharpen.Thread.Sleep(1000);
								}
								catch (Exception)
								{
									Sharpen.Thread.CurrentThread().Interrupt();
									break;
								}
								++numRetries;
								continue;
							}
							throw;
						}
					}
					if (dn == null)
					{
						throw new IOException("Cannot start DataNode in " + dnConf.Get(DFSConfigKeys.DfsDatanodeDataDirKey
							));
					}
					//since the HDFS does things based on host|ip:port, we need to add the
					//mapping for the service to rackId
					string service = SecurityUtil.BuildTokenService(dn.GetXferAddress()).ToString();
					if (racks != null)
					{
						Log.Info("Adding node with service : " + service + " to rack " + racks[i_1 - curDatanodesNum
							]);
						StaticMapping.AddNodeToRack(service, racks[i_1 - curDatanodesNum]);
					}
					dn.RunDatanodeDaemon();
					dataNodes.AddItem(new MiniDFSCluster.DataNodeProperties(this, dn, newconf, dnArgs
						, secureResources, dn.GetIpcPort()));
					dns[i_1 - curDatanodesNum] = dn;
				}
				this.numDataNodes += numDataNodes;
				WaitActive();
				if (storageCapacities != null)
				{
					for (int i = curDatanodesNumSaved; i_1 < curDatanodesNumSaved + numDataNodes; ++i_1)
					{
						int index = i_1 - curDatanodesNum;
						IList<FsVolumeSpi> volumes = dns[index].GetFSDataset().GetVolumes();
						System.Diagnostics.Debug.Assert(storageCapacities[index].Length == storagesPerDatanode
							);
						System.Diagnostics.Debug.Assert(volumes.Count == storagesPerDatanode);
						for (int j = 0; j < volumes.Count; ++j)
						{
							FsVolumeImpl volume = (FsVolumeImpl)volumes[j];
							Log.Info("setCapacityForTesting " + storageCapacities[index][j] + " for [" + volume
								.GetStorageType() + "]" + volume.GetStorageID());
							volume.SetCapacityForTesting(storageCapacities[index][j]);
						}
					}
				}
			}
		}

		/// <summary>Modify the config and start up the DataNodes.</summary>
		/// <remarks>
		/// Modify the config and start up the DataNodes.  The info port for
		/// DataNodes is guaranteed to use a free port.
		/// </remarks>
		/// <param name="conf">
		/// the base configuration to use in starting the DataNodes.  This
		/// will be modified as necessary.
		/// </param>
		/// <param name="numDataNodes">Number of DataNodes to start; may be zero</param>
		/// <param name="manageDfsDirs">
		/// if true, the data directories for DataNodes will be
		/// created and
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// will be
		/// set in the conf
		/// </param>
		/// <param name="operation">
		/// the operation with which to start the DataNodes.  If null
		/// or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
		/// </param>
		/// <param name="racks">array of strings indicating the rack that each DataNode is on
		/// 	</param>
		/// <exception cref="System.InvalidOperationException">if NameNode has been shutdown</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void StartDataNodes(Configuration conf, int numDataNodes, bool manageDfsDirs
			, HdfsServerConstants.StartupOption operation, string[] racks)
		{
			StartDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, null, null, false
				);
		}

		/// <summary>Modify the config and start up additional DataNodes.</summary>
		/// <remarks>
		/// Modify the config and start up additional DataNodes.  The info port for
		/// DataNodes is guaranteed to use a free port.
		/// Data nodes can run with the name node in the mini cluster or
		/// a real name node. For example, running with a real name node is useful
		/// when running simulated data nodes with a real name node.
		/// If minicluster's name node is null assume that the conf has been
		/// set with the right address:port of the name node.
		/// </remarks>
		/// <param name="conf">
		/// the base configuration to use in starting the DataNodes.  This
		/// will be modified as necessary.
		/// </param>
		/// <param name="numDataNodes">Number of DataNodes to start; may be zero</param>
		/// <param name="manageDfsDirs">
		/// if true, the data directories for DataNodes will be
		/// created and
		/// <see cref="DFSConfigKeys.DfsDatanodeDataDirKey"/>
		/// will
		/// be set in the conf
		/// </param>
		/// <param name="operation">
		/// the operation with which to start the DataNodes.  If null
		/// or StartupOption.FORMAT, then StartupOption.REGULAR will be used.
		/// </param>
		/// <param name="racks">array of strings indicating the rack that each DataNode is on
		/// 	</param>
		/// <param name="simulatedCapacities">array of capacities of the simulated data nodes
		/// 	</param>
		/// <exception cref="System.InvalidOperationException">if NameNode has been shutdown</exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void StartDataNodes(Configuration conf, int numDataNodes, bool manageDfsDirs
			, HdfsServerConstants.StartupOption operation, string[] racks, long[] simulatedCapacities
			)
		{
			StartDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, null, simulatedCapacities
				, false);
		}

		/// <summary>Finalize the namenode.</summary>
		/// <remarks>
		/// Finalize the namenode. Block pools corresponding to the namenode are
		/// finalized on the datanode.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		private void FinalizeNamenode(NameNode nn, Configuration conf)
		{
			if (nn == null)
			{
				throw new InvalidOperationException("Attempting to finalize " + "Namenode but it is not running"
					);
			}
			ToolRunner.Run(new DFSAdmin(conf), new string[] { "-finalizeUpgrade" });
		}

		/// <summary>Finalize cluster for the namenode at the given index</summary>
		/// <seealso cref="FinalizeCluster(Org.Apache.Hadoop.Conf.Configuration)"/>
		/// <param name="nnIndex">index of the namenode</param>
		/// <param name="conf">configuration</param>
		/// <exception cref="System.Exception"/>
		public virtual void FinalizeCluster(int nnIndex, Configuration conf)
		{
			FinalizeNamenode(nameNodes[nnIndex].nameNode, nameNodes[nnIndex].conf);
		}

		/// <summary>If the NameNode is running, attempt to finalize a previous upgrade.</summary>
		/// <remarks>
		/// If the NameNode is running, attempt to finalize a previous upgrade.
		/// When this method return, the NameNode should be finalized, but
		/// DataNodes may not be since that occurs asynchronously.
		/// </remarks>
		/// <exception cref="System.InvalidOperationException">if the Namenode is not running.
		/// 	</exception>
		/// <exception cref="System.Exception"/>
		public virtual void FinalizeCluster(Configuration conf)
		{
			foreach (MiniDFSCluster.NameNodeInfo nnInfo in nameNodes)
			{
				if (nnInfo == null)
				{
					throw new InvalidOperationException("Attempting to finalize " + "Namenode but it is not running"
						);
				}
				FinalizeNamenode(nnInfo.nameNode, nnInfo.conf);
			}
		}

		public virtual int GetNumNameNodes()
		{
			return nameNodes.Length;
		}

		/// <summary>Gets the started NameNode.</summary>
		/// <remarks>Gets the started NameNode.  May be null.</remarks>
		public virtual NameNode GetNameNode()
		{
			CheckSingleNameNode();
			return GetNameNode(0);
		}

		/// <summary>Get an instance of the NameNode's RPC handler.</summary>
		public virtual NamenodeProtocols GetNameNodeRpc()
		{
			CheckSingleNameNode();
			return GetNameNodeRpc(0);
		}

		/// <summary>Get an instance of the NameNode's RPC handler.</summary>
		public virtual NamenodeProtocols GetNameNodeRpc(int nnIndex)
		{
			return GetNameNode(nnIndex).GetRpcServer();
		}

		/// <summary>Gets the NameNode for the index.</summary>
		/// <remarks>Gets the NameNode for the index.  May be null.</remarks>
		public virtual NameNode GetNameNode(int nnIndex)
		{
			return nameNodes[nnIndex].nameNode;
		}

		/// <summary>
		/// Return the
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem"/>
		/// object.
		/// </summary>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem"/>
		/// object.
		/// </returns>
		public virtual FSNamesystem GetNamesystem()
		{
			CheckSingleNameNode();
			return NameNodeAdapter.GetNamesystem(nameNodes[0].nameNode);
		}

		public virtual FSNamesystem GetNamesystem(int nnIndex)
		{
			return NameNodeAdapter.GetNamesystem(nameNodes[nnIndex].nameNode);
		}

		/// <summary>Gets a list of the started DataNodes.</summary>
		/// <remarks>Gets a list of the started DataNodes.  May be empty.</remarks>
		public virtual AList<DataNode> GetDataNodes()
		{
			AList<DataNode> list = new AList<DataNode>();
			for (int i = 0; i < dataNodes.Count; i++)
			{
				DataNode node = dataNodes[i].datanode;
				list.AddItem(node);
			}
			return list;
		}

		/// <returns>the datanode having the ipc server listen port</returns>
		public virtual DataNode GetDataNode(int ipcPort)
		{
			foreach (DataNode dn in GetDataNodes())
			{
				if (dn.ipcServer.GetListenerAddress().Port == ipcPort)
				{
					return dn;
				}
			}
			return null;
		}

		/// <summary>
		/// Gets the rpc port used by the NameNode, because the caller
		/// supplied port is not necessarily the actual port used.
		/// </summary>
		/// <remarks>
		/// Gets the rpc port used by the NameNode, because the caller
		/// supplied port is not necessarily the actual port used.
		/// Assumption: cluster has a single namenode
		/// </remarks>
		public virtual int GetNameNodePort()
		{
			CheckSingleNameNode();
			return GetNameNodePort(0);
		}

		/// <summary>
		/// Gets the rpc port used by the NameNode at the given index, because the
		/// caller supplied port is not necessarily the actual port used.
		/// </summary>
		public virtual int GetNameNodePort(int nnIndex)
		{
			return nameNodes[nnIndex].nameNode.GetNameNodeAddress().Port;
		}

		/// <returns>the service rpc port used by the NameNode at the given index.</returns>
		public virtual int GetNameNodeServicePort(int nnIndex)
		{
			return nameNodes[nnIndex].nameNode.GetServiceRpcAddress().Port;
		}

		/// <summary>Shutdown all the nodes in the cluster.</summary>
		public virtual void Shutdown()
		{
			Shutdown(false);
		}

		/// <summary>Shutdown all the nodes in the cluster.</summary>
		public virtual void Shutdown(bool deleteDfsDir)
		{
			Log.Info("Shutting down the Mini HDFS Cluster");
			if (checkExitOnShutdown)
			{
				if (ExitUtil.TerminateCalled())
				{
					Log.Fatal("Test resulted in an unexpected exit", ExitUtil.GetFirstExitException()
						);
					ExitUtil.ResetFirstExitException();
					throw new Exception("Test resulted in an unexpected exit");
				}
			}
			ShutdownDataNodes();
			foreach (MiniDFSCluster.NameNodeInfo nnInfo in nameNodes)
			{
				if (nnInfo == null)
				{
					continue;
				}
				NameNode nameNode = nnInfo.nameNode;
				if (nameNode != null)
				{
					nameNode.Stop();
					nameNode.Join();
					nameNode = null;
				}
			}
			if (deleteDfsDir)
			{
				base_dir.Delete();
			}
			else
			{
				base_dir.DeleteOnExit();
			}
		}

		/// <summary>Shutdown all DataNodes started by this class.</summary>
		/// <remarks>
		/// Shutdown all DataNodes started by this class.  The NameNode
		/// is left running so that new DataNodes may be started.
		/// </remarks>
		public virtual void ShutdownDataNodes()
		{
			for (int i = dataNodes.Count - 1; i >= 0; i--)
			{
				Log.Info("Shutting down DataNode " + i);
				DataNode dn = dataNodes.Remove(i).datanode;
				dn.Shutdown();
				numDataNodes--;
			}
		}

		/// <summary>Shutdown all the namenodes.</summary>
		public virtual void ShutdownNameNodes()
		{
			lock (this)
			{
				for (int i = 0; i < nameNodes.Length; i++)
				{
					ShutdownNameNode(i);
				}
			}
		}

		/// <summary>Shutdown the namenode at a given index.</summary>
		public virtual void ShutdownNameNode(int nnIndex)
		{
			lock (this)
			{
				NameNode nn = nameNodes[nnIndex].nameNode;
				if (nn != null)
				{
					Log.Info("Shutting down the namenode");
					nn.Stop();
					nn.Join();
					Configuration conf = nameNodes[nnIndex].conf;
					nameNodes[nnIndex] = new MiniDFSCluster.NameNodeInfo(null, null, null, null, conf
						);
				}
			}
		}

		/// <summary>Restart all namenodes.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RestartNameNodes()
		{
			lock (this)
			{
				for (int i = 0; i < nameNodes.Length; i++)
				{
					RestartNameNode(i, false);
				}
				WaitActive();
			}
		}

		/// <summary>Restart the namenode.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RestartNameNode(params string[] args)
		{
			lock (this)
			{
				CheckSingleNameNode();
				RestartNameNode(0, true, args);
			}
		}

		/// <summary>Restart the namenode.</summary>
		/// <remarks>Restart the namenode. Optionally wait for the cluster to become active.</remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RestartNameNode(bool waitActive)
		{
			lock (this)
			{
				CheckSingleNameNode();
				RestartNameNode(0, waitActive);
			}
		}

		/// <summary>Restart the namenode at a given index.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RestartNameNode(int nnIndex)
		{
			lock (this)
			{
				RestartNameNode(nnIndex, true);
			}
		}

		/// <summary>Restart the namenode at a given index.</summary>
		/// <remarks>
		/// Restart the namenode at a given index. Optionally wait for the cluster
		/// to become active.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RestartNameNode(int nnIndex, bool waitActive, params string[]
			 args)
		{
			lock (this)
			{
				string nameserviceId = nameNodes[nnIndex].nameserviceId;
				string nnId = nameNodes[nnIndex].nnId;
				HdfsServerConstants.StartupOption startOpt = nameNodes[nnIndex].startOpt;
				Configuration conf = nameNodes[nnIndex].conf;
				ShutdownNameNode(nnIndex);
				if (args.Length != 0)
				{
					startOpt = null;
				}
				else
				{
					args = CreateArgs(startOpt);
				}
				NameNode nn = NameNode.CreateNameNode(args, conf);
				nameNodes[nnIndex] = new MiniDFSCluster.NameNodeInfo(nn, nameserviceId, nnId, startOpt
					, conf);
				if (waitActive)
				{
					WaitClusterUp();
					Log.Info("Restarted the namenode");
					WaitActive();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private int CorruptBlockOnDataNodesHelper(ExtendedBlock block, bool deleteBlockFile
			)
		{
			int blocksCorrupted = 0;
			FilePath[] blockFiles = GetAllBlockFiles(block);
			foreach (FilePath f in blockFiles)
			{
				if ((deleteBlockFile && CorruptBlockByDeletingBlockFile(f)) || (!deleteBlockFile 
					&& CorruptBlock(f)))
				{
					blocksCorrupted++;
				}
			}
			return blocksCorrupted;
		}

		/// <summary>Return the number of corrupted replicas of the given block.</summary>
		/// <param name="block">block to be corrupted</param>
		/// <exception cref="System.IO.IOException">on error accessing the file for the given block
		/// 	</exception>
		public virtual int CorruptBlockOnDataNodes(ExtendedBlock block)
		{
			return CorruptBlockOnDataNodesHelper(block, false);
		}

		/// <summary>Return the number of corrupted replicas of the given block.</summary>
		/// <param name="block">block to be corrupted</param>
		/// <exception cref="System.IO.IOException">on error accessing the file for the given block
		/// 	</exception>
		public virtual int CorruptBlockOnDataNodesByDeletingBlockFile(ExtendedBlock block
			)
		{
			return CorruptBlockOnDataNodesHelper(block, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string ReadBlockOnDataNode(int i, ExtendedBlock block)
		{
			System.Diagnostics.Debug.Assert((i >= 0 && i < dataNodes.Count), "Invalid datanode "
				 + i);
			FilePath blockFile = GetBlockFile(i, block);
			if (blockFile != null && blockFile.Exists())
			{
				return DFSTestUtil.ReadFile(blockFile);
			}
			return null;
		}

		/// <summary>Corrupt a block on a particular datanode.</summary>
		/// <param name="i">index of the datanode</param>
		/// <param name="blk">name of the block</param>
		/// <exception cref="System.IO.IOException">
		/// on error accessing the given block or if
		/// the contents of the block (on the same datanode) differ.
		/// </exception>
		/// <returns>
		/// true if a replica was corrupted, false otherwise
		/// Types: delete, write bad data, truncate
		/// </returns>
		public virtual bool CorruptReplica(int i, ExtendedBlock blk)
		{
			FilePath blockFile = GetBlockFile(i, blk);
			return CorruptBlock(blockFile);
		}

		/*
		* Corrupt a block on a particular datanode
		*/
		/// <exception cref="System.IO.IOException"/>
		public static bool CorruptBlock(FilePath blockFile)
		{
			if (blockFile == null || !blockFile.Exists())
			{
				return false;
			}
			// Corrupt replica by writing random bytes into replica
			Random random = new Random();
			RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
			FileChannel channel = raFile.GetChannel();
			string badString = "BADBAD";
			int rand = random.Next((int)channel.Size() / 2);
			raFile.Seek(rand);
			raFile.Write(Sharpen.Runtime.GetBytesForString(badString));
			raFile.Close();
			Log.Warn("Corrupting the block " + blockFile);
			return true;
		}

		/*
		* Corrupt a block on a particular datanode by deleting the block file
		*/
		/// <exception cref="System.IO.IOException"/>
		public static bool CorruptBlockByDeletingBlockFile(FilePath blockFile)
		{
			if (blockFile == null || !blockFile.Exists())
			{
				return false;
			}
			return blockFile.Delete();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool ChangeGenStampOfBlock(int dnIndex, ExtendedBlock blk, long newGenStamp
			)
		{
			FilePath blockFile = GetBlockFile(dnIndex, blk);
			FilePath metaFile = FsDatasetUtil.FindMetaFile(blockFile);
			return metaFile.RenameTo(new FilePath(DatanodeUtil.GetMetaName(blockFile.GetAbsolutePath
				(), newGenStamp)));
		}

		/*
		* Shutdown a particular datanode
		* @param i node index
		* @return null if the node index is out of range, else the properties of the
		* removed node
		*/
		public virtual MiniDFSCluster.DataNodeProperties StopDataNode(int i)
		{
			lock (this)
			{
				if (i < 0 || i >= dataNodes.Count)
				{
					return null;
				}
				MiniDFSCluster.DataNodeProperties dnprop = dataNodes.Remove(i);
				DataNode dn = dnprop.datanode;
				Log.Info("MiniDFSCluster Stopping DataNode " + dn.GetDisplayName() + " from a total of "
					 + (dataNodes.Count + 1) + " datanodes.");
				dn.Shutdown();
				numDataNodes--;
				return dnprop;
			}
		}

		/*
		* Shutdown a datanode by name.
		* @return the removed datanode or null if there was no match
		*/
		public virtual MiniDFSCluster.DataNodeProperties StopDataNode(string dnName)
		{
			lock (this)
			{
				int node = -1;
				for (int i = 0; i < dataNodes.Count; i++)
				{
					DataNode dn = dataNodes[i].datanode;
					Log.Info("DN name=" + dnName + " found DN=" + dn + " with name=" + dn.GetDisplayName
						());
					if (dnName.Equals(dn.GetDatanodeId().GetXferAddr()))
					{
						node = i;
						break;
					}
				}
				return StopDataNode(node);
			}
		}

		/// <summary>Restart a datanode</summary>
		/// <param name="dnprop">datanode's property</param>
		/// <returns>true if restarting is successful</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool RestartDataNode(MiniDFSCluster.DataNodeProperties dnprop)
		{
			return RestartDataNode(dnprop, false);
		}

		/// <summary>Restart a datanode, on the same port if requested</summary>
		/// <param name="dnprop">the datanode to restart</param>
		/// <param name="keepPort">whether to use the same port</param>
		/// <returns>true if restarting is successful</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool RestartDataNode(MiniDFSCluster.DataNodeProperties dnprop, bool
			 keepPort)
		{
			lock (this)
			{
				Configuration conf = dnprop.conf;
				string[] args = dnprop.dnArgs;
				SecureDataNodeStarter.SecureResources secureResources = dnprop.secureResources;
				Configuration newconf = new HdfsConfiguration(conf);
				// save cloned config
				if (keepPort)
				{
					IPEndPoint addr = dnprop.datanode.GetXferAddress();
					conf.Set(DFSConfigKeys.DfsDatanodeAddressKey, addr.Address.GetHostAddress() + ":"
						 + addr.Port);
					conf.Set(DFSConfigKeys.DfsDatanodeIpcAddressKey, addr.Address.GetHostAddress() + 
						":" + dnprop.ipcPort);
				}
				DataNode newDn = DataNode.CreateDataNode(args, conf, secureResources);
				dataNodes.AddItem(new MiniDFSCluster.DataNodeProperties(this, newDn, newconf, args
					, secureResources, newDn.GetIpcPort()));
				numDataNodes++;
				return true;
			}
		}

		/*
		* Restart a particular datanode, use newly assigned port
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual bool RestartDataNode(int i)
		{
			return RestartDataNode(i, false);
		}

		/*
		* Restart a particular datanode, on the same port if keepPort is true
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual bool RestartDataNode(int i, bool keepPort)
		{
			lock (this)
			{
				return RestartDataNode(i, keepPort, false);
			}
		}

		/// <summary>Restart a particular DataNode.</summary>
		/// <param name="idn">index of the DataNode</param>
		/// <param name="keepPort">true if should restart on the same port</param>
		/// <param name="expireOnNN">true if NameNode should expire the DataNode heartbeat</param>
		/// <returns/>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool RestartDataNode(int idn, bool keepPort, bool expireOnNN)
		{
			lock (this)
			{
				MiniDFSCluster.DataNodeProperties dnprop = StopDataNode(idn);
				if (expireOnNN)
				{
					SetDataNodeDead(dnprop.datanode.GetDatanodeId());
				}
				if (dnprop == null)
				{
					return false;
				}
				else
				{
					return RestartDataNode(dnprop, keepPort);
				}
			}
		}

		/// <summary>Expire a DataNode heartbeat on the NameNode</summary>
		/// <param name="dnId"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetDataNodeDead(DatanodeID dnId)
		{
			DatanodeDescriptor dnd = NameNodeAdapter.GetDatanode(GetNamesystem(), dnId);
			DFSTestUtil.SetDatanodeDead(dnd);
			BlockManagerTestUtil.CheckHeartbeat(GetNamesystem().GetBlockManager());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetDataNodesDead()
		{
			foreach (MiniDFSCluster.DataNodeProperties dnp in dataNodes)
			{
				SetDataNodeDead(dnp.datanode.GetDatanodeId());
			}
		}

		/*
		* Restart all datanodes, on the same ports if keepPort is true
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual bool RestartDataNodes(bool keepPort)
		{
			lock (this)
			{
				for (int i = dataNodes.Count - 1; i >= 0; i--)
				{
					if (!RestartDataNode(i, keepPort))
					{
						return false;
					}
					Log.Info("Restarted DataNode " + i);
				}
				return true;
			}
		}

		/*
		* Restart all datanodes, use newly assigned ports
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual bool RestartDataNodes()
		{
			return RestartDataNodes(false);
		}

		/// <summary>
		/// Returns true if the NameNode is running and is out of Safe Mode
		/// or if waiting for safe mode is disabled.
		/// </summary>
		public virtual bool IsNameNodeUp(int nnIndex)
		{
			NameNode nameNode = nameNodes[nnIndex].nameNode;
			if (nameNode == null)
			{
				return false;
			}
			long[] sizes;
			sizes = NameNodeAdapter.GetStats(nameNode.GetNamesystem());
			bool isUp = false;
			lock (this)
			{
				isUp = ((!nameNode.IsInSafeMode() || !waitSafeMode) && sizes[ClientProtocol.GetStatsCapacityIdx
					] != 0);
			}
			return isUp;
		}

		/// <summary>Returns true if all the NameNodes are running and is out of Safe Mode.</summary>
		public virtual bool IsClusterUp()
		{
			for (int index = 0; index < nameNodes.Length; index++)
			{
				if (!IsNameNodeUp(index))
				{
					return false;
				}
			}
			return true;
		}

		/// <summary>Returns true if there is at least one DataNode running.</summary>
		public virtual bool IsDataNodeUp()
		{
			if (dataNodes == null || dataNodes.Count == 0)
			{
				return false;
			}
			foreach (MiniDFSCluster.DataNodeProperties dn in dataNodes)
			{
				if (dn.datanode.IsDatanodeUp())
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>Get a client handle to the DFS cluster with a single namenode.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual DistributedFileSystem GetFileSystem()
		{
			CheckSingleNameNode();
			return GetFileSystem(0);
		}

		/// <summary>Get a client handle to the DFS cluster for the namenode at given index.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual DistributedFileSystem GetFileSystem(int nnIndex)
		{
			return (DistributedFileSystem)FileSystem.Get(GetURI(nnIndex), nameNodes[nnIndex].
				conf);
		}

		/// <summary>Get another FileSystem instance that is different from FileSystem.get(conf).
		/// 	</summary>
		/// <remarks>
		/// Get another FileSystem instance that is different from FileSystem.get(conf).
		/// This simulating different threads working on different FileSystem instances.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual FileSystem GetNewFileSystemInstance(int nnIndex)
		{
			return FileSystem.NewInstance(GetURI(nnIndex), nameNodes[nnIndex].conf);
		}

		/// <returns>a http URL</returns>
		public virtual string GetHttpUri(int nnIndex)
		{
			return "http://" + nameNodes[nnIndex].conf.Get(DFSConfigKeys.DfsNamenodeHttpAddressKey
				);
		}

		/// <returns>
		/// a
		/// <see cref="Org.Apache.Hadoop.Hdfs.Web.HftpFileSystem"/>
		/// object.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual HftpFileSystem GetHftpFileSystem(int nnIndex)
		{
			string uri = "hftp://" + nameNodes[nnIndex].conf.Get(DFSConfigKeys.DfsNamenodeHttpAddressKey
				);
			try
			{
				return (HftpFileSystem)FileSystem.Get(new URI(uri), conf);
			}
			catch (URISyntaxException e)
			{
				throw new IOException(e);
			}
		}

		/// <returns>
		/// a
		/// <see cref="Org.Apache.Hadoop.Hdfs.Web.HftpFileSystem"/>
		/// object as specified user.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual HftpFileSystem GetHftpFileSystemAs(string username, Configuration 
			conf, int nnIndex, params string[] groups)
		{
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting(username, groups
				);
			return ugi.DoAs(new _PrivilegedExceptionAction_2188(this, nnIndex));
		}

		private sealed class _PrivilegedExceptionAction_2188 : PrivilegedExceptionAction<
			HftpFileSystem>
		{
			public _PrivilegedExceptionAction_2188(MiniDFSCluster _enclosing, int nnIndex)
			{
				this._enclosing = _enclosing;
				this.nnIndex = nnIndex;
			}

			/// <exception cref="System.Exception"/>
			public HftpFileSystem Run()
			{
				return this._enclosing.GetHftpFileSystem(nnIndex);
			}

			private readonly MiniDFSCluster _enclosing;

			private readonly int nnIndex;
		}

		/// <summary>Get the directories where the namenode stores its image.</summary>
		public virtual ICollection<URI> GetNameDirs(int nnIndex)
		{
			return FSNamesystem.GetNamespaceDirs(nameNodes[nnIndex].conf);
		}

		/// <summary>Get the directories where the namenode stores its edits.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual ICollection<URI> GetNameEditsDirs(int nnIndex)
		{
			return FSNamesystem.GetNamespaceEditsDirs(nameNodes[nnIndex].conf);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		public virtual void TransitionToActive(int nnIndex)
		{
			GetNameNode(nnIndex).GetRpcServer().TransitionToActive(new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUserForced));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		public virtual void TransitionToStandby(int nnIndex)
		{
			GetNameNode(nnIndex).GetRpcServer().TransitionToStandby(new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUserForced));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TriggerBlockReports()
		{
			foreach (DataNode dn in GetDataNodes())
			{
				DataNodeTestUtils.TriggerBlockReport(dn);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TriggerDeletionReports()
		{
			foreach (DataNode dn in GetDataNodes())
			{
				DataNodeTestUtils.TriggerDeletionReport(dn);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TriggerHeartbeats()
		{
			foreach (DataNode dn in GetDataNodes())
			{
				DataNodeTestUtils.TriggerHeartbeat(dn);
			}
		}

		/// <summary>Wait until the given namenode gets registration from all the datanodes</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void WaitActive(int nnIndex)
		{
			if (nameNodes.Length == 0 || nameNodes[nnIndex] == null || nameNodes[nnIndex].nameNode
				 == null)
			{
				return;
			}
			IPEndPoint addr = nameNodes[nnIndex].nameNode.GetServiceRpcAddress();
			System.Diagnostics.Debug.Assert(addr.Port != 0);
			DFSClient client = new DFSClient(addr, conf);
			// ensure all datanodes have registered and sent heartbeat to the namenode
			while (ShouldWait(client.DatanodeReport(HdfsConstants.DatanodeReportType.Live), addr
				))
			{
				try
				{
					Log.Info("Waiting for cluster to become active");
					Sharpen.Thread.Sleep(100);
				}
				catch (Exception)
				{
				}
			}
			client.Close();
		}

		/// <summary>Wait until the cluster is active and running.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void WaitActive()
		{
			for (int index = 0; index < nameNodes.Length; index++)
			{
				int failedCount = 0;
				while (true)
				{
					try
					{
						WaitActive(index);
						break;
					}
					catch (IOException e)
					{
						failedCount++;
						// Cached RPC connection to namenode, if any, is expected to fail once
						if (failedCount > 1)
						{
							Log.Warn("Tried waitActive() " + failedCount + " time(s) and failed, giving up.  "
								 + StringUtils.StringifyException(e));
							throw;
						}
					}
				}
			}
			Log.Info("Cluster is active");
		}

		private bool ShouldWait(DatanodeInfo[] dnInfo, IPEndPoint addr)
		{
			lock (this)
			{
				// If a datanode failed to start, then do not wait
				foreach (MiniDFSCluster.DataNodeProperties dn in dataNodes)
				{
					// the datanode thread communicating with the namenode should be alive
					if (!dn.datanode.IsConnectedToNN(addr))
					{
						Log.Warn("BPOfferService in datanode " + dn.datanode + " failed to connect to namenode at "
							 + addr);
						return false;
					}
				}
				// Wait for expected number of datanodes to start
				if (dnInfo.Length != numDataNodes)
				{
					Log.Info("dnInfo.length != numDataNodes");
					return true;
				}
				// if one of the data nodes is not fully started, continue to wait
				foreach (MiniDFSCluster.DataNodeProperties dn_1 in dataNodes)
				{
					if (!dn_1.datanode.IsDatanodeFullyStarted())
					{
						Log.Info("!dn.datanode.isDatanodeFullyStarted()");
						return true;
					}
				}
				// make sure all datanodes have sent first heartbeat to namenode,
				// using (capacity == 0) as proxy.
				foreach (DatanodeInfo dn_2 in dnInfo)
				{
					if (dn_2.GetCapacity() == 0 || dn_2.GetLastUpdate() <= 0)
					{
						Log.Info("No heartbeat from DataNode: " + dn_2.ToString());
						return true;
					}
				}
				// If datanode dataset is not initialized then wait
				foreach (MiniDFSCluster.DataNodeProperties dn_3 in dataNodes)
				{
					if (DataNodeTestUtils.GetFSDataset(dn_3.datanode) == null)
					{
						Log.Info("DataNodeTestUtils.getFSDataset(dn.datanode) == null");
						return true;
					}
				}
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void FormatDataNodeDirs()
		{
			base_dir = new FilePath(DetermineDfsBaseDir());
			data_dir = new FilePath(base_dir, "data");
			if (data_dir.Exists() && !FileUtil.FullyDelete(data_dir))
			{
				throw new IOException("Cannot remove data directory: " + data_dir);
			}
		}

		/// <param name="dataNodeIndex">- data node whose block report is desired - the index is same as for getDataNodes()
		/// 	</param>
		/// <returns>the block report for the specified data node</returns>
		public virtual IDictionary<DatanodeStorage, BlockListAsLongs> GetBlockReport(string
			 bpid, int dataNodeIndex)
		{
			if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.Count)
			{
				throw new IndexOutOfRangeException();
			}
			DataNode dn = dataNodes[dataNodeIndex].datanode;
			return DataNodeTestUtils.GetFSDataset(dn).GetBlockReports(bpid);
		}

		/// <returns>
		/// block reports from all data nodes
		/// BlockListAsLongs is indexed in the same order as the list of datanodes returned by getDataNodes()
		/// </returns>
		public virtual IList<IDictionary<DatanodeStorage, BlockListAsLongs>> GetAllBlockReports
			(string bpid)
		{
			int numDataNodes = dataNodes.Count;
			IList<IDictionary<DatanodeStorage, BlockListAsLongs>> result = new AList<IDictionary
				<DatanodeStorage, BlockListAsLongs>>(numDataNodes);
			for (int i = 0; i < numDataNodes; ++i)
			{
				result.AddItem(GetBlockReport(bpid, i));
			}
			return result;
		}

		/// <summary>This method is valid only if the data nodes have simulated data</summary>
		/// <param name="dataNodeIndex">- data node i which to inject - the index is same as for getDataNodes()
		/// 	</param>
		/// <param name="blocksToInject">- the blocks</param>
		/// <param name="bpid">
		/// - (optional) the block pool id to use for injecting blocks.
		/// If not supplied then it is queried from the in-process NameNode.
		/// </param>
		/// <exception cref="System.IO.IOException">
		/// if not simulatedFSDataset
		/// if any of blocks already exist in the data node
		/// </exception>
		public virtual void InjectBlocks(int dataNodeIndex, IEnumerable<Block> blocksToInject
			, string bpid)
		{
			if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.Count)
			{
				throw new IndexOutOfRangeException();
			}
			DataNode dn = dataNodes[dataNodeIndex].datanode;
			FsDatasetSpi<object> dataSet = DataNodeTestUtils.GetFSDataset(dn);
			if (!(dataSet is SimulatedFSDataset))
			{
				throw new IOException("injectBlocks is valid only for SimilatedFSDataset");
			}
			if (bpid == null)
			{
				bpid = GetNamesystem().GetBlockPoolId();
			}
			SimulatedFSDataset sdataset = (SimulatedFSDataset)dataSet;
			sdataset.InjectBlocks(bpid, blocksToInject);
			dataNodes[dataNodeIndex].datanode.ScheduleAllBlockReport(0);
		}

		/// <summary>Multiple-NameNode version of injectBlocks.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void InjectBlocks(int nameNodeIndex, int dataNodeIndex, IEnumerable
			<Block> blocksToInject)
		{
			if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.Count)
			{
				throw new IndexOutOfRangeException();
			}
			DataNode dn = dataNodes[dataNodeIndex].datanode;
			FsDatasetSpi<object> dataSet = DataNodeTestUtils.GetFSDataset(dn);
			if (!(dataSet is SimulatedFSDataset))
			{
				throw new IOException("injectBlocks is valid only for SimilatedFSDataset");
			}
			string bpid = GetNamesystem(nameNodeIndex).GetBlockPoolId();
			SimulatedFSDataset sdataset = (SimulatedFSDataset)dataSet;
			sdataset.InjectBlocks(bpid, blocksToInject);
			dataNodes[dataNodeIndex].datanode.ScheduleAllBlockReport(0);
		}

		/// <summary>Set the softLimit and hardLimit of client lease periods</summary>
		public virtual void SetLeasePeriod(long soft, long hard)
		{
			NameNodeAdapter.SetLeasePeriod(GetNamesystem(), soft, hard);
		}

		public virtual void SetLeasePeriod(long soft, long hard, int nnIndex)
		{
			NameNodeAdapter.SetLeasePeriod(GetNamesystem(nnIndex), soft, hard);
		}

		public virtual void SetWaitSafeMode(bool wait)
		{
			this.waitSafeMode = wait;
		}

		/// <summary>Returns the current set of datanodes</summary>
		internal virtual DataNode[] ListDataNodes()
		{
			DataNode[] list = new DataNode[dataNodes.Count];
			for (int i = 0; i < dataNodes.Count; i++)
			{
				list[i] = dataNodes[i].datanode;
			}
			return list;
		}

		/// <summary>Access to the data directory used for Datanodes</summary>
		public virtual string GetDataDirectory()
		{
			return data_dir.GetAbsolutePath();
		}

		/// <summary>Get the base directory for this MiniDFS instance.</summary>
		/// <remarks>
		/// Get the base directory for this MiniDFS instance.
		/// <p/>
		/// Within the MiniDFCluster class and any subclasses, this method should be
		/// used instead of
		/// <see cref="GetBaseDirectory()"/>
		/// which doesn't support
		/// configuration-specific base directories.
		/// <p/>
		/// First the Configuration property
		/// <see cref="HdfsMinidfsBasedir"/>
		/// is fetched.
		/// If non-null, this is returned.
		/// If this is null, then
		/// <see cref="GetBaseDirectory()"/>
		/// is called.
		/// </remarks>
		/// <returns>the base directory for this instance.</returns>
		protected internal virtual string DetermineDfsBaseDir()
		{
			if (conf != null)
			{
				string dfsdir = conf.Get(HdfsMinidfsBasedir, null);
				if (dfsdir != null)
				{
					return dfsdir;
				}
			}
			return GetBaseDirectory();
		}

		/// <summary>
		/// Get the base directory for any DFS cluster whose configuration does
		/// not explicitly set it.
		/// </summary>
		/// <remarks>
		/// Get the base directory for any DFS cluster whose configuration does
		/// not explicitly set it. This is done by retrieving the system property
		/// <see cref="PropTestBuildData"/>
		/// (defaulting to "build/test/data" ),
		/// and returning that directory with a subdir of /dfs.
		/// </remarks>
		/// <returns>a directory for use as a miniDFS filesystem.</returns>
		public static string GetBaseDirectory()
		{
			return Runtime.GetProperty(PropTestBuildData, "build/test/data") + "/dfs/";
		}

		/// <summary>
		/// Get a storage directory for a datanode in this specific instance of
		/// a MiniCluster.
		/// </summary>
		/// <param name="dnIndex">datanode index (starts from 0)</param>
		/// <param name="dirIndex">
		/// directory index (0 or 1). Index 0 provides access to the
		/// first storage directory. Index 1 provides access to the second
		/// storage directory.
		/// </param>
		/// <returns>Storage directory</returns>
		public virtual FilePath GetInstanceStorageDir(int dnIndex, int dirIndex)
		{
			return new FilePath(base_dir, GetStorageDirPath(dnIndex, dirIndex));
		}

		/// <summary>Get a storage directory for a datanode.</summary>
		/// <remarks>
		/// Get a storage directory for a datanode.
		/// <ol>
		/// <li><base directory>/data/data<2*dnIndex + 1></li>
		/// <li><base directory>/data/data<2*dnIndex + 2></li>
		/// </ol>
		/// </remarks>
		/// <param name="dnIndex">datanode index (starts from 0)</param>
		/// <param name="dirIndex">directory index.</param>
		/// <returns>Storage directory</returns>
		public virtual FilePath GetStorageDir(int dnIndex, int dirIndex)
		{
			return new FilePath(GetBaseDirectory(), GetStorageDirPath(dnIndex, dirIndex));
		}

		/// <summary>
		/// Calculate the DN instance-specific path for appending to the base dir
		/// to determine the location of the storage of a DN instance in the mini cluster
		/// </summary>
		/// <param name="dnIndex">datanode index</param>
		/// <param name="dirIndex">directory index.</param>
		/// <returns>storage directory path</returns>
		private string GetStorageDirPath(int dnIndex, int dirIndex)
		{
			return "data/data" + (storagesPerDatanode * dnIndex + 1 + dirIndex);
		}

		/// <summary>
		/// Get current directory corresponding to the datanode as defined in
		/// (@link Storage#STORAGE_DIR_CURRENT}
		/// </summary>
		/// <param name="storageDir">the storage directory of a datanode.</param>
		/// <returns>the datanode current directory</returns>
		public static string GetDNCurrentDir(FilePath storageDir)
		{
			return storageDir + "/" + Storage.StorageDirCurrent + "/";
		}

		/// <summary>Get directory corresponding to block pool directory in the datanode</summary>
		/// <param name="storageDir">the storage directory of a datanode.</param>
		/// <returns>the block pool directory</returns>
		public static string GetBPDir(FilePath storageDir, string bpid)
		{
			return GetDNCurrentDir(storageDir) + bpid + "/";
		}

		/// <summary>Get directory relative to block pool directory in the datanode</summary>
		/// <param name="storageDir">storage directory</param>
		/// <returns>current directory in the given storage directory</returns>
		public static string GetBPDir(FilePath storageDir, string bpid, string dirName)
		{
			return GetBPDir(storageDir, bpid) + dirName + "/";
		}

		/// <summary>Get finalized directory for a block pool</summary>
		/// <param name="storageDir">storage directory</param>
		/// <param name="bpid">Block pool Id</param>
		/// <returns>finalized directory for a block pool</returns>
		public static FilePath GetRbwDir(FilePath storageDir, string bpid)
		{
			return new FilePath(GetBPDir(storageDir, bpid, Storage.StorageDirCurrent) + DataStorage
				.StorageDirRbw);
		}

		/// <summary>Get finalized directory for a block pool</summary>
		/// <param name="storageDir">storage directory</param>
		/// <param name="bpid">Block pool Id</param>
		/// <returns>finalized directory for a block pool</returns>
		public static FilePath GetFinalizedDir(FilePath storageDir, string bpid)
		{
			return new FilePath(GetBPDir(storageDir, bpid, Storage.StorageDirCurrent) + DataStorage
				.StorageDirFinalized);
		}

		/// <summary>Get file correpsonding to a block</summary>
		/// <param name="storageDir">storage directory</param>
		/// <param name="blk">the block</param>
		/// <returns>data file corresponding to the block</returns>
		public static FilePath GetBlockFile(FilePath storageDir, ExtendedBlock blk)
		{
			return new FilePath(DatanodeUtil.IdToBlockDir(GetFinalizedDir(storageDir, blk.GetBlockPoolId
				()), blk.GetBlockId()), blk.GetBlockName());
		}

		/// <summary>Get the latest metadata file correpsonding to a block</summary>
		/// <param name="storageDir">storage directory</param>
		/// <param name="blk">the block</param>
		/// <returns>metadata file corresponding to the block</returns>
		public static FilePath GetBlockMetadataFile(FilePath storageDir, ExtendedBlock blk
			)
		{
			return new FilePath(DatanodeUtil.IdToBlockDir(GetFinalizedDir(storageDir, blk.GetBlockPoolId
				()), blk.GetBlockId()), blk.GetBlockName() + "_" + blk.GetGenerationStamp() + Block
				.MetadataExtension);
		}

		/// <summary>Return all block metadata files in given directory (recursive search)</summary>
		public static IList<FilePath> GetAllBlockMetadataFiles(FilePath storageDir)
		{
			IList<FilePath> results = new AList<FilePath>();
			FilePath[] files = storageDir.ListFiles();
			if (files == null)
			{
				return null;
			}
			foreach (FilePath f in files)
			{
				if (f.GetName().StartsWith(Block.BlockFilePrefix) && f.GetName().EndsWith(Block.MetadataExtension
					))
				{
					results.AddItem(f);
				}
				else
				{
					if (f.IsDirectory())
					{
						IList<FilePath> subdirResults = GetAllBlockMetadataFiles(f);
						if (subdirResults != null)
						{
							Sharpen.Collections.AddAll(results, subdirResults);
						}
					}
				}
			}
			return results;
		}

		/// <summary>Shut down a cluster if it is not null</summary>
		/// <param name="cluster">cluster reference or null</param>
		public static void ShutdownCluster(MiniDFSCluster cluster)
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Get all files related to a block from all the datanodes</summary>
		/// <param name="block">block for which corresponding files are needed</param>
		public virtual FilePath[] GetAllBlockFiles(ExtendedBlock block)
		{
			if (dataNodes.Count == 0)
			{
				return new FilePath[0];
			}
			AList<FilePath> list = new AList<FilePath>();
			for (int i = 0; i < dataNodes.Count; i++)
			{
				FilePath blockFile = GetBlockFile(i, block);
				if (blockFile != null)
				{
					list.AddItem(blockFile);
				}
			}
			return Sharpen.Collections.ToArray(list, new FilePath[list.Count]);
		}

		/// <summary>Get the block data file for a block from a given datanode</summary>
		/// <param name="dnIndex">Index of the datanode to get block files for</param>
		/// <param name="block">block for which corresponding files are needed</param>
		public virtual FilePath GetBlockFile(int dnIndex, ExtendedBlock block)
		{
			// Check for block file in the two storage directories of the datanode
			for (int i = 0; i <= 1; i++)
			{
				FilePath storageDir = GetStorageDir(dnIndex, i);
				FilePath blockFile = GetBlockFile(storageDir, block);
				if (blockFile.Exists())
				{
					return blockFile;
				}
			}
			return null;
		}

		/// <summary>Get the block metadata file for a block from a given datanode</summary>
		/// <param name="dnIndex">Index of the datanode to get block files for</param>
		/// <param name="block">block for which corresponding files are needed</param>
		public virtual FilePath GetBlockMetadataFile(int dnIndex, ExtendedBlock block)
		{
			// Check for block file in the two storage directories of the datanode
			for (int i = 0; i <= 1; i++)
			{
				FilePath storageDir = GetStorageDir(dnIndex, i);
				FilePath blockMetaFile = GetBlockMetadataFile(storageDir, block);
				if (blockMetaFile.Exists())
				{
					return blockMetaFile;
				}
			}
			return null;
		}

		/// <summary>
		/// Throw an exception if the MiniDFSCluster is not started with a single
		/// namenode
		/// </summary>
		private void CheckSingleNameNode()
		{
			if (nameNodes.Length != 1)
			{
				throw new ArgumentException("Namenode index is needed");
			}
		}

		/// <summary>Add a namenode to a federated cluster and start it.</summary>
		/// <remarks>
		/// Add a namenode to a federated cluster and start it. Configuration of
		/// datanodes in the cluster is refreshed to register with the new namenode.
		/// </remarks>
		/// <returns>newly started namenode</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual NameNode AddNameNode(Configuration conf, int namenodePort)
		{
			if (!federation)
			{
				throw new IOException("cannot add namenode to non-federated cluster");
			}
			int nnIndex = nameNodes.Length;
			int numNameNodes = nameNodes.Length + 1;
			MiniDFSCluster.NameNodeInfo[] newlist = new MiniDFSCluster.NameNodeInfo[numNameNodes
				];
			System.Array.Copy(nameNodes, 0, newlist, 0, nameNodes.Length);
			nameNodes = newlist;
			string nameserviceId = NameserviceIdPrefix + (nnIndex + 1);
			string nameserviceIds = conf.Get(DFSConfigKeys.DfsNameservices);
			nameserviceIds += "," + nameserviceId;
			conf.Set(DFSConfigKeys.DfsNameservices, nameserviceIds);
			string nnId = null;
			InitNameNodeAddress(conf, nameserviceId, new MiniDFSNNTopology.NNConf(nnId).SetIpcPort
				(namenodePort));
			InitNameNodeConf(conf, nameserviceId, nnId, true, true, nnIndex);
			CreateNameNode(nnIndex, conf, numDataNodes, true, null, null, nameserviceId, nnId
				);
			// Refresh datanodes with the newly started namenode
			foreach (MiniDFSCluster.DataNodeProperties dn in dataNodes)
			{
				DataNode datanode = dn.datanode;
				datanode.RefreshNamenodes(conf);
			}
			// Wait for new namenode to get registrations from all the datanodes
			WaitActive(nnIndex);
			return nameNodes[nnIndex].nameNode;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void SetupDatanodeAddress(Configuration conf, bool setupHostsFile
			, bool checkDataNodeAddrConfig)
		{
			if (setupHostsFile)
			{
				string hostsFile = conf.Get(DFSConfigKeys.DfsHosts, string.Empty).Trim();
				if (hostsFile.Length == 0)
				{
					throw new IOException("Parameter dfs.hosts is not setup in conf");
				}
				// Setup datanode in the include file, if it is defined in the conf
				string address = "127.0.0.1:" + NetUtils.GetFreeSocketPort();
				if (checkDataNodeAddrConfig)
				{
					conf.SetIfUnset(DFSConfigKeys.DfsDatanodeAddressKey, address);
				}
				else
				{
					conf.Set(DFSConfigKeys.DfsDatanodeAddressKey, address);
				}
				AddToFile(hostsFile, address);
				Log.Info("Adding datanode " + address + " to hosts file " + hostsFile);
			}
			else
			{
				if (checkDataNodeAddrConfig)
				{
					conf.SetIfUnset(DFSConfigKeys.DfsDatanodeAddressKey, "127.0.0.1:0");
				}
				else
				{
					conf.Set(DFSConfigKeys.DfsDatanodeAddressKey, "127.0.0.1:0");
				}
			}
			if (checkDataNodeAddrConfig)
			{
				conf.SetIfUnset(DFSConfigKeys.DfsDatanodeHttpAddressKey, "127.0.0.1:0");
				conf.SetIfUnset(DFSConfigKeys.DfsDatanodeIpcAddressKey, "127.0.0.1:0");
			}
			else
			{
				conf.Set(DFSConfigKeys.DfsDatanodeHttpAddressKey, "127.0.0.1:0");
				conf.Set(DFSConfigKeys.DfsDatanodeIpcAddressKey, "127.0.0.1:0");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void AddToFile(string p, string address)
		{
			FilePath f = new FilePath(p);
			f.CreateNewFile();
			PrintWriter writer = new PrintWriter(new FileWriter(f, true));
			try
			{
				writer.WriteLine(address);
			}
			finally
			{
				writer.Close();
			}
		}
	}
}
