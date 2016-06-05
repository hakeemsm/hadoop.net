using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Security;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Cache;
using Com.Google.Common.Collect;
using Com.Google.Protobuf;
using Javax.Management;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Metrics;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Web;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Net.Unix;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Tracing;
using Org.Apache.Hadoop.Util;
using Org.Mortbay.Util.Ajax;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// DataNode is a class (and program) that stores a set of
	/// blocks for a DFS deployment.
	/// </summary>
	/// <remarks>
	/// DataNode is a class (and program) that stores a set of
	/// blocks for a DFS deployment.  A single deployment can
	/// have one or many DataNodes.  Each DataNode communicates
	/// regularly with a single NameNode.  It also communicates
	/// with client code and other DataNodes from time to time.
	/// DataNodes store a series of named blocks.  The DataNode
	/// allows client code to read these blocks, or to write new
	/// block data.  The DataNode may also, in response to instructions
	/// from its NameNode, delete blocks or copy blocks to/from other
	/// DataNodes.
	/// The DataNode maintains just one critical table:
	/// block-&gt; stream of bytes (of BLOCK_SIZE or less)
	/// This info is stored on a local disk.  The DataNode
	/// reports the table's contents to the NameNode upon startup
	/// and every so often afterwards.
	/// DataNodes spend their lives in an endless loop of asking
	/// the NameNode for something to do.  A NameNode cannot connect
	/// to a DataNode directly; a NameNode simply returns values from
	/// functions invoked by a DataNode.
	/// DataNodes maintain an open server socket so that client code
	/// or other DataNodes can read/write data.  The host/port for
	/// this server is reported to the NameNode, which then sends that
	/// information to clients or other DataNodes that might be interested.
	/// </remarks>
	public class DataNode : ReconfigurableBase, InterDatanodeProtocol, ClientDatanodeProtocol
		, TraceAdminProtocol, DataNodeMXBean
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.DataNode
			));

		static DataNode()
		{
			HdfsConfiguration.Init();
		}

		public const string DnClienttraceFormat = "src: %s" + ", dest: %s" + ", bytes: %s"
			 + ", op: %s" + ", cliID: %s" + ", offset: %s" + ", srvID: %s" + ", blockid: %s"
			 + ", duration: %s";

		internal static readonly Log ClientTraceLog = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.DataNode
			).FullName + ".clienttrace");

		private const string Usage = "Usage: java DataNode [-regular | -rollback]\n" + "    -regular                 : Normal DataNode startup (default).\n"
			 + "    -rollback                : Rollback a standard or rolling upgrade.\n" + 
			"  Refer to HDFS documentation for the difference between standard\n" + "  and rolling upgrades.";

		internal const int CurrentBlockFormatVersion = 1;

		// src IP
		// dst IP
		// byte count
		// operation
		// DFSClient id
		// offset
		// DatanodeRegistration
		// block id
		// duration time
		/// <summary>
		/// Use
		/// <see cref="Org.Apache.Hadoop.Net.NetUtils.CreateSocketAddr(string)"/>
		/// instead.
		/// </summary>
		[Obsolete]
		public static IPEndPoint CreateSocketAddr(string target)
		{
			return NetUtils.CreateSocketAddr(target);
		}

		internal volatile bool shouldRun = true;

		internal volatile bool shutdownForUpgrade = false;

		private bool shutdownInProgress = false;

		private BlockPoolManager blockPoolManager;

		internal volatile FsDatasetSpi<FsVolumeSpi> data = null;

		private string clusterId = null;

		public const string EmptyDelHint = string.Empty;

		internal readonly AtomicInteger xmitsInProgress = new AtomicInteger();

		internal Daemon dataXceiverServer = null;

		internal DataXceiverServer xserver = null;

		internal Daemon localDataXceiverServer = null;

		internal ShortCircuitRegistry shortCircuitRegistry = null;

		internal ThreadGroup threadGroup = null;

		private DNConf dnConf;

		private volatile bool heartbeatsDisabledForTests = false;

		private DataStorage storage = null;

		private DatanodeHttpServer httpServer = null;

		private int infoPort;

		private int infoSecurePort;

		internal DataNodeMetrics metrics;

		private IPEndPoint streamingAddr;

		private LoadingCache<string, IDictionary<string, long>> datanodeNetworkCounts;

		private string hostName;

		private DatanodeID id;

		private readonly string fileDescriptorPassingDisabledReason;

		internal bool isBlockTokenEnabled;

		internal BlockPoolTokenSecretManager blockPoolTokenSecretManager;

		private bool hasAnyBlockPoolRegistered = false;

		private readonly BlockScanner blockScanner;

		private DirectoryScanner directoryScanner = null;

		/// <summary>Activated plug-ins.</summary>
		private IList<ServicePlugin> plugins;

		public RPC.Server ipcServer;

		private JvmPauseMonitor pauseMonitor;

		private SecureDataNodeStarter.SecureResources secureResources = null;

		private IList<StorageLocation> dataDirs;

		private Configuration conf;

		private readonly string confVersion;

		private readonly long maxNumberOfBlocksToLog;

		private readonly bool pipelineSupportECN;

		private readonly IList<string> usersWithLocalPathAccess;

		private readonly bool connectToDnViaHostname;

		internal ReadaheadPool readaheadPool;

		internal SaslDataTransferClient saslClient;

		internal SaslDataTransferServer saslServer;

		private readonly bool getHdfsBlockLocationsEnabled;

		private ObjectName dataNodeInfoBeanName;

		private Sharpen.Thread checkDiskErrorThread = null;

		protected internal readonly int checkDiskErrorInterval = 5 * 1000;

		private bool checkDiskErrorFlag = false;

		private object checkDiskErrorMutex = new object();

		private long lastDiskErrorCheck;

		private string supergroup;

		private bool isPermissionEnabled;

		private string dnUserName = null;

		private SpanReceiverHost spanReceiverHost;

		/// <summary>Creates a dummy DataNode for testing purpose.</summary>
		[VisibleForTesting]
		internal DataNode(Configuration conf)
			: base(conf)
		{
			// See the note below in incrDatanodeNetworkErrors re: concurrency.
			// For InterDataNodeProtocol
			// dataDirs must be accessed while holding the DataNode lock.
			this.blockScanner = new BlockScanner(this, conf);
			this.fileDescriptorPassingDisabledReason = null;
			this.maxNumberOfBlocksToLog = 0;
			this.confVersion = null;
			this.usersWithLocalPathAccess = null;
			this.connectToDnViaHostname = false;
			this.getHdfsBlockLocationsEnabled = false;
			this.pipelineSupportECN = false;
		}

		/// <summary>
		/// Create the DataNode given a configuration, an array of dataDirs,
		/// and a namenode proxy
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal DataNode(Configuration conf, IList<StorageLocation> dataDirs, SecureDataNodeStarter.SecureResources
			 resources)
			: base(conf)
		{
			this.blockScanner = new BlockScanner(this, conf);
			this.lastDiskErrorCheck = 0;
			this.maxNumberOfBlocksToLog = conf.GetLong(DFSConfigKeys.DfsMaxNumBlocksToLogKey, 
				DFSConfigKeys.DfsMaxNumBlocksToLogDefault);
			this.usersWithLocalPathAccess = Arrays.AsList(conf.GetTrimmedStrings(DFSConfigKeys
				.DfsBlockLocalPathAccessUserKey));
			this.connectToDnViaHostname = conf.GetBoolean(DFSConfigKeys.DfsDatanodeUseDnHostname
				, DFSConfigKeys.DfsDatanodeUseDnHostnameDefault);
			this.getHdfsBlockLocationsEnabled = conf.GetBoolean(DFSConfigKeys.DfsHdfsBlocksMetadataEnabled
				, DFSConfigKeys.DfsHdfsBlocksMetadataEnabledDefault);
			this.supergroup = conf.Get(DFSConfigKeys.DfsPermissionsSuperusergroupKey, DFSConfigKeys
				.DfsPermissionsSuperusergroupDefault);
			this.isPermissionEnabled = conf.GetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey
				, DFSConfigKeys.DfsPermissionsEnabledDefault);
			this.pipelineSupportECN = conf.GetBoolean(DFSConfigKeys.DfsPipelineEcnEnabled, DFSConfigKeys
				.DfsPipelineEcnEnabledDefault);
			confVersion = "core-" + conf.Get("hadoop.common.configuration.version", "UNSPECIFIED"
				) + ",hdfs-" + conf.Get("hadoop.hdfs.configuration.version", "UNSPECIFIED");
			// Determine whether we should try to pass file descriptors to clients.
			if (conf.GetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, DFSConfigKeys.DfsClientReadShortcircuitDefault
				))
			{
				string reason = DomainSocket.GetLoadingFailureReason();
				if (reason != null)
				{
					Log.Warn("File descriptor passing is disabled because " + reason);
					this.fileDescriptorPassingDisabledReason = reason;
				}
				else
				{
					Log.Info("File descriptor passing is enabled.");
					this.fileDescriptorPassingDisabledReason = null;
				}
			}
			else
			{
				this.fileDescriptorPassingDisabledReason = "File descriptor passing was not configured.";
				Log.Debug(this.fileDescriptorPassingDisabledReason);
			}
			try
			{
				hostName = GetHostName(conf);
				Log.Info("Configured hostname is " + hostName);
				StartDataNode(conf, dataDirs, resources);
			}
			catch (IOException ie)
			{
				Shutdown();
				throw;
			}
			int dncCacheMaxSize = conf.GetInt(DFSConfigKeys.DfsDatanodeNetworkCountsCacheMaxSizeKey
				, DFSConfigKeys.DfsDatanodeNetworkCountsCacheMaxSizeDefault);
			datanodeNetworkCounts = CacheBuilder.NewBuilder().MaximumSize(dncCacheMaxSize).Build
				(new _CacheLoader_439());
		}

		private sealed class _CacheLoader_439 : CacheLoader<string, IDictionary<string, long
			>>
		{
			public _CacheLoader_439()
			{
			}

			/// <exception cref="System.Exception"/>
			public override IDictionary<string, long> Load(string key)
			{
				IDictionary<string, long> ret = new Dictionary<string, long>();
				ret["networkErrors"] = 0L;
				return ret;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		protected override void ReconfigurePropertyImpl(string property, string newVal)
		{
			if (property.Equals(DFSConfigKeys.DfsDatanodeDataDirKey))
			{
				try
				{
					Log.Info("Reconfiguring " + property + " to " + newVal);
					this.RefreshVolumes(newVal);
				}
				catch (IOException e)
				{
					throw new ReconfigurationException(property, newVal, GetConf().Get(property), e);
				}
			}
			else
			{
				throw new ReconfigurationException(property, newVal, GetConf().Get(property));
			}
		}

		/// <summary>Get a list of the keys of the re-configurable properties in configuration.
		/// 	</summary>
		public override ICollection<string> GetReconfigurableProperties()
		{
			IList<string> reconfigurable = Sharpen.Collections.UnmodifiableList(Arrays.AsList
				(DFSConfigKeys.DfsDatanodeDataDirKey));
			return reconfigurable;
		}

		/// <summary>The ECN bit for the DataNode.</summary>
		/// <remarks>
		/// The ECN bit for the DataNode. The DataNode should return:
		/// <ul>
		/// <li>ECN.DISABLED when ECN is disabled.</li>
		/// <li>ECN.SUPPORTED when ECN is enabled but the DN still has capacity.</li>
		/// <li>ECN.CONGESTED when ECN is enabled and the DN is congested.</li>
		/// </ul>
		/// </remarks>
		public virtual PipelineAck.ECN GetECN()
		{
			return pipelineSupportECN ? PipelineAck.ECN.Supported : PipelineAck.ECN.Disabled;
		}

		/// <summary>Contains the StorageLocations for changed data volumes.</summary>
		internal class ChangedVolumes
		{
			/// <summary>The storage locations of the newly added volumes.</summary>
			internal IList<StorageLocation> newLocations = Lists.NewArrayList();

			/// <summary>The storage locations of the volumes that are removed.</summary>
			internal IList<StorageLocation> deactivateLocations = Lists.NewArrayList();

			/// <summary>The unchanged locations that existed in the old configuration.</summary>
			internal IList<StorageLocation> unchangedLocations = Lists.NewArrayList();
		}

		/// <summary>
		/// Parse the new DFS_DATANODE_DATA_DIR value in the configuration to detect
		/// changed volumes.
		/// </summary>
		/// <param name="newVolumes">a comma separated string that specifies the data volumes.
		/// 	</param>
		/// <returns>changed volumes.</returns>
		/// <exception cref="System.IO.IOException">
		/// if none of the directories are specified in the
		/// configuration.
		/// </exception>
		[VisibleForTesting]
		internal virtual DataNode.ChangedVolumes ParseChangedVolumes(string newVolumes)
		{
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, newVolumes);
			IList<StorageLocation> locations = GetStorageLocations(conf);
			if (locations.IsEmpty())
			{
				throw new IOException("No directory is specified.");
			}
			DataNode.ChangedVolumes results = new DataNode.ChangedVolumes();
			Sharpen.Collections.AddAll(results.newLocations, locations);
			for (IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(); it.HasNext
				(); )
			{
				Storage.StorageDirectory dir = it.Next();
				bool found = false;
				for (IEnumerator<StorageLocation> sl = results.newLocations.GetEnumerator(); sl.HasNext
					(); )
				{
					StorageLocation location = sl.Next();
					if (location.GetFile().GetCanonicalPath().Equals(dir.GetRoot().GetCanonicalPath()
						))
					{
						sl.Remove();
						results.unchangedLocations.AddItem(location);
						found = true;
						break;
					}
				}
				if (!found)
				{
					results.deactivateLocations.AddItem(StorageLocation.Parse(dir.GetRoot().ToString(
						)));
				}
			}
			return results;
		}

		/// <summary>Attempts to reload data volumes with new configuration.</summary>
		/// <param name="newVolumes">a comma separated string that specifies the data volumes.
		/// 	</param>
		/// <exception cref="System.IO.IOException">
		/// on error. If an IOException is thrown, some new volumes
		/// may have been successfully added and removed.
		/// </exception>
		private void RefreshVolumes(string newVolumes)
		{
			lock (this)
			{
				Configuration conf = GetConf();
				conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, newVolumes);
				int numOldDataDirs = dataDirs.Count;
				DataNode.ChangedVolumes changedVolumes = ParseChangedVolumes(newVolumes);
				StringBuilder errorMessageBuilder = new StringBuilder();
				IList<string> effectiveVolumes = Lists.NewArrayList();
				foreach (StorageLocation sl in changedVolumes.unchangedLocations)
				{
					effectiveVolumes.AddItem(sl.ToString());
				}
				try
				{
					if (numOldDataDirs + changedVolumes.newLocations.Count - changedVolumes.deactivateLocations
						.Count <= 0)
					{
						throw new IOException("Attempt to remove all volumes.");
					}
					if (!changedVolumes.newLocations.IsEmpty())
					{
						Log.Info("Adding new volumes: " + Joiner.On(",").Join(changedVolumes.newLocations
							));
						// Add volumes for each Namespace
						IList<NamespaceInfo> nsInfos = Lists.NewArrayList();
						foreach (BPOfferService bpos in blockPoolManager.GetAllNamenodeThreads())
						{
							nsInfos.AddItem(bpos.GetNamespaceInfo());
						}
						ExecutorService service = Executors.NewFixedThreadPool(changedVolumes.newLocations
							.Count);
						IList<Future<IOException>> exceptions = Lists.NewArrayList();
						foreach (StorageLocation location in changedVolumes.newLocations)
						{
							exceptions.AddItem(service.Submit(new _Callable_584(this, location, nsInfos)));
						}
						for (int i = 0; i < changedVolumes.newLocations.Count; i++)
						{
							StorageLocation volume = changedVolumes.newLocations[i];
							Future<IOException> ioExceptionFuture = exceptions[i];
							try
							{
								IOException ioe = ioExceptionFuture.Get();
								if (ioe != null)
								{
									errorMessageBuilder.Append(string.Format("FAILED TO ADD: %s: %s%n", volume, ioe.Message
										));
									Log.Error("Failed to add volume: " + volume, ioe);
								}
								else
								{
									effectiveVolumes.AddItem(volume.ToString());
									Log.Info("Successfully added volume: " + volume);
								}
							}
							catch (Exception e)
							{
								errorMessageBuilder.Append(string.Format("FAILED to ADD: %s: %s%n", volume, e.ToString
									()));
								Log.Error("Failed to add volume: " + volume, e);
							}
						}
					}
					try
					{
						RemoveVolumes(changedVolumes.deactivateLocations);
					}
					catch (IOException e)
					{
						errorMessageBuilder.Append(e.Message);
						Log.Error("Failed to remove volume: " + e.Message, e);
					}
					if (errorMessageBuilder.Length > 0)
					{
						throw new IOException(errorMessageBuilder.ToString());
					}
				}
				finally
				{
					conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, Joiner.On(",").Join(effectiveVolumes
						));
					dataDirs = GetStorageLocations(conf);
					// Send a full block report to let NN acknowledge the volume changes.
					TriggerBlockReport(new BlockReportOptions.Factory().SetIncremental(false).Build()
						);
				}
			}
		}

		private sealed class _Callable_584 : Callable<IOException>
		{
			public _Callable_584(DataNode _enclosing, StorageLocation location, IList<NamespaceInfo
				> nsInfos)
			{
				this._enclosing = _enclosing;
				this.location = location;
				this.nsInfos = nsInfos;
			}

			public IOException Call()
			{
				try
				{
					this._enclosing.data.AddVolume(location, nsInfos);
				}
				catch (IOException e)
				{
					return e;
				}
				return null;
			}

			private readonly DataNode _enclosing;

			private readonly StorageLocation location;

			private readonly IList<NamespaceInfo> nsInfos;
		}

		/// <summary>Remove volumes from DataNode.</summary>
		/// <remarks>
		/// Remove volumes from DataNode.
		/// See
		/// <see>removeVolumes(final Set<File>, boolean)</see>
		/// for details.
		/// </remarks>
		/// <param name="locations">the StorageLocations of the volumes to be removed.</param>
		/// <exception cref="System.IO.IOException"/>
		private void RemoveVolumes(ICollection<StorageLocation> locations)
		{
			if (locations.IsEmpty())
			{
				return;
			}
			ICollection<FilePath> volumesToRemove = new HashSet<FilePath>();
			foreach (StorageLocation loc in locations)
			{
				volumesToRemove.AddItem(loc.GetFile().GetAbsoluteFile());
			}
			RemoveVolumes(volumesToRemove, true);
		}

		/// <summary>Remove volumes from DataNode.</summary>
		/// <remarks>
		/// Remove volumes from DataNode.
		/// It does three things:
		/// <li>
		/// <ul>Remove volumes and block info from FsDataset.</ul>
		/// <ul>Remove volumes from DataStorage.</ul>
		/// <ul>Reset configuration DATA_DIR and
		/// <see cref="dataDirs"/>
		/// to represent
		/// active volumes.</ul>
		/// </li>
		/// </remarks>
		/// <param name="absoluteVolumePaths">the absolute path of volumes.</param>
		/// <param name="clearFailure">
		/// if true, clears the failure information related to the
		/// volumes.
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		private void RemoveVolumes(ICollection<FilePath> absoluteVolumePaths, bool clearFailure
			)
		{
			lock (this)
			{
				foreach (FilePath vol in absoluteVolumePaths)
				{
					Preconditions.CheckArgument(vol.IsAbsolute());
				}
				if (absoluteVolumePaths.IsEmpty())
				{
					return;
				}
				Log.Info(string.Format("Deactivating volumes (clear failure=%b): %s", clearFailure
					, Joiner.On(",").Join(absoluteVolumePaths)));
				IOException ioe = null;
				// Remove volumes and block infos from FsDataset.
				data.RemoveVolumes(absoluteVolumePaths, clearFailure);
				// Remove volumes from DataStorage.
				try
				{
					storage.RemoveVolumes(absoluteVolumePaths);
				}
				catch (IOException e)
				{
					ioe = e;
				}
				// Set configuration and dataDirs to reflect volume changes.
				for (IEnumerator<StorageLocation> it = dataDirs.GetEnumerator(); it.HasNext(); )
				{
					StorageLocation loc = it.Next();
					if (absoluteVolumePaths.Contains(loc.GetFile().GetAbsoluteFile()))
					{
						it.Remove();
					}
				}
				conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, Joiner.On(",").Join(dataDirs));
				if (ioe != null)
				{
					throw ioe;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void SetClusterId(string nsCid, string bpid)
		{
			lock (this)
			{
				if (clusterId != null && !clusterId.Equals(nsCid))
				{
					throw new IOException("Cluster IDs not matched: dn cid=" + clusterId + " but ns cid="
						 + nsCid + "; bpid=" + bpid);
				}
				// else
				clusterId = nsCid;
			}
		}

		/// <summary>Returns the hostname for this datanode.</summary>
		/// <remarks>
		/// Returns the hostname for this datanode. If the hostname is not
		/// explicitly configured in the given config, then it is determined
		/// via the DNS class.
		/// </remarks>
		/// <param name="config">configuration</param>
		/// <returns>the hostname (NB: may not be a FQDN)</returns>
		/// <exception cref="Sharpen.UnknownHostException">
		/// if the dfs.datanode.dns.interface
		/// option is used and the hostname can not be determined
		/// </exception>
		private static string GetHostName(Configuration config)
		{
			string name = config.Get(DFSConfigKeys.DfsDatanodeHostNameKey);
			if (name == null)
			{
				name = DNS.GetDefaultHost(config.Get(DFSConfigKeys.DfsDatanodeDnsInterfaceKey, DFSConfigKeys
					.DfsDatanodeDnsInterfaceDefault), config.Get(DFSConfigKeys.DfsDatanodeDnsNameserverKey
					, DFSConfigKeys.DfsDatanodeDnsNameserverDefault));
			}
			return name;
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.DFSUtil.GetHttpPolicy(Org.Apache.Hadoop.Conf.Configuration)
		/// 	">
		/// for information related to the different configuration options and
		/// Http Policy is decided.
		/// </seealso>
		/// <exception cref="System.IO.IOException"/>
		private void StartInfoServer(Configuration conf)
		{
			// SecureDataNodeStarter will bind the privileged port to the channel if
			// the DN is started by JSVC, pass it along.
			ServerSocketChannel httpServerChannel = secureResources != null ? secureResources
				.GetHttpServerChannel() : null;
			this.httpServer = new DatanodeHttpServer(conf, this, httpServerChannel);
			httpServer.Start();
			if (httpServer.GetHttpAddress() != null)
			{
				infoPort = httpServer.GetHttpAddress().Port;
			}
			if (httpServer.GetHttpsAddress() != null)
			{
				infoSecurePort = httpServer.GetHttpsAddress().Port;
			}
		}

		private void StartPlugins(Configuration conf)
		{
			plugins = conf.GetInstances<ServicePlugin>(DFSConfigKeys.DfsDatanodePluginsKey);
			foreach (ServicePlugin p in plugins)
			{
				try
				{
					p.Start(this);
					Log.Info("Started plug-in " + p);
				}
				catch (Exception t)
				{
					Log.Warn("ServicePlugin " + p + " could not be started", t);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void InitIpcServer(Configuration conf)
		{
			IPEndPoint ipcAddr = NetUtils.CreateSocketAddr(conf.GetTrimmed(DFSConfigKeys.DfsDatanodeIpcAddressKey
				));
			// Add all the RPC protocols that the Datanode implements    
			RPC.SetProtocolEngine(conf, typeof(ClientDatanodeProtocolPB), typeof(ProtobufRpcEngine
				));
			ClientDatanodeProtocolServerSideTranslatorPB clientDatanodeProtocolXlator = new ClientDatanodeProtocolServerSideTranslatorPB
				(this);
			BlockingService service = ClientDatanodeProtocolProtos.ClientDatanodeProtocolService
				.NewReflectiveBlockingService(clientDatanodeProtocolXlator);
			ipcServer = new RPC.Builder(conf).SetProtocol(typeof(ClientDatanodeProtocolPB)).SetInstance
				(service).SetBindAddress(ipcAddr.GetHostName()).SetPort(ipcAddr.Port).SetNumHandlers
				(conf.GetInt(DFSConfigKeys.DfsDatanodeHandlerCountKey, DFSConfigKeys.DfsDatanodeHandlerCountDefault
				)).SetVerbose(false).SetSecretManager(blockPoolTokenSecretManager).Build();
			InterDatanodeProtocolServerSideTranslatorPB interDatanodeProtocolXlator = new InterDatanodeProtocolServerSideTranslatorPB
				(this);
			service = InterDatanodeProtocolProtos.InterDatanodeProtocolService.NewReflectiveBlockingService
				(interDatanodeProtocolXlator);
			DFSUtil.AddPBProtocol(conf, typeof(InterDatanodeProtocolPB), service, ipcServer);
			TraceAdminProtocolServerSideTranslatorPB traceAdminXlator = new TraceAdminProtocolServerSideTranslatorPB
				(this);
			BlockingService traceAdminService = TraceAdminPB.TraceAdminService.NewReflectiveBlockingService
				(traceAdminXlator);
			DFSUtil.AddPBProtocol(conf, typeof(TraceAdminProtocolPB), traceAdminService, ipcServer
				);
			Log.Info("Opened IPC server at " + ipcServer.GetListenerAddress());
			// set service-level authorization security policy
			if (conf.GetBoolean(CommonConfigurationKeys.HadoopSecurityAuthorization, false))
			{
				ipcServer.RefreshServiceAcl(conf, new HDFSPolicyProvider());
			}
		}

		/// <summary>Check whether the current user is in the superuser group.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		private void CheckSuperuserPrivilege()
		{
			if (!isPermissionEnabled)
			{
				return;
			}
			// Try to get the ugi in the RPC call.
			UserGroupInformation callerUgi = RPC.Server.GetRemoteUser();
			if (callerUgi == null)
			{
				// This is not from RPC.
				callerUgi = UserGroupInformation.GetCurrentUser();
			}
			// Is this by the DN user itself?
			System.Diagnostics.Debug.Assert(dnUserName != null);
			if (callerUgi.GetShortUserName().Equals(dnUserName))
			{
				return;
			}
			// Is the user a member of the super group?
			IList<string> groups = Arrays.AsList(callerUgi.GetGroupNames());
			if (groups.Contains(supergroup))
			{
				return;
			}
			// Not a superuser.
			throw new AccessControlException();
		}

		private void ShutdownPeriodicScanners()
		{
			ShutdownDirectoryScanner();
			blockScanner.RemoveAllVolumeScanners();
		}

		/// <summary>
		/// See
		/// <see cref="DirectoryScanner"/>
		/// </summary>
		private void InitDirectoryScanner(Configuration conf)
		{
			lock (this)
			{
				if (directoryScanner != null)
				{
					return;
				}
				string reason = null;
				if (conf.GetInt(DFSConfigKeys.DfsDatanodeDirectoryscanIntervalKey, DFSConfigKeys.
					DfsDatanodeDirectoryscanIntervalDefault) < 0)
				{
					reason = "verification is turned off by configuration";
				}
				else
				{
					if ("SimulatedFSDataset".Equals(data.GetType().Name))
					{
						reason = "verifcation is not supported by SimulatedFSDataset";
					}
				}
				if (reason == null)
				{
					directoryScanner = new DirectoryScanner(this, data, conf);
					directoryScanner.Start();
				}
				else
				{
					Log.Info("Periodic Directory Tree Verification scan is disabled because " + reason
						);
				}
			}
		}

		private void ShutdownDirectoryScanner()
		{
			lock (this)
			{
				if (directoryScanner != null)
				{
					directoryScanner.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void InitDataXceiver(Configuration conf)
		{
			// find free port or use privileged port provided
			TcpPeerServer tcpPeerServer;
			if (secureResources != null)
			{
				tcpPeerServer = new TcpPeerServer(secureResources);
			}
			else
			{
				tcpPeerServer = new TcpPeerServer(dnConf.socketWriteTimeout, DataNode.GetStreamingAddr
					(conf));
			}
			tcpPeerServer.SetReceiveBufferSize(HdfsConstants.DefaultDataSocketSize);
			streamingAddr = tcpPeerServer.GetStreamingAddr();
			Log.Info("Opened streaming server at " + streamingAddr);
			this.threadGroup = new ThreadGroup("dataXceiverServer");
			xserver = new DataXceiverServer(tcpPeerServer, conf, this);
			this.dataXceiverServer = new Daemon(threadGroup, xserver);
			this.threadGroup.SetDaemon(true);
			// auto destroy when empty
			if (conf.GetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, DFSConfigKeys.DfsClientReadShortcircuitDefault
				) || conf.GetBoolean(DFSConfigKeys.DfsClientDomainSocketDataTraffic, DFSConfigKeys
				.DfsClientDomainSocketDataTrafficDefault))
			{
				DomainPeerServer domainPeerServer = GetDomainPeerServer(conf, streamingAddr.Port);
				if (domainPeerServer != null)
				{
					this.localDataXceiverServer = new Daemon(threadGroup, new DataXceiverServer(domainPeerServer
						, conf, this));
					Log.Info("Listening on UNIX domain socket: " + domainPeerServer.GetBindPath());
				}
			}
			this.shortCircuitRegistry = new ShortCircuitRegistry(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static DomainPeerServer GetDomainPeerServer(Configuration conf, int port
			)
		{
			string domainSocketPath = conf.GetTrimmed(DFSConfigKeys.DfsDomainSocketPathKey, DFSConfigKeys
				.DfsDomainSocketPathDefault);
			if (domainSocketPath.IsEmpty())
			{
				if (conf.GetBoolean(DFSConfigKeys.DfsClientReadShortcircuitKey, DFSConfigKeys.DfsClientReadShortcircuitDefault
					) && (!conf.GetBoolean(DFSConfigKeys.DfsClientUseLegacyBlockreaderlocal, DFSConfigKeys
					.DfsClientUseLegacyBlockreaderlocalDefault)))
				{
					Log.Warn("Although short-circuit local reads are configured, " + "they are disabled because you didn't configure "
						 + DFSConfigKeys.DfsDomainSocketPathKey);
				}
				return null;
			}
			if (DomainSocket.GetLoadingFailureReason() != null)
			{
				throw new RuntimeException("Although a UNIX domain socket " + "path is configured as "
					 + domainSocketPath + ", we cannot " + "start a localDataXceiverServer because "
					 + DomainSocket.GetLoadingFailureReason());
			}
			DomainPeerServer domainPeerServer = new DomainPeerServer(domainSocketPath, port);
			domainPeerServer.SetReceiveBufferSize(HdfsConstants.DefaultDataSocketSize);
			return domainPeerServer;
		}

		// calls specific to BP
		public virtual void NotifyNamenodeReceivedBlock(ExtendedBlock block, string delHint
			, string storageUuid)
		{
			BPOfferService bpos = blockPoolManager.Get(block.GetBlockPoolId());
			if (bpos != null)
			{
				bpos.NotifyNamenodeReceivedBlock(block, delHint, storageUuid);
			}
			else
			{
				Log.Error("Cannot find BPOfferService for reporting block received for bpid=" + block
					.GetBlockPoolId());
			}
		}

		// calls specific to BP
		protected internal virtual void NotifyNamenodeReceivingBlock(ExtendedBlock block, 
			string storageUuid)
		{
			BPOfferService bpos = blockPoolManager.Get(block.GetBlockPoolId());
			if (bpos != null)
			{
				bpos.NotifyNamenodeReceivingBlock(block, storageUuid);
			}
			else
			{
				Log.Error("Cannot find BPOfferService for reporting block receiving for bpid=" + 
					block.GetBlockPoolId());
			}
		}

		/// <summary>Notify the corresponding namenode to delete the block.</summary>
		public virtual void NotifyNamenodeDeletedBlock(ExtendedBlock block, string storageUuid
			)
		{
			BPOfferService bpos = blockPoolManager.Get(block.GetBlockPoolId());
			if (bpos != null)
			{
				bpos.NotifyNamenodeDeletedBlock(block, storageUuid);
			}
			else
			{
				Log.Error("Cannot find BPOfferService for reporting block deleted for bpid=" + block
					.GetBlockPoolId());
			}
		}

		/// <summary>Report a bad block which is hosted on the local DN.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReportBadBlocks(ExtendedBlock block)
		{
			BPOfferService bpos = GetBPOSForBlock(block);
			FsVolumeSpi volume = GetFSDataset().GetVolume(block);
			bpos.ReportBadBlocks(block, volume.GetStorageID(), volume.GetStorageType());
		}

		/// <summary>
		/// Report a bad block on another DN (eg if we received a corrupt replica
		/// from a remote host).
		/// </summary>
		/// <param name="srcDataNode">the DN hosting the bad block</param>
		/// <param name="block">the block itself</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReportRemoteBadBlock(DatanodeInfo srcDataNode, ExtendedBlock 
			block)
		{
			BPOfferService bpos = GetBPOSForBlock(block);
			bpos.ReportRemoteBadBlock(srcDataNode, block);
		}

		/// <summary>
		/// Try to send an error report to the NNs associated with the given
		/// block pool.
		/// </summary>
		/// <param name="bpid">the block pool ID</param>
		/// <param name="errCode">error code to send</param>
		/// <param name="errMsg">textual message to send</param>
		internal virtual void TrySendErrorReport(string bpid, int errCode, string errMsg)
		{
			BPOfferService bpos = blockPoolManager.Get(bpid);
			if (bpos == null)
			{
				throw new ArgumentException("Bad block pool: " + bpid);
			}
			bpos.TrySendErrorReport(errCode, errMsg);
		}

		/// <summary>Return the BPOfferService instance corresponding to the given block.</summary>
		/// <returns>the BPOS</returns>
		/// <exception cref="System.IO.IOException">if no such BPOS can be found</exception>
		private BPOfferService GetBPOSForBlock(ExtendedBlock block)
		{
			Preconditions.CheckNotNull(block);
			BPOfferService bpos = blockPoolManager.Get(block.GetBlockPoolId());
			if (bpos == null)
			{
				throw new IOException("cannot locate OfferService thread for bp=" + block.GetBlockPoolId
					());
			}
			return bpos;
		}

		// used only for testing
		internal virtual void SetHeartbeatsDisabledForTests(bool heartbeatsDisabledForTests
			)
		{
			this.heartbeatsDisabledForTests = heartbeatsDisabledForTests;
		}

		internal virtual bool AreHeartbeatsDisabledForTests()
		{
			return this.heartbeatsDisabledForTests;
		}

		/// <summary>This method starts the data node with the specified conf.</summary>
		/// <param name="conf">
		/// - the configuration
		/// if conf's CONFIG_PROPERTY_SIMULATED property is set
		/// then a simulated storage based data node is created.
		/// </param>
		/// <param name="dataDirs">- only for a non-simulated storage data node</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void StartDataNode(Configuration conf, IList<StorageLocation> dataDirs
			, SecureDataNodeStarter.SecureResources resources)
		{
			// settings global for all BPs in the Data Node
			this.secureResources = resources;
			lock (this)
			{
				this.dataDirs = dataDirs;
			}
			this.conf = conf;
			this.dnConf = new DNConf(conf);
			CheckSecureConfig(dnConf, conf, resources);
			this.spanReceiverHost = SpanReceiverHost.Get(conf, DFSConfigKeys.DfsServerHtracePrefix
				);
			if (dnConf.maxLockedMemory > 0)
			{
				if (!NativeIO.POSIX.GetCacheManipulator().VerifyCanMlock())
				{
					throw new RuntimeException(string.Format("Cannot start datanode because the configured max locked memory"
						 + " size (%s) is greater than zero and native code is not available.", DFSConfigKeys
						.DfsDatanodeMaxLockedMemoryKey));
				}
				if (Path.Windows)
				{
					NativeIO.Windows.ExtendWorkingSetSize(dnConf.maxLockedMemory);
				}
				else
				{
					long ulimit = NativeIO.POSIX.GetCacheManipulator().GetMemlockLimit();
					if (dnConf.maxLockedMemory > ulimit)
					{
						throw new RuntimeException(string.Format("Cannot start datanode because the configured max locked memory"
							 + " size (%s) of %d bytes is more than the datanode's available" + " RLIMIT_MEMLOCK ulimit of %d bytes."
							, DFSConfigKeys.DfsDatanodeMaxLockedMemoryKey, dnConf.maxLockedMemory, ulimit));
					}
				}
			}
			Log.Info("Starting DataNode with maxLockedMemory = " + dnConf.maxLockedMemory);
			storage = new DataStorage();
			// global DN settings
			RegisterMXBean();
			InitDataXceiver(conf);
			StartInfoServer(conf);
			pauseMonitor = new JvmPauseMonitor(conf);
			pauseMonitor.Start();
			// BlockPoolTokenSecretManager is required to create ipc server.
			this.blockPoolTokenSecretManager = new BlockPoolTokenSecretManager();
			// Login is done by now. Set the DN user name.
			dnUserName = UserGroupInformation.GetCurrentUser().GetShortUserName();
			Log.Info("dnUserName = " + dnUserName);
			Log.Info("supergroup = " + supergroup);
			InitIpcServer(conf);
			metrics = DataNodeMetrics.Create(conf, GetDisplayName());
			metrics.GetJvmMetrics().SetPauseMonitor(pauseMonitor);
			blockPoolManager = new BlockPoolManager(this);
			blockPoolManager.RefreshNamenodes(conf);
			// Create the ReadaheadPool from the DataNode context so we can
			// exit without having to explicitly shutdown its thread pool.
			readaheadPool = ReadaheadPool.GetInstance();
			saslClient = new SaslDataTransferClient(dnConf.conf, dnConf.saslPropsResolver, dnConf
				.trustedChannelResolver);
			saslServer = new SaslDataTransferServer(dnConf, blockPoolTokenSecretManager);
		}

		/// <summary>Checks if the DataNode has a secure configuration if security is enabled.
		/// 	</summary>
		/// <remarks>
		/// Checks if the DataNode has a secure configuration if security is enabled.
		/// There are 2 possible configurations that are considered secure:
		/// 1. The server has bound to privileged ports for RPC and HTTP via
		/// SecureDataNodeStarter.
		/// 2. The configuration enables SASL on DataTransferProtocol and HTTPS (no
		/// plain HTTP) for the HTTP server.  The SASL handshake guarantees
		/// authentication of the RPC server before a client transmits a secret, such
		/// as a block access token.  Similarly, SSL guarantees authentication of the
		/// HTTP server before a client transmits a secret, such as a delegation
		/// token.
		/// It is not possible to run with both privileged ports and SASL on
		/// DataTransferProtocol.  For backwards-compatibility, the connection logic
		/// must check if the target port is a privileged port, and if so, skip the
		/// SASL handshake.
		/// </remarks>
		/// <param name="dnConf">DNConf to check</param>
		/// <param name="conf">Configuration to check</param>
		/// <param name="resources">SecuredResources obtained for DataNode</param>
		/// <exception cref="Sharpen.RuntimeException">if security enabled, but configuration is insecure
		/// 	</exception>
		private static void CheckSecureConfig(DNConf dnConf, Configuration conf, SecureDataNodeStarter.SecureResources
			 resources)
		{
			if (!UserGroupInformation.IsSecurityEnabled())
			{
				return;
			}
			SaslPropertiesResolver saslPropsResolver = dnConf.GetSaslPropsResolver();
			if (resources != null && saslPropsResolver == null)
			{
				return;
			}
			if (dnConf.GetIgnoreSecurePortsForTesting())
			{
				return;
			}
			if (saslPropsResolver != null && DFSUtil.GetHttpPolicy(conf) == HttpConfig.Policy
				.HttpsOnly && resources == null)
			{
				return;
			}
			throw new RuntimeException("Cannot start secure DataNode without " + "configuring either privileged resources or SASL RPC data transfer "
				 + "protection and SSL for HTTP.  Using privileged resources in " + "combination with SASL RPC data transfer protection is not supported."
				);
		}

		public static string GenerateUuid()
		{
			return UUID.RandomUUID().ToString();
		}

		/// <summary>Verify that the DatanodeUuid has been initialized.</summary>
		/// <remarks>
		/// Verify that the DatanodeUuid has been initialized. If this is a new
		/// datanode then we generate a new Datanode Uuid and persist it to disk.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void CheckDatanodeUuid()
		{
			lock (this)
			{
				if (storage.GetDatanodeUuid() == null)
				{
					storage.SetDatanodeUuid(GenerateUuid());
					storage.WriteAll();
					Log.Info("Generated and persisted new Datanode UUID " + storage.GetDatanodeUuid()
						);
				}
			}
		}

		/// <summary>Create a DatanodeRegistration for a specific block pool.</summary>
		/// <param name="nsInfo">the namespace info from the first part of the NN handshake</param>
		internal virtual DatanodeRegistration CreateBPRegistration(NamespaceInfo nsInfo)
		{
			StorageInfo storageInfo = storage.GetBPStorage(nsInfo.GetBlockPoolID());
			if (storageInfo == null)
			{
				// it's null in the case of SimulatedDataSet
				storageInfo = new StorageInfo(DataNodeLayoutVersion.CurrentLayoutVersion, nsInfo.
					GetNamespaceID(), nsInfo.clusterID, nsInfo.GetCTime(), HdfsServerConstants.NodeType
					.DataNode);
			}
			DatanodeID dnId = new DatanodeID(streamingAddr.Address.GetHostAddress(), hostName
				, storage.GetDatanodeUuid(), GetXferPort(), GetInfoPort(), infoSecurePort, GetIpcPort
				());
			return new DatanodeRegistration(dnId, storageInfo, new ExportedBlockKeys(), VersionInfo
				.GetVersion());
		}

		/// <summary>
		/// Check that the registration returned from a NameNode is consistent
		/// with the information in the storage.
		/// </summary>
		/// <remarks>
		/// Check that the registration returned from a NameNode is consistent
		/// with the information in the storage. If the storage is fresh/unformatted,
		/// sets the storage ID based on this registration.
		/// Also updates the block pool's state in the secret manager.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void BpRegistrationSucceeded(DatanodeRegistration bpRegistration
			, string blockPoolId)
		{
			lock (this)
			{
				id = bpRegistration;
				if (!storage.GetDatanodeUuid().Equals(bpRegistration.GetDatanodeUuid()))
				{
					throw new IOException("Inconsistent Datanode IDs. Name-node returned " + bpRegistration
						.GetDatanodeUuid() + ". Expecting " + storage.GetDatanodeUuid());
				}
				RegisterBlockPoolWithSecretManager(bpRegistration, blockPoolId);
			}
		}

		/// <summary>
		/// After the block pool has contacted the NN, registers that block pool
		/// with the secret manager, updating it with the secrets provided by the NN.
		/// </summary>
		/// <exception cref="System.IO.IOException">on error</exception>
		private void RegisterBlockPoolWithSecretManager(DatanodeRegistration bpRegistration
			, string blockPoolId)
		{
			lock (this)
			{
				ExportedBlockKeys keys = bpRegistration.GetExportedKeys();
				if (!hasAnyBlockPoolRegistered)
				{
					hasAnyBlockPoolRegistered = true;
					isBlockTokenEnabled = keys.IsBlockTokenEnabled();
				}
				else
				{
					if (isBlockTokenEnabled != keys.IsBlockTokenEnabled())
					{
						throw new RuntimeException("Inconsistent configuration of block access" + " tokens. Either all block pools must be configured to use block"
							 + " tokens, or none may be.");
					}
				}
				if (!isBlockTokenEnabled)
				{
					return;
				}
				if (!blockPoolTokenSecretManager.IsBlockPoolRegistered(blockPoolId))
				{
					long blockKeyUpdateInterval = keys.GetKeyUpdateInterval();
					long blockTokenLifetime = keys.GetTokenLifetime();
					Log.Info("Block token params received from NN: for block pool " + blockPoolId + " keyUpdateInterval="
						 + blockKeyUpdateInterval / (60 * 1000) + " min(s), tokenLifetime=" + blockTokenLifetime
						 / (60 * 1000) + " min(s)");
					BlockTokenSecretManager secretMgr = new BlockTokenSecretManager(0, blockTokenLifetime
						, blockPoolId, dnConf.encryptionAlgorithm);
					blockPoolTokenSecretManager.AddBlockPool(blockPoolId, secretMgr);
				}
			}
		}

		/// <summary>Remove the given block pool from the block scanner, dataset, and storage.
		/// 	</summary>
		internal virtual void ShutdownBlockPool(BPOfferService bpos)
		{
			blockPoolManager.Remove(bpos);
			if (bpos.HasBlockPoolId())
			{
				// Possible that this is shutting down before successfully
				// registering anywhere. If that's the case, we wouldn't have
				// a block pool id
				string bpId = bpos.GetBlockPoolId();
				blockScanner.DisableBlockPoolId(bpId);
				if (data != null)
				{
					data.ShutdownBlockPool(bpId);
				}
				if (storage != null)
				{
					storage.RemoveBlockPoolStorage(bpId);
				}
			}
		}

		/// <summary>One of the Block Pools has successfully connected to its NN.</summary>
		/// <remarks>
		/// One of the Block Pools has successfully connected to its NN.
		/// This initializes the local storage for that block pool,
		/// checks consistency of the NN's cluster ID, etc.
		/// If this is the first block pool to register, this also initializes
		/// the datanode-scoped storage.
		/// </remarks>
		/// <param name="bpos">Block pool offer service</param>
		/// <exception cref="System.IO.IOException">if the NN is inconsistent with the local storage.
		/// 	</exception>
		internal virtual void InitBlockPool(BPOfferService bpos)
		{
			NamespaceInfo nsInfo = bpos.GetNamespaceInfo();
			if (nsInfo == null)
			{
				throw new IOException("NamespaceInfo not found: Block pool " + bpos + " should have retrieved namespace info before initBlockPool."
					);
			}
			SetClusterId(nsInfo.clusterID, nsInfo.GetBlockPoolID());
			// Register the new block pool with the BP manager.
			blockPoolManager.AddBlockPool(bpos);
			// In the case that this is the first block pool to connect, initialize
			// the dataset, block scanners, etc.
			InitStorage(nsInfo);
			// Exclude failed disks before initializing the block pools to avoid startup
			// failures.
			CheckDiskError();
			data.AddBlockPool(nsInfo.GetBlockPoolID(), conf);
			blockScanner.EnableBlockPoolId(bpos.GetBlockPoolId());
			InitDirectoryScanner(conf);
		}

		internal virtual BPOfferService[] GetAllBpOs()
		{
			return blockPoolManager.GetAllNamenodeThreads();
		}

		internal virtual int GetBpOsCount()
		{
			return blockPoolManager.GetAllNamenodeThreads().Length;
		}

		/// <summary>
		/// Initializes the
		/// <see cref="data"/>
		/// . The initialization is done only once, when
		/// handshake with the the first namenode is completed.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void InitStorage(NamespaceInfo nsInfo)
		{
			FsDatasetSpi.Factory<FsDatasetSpi<object>> factory = FsDatasetSpi.Factory.GetFactory
				(conf);
			if (!factory.IsSimulated())
			{
				HdfsServerConstants.StartupOption startOpt = GetStartupOption(conf);
				if (startOpt == null)
				{
					throw new IOException("Startup option not set.");
				}
				string bpid = nsInfo.GetBlockPoolID();
				//read storage info, lock data dirs and transition fs state if necessary
				lock (this)
				{
					storage.RecoverTransitionRead(this, nsInfo, dataDirs, startOpt);
				}
				StorageInfo bpStorage = storage.GetBPStorage(bpid);
				Log.Info("Setting up storage: nsid=" + bpStorage.GetNamespaceID() + ";bpid=" + bpid
					 + ";lv=" + storage.GetLayoutVersion() + ";nsInfo=" + nsInfo + ";dnuuid=" + storage
					.GetDatanodeUuid());
			}
			// If this is a newly formatted DataNode then assign a new DatanodeUuid.
			CheckDatanodeUuid();
			lock (this)
			{
				if (data == null)
				{
					data = factory.NewInstance(this, storage, conf);
				}
			}
		}

		/// <summary>Determine the http server's effective addr</summary>
		public static IPEndPoint GetInfoAddr(Configuration conf)
		{
			return NetUtils.CreateSocketAddr(conf.GetTrimmed(DFSConfigKeys.DfsDatanodeHttpAddressKey
				, DFSConfigKeys.DfsDatanodeHttpAddressDefault));
		}

		private void RegisterMXBean()
		{
			dataNodeInfoBeanName = MBeans.Register("DataNode", "DataNodeInfo", this);
		}

		[VisibleForTesting]
		public virtual DataXceiverServer GetXferServer()
		{
			return xserver;
		}

		[VisibleForTesting]
		public virtual int GetXferPort()
		{
			return streamingAddr.Port;
		}

		/// <returns>name useful for logging</returns>
		public virtual string GetDisplayName()
		{
			// NB: our DatanodeID may not be set yet
			return hostName + ":" + GetXferPort();
		}

		/// <summary>
		/// NB: The datanode can perform data transfer on the streaming
		/// address however clients are given the IPC IP address for data
		/// transfer, and that may be a different address.
		/// </summary>
		/// <returns>socket address for data transfer</returns>
		public virtual IPEndPoint GetXferAddress()
		{
			return streamingAddr;
		}

		/// <returns>the datanode's IPC port</returns>
		public virtual int GetIpcPort()
		{
			return ipcServer.GetListenerAddress().Port;
		}

		/// <summary>get BP registration by blockPool id</summary>
		/// <returns>BP registration object</returns>
		/// <exception cref="System.IO.IOException">on error</exception>
		[VisibleForTesting]
		public virtual DatanodeRegistration GetDNRegistrationForBP(string bpid)
		{
			DataNodeFaultInjector.Get().NoRegistration();
			BPOfferService bpos = blockPoolManager.Get(bpid);
			if (bpos == null || bpos.bpRegistration == null)
			{
				throw new IOException("cannot find BPOfferService for bpid=" + bpid);
			}
			return bpos.bpRegistration;
		}

		/// <summary>Creates either NIO or regular depending on socketWriteTimeout.</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual Socket NewSocket()
		{
			return (dnConf.socketWriteTimeout > 0) ? SocketChannel.Open().Socket() : new Socket
				();
		}

		/// <summary>Connect to the NN.</summary>
		/// <remarks>Connect to the NN. This is separated out for easier testing.</remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual DatanodeProtocolClientSideTranslatorPB ConnectToNN(IPEndPoint nnAddr
			)
		{
			return new DatanodeProtocolClientSideTranslatorPB(nnAddr, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public static InterDatanodeProtocol CreateInterDataNodeProtocolProxy(DatanodeID datanodeid
			, Configuration conf, int socketTimeout, bool connectToDnViaHostname)
		{
			string dnAddr = datanodeid.GetIpcAddr(connectToDnViaHostname);
			IPEndPoint addr = NetUtils.CreateSocketAddr(dnAddr);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Connecting to datanode " + dnAddr + " addr=" + addr);
			}
			UserGroupInformation loginUgi = UserGroupInformation.GetLoginUser();
			try
			{
				return loginUgi.DoAs(new _PrivilegedExceptionAction_1467(addr, loginUgi, conf, socketTimeout
					));
			}
			catch (Exception ie)
			{
				throw new IOException(ie.Message);
			}
		}

		private sealed class _PrivilegedExceptionAction_1467 : PrivilegedExceptionAction<
			InterDatanodeProtocol>
		{
			public _PrivilegedExceptionAction_1467(IPEndPoint addr, UserGroupInformation loginUgi
				, Configuration conf, int socketTimeout)
			{
				this.addr = addr;
				this.loginUgi = loginUgi;
				this.conf = conf;
				this.socketTimeout = socketTimeout;
			}

			/// <exception cref="System.IO.IOException"/>
			public InterDatanodeProtocol Run()
			{
				return new InterDatanodeProtocolTranslatorPB(addr, loginUgi, conf, NetUtils.GetDefaultSocketFactory
					(conf), socketTimeout);
			}

			private readonly IPEndPoint addr;

			private readonly UserGroupInformation loginUgi;

			private readonly Configuration conf;

			private readonly int socketTimeout;
		}

		public virtual DataNodeMetrics GetMetrics()
		{
			return metrics;
		}

		/// <summary>Ensure the authentication method is kerberos</summary>
		/// <exception cref="System.IO.IOException"/>
		private void CheckKerberosAuthMethod(string msg)
		{
			// User invoking the call must be same as the datanode user
			if (!UserGroupInformation.IsSecurityEnabled())
			{
				return;
			}
			if (UserGroupInformation.GetCurrentUser().GetAuthenticationMethod() != UserGroupInformation.AuthenticationMethod
				.Kerberos)
			{
				throw new AccessControlException("Error in " + msg + "Only kerberos based authentication is allowed."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckBlockLocalPathAccess()
		{
			CheckKerberosAuthMethod("getBlockLocalPathInfo()");
			string currentUser = UserGroupInformation.GetCurrentUser().GetShortUserName();
			if (!usersWithLocalPathAccess.Contains(currentUser))
			{
				throw new AccessControlException("Can't continue with getBlockLocalPathInfo() " +
					 "authorization. The user " + currentUser + " is not allowed to call getBlockLocalPathInfo"
					);
			}
		}

		public virtual long GetMaxNumberOfBlocksToLog()
		{
			return maxNumberOfBlocksToLog;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual BlockLocalPathInfo GetBlockLocalPathInfo(ExtendedBlock block, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> token)
		{
			CheckBlockLocalPathAccess();
			CheckBlockToken(block, token, BlockTokenSecretManager.AccessMode.Read);
			Preconditions.CheckNotNull(data, "Storage not yet initialized");
			BlockLocalPathInfo info = data.GetBlockLocalPathInfo(block);
			if (Log.IsDebugEnabled())
			{
				if (info != null)
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace("getBlockLocalPathInfo successful block=" + block + " blockfile " + info
							.GetBlockPath() + " metafile " + info.GetMetaPath());
					}
				}
				else
				{
					if (Log.IsTraceEnabled())
					{
						Log.Trace("getBlockLocalPathInfo for block=" + block + " returning null");
					}
				}
			}
			metrics.IncrBlocksGetLocalPathInfo();
			return info;
		}

		[System.Serializable]
		public class ShortCircuitFdsUnsupportedException : IOException
		{
			private const long serialVersionUID = 1L;

			public ShortCircuitFdsUnsupportedException(string msg)
				: base(msg)
			{
			}
		}

		[System.Serializable]
		public class ShortCircuitFdsVersionException : IOException
		{
			private const long serialVersionUID = 1L;

			public ShortCircuitFdsVersionException(string msg)
				: base(msg)
			{
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.DataNode.ShortCircuitFdsUnsupportedException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.DataNode.ShortCircuitFdsVersionException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FileInputStream[] RequestShortCircuitFdsForRead(ExtendedBlock blk
			, Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token, int maxVersion
			)
		{
			if (fileDescriptorPassingDisabledReason != null)
			{
				throw new DataNode.ShortCircuitFdsUnsupportedException(fileDescriptorPassingDisabledReason
					);
			}
			int blkVersion = CurrentBlockFormatVersion;
			if (maxVersion < blkVersion)
			{
				throw new DataNode.ShortCircuitFdsVersionException("Your client is too old " + "to read this block!  Its format version is "
					 + blkVersion + ", but the highest format version you can read is " + maxVersion
					);
			}
			metrics.IncrBlocksGetLocalPathInfo();
			FileInputStream[] fis = new FileInputStream[2];
			try
			{
				fis[0] = (FileInputStream)data.GetBlockInputStream(blk, 0);
				fis[1] = DatanodeUtil.GetMetaDataInputStream(blk, data);
			}
			catch (InvalidCastException e)
			{
				Log.Debug("requestShortCircuitFdsForRead failed", e);
				throw new DataNode.ShortCircuitFdsUnsupportedException("This DataNode's " + "FsDatasetSpi does not support short-circuit local reads"
					);
			}
			return fis;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.NotSupportedException"/>
		public virtual HdfsBlocksMetadata GetHdfsBlocksMetadata(string bpId, long[] blockIds
			, IList<Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>> tokens)
		{
			if (!getHdfsBlockLocationsEnabled)
			{
				throw new NotSupportedException("Datanode#getHdfsBlocksMetadata " + " is not enabled in datanode config"
					);
			}
			if (blockIds.Length != tokens.Count)
			{
				throw new IOException("Differing number of blocks and tokens");
			}
			// Check access for each block
			for (int i = 0; i < blockIds.Length; i++)
			{
				CheckBlockToken(new ExtendedBlock(bpId, blockIds[i]), tokens[i], BlockTokenSecretManager.AccessMode
					.Read);
			}
			DataNodeFaultInjector.Get().GetHdfsBlocksMetadata();
			return data.GetHdfsBlocksMetadata(bpId, blockIds);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckBlockToken(ExtendedBlock block, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> token, BlockTokenSecretManager.AccessMode accessMode)
		{
			if (isBlockTokenEnabled)
			{
				BlockTokenIdentifier id = new BlockTokenIdentifier();
				ByteArrayInputStream buf = new ByteArrayInputStream(token.GetIdentifier());
				DataInputStream @in = new DataInputStream(buf);
				id.ReadFields(@in);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Got: " + id.ToString());
				}
				blockPoolTokenSecretManager.CheckAccess(id, null, block, accessMode);
			}
		}

		/// <summary>Shut down this instance of the datanode.</summary>
		/// <remarks>
		/// Shut down this instance of the datanode.
		/// Returns only after shutdown is complete.
		/// This method can only be called by the offerService thread.
		/// Otherwise, deadlock might occur.
		/// </remarks>
		public virtual void Shutdown()
		{
			if (plugins != null)
			{
				foreach (ServicePlugin p in plugins)
				{
					try
					{
						p.Stop();
						Log.Info("Stopped plug-in " + p);
					}
					catch (Exception t)
					{
						Log.Warn("ServicePlugin " + p + " could not be stopped", t);
					}
				}
			}
			// We need to make a copy of the original blockPoolManager#offerServices to
			// make sure blockPoolManager#shutDownAll() can still access all the 
			// BPOfferServices, since after setting DataNode#shouldRun to false the 
			// offerServices may be modified.
			BPOfferService[] bposArray = this.blockPoolManager == null ? null : this.blockPoolManager
				.GetAllNamenodeThreads();
			// If shutdown is not for restart, set shouldRun to false early. 
			if (!shutdownForUpgrade)
			{
				shouldRun = false;
			}
			// When shutting down for restart, DataXceiverServer is interrupted
			// in order to avoid any further acceptance of requests, but the peers
			// for block writes are not closed until the clients are notified.
			if (dataXceiverServer != null)
			{
				try
				{
					xserver.SendOOBToPeers();
					((DataXceiverServer)this.dataXceiverServer.GetRunnable()).Kill();
					this.dataXceiverServer.Interrupt();
				}
				catch
				{
				}
			}
			// Ignore, since the out of band messaging is advisory.
			// Interrupt the checkDiskErrorThread and terminate it.
			if (this.checkDiskErrorThread != null)
			{
				this.checkDiskErrorThread.Interrupt();
			}
			// Record the time of initial notification
			long timeNotified = Time.MonotonicNow();
			if (localDataXceiverServer != null)
			{
				((DataXceiverServer)this.localDataXceiverServer.GetRunnable()).Kill();
				this.localDataXceiverServer.Interrupt();
			}
			// Terminate directory scanner and block scanner
			ShutdownPeriodicScanners();
			// Stop the web server
			if (httpServer != null)
			{
				try
				{
					httpServer.Close();
				}
				catch (Exception e)
				{
					Log.Warn("Exception shutting down DataNode HttpServer", e);
				}
			}
			if (pauseMonitor != null)
			{
				pauseMonitor.Stop();
			}
			// shouldRun is set to false here to prevent certain threads from exiting
			// before the restart prep is done.
			this.shouldRun = false;
			// wait reconfiguration thread, if any, to exit
			ShutdownReconfigurationTask();
			// wait for all data receiver threads to exit
			if (this.threadGroup != null)
			{
				int sleepMs = 2;
				while (true)
				{
					// When shutting down for restart, wait 2.5 seconds before forcing
					// termination of receiver threads.
					if (!this.shutdownForUpgrade || (this.shutdownForUpgrade && (Time.MonotonicNow() 
						- timeNotified > 1000)))
					{
						this.threadGroup.Interrupt();
						break;
					}
					Log.Info("Waiting for threadgroup to exit, active threads is " + this.threadGroup
						.ActiveCount());
					if (this.threadGroup.ActiveCount() == 0)
					{
						break;
					}
					try
					{
						Sharpen.Thread.Sleep(sleepMs);
					}
					catch (Exception)
					{
					}
					sleepMs = sleepMs * 3 / 2;
					// exponential backoff
					if (sleepMs > 200)
					{
						sleepMs = 200;
					}
				}
				this.threadGroup = null;
			}
			if (this.dataXceiverServer != null)
			{
				// wait for dataXceiverServer to terminate
				try
				{
					this.dataXceiverServer.Join();
				}
				catch (Exception)
				{
				}
			}
			if (this.localDataXceiverServer != null)
			{
				// wait for localDataXceiverServer to terminate
				try
				{
					this.localDataXceiverServer.Join();
				}
				catch (Exception)
				{
				}
			}
			// IPC server needs to be shutdown late in the process, otherwise
			// shutdown command response won't get sent.
			if (ipcServer != null)
			{
				ipcServer.Stop();
			}
			if (blockPoolManager != null)
			{
				try
				{
					this.blockPoolManager.ShutDownAll(bposArray);
				}
				catch (Exception ie)
				{
					Log.Warn("Received exception in BlockPoolManager#shutDownAll: ", ie);
				}
			}
			if (storage != null)
			{
				try
				{
					this.storage.UnlockAll();
				}
				catch (IOException ie)
				{
					Log.Warn("Exception when unlocking storage: " + ie, ie);
				}
			}
			if (data != null)
			{
				data.Shutdown();
			}
			if (metrics != null)
			{
				metrics.Shutdown();
			}
			if (dataNodeInfoBeanName != null)
			{
				MBeans.Unregister(dataNodeInfoBeanName);
				dataNodeInfoBeanName = null;
			}
			if (this.spanReceiverHost != null)
			{
				this.spanReceiverHost.CloseReceivers();
			}
			if (shortCircuitRegistry != null)
			{
				shortCircuitRegistry.Shutdown();
			}
			Log.Info("Shutdown complete.");
			lock (this)
			{
				// it is already false, but setting it again to avoid a findbug warning.
				this.shouldRun = false;
				// Notify the main thread.
				Sharpen.Runtime.NotifyAll(this);
			}
		}

		/// <summary>Check if there is a disk failure asynchronously and if so, handle the error
		/// 	</summary>
		public virtual void CheckDiskErrorAsync()
		{
			lock (checkDiskErrorMutex)
			{
				checkDiskErrorFlag = true;
				if (checkDiskErrorThread == null)
				{
					StartCheckDiskErrorThread();
					checkDiskErrorThread.Start();
					Log.Info("Starting CheckDiskError Thread");
				}
			}
		}

		private void HandleDiskError(string errMsgr)
		{
			bool hasEnoughResources = data.HasEnoughResource();
			Log.Warn("DataNode.handleDiskError: Keep Running: " + hasEnoughResources);
			// If we have enough active valid volumes then we do not want to 
			// shutdown the DN completely.
			int dpError = hasEnoughResources ? DatanodeProtocol.DiskError : DatanodeProtocol.
				FatalDiskError;
			metrics.IncrVolumeFailures();
			//inform NameNodes
			foreach (BPOfferService bpos in blockPoolManager.GetAllNamenodeThreads())
			{
				bpos.TrySendErrorReport(dpError, errMsgr);
			}
			if (hasEnoughResources)
			{
				ScheduleAllBlockReport(0);
				return;
			}
			// do not shutdown
			Log.Warn("DataNode is shutting down: " + errMsgr);
			shouldRun = false;
		}

		/// <summary>Number of concurrent xceivers per node.</summary>
		public virtual int GetXceiverCount()
		{
			// DataNodeMXBean
			return threadGroup == null ? 0 : threadGroup.ActiveCount();
		}

		public virtual IDictionary<string, IDictionary<string, long>> GetDatanodeNetworkCounts
			()
		{
			// DataNodeMXBean
			return datanodeNetworkCounts.AsMap();
		}

		internal virtual void IncrDatanodeNetworkErrors(string host)
		{
			metrics.IncrDatanodeNetworkErrors();
			/*
			* Synchronizing on the whole cache is a big hammer, but since it's only
			* accumulating errors, it should be ok. If this is ever expanded to include
			* non-error stats, then finer-grained concurrency should be applied.
			*/
			lock (datanodeNetworkCounts)
			{
				try
				{
					IDictionary<string, long> curCount = datanodeNetworkCounts.Get(host);
					curCount["networkErrors"] = curCount["networkErrors"] + 1L;
					datanodeNetworkCounts.Put(host, curCount);
				}
				catch (ExecutionException)
				{
					Log.Warn("failed to increment network error counts for " + host);
				}
			}
		}

		internal virtual int GetXmitsInProgress()
		{
			return xmitsInProgress.Get();
		}

		private void ReportBadBlock(BPOfferService bpos, ExtendedBlock block, string msg)
		{
			FsVolumeSpi volume = GetFSDataset().GetVolume(block);
			bpos.ReportBadBlocks(block, volume.GetStorageID(), volume.GetStorageType());
			Log.Warn(msg);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TransferBlock(ExtendedBlock block, DatanodeInfo[] xferTargets, StorageType
			[] xferTargetStorageTypes)
		{
			BPOfferService bpos = GetBPOSForBlock(block);
			DatanodeRegistration bpReg = GetDNRegistrationForBP(block.GetBlockPoolId());
			bool replicaNotExist = false;
			bool replicaStateNotFinalized = false;
			bool blockFileNotExist = false;
			bool lengthTooShort = false;
			try
			{
				data.CheckBlock(block, block.GetNumBytes(), HdfsServerConstants.ReplicaState.Finalized
					);
			}
			catch (ReplicaNotFoundException)
			{
				replicaNotExist = true;
			}
			catch (UnexpectedReplicaStateException)
			{
				replicaStateNotFinalized = true;
			}
			catch (FileNotFoundException)
			{
				blockFileNotExist = true;
			}
			catch (EOFException)
			{
				lengthTooShort = true;
			}
			catch (IOException)
			{
				// The IOException indicates not being able to access block file,
				// treat it the same here as blockFileNotExist, to trigger 
				// reporting it as a bad block
				blockFileNotExist = true;
			}
			if (replicaNotExist || replicaStateNotFinalized)
			{
				string errStr = "Can't send invalid block " + block;
				Log.Info(errStr);
				bpos.TrySendErrorReport(DatanodeProtocol.InvalidBlock, errStr);
				return;
			}
			if (blockFileNotExist)
			{
				// Report back to NN bad block caused by non-existent block file.
				ReportBadBlock(bpos, block, "Can't replicate block " + block + " because the block file doesn't exist, or is not accessible"
					);
				return;
			}
			if (lengthTooShort)
			{
				// Check if NN recorded length matches on-disk length 
				// Shorter on-disk len indicates corruption so report NN the corrupt block
				ReportBadBlock(bpos, block, "Can't replicate block " + block + " because on-disk length "
					 + data.GetLength(block) + " is shorter than NameNode recorded length " + block.
					GetNumBytes());
				return;
			}
			int numTargets = xferTargets.Length;
			if (numTargets > 0)
			{
				StringBuilder xfersBuilder = new StringBuilder();
				for (int i = 0; i < numTargets; i++)
				{
					xfersBuilder.Append(xferTargets[i]);
					xfersBuilder.Append(" ");
				}
				Log.Info(bpReg + " Starting thread to transfer " + block + " to " + xfersBuilder);
				new Daemon(new DataNode.DataTransfer(this, xferTargets, xferTargetStorageTypes, block
					, BlockConstructionStage.PipelineSetupCreate, string.Empty)).Start();
			}
		}

		internal virtual void TransferBlocks(string poolId, Org.Apache.Hadoop.Hdfs.Protocol.Block
			[] blocks, DatanodeInfo[][] xferTargets, StorageType[][] xferTargetStorageTypes)
		{
			for (int i = 0; i < blocks.Length; i++)
			{
				try
				{
					TransferBlock(new ExtendedBlock(poolId, blocks[i]), xferTargets[i], xferTargetStorageTypes
						[i]);
				}
				catch (IOException ie)
				{
					Log.Warn("Failed to transfer block " + blocks[i], ie);
				}
			}
		}

		/// <summary>Used for transferring a block of data.</summary>
		/// <remarks>
		/// Used for transferring a block of data.  This class
		/// sends a piece of data to another DataNode.
		/// </remarks>
		private class DataTransfer : Runnable
		{
			internal readonly DatanodeInfo[] targets;

			internal readonly StorageType[] targetStorageTypes;

			internal readonly ExtendedBlock b;

			internal readonly BlockConstructionStage stage;

			private readonly DatanodeRegistration bpReg;

			internal readonly string clientname;

			internal readonly CachingStrategy cachingStrategy;

			/// <summary>Connect to the first item in the target list.</summary>
			/// <remarks>
			/// Connect to the first item in the target list.  Pass along the
			/// entire target list, the block, and the data.
			/// </remarks>
			internal DataTransfer(DataNode _enclosing, DatanodeInfo[] targets, StorageType[] 
				targetStorageTypes, ExtendedBlock b, BlockConstructionStage stage, string clientname
				)
			{
				this._enclosing = _enclosing;
				/* ********************************************************************
				Protocol when a client reads data from Datanode (Cur Ver: 9):
				
				Client's Request :
				=================
				
				Processed in DataXceiver:
				+----------------------------------------------+
				| Common Header   | 1 byte OP == OP_READ_BLOCK |
				+----------------------------------------------+
				
				Processed in readBlock() :
				+-------------------------------------------------------------------------+
				| 8 byte Block ID | 8 byte genstamp | 8 byte start offset | 8 byte length |
				+-------------------------------------------------------------------------+
				|   vInt length   |  <DFSClient id> |
				+-----------------------------------+
				
				Client sends optional response only at the end of receiving data.
				
				DataNode Response :
				===================
				
				In readBlock() :
				If there is an error while initializing BlockSender :
				+---------------------------+
				| 2 byte OP_STATUS_ERROR    | and connection will be closed.
				+---------------------------+
				Otherwise
				+---------------------------+
				| 2 byte OP_STATUS_SUCCESS  |
				+---------------------------+
				
				Actual data, sent by BlockSender.sendBlock() :
				
				ChecksumHeader :
				+--------------------------------------------------+
				| 1 byte CHECKSUM_TYPE | 4 byte BYTES_PER_CHECKSUM |
				+--------------------------------------------------+
				Followed by actual data in the form of PACKETS:
				+------------------------------------+
				| Sequence of data PACKETs ....      |
				+------------------------------------+
				
				A "PACKET" is defined further below.
				
				The client reads data until it receives a packet with
				"LastPacketInBlock" set to true or with a zero length. It then replies
				to DataNode with one of the status codes:
				- CHECKSUM_OK:    All the chunk checksums have been verified
				- SUCCESS:        Data received; checksums not verified
				- ERROR_CHECKSUM: (Currently not used) Detected invalid checksums
				
				+---------------+
				| 2 byte Status |
				+---------------+
				
				The DataNode expects all well behaved clients to send the 2 byte
				status code. And if the the client doesn't, the DN will close the
				connection. So the status code is optional in the sense that it
				does not affect the correctness of the data. (And the client can
				always reconnect.)
				
				PACKET : Contains a packet header, checksum and data. Amount of data
				======== carried is set by BUFFER_SIZE.
				
				+-----------------------------------------------------+
				| 4 byte packet length (excluding packet header)      |
				+-----------------------------------------------------+
				| 8 byte offset in the block | 8 byte sequence number |
				+-----------------------------------------------------+
				| 1 byte isLastPacketInBlock                          |
				+-----------------------------------------------------+
				| 4 byte Length of actual data                        |
				+-----------------------------------------------------+
				| x byte checksum data. x is defined below            |
				+-----------------------------------------------------+
				| actual data ......                                  |
				+-----------------------------------------------------+
				
				x = (length of data + BYTE_PER_CHECKSUM - 1)/BYTES_PER_CHECKSUM *
				CHECKSUM_SIZE
				
				CHECKSUM_SIZE depends on CHECKSUM_TYPE (usually, 4 for CRC32)
				
				The above packet format is used while writing data to DFS also.
				Not all the fields might be used while reading.
				
				************************************************************************ */
				if (DataTransferProtocol.Log.IsDebugEnabled())
				{
					DataTransferProtocol.Log.Debug(this.GetType().Name + ": " + b + " (numBytes=" + b
						.GetNumBytes() + ")" + ", stage=" + stage + ", clientname=" + clientname + ", targets="
						 + Arrays.AsList(targets) + ", target storage types=" + (targetStorageTypes == null
						 ? "[]" : Arrays.AsList(targetStorageTypes)));
				}
				this.targets = targets;
				this.targetStorageTypes = targetStorageTypes;
				this.b = b;
				this.stage = stage;
				BPOfferService bpos = this._enclosing.blockPoolManager.Get(b.GetBlockPoolId());
				this.bpReg = bpos.bpRegistration;
				this.clientname = clientname;
				this.cachingStrategy = new CachingStrategy(true, this._enclosing.GetDnConf().readaheadLength
					);
			}

			/// <summary>Do the deed, write the bytes</summary>
			public virtual void Run()
			{
				this._enclosing.xmitsInProgress.GetAndIncrement();
				Socket sock = null;
				DataOutputStream @out = null;
				DataInputStream @in = null;
				BlockSender blockSender = null;
				bool isClient = this.clientname.Length > 0;
				try
				{
					string dnAddr = this.targets[0].GetXferAddr(this._enclosing.connectToDnViaHostname
						);
					IPEndPoint curTarget = NetUtils.CreateSocketAddr(dnAddr);
					if (DataNode.Log.IsDebugEnabled())
					{
						DataNode.Log.Debug("Connecting to datanode " + dnAddr);
					}
					sock = this._enclosing.NewSocket();
					NetUtils.Connect(sock, curTarget, this._enclosing.dnConf.socketTimeout);
					sock.ReceiveTimeout = this.targets.Length * this._enclosing.dnConf.socketTimeout;
					//
					// Header info
					//
					Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> accessToken = BlockTokenSecretManager
						.DummyToken;
					if (this._enclosing.isBlockTokenEnabled)
					{
						accessToken = this._enclosing.blockPoolTokenSecretManager.GenerateToken(this.b, EnumSet
							.Of(BlockTokenSecretManager.AccessMode.Write));
					}
					long writeTimeout = this._enclosing.dnConf.socketWriteTimeout + HdfsServerConstants
						.WriteTimeoutExtension * (this.targets.Length - 1);
					OutputStream unbufOut = NetUtils.GetOutputStream(sock, writeTimeout);
					InputStream unbufIn = NetUtils.GetInputStream(sock);
					DataEncryptionKeyFactory keyFactory = this._enclosing.GetDataEncryptionKeyFactoryForBlock
						(this.b);
					IOStreamPair saslStreams = this._enclosing.saslClient.SocketSend(sock, unbufOut, 
						unbufIn, keyFactory, accessToken, this.bpReg);
					unbufOut = saslStreams.@out;
					unbufIn = saslStreams.@in;
					@out = new DataOutputStream(new BufferedOutputStream(unbufOut, HdfsConstants.SmallBufferSize
						));
					@in = new DataInputStream(unbufIn);
					blockSender = new BlockSender(this.b, 0, this.b.GetNumBytes(), false, false, true
						, this._enclosing, null, this.cachingStrategy);
					DatanodeInfo srcNode = new DatanodeInfo(this.bpReg);
					new Sender(@out).WriteBlock(this.b, this.targetStorageTypes[0], accessToken, this
						.clientname, this.targets, this.targetStorageTypes, srcNode, this.stage, 0, 0, 0
						, 0, blockSender.GetChecksum(), this.cachingStrategy, false, false, null);
					// send data & checksum
					blockSender.SendBlock(@out, unbufOut, null);
					// no response necessary
					DataNode.Log.Info(this.GetType().Name + ": Transmitted " + this.b + " (numBytes="
						 + this.b.GetNumBytes() + ") to " + curTarget);
					// read ack
					if (isClient)
					{
						DataTransferProtos.DNTransferAckProto closeAck = DataTransferProtos.DNTransferAckProto
							.ParseFrom(PBHelper.VintPrefixed(@in));
						if (DataNode.Log.IsDebugEnabled())
						{
							DataNode.Log.Debug(this.GetType().Name + ": close-ack=" + closeAck);
						}
						if (closeAck.GetStatus() != DataTransferProtos.Status.Success)
						{
							if (closeAck.GetStatus() == DataTransferProtos.Status.ErrorAccessToken)
							{
								throw new InvalidBlockTokenException("Got access token error for connect ack, targets="
									 + Arrays.AsList(this.targets));
							}
							else
							{
								throw new IOException("Bad connect ack, targets=" + Arrays.AsList(this.targets));
							}
						}
					}
				}
				catch (IOException ie)
				{
					DataNode.Log.Warn(this.bpReg + ":Failed to transfer " + this.b + " to " + this.targets
						[0] + " got ", ie);
					// check if there are any disk problem
					this._enclosing.CheckDiskErrorAsync();
				}
				finally
				{
					this._enclosing.xmitsInProgress.GetAndDecrement();
					IOUtils.CloseStream(blockSender);
					IOUtils.CloseStream(@out);
					IOUtils.CloseStream(@in);
					IOUtils.CloseSocket(sock);
				}
			}

			private readonly DataNode _enclosing;
		}

		/// <summary>
		/// Returns a new DataEncryptionKeyFactory that generates a key from the
		/// BlockPoolTokenSecretManager, using the block pool ID of the given block.
		/// </summary>
		/// <param name="block">for which the factory needs to create a key</param>
		/// <returns>DataEncryptionKeyFactory for block's block pool ID</returns>
		internal virtual DataEncryptionKeyFactory GetDataEncryptionKeyFactoryForBlock(ExtendedBlock
			 block)
		{
			return new _DataEncryptionKeyFactory_2171(this, block);
		}

		private sealed class _DataEncryptionKeyFactory_2171 : DataEncryptionKeyFactory
		{
			public _DataEncryptionKeyFactory_2171(DataNode _enclosing, ExtendedBlock block)
			{
				this._enclosing = _enclosing;
				this.block = block;
			}

			public DataEncryptionKey NewDataEncryptionKey()
			{
				return this._enclosing.dnConf.encryptDataTransfer ? this._enclosing.blockPoolTokenSecretManager
					.GenerateDataEncryptionKey(block.GetBlockPoolId()) : null;
			}

			private readonly DataNode _enclosing;

			private readonly ExtendedBlock block;
		}

		/// <summary>
		/// After a block becomes finalized, a datanode increases metric counter,
		/// notifies namenode, and adds it to the block scanner
		/// </summary>
		/// <param name="block">block to close</param>
		/// <param name="delHint">hint on which excess block to delete</param>
		/// <param name="storageUuid">UUID of the storage where block is stored</param>
		internal virtual void CloseBlock(ExtendedBlock block, string delHint, string storageUuid
			)
		{
			metrics.IncrBlocksWritten();
			BPOfferService bpos = blockPoolManager.Get(block.GetBlockPoolId());
			if (bpos != null)
			{
				bpos.NotifyNamenodeReceivedBlock(block, delHint, storageUuid);
			}
			else
			{
				Log.Warn("Cannot find BPOfferService for reporting block received for bpid=" + block
					.GetBlockPoolId());
			}
		}

		/// <summary>Start a single datanode daemon and wait for it to finish.</summary>
		/// <remarks>
		/// Start a single datanode daemon and wait for it to finish.
		/// If this thread is specifically interrupted, it will stop waiting.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void RunDatanodeDaemon()
		{
			blockPoolManager.StartAll();
			// start dataXceiveServer
			dataXceiverServer.Start();
			if (localDataXceiverServer != null)
			{
				localDataXceiverServer.Start();
			}
			ipcServer.Start();
			StartPlugins(conf);
		}

		/// <summary>A data node is considered to be up if one of the bp services is up</summary>
		public virtual bool IsDatanodeUp()
		{
			foreach (BPOfferService bp in blockPoolManager.GetAllNamenodeThreads())
			{
				if (bp.IsAlive())
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>Instantiate a single datanode object.</summary>
		/// <remarks>
		/// Instantiate a single datanode object. This must be run by invoking
		/// <see cref="RunDatanodeDaemon()"/>
		/// subsequently.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static DataNode InstantiateDataNode(string[] args, Configuration conf)
		{
			return InstantiateDataNode(args, conf, null);
		}

		/// <summary>Instantiate a single datanode object, along with its secure resources.</summary>
		/// <remarks>
		/// Instantiate a single datanode object, along with its secure resources.
		/// This must be run by invoking
		/// <see cref="RunDatanodeDaemon()"/>
		/// 
		/// subsequently.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static DataNode InstantiateDataNode(string[] args, Configuration conf, SecureDataNodeStarter.SecureResources
			 resources)
		{
			if (conf == null)
			{
				conf = new HdfsConfiguration();
			}
			if (args != null)
			{
				// parse generic hadoop options
				GenericOptionsParser hParser = new GenericOptionsParser(conf, args);
				args = hParser.GetRemainingArgs();
			}
			if (!ParseArguments(args, conf))
			{
				PrintUsage(System.Console.Error);
				return null;
			}
			ICollection<StorageLocation> dataLocations = GetStorageLocations(conf);
			UserGroupInformation.SetConfiguration(conf);
			SecurityUtil.Login(conf, DFSConfigKeys.DfsDatanodeKeytabFileKey, DFSConfigKeys.DfsDatanodeKerberosPrincipalKey
				);
			return MakeInstance(dataLocations, conf, resources);
		}

		public static IList<StorageLocation> GetStorageLocations(Configuration conf)
		{
			ICollection<string> rawLocations = conf.GetTrimmedStringCollection(DFSConfigKeys.
				DfsDatanodeDataDirKey);
			IList<StorageLocation> locations = new AList<StorageLocation>(rawLocations.Count);
			foreach (string locationString in rawLocations)
			{
				StorageLocation location;
				try
				{
					location = StorageLocation.Parse(locationString);
				}
				catch (IOException ioe)
				{
					Log.Error("Failed to initialize storage directory " + locationString + ". Exception details: "
						 + ioe);
					// Ignore the exception.
					continue;
				}
				catch (SecurityException se)
				{
					Log.Error("Failed to initialize storage directory " + locationString + ". Exception details: "
						 + se);
					// Ignore the exception.
					continue;
				}
				locations.AddItem(location);
			}
			return locations;
		}

		/// <summary>Instantiate & Start a single datanode daemon and wait for it to finish.</summary>
		/// <remarks>
		/// Instantiate & Start a single datanode daemon and wait for it to finish.
		/// If this thread is specifically interrupted, it will stop waiting.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public static DataNode CreateDataNode(string[] args, Configuration conf)
		{
			return CreateDataNode(args, conf, null);
		}

		/// <summary>Instantiate & Start a single datanode daemon and wait for it to finish.</summary>
		/// <remarks>
		/// Instantiate & Start a single datanode daemon and wait for it to finish.
		/// If this thread is specifically interrupted, it will stop waiting.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		[InterfaceAudience.Private]
		public static DataNode CreateDataNode(string[] args, Configuration conf, SecureDataNodeStarter.SecureResources
			 resources)
		{
			DataNode dn = InstantiateDataNode(args, conf, resources);
			if (dn != null)
			{
				dn.RunDatanodeDaemon();
			}
			return dn;
		}

		internal virtual void Join()
		{
			while (shouldRun)
			{
				try
				{
					blockPoolManager.JoinAll();
					if (blockPoolManager.GetAllNamenodeThreads() != null && blockPoolManager.GetAllNamenodeThreads
						().Length == 0)
					{
						shouldRun = false;
					}
					// Terminate if shutdown is complete or 2 seconds after all BPs
					// are shutdown.
					lock (this)
					{
						Sharpen.Runtime.Wait(this, 2000);
					}
				}
				catch (Exception ex)
				{
					Log.Warn("Received exception in Datanode#join: " + ex);
				}
			}
		}

		internal class DataNodeDiskChecker
		{
			private readonly FsPermission expectedPermission;

			public DataNodeDiskChecker(FsPermission expectedPermission)
			{
				// Small wrapper around the DiskChecker class that provides means to mock
				// DiskChecker static methods and unittest DataNode#getDataDirsFromURIs.
				this.expectedPermission = expectedPermission;
			}

			/// <exception cref="Org.Apache.Hadoop.Util.DiskChecker.DiskErrorException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void CheckDir(LocalFileSystem localFS, Path path)
			{
				DiskChecker.CheckDir(localFS, path, expectedPermission);
			}
		}

		/// <summary>
		/// Make an instance of DataNode after ensuring that at least one of the
		/// given data directories (and their parent directories, if necessary)
		/// can be created.
		/// </summary>
		/// <param name="dataDirs">
		/// List of directories, where the new DataNode instance should
		/// keep its files.
		/// </param>
		/// <param name="conf">Configuration instance to use.</param>
		/// <param name="resources">Secure resources needed to run under Kerberos</param>
		/// <returns>
		/// DataNode instance for given list of data dirs and conf, or null if
		/// no directory from this directory list can be created.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal static DataNode MakeInstance(ICollection<StorageLocation> dataDirs, Configuration
			 conf, SecureDataNodeStarter.SecureResources resources)
		{
			LocalFileSystem localFS = FileSystem.GetLocal(conf);
			FsPermission permission = new FsPermission(conf.Get(DFSConfigKeys.DfsDatanodeDataDirPermissionKey
				, DFSConfigKeys.DfsDatanodeDataDirPermissionDefault));
			DataNode.DataNodeDiskChecker dataNodeDiskChecker = new DataNode.DataNodeDiskChecker
				(permission);
			IList<StorageLocation> locations = CheckStorageLocations(dataDirs, localFS, dataNodeDiskChecker
				);
			DefaultMetricsSystem.Initialize("DataNode");
			System.Diagnostics.Debug.Assert(locations.Count > 0, "number of data directories should be > 0"
				);
			return new DataNode(conf, locations, resources);
		}

		// DataNode ctor expects AbstractList instead of List or Collection...
		/// <exception cref="System.IO.IOException"/>
		internal static IList<StorageLocation> CheckStorageLocations(ICollection<StorageLocation
			> dataDirs, LocalFileSystem localFS, DataNode.DataNodeDiskChecker dataNodeDiskChecker
			)
		{
			AList<StorageLocation> locations = new AList<StorageLocation>();
			StringBuilder invalidDirs = new StringBuilder();
			foreach (StorageLocation location in dataDirs)
			{
				URI uri = location.GetUri();
				try
				{
					dataNodeDiskChecker.CheckDir(localFS, new Path(uri));
					locations.AddItem(location);
				}
				catch (IOException ioe)
				{
					Log.Warn("Invalid " + DFSConfigKeys.DfsDatanodeDataDirKey + " " + location.GetFile
						() + " : ", ioe);
					invalidDirs.Append("\"").Append(uri.GetPath()).Append("\" ");
				}
			}
			if (locations.Count == 0)
			{
				throw new IOException("All directories in " + DFSConfigKeys.DfsDatanodeDataDirKey
					 + " are invalid: " + invalidDirs);
			}
			return locations;
		}

		public override string ToString()
		{
			return "DataNode{data=" + data + ", localName='" + GetDisplayName() + "', datanodeUuid='"
				 + storage.GetDatanodeUuid() + "', xmitsInProgress=" + xmitsInProgress.Get() + "}";
		}

		private static void PrintUsage(TextWriter @out)
		{
			@out.WriteLine(Usage + "\n");
		}

		/// <summary>Parse and verify command line arguments and set configuration parameters.
		/// 	</summary>
		/// <returns>false if passed argements are incorrect</returns>
		[VisibleForTesting]
		internal static bool ParseArguments(string[] args, Configuration conf)
		{
			HdfsServerConstants.StartupOption startOpt = HdfsServerConstants.StartupOption.Regular;
			int i = 0;
			if (args != null && args.Length != 0)
			{
				string cmd = args[i++];
				if (Sharpen.Runtime.EqualsIgnoreCase("-r", cmd) || Sharpen.Runtime.EqualsIgnoreCase
					("--rack", cmd))
				{
					Log.Error("-r, --rack arguments are not supported anymore. RackID " + "resolution is handled by the NameNode."
						);
					return false;
				}
				else
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Rollback.GetName
						(), cmd))
					{
						startOpt = HdfsServerConstants.StartupOption.Rollback;
					}
					else
					{
						if (Sharpen.Runtime.EqualsIgnoreCase(HdfsServerConstants.StartupOption.Regular.GetName
							(), cmd))
						{
							startOpt = HdfsServerConstants.StartupOption.Regular;
						}
						else
						{
							return false;
						}
					}
				}
			}
			SetStartupOption(conf, startOpt);
			return (args == null || i == args.Length);
		}

		// Fail if more than one cmd specified!
		private static void SetStartupOption(Configuration conf, HdfsServerConstants.StartupOption
			 opt)
		{
			conf.Set(DFSConfigKeys.DfsDatanodeStartupKey, opt.ToString());
		}

		internal static HdfsServerConstants.StartupOption GetStartupOption(Configuration 
			conf)
		{
			string value = conf.Get(DFSConfigKeys.DfsDatanodeStartupKey, HdfsServerConstants.StartupOption
				.Regular.ToString());
			return HdfsServerConstants.StartupOption.GetEnum(value);
		}

		/// <summary>
		/// This methods  arranges for the data node to send
		/// the block report at the next heartbeat.
		/// </summary>
		public virtual void ScheduleAllBlockReport(long delay)
		{
			foreach (BPOfferService bpos in blockPoolManager.GetAllNamenodeThreads())
			{
				bpos.ScheduleBlockReport(delay);
			}
		}

		/// <summary>Examples are adding and deleting blocks directly.</summary>
		/// <remarks>
		/// Examples are adding and deleting blocks directly.
		/// The most common usage will be when the data node's storage is simulated.
		/// </remarks>
		/// <returns>the fsdataset that stores the blocks</returns>
		[VisibleForTesting]
		public virtual FsDatasetSpi<object> GetFSDataset()
		{
			return data;
		}

		[VisibleForTesting]
		public virtual BlockScanner GetBlockScanner()
		{
			return blockScanner;
		}

		public static void SecureMain(string[] args, SecureDataNodeStarter.SecureResources
			 resources)
		{
			int errorCode = 0;
			try
			{
				StringUtils.StartupShutdownMessage(typeof(DataNode), args, Log);
				DataNode datanode = CreateDataNode(args, null, resources);
				if (datanode != null)
				{
					datanode.Join();
				}
				else
				{
					errorCode = 1;
				}
			}
			catch (Exception e)
			{
				Log.Fatal("Exception in secureMain", e);
				ExitUtil.Terminate(1, e);
			}
			finally
			{
				// We need to terminate the process here because either shutdown was called
				// or some disk related conditions like volumes tolerated or volumes required
				// condition was not met. Also, In secure mode, control will go to Jsvc
				// and Datanode process hangs if it does not exit.
				Log.Warn("Exiting Datanode");
				ExitUtil.Terminate(errorCode);
			}
		}

		public static void Main(string[] args)
		{
			if (DFSUtil.ParseHelpArgument(args, DataNode.Usage, System.Console.Out, true))
			{
				System.Environment.Exit(0);
			}
			SecureMain(args, null);
		}

		public virtual Daemon RecoverBlocks(string who, ICollection<BlockRecoveryCommand.RecoveringBlock
			> blocks)
		{
			Daemon d = new Daemon(threadGroup, new _Runnable_2512(this, blocks, who));
			d.Start();
			return d;
		}

		private sealed class _Runnable_2512 : Runnable
		{
			public _Runnable_2512(DataNode _enclosing, ICollection<BlockRecoveryCommand.RecoveringBlock
				> blocks, string who)
			{
				this._enclosing = _enclosing;
				this.blocks = blocks;
				this.who = who;
			}

			/// <summary>Recover a list of blocks.</summary>
			/// <remarks>Recover a list of blocks. It is run by the primary datanode.</remarks>
			public void Run()
			{
				foreach (BlockRecoveryCommand.RecoveringBlock b in blocks)
				{
					try
					{
						DataNode.LogRecoverBlock(who, b);
						this._enclosing.RecoverBlock(b);
					}
					catch (IOException e)
					{
						DataNode.Log.Warn("recoverBlocks FAILED: " + b, e);
					}
				}
			}

			private readonly DataNode _enclosing;

			private readonly ICollection<BlockRecoveryCommand.RecoveringBlock> blocks;

			private readonly string who;
		}

		// InterDataNodeProtocol implementation
		/// <exception cref="System.IO.IOException"/>
		public virtual ReplicaRecoveryInfo InitReplicaRecovery(BlockRecoveryCommand.RecoveringBlock
			 rBlock)
		{
			// InterDatanodeProtocol
			return data.InitReplicaRecovery(rBlock);
		}

		/// <summary>Convenience method, which unwraps RemoteException.</summary>
		/// <exception cref="System.IO.IOException">not a RemoteException.</exception>
		private static ReplicaRecoveryInfo CallInitReplicaRecovery(InterDatanodeProtocol 
			datanode, BlockRecoveryCommand.RecoveringBlock rBlock)
		{
			try
			{
				return datanode.InitReplicaRecovery(rBlock);
			}
			catch (RemoteException re)
			{
				throw re.UnwrapRemoteException();
			}
		}

		/// <summary>Update replica with the new generation stamp and length.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual string UpdateReplicaUnderRecovery(ExtendedBlock oldBlock, long recoveryId
			, long newBlockId, long newLength)
		{
			// InterDatanodeProtocol
			string storageID = data.UpdateReplicaUnderRecovery(oldBlock, recoveryId, newBlockId
				, newLength);
			// Notify the namenode of the updated block info. This is important
			// for HA, since otherwise the standby node may lose track of the
			// block locations until the next block report.
			ExtendedBlock newBlock = new ExtendedBlock(oldBlock);
			newBlock.SetGenerationStamp(recoveryId);
			newBlock.SetBlockId(newBlockId);
			newBlock.SetNumBytes(newLength);
			NotifyNamenodeReceivedBlock(newBlock, string.Empty, storageID);
			return storageID;
		}

		/// <summary>A convenient class used in block recovery</summary>
		internal class BlockRecord
		{
			internal readonly DatanodeID id;

			internal readonly InterDatanodeProtocol datanode;

			internal readonly ReplicaRecoveryInfo rInfo;

			private string storageID;

			internal BlockRecord(DatanodeID id, InterDatanodeProtocol datanode, ReplicaRecoveryInfo
				 rInfo)
			{
				this.id = id;
				this.datanode = datanode;
				this.rInfo = rInfo;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void UpdateReplicaUnderRecovery(string bpid, long recoveryId, long
				 newBlockId, long newLength)
			{
				ExtendedBlock b = new ExtendedBlock(bpid, rInfo);
				storageID = datanode.UpdateReplicaUnderRecovery(b, recoveryId, newBlockId, newLength
					);
			}

			public override string ToString()
			{
				return "block:" + rInfo + " node:" + id;
			}
		}

		/// <summary>Recover a block</summary>
		/// <exception cref="System.IO.IOException"/>
		private void RecoverBlock(BlockRecoveryCommand.RecoveringBlock rBlock)
		{
			ExtendedBlock block = rBlock.GetBlock();
			string blookPoolId = block.GetBlockPoolId();
			DatanodeID[] datanodeids = rBlock.GetLocations();
			IList<DataNode.BlockRecord> syncList = new AList<DataNode.BlockRecord>(datanodeids
				.Length);
			int errorCount = 0;
			//check generation stamps
			foreach (DatanodeID id in datanodeids)
			{
				try
				{
					BPOfferService bpos = blockPoolManager.Get(blookPoolId);
					DatanodeRegistration bpReg = bpos.bpRegistration;
					InterDatanodeProtocol datanode = bpReg.Equals(id) ? this : DataNode.CreateInterDataNodeProtocolProxy
						(id, GetConf(), dnConf.socketTimeout, dnConf.connectToDnViaHostname);
					ReplicaRecoveryInfo info = CallInitReplicaRecovery(datanode, rBlock);
					if (info != null && info.GetGenerationStamp() >= block.GetGenerationStamp() && info
						.GetNumBytes() > 0)
					{
						syncList.AddItem(new DataNode.BlockRecord(id, datanode, info));
					}
				}
				catch (RecoveryInProgressException ripE)
				{
					InterDatanodeProtocol.Log.Warn("Recovery for replica " + block + " on data-node "
						 + id + " is already in progress. Recovery id = " + rBlock.GetNewGenerationStamp
						() + " is aborted.", ripE);
					return;
				}
				catch (IOException e)
				{
					++errorCount;
					InterDatanodeProtocol.Log.Warn("Failed to obtain replica info for block (=" + block
						 + ") from datanode (=" + id + ")", e);
				}
			}
			if (errorCount == datanodeids.Length)
			{
				throw new IOException("All datanodes failed: block=" + block + ", datanodeids=" +
					 Arrays.AsList(datanodeids));
			}
			SyncBlock(rBlock, syncList);
		}

		/// <summary>Get the NameNode corresponding to the given block pool.</summary>
		/// <param name="bpid">Block pool Id</param>
		/// <returns>Namenode corresponding to the bpid</returns>
		/// <exception cref="System.IO.IOException">if unable to get the corresponding NameNode
		/// 	</exception>
		public virtual DatanodeProtocolClientSideTranslatorPB GetActiveNamenodeForBP(string
			 bpid)
		{
			BPOfferService bpos = blockPoolManager.Get(bpid);
			if (bpos == null)
			{
				throw new IOException("No block pool offer service for bpid=" + bpid);
			}
			DatanodeProtocolClientSideTranslatorPB activeNN = bpos.GetActiveNN();
			if (activeNN == null)
			{
				throw new IOException("Block pool " + bpid + " has not recognized an active NN");
			}
			return activeNN;
		}

		/// <summary>Block synchronization</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SyncBlock(BlockRecoveryCommand.RecoveringBlock rBlock, IList
			<DataNode.BlockRecord> syncList)
		{
			ExtendedBlock block = rBlock.GetBlock();
			string bpid = block.GetBlockPoolId();
			DatanodeProtocolClientSideTranslatorPB nn = GetActiveNamenodeForBP(block.GetBlockPoolId
				());
			long recoveryId = rBlock.GetNewGenerationStamp();
			bool isTruncateRecovery = rBlock.GetNewBlock() != null;
			long blockId = (isTruncateRecovery) ? rBlock.GetNewBlock().GetBlockId() : block.GetBlockId
				();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("block=" + block + ", (length=" + block.GetNumBytes() + "), syncList=" 
					+ syncList);
			}
			// syncList.isEmpty() means that all data-nodes do not have the block
			// or their replicas have 0 length.
			// The block can be deleted.
			if (syncList.IsEmpty())
			{
				nn.CommitBlockSynchronization(block, recoveryId, 0, true, true, DatanodeID.EmptyArray
					, null);
				return;
			}
			// Calculate the best available replica state.
			HdfsServerConstants.ReplicaState bestState = HdfsServerConstants.ReplicaState.Rwr;
			long finalizedLength = -1;
			foreach (DataNode.BlockRecord r in syncList)
			{
				System.Diagnostics.Debug.Assert(r.rInfo.GetNumBytes() > 0, "zero length replica");
				HdfsServerConstants.ReplicaState rState = r.rInfo.GetOriginalReplicaState();
				if (rState.GetValue() < bestState.GetValue())
				{
					bestState = rState;
				}
				if (rState == HdfsServerConstants.ReplicaState.Finalized)
				{
					if (finalizedLength > 0 && finalizedLength != r.rInfo.GetNumBytes())
					{
						throw new IOException("Inconsistent size of finalized replicas. " + "Replica " + 
							r.rInfo + " expected size: " + finalizedLength);
					}
					finalizedLength = r.rInfo.GetNumBytes();
				}
			}
			// Calculate list of nodes that will participate in the recovery
			// and the new block size
			IList<DataNode.BlockRecord> participatingList = new AList<DataNode.BlockRecord>();
			ExtendedBlock newBlock = new ExtendedBlock(bpid, blockId, -1, recoveryId);
			switch (bestState)
			{
				case HdfsServerConstants.ReplicaState.Finalized:
				{
					System.Diagnostics.Debug.Assert(finalizedLength > 0, "finalizedLength is not positive"
						);
					foreach (DataNode.BlockRecord r_1 in syncList)
					{
						HdfsServerConstants.ReplicaState rState = r_1.rInfo.GetOriginalReplicaState();
						if (rState == HdfsServerConstants.ReplicaState.Finalized || rState == HdfsServerConstants.ReplicaState
							.Rbw && r_1.rInfo.GetNumBytes() == finalizedLength)
						{
							participatingList.AddItem(r_1);
						}
					}
					newBlock.SetNumBytes(finalizedLength);
					break;
				}

				case HdfsServerConstants.ReplicaState.Rbw:
				case HdfsServerConstants.ReplicaState.Rwr:
				{
					long minLength = long.MaxValue;
					foreach (DataNode.BlockRecord r_2 in syncList)
					{
						HdfsServerConstants.ReplicaState rState = r_2.rInfo.GetOriginalReplicaState();
						if (rState == bestState)
						{
							minLength = Math.Min(minLength, r_2.rInfo.GetNumBytes());
							participatingList.AddItem(r_2);
						}
					}
					newBlock.SetNumBytes(minLength);
					break;
				}

				case HdfsServerConstants.ReplicaState.Rur:
				case HdfsServerConstants.ReplicaState.Temporary:
				{
					System.Diagnostics.Debug.Assert(false, "bad replica state: " + bestState);
					break;
				}
			}
			if (isTruncateRecovery)
			{
				newBlock.SetNumBytes(rBlock.GetNewBlock().GetNumBytes());
			}
			IList<DatanodeID> failedList = new AList<DatanodeID>();
			IList<DataNode.BlockRecord> successList = new AList<DataNode.BlockRecord>();
			foreach (DataNode.BlockRecord r_3 in participatingList)
			{
				try
				{
					r_3.UpdateReplicaUnderRecovery(bpid, recoveryId, blockId, newBlock.GetNumBytes());
					successList.AddItem(r_3);
				}
				catch (IOException e)
				{
					InterDatanodeProtocol.Log.Warn("Failed to updateBlock (newblock=" + newBlock + ", datanode="
						 + r_3.id + ")", e);
					failedList.AddItem(r_3.id);
				}
			}
			// If any of the data-nodes failed, the recovery fails, because
			// we never know the actual state of the replica on failed data-nodes.
			// The recovery should be started over.
			if (!failedList.IsEmpty())
			{
				StringBuilder b = new StringBuilder();
				foreach (DatanodeID id in failedList)
				{
					b.Append("\n  " + id);
				}
				throw new IOException("Cannot recover " + block + ", the following " + failedList
					.Count + " data-nodes failed {" + b + "\n}");
			}
			// Notify the name-node about successfully recovered replicas.
			DatanodeID[] datanodes = new DatanodeID[successList.Count];
			string[] storages = new string[datanodes.Length];
			for (int i = 0; i < datanodes.Length; i++)
			{
				DataNode.BlockRecord r_4 = successList[i];
				datanodes[i] = r_4.id;
				storages[i] = r_4.storageID;
			}
			nn.CommitBlockSynchronization(block, newBlock.GetGenerationStamp(), newBlock.GetNumBytes
				(), true, false, datanodes, storages);
		}

		private static void LogRecoverBlock(string who, BlockRecoveryCommand.RecoveringBlock
			 rb)
		{
			ExtendedBlock block = rb.GetBlock();
			DatanodeInfo[] targets = rb.GetLocations();
			Log.Info(who + " calls recoverBlock(" + block + ", targets=[" + Joiner.On(", ").Join
				(targets) + "]" + ", newGenerationStamp=" + rb.GetNewGenerationStamp() + ")");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetReplicaVisibleLength(ExtendedBlock block)
		{
			// ClientDataNodeProtocol
			CheckReadAccess(block);
			return data.GetReplicaVisibleLength(block);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckReadAccess(ExtendedBlock block)
		{
			// Make sure this node has registered for the block pool.
			try
			{
				GetDNRegistrationForBP(block.GetBlockPoolId());
			}
			catch (IOException)
			{
				// if it has not registered with the NN, throw an exception back.
				throw new RetriableException("Datanode not registered. Try again later.");
			}
			if (isBlockTokenEnabled)
			{
				ICollection<TokenIdentifier> tokenIds = UserGroupInformation.GetCurrentUser().GetTokenIdentifiers
					();
				if (tokenIds.Count != 1)
				{
					throw new IOException("Can't continue since none or more than one " + "BlockTokenIdentifier is found."
						);
				}
				foreach (TokenIdentifier tokenId in tokenIds)
				{
					BlockTokenIdentifier id = (BlockTokenIdentifier)tokenId;
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Got: " + id.ToString());
					}
					blockPoolTokenSecretManager.CheckAccess(id, null, block, BlockTokenSecretManager.AccessMode
						.Read);
				}
			}
		}

		/// <summary>Transfer a replica to the datanode targets.</summary>
		/// <param name="b">
		/// the block to transfer.
		/// The corresponding replica must be an RBW or a Finalized.
		/// Its GS and numBytes will be set to
		/// the stored GS and the visible length.
		/// </param>
		/// <param name="targets">targets to transfer the block to</param>
		/// <param name="client">client name</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void TransferReplicaForPipelineRecovery(ExtendedBlock b, DatanodeInfo
			[] targets, StorageType[] targetStorageTypes, string client)
		{
			long storedGS;
			long visible;
			BlockConstructionStage stage;
			//get replica information
			lock (data)
			{
				Org.Apache.Hadoop.Hdfs.Protocol.Block storedBlock = data.GetStoredBlock(b.GetBlockPoolId
					(), b.GetBlockId());
				if (null == storedBlock)
				{
					throw new IOException(b + " not found in datanode.");
				}
				storedGS = storedBlock.GetGenerationStamp();
				if (storedGS < b.GetGenerationStamp())
				{
					throw new IOException(storedGS + " = storedGS < b.getGenerationStamp(), b=" + b);
				}
				// Update the genstamp with storedGS
				b.SetGenerationStamp(storedGS);
				if (data.IsValidRbw(b))
				{
					stage = BlockConstructionStage.TransferRbw;
				}
				else
				{
					if (data.IsValidBlock(b))
					{
						stage = BlockConstructionStage.TransferFinalized;
					}
					else
					{
						string r = data.GetReplicaString(b.GetBlockPoolId(), b.GetBlockId());
						throw new IOException(b + " is neither a RBW nor a Finalized, r=" + r);
					}
				}
				visible = data.GetReplicaVisibleLength(b);
			}
			//set visible length
			b.SetNumBytes(visible);
			if (targets.Length > 0)
			{
				new DataNode.DataTransfer(this, targets, targetStorageTypes, b, stage, client).Run
					();
			}
		}

		/// <summary>Finalize a pending upgrade in response to DNA_FINALIZE.</summary>
		/// <param name="blockPoolId">the block pool to finalize</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void FinalizeUpgradeForPool(string blockPoolId)
		{
			storage.FinalizeUpgrade(blockPoolId);
		}

		internal static IPEndPoint GetStreamingAddr(Configuration conf)
		{
			return NetUtils.CreateSocketAddr(conf.GetTrimmed(DFSConfigKeys.DfsDatanodeAddressKey
				, DFSConfigKeys.DfsDatanodeAddressDefault));
		}

		public virtual string GetVersion()
		{
			// DataNodeMXBean
			return VersionInfo.GetVersion();
		}

		public virtual string GetRpcPort()
		{
			// DataNodeMXBean
			IPEndPoint ipcAddr = NetUtils.CreateSocketAddr(this.GetConf().Get(DFSConfigKeys.DfsDatanodeIpcAddressKey
				));
			return Sharpen.Extensions.ToString(ipcAddr.Port);
		}

		public virtual string GetHttpPort()
		{
			// DataNodeMXBean
			return this.GetConf().Get("dfs.datanode.info.port");
		}

		/// <returns>the datanode's http port</returns>
		public virtual int GetInfoPort()
		{
			return infoPort;
		}

		/// <returns>the datanode's https port</returns>
		public virtual int GetInfoSecurePort()
		{
			return infoSecurePort;
		}

		/// <summary>
		/// Returned information is a JSON representation of a map with
		/// name node host name as the key and block pool Id as the value.
		/// </summary>
		/// <remarks>
		/// Returned information is a JSON representation of a map with
		/// name node host name as the key and block pool Id as the value.
		/// Note that, if there are multiple NNs in an NA nameservice,
		/// a given block pool may be represented twice.
		/// </remarks>
		public virtual string GetNamenodeAddresses()
		{
			// DataNodeMXBean
			IDictionary<string, string> info = new Dictionary<string, string>();
			foreach (BPOfferService bpos in blockPoolManager.GetAllNamenodeThreads())
			{
				if (bpos != null)
				{
					foreach (BPServiceActor actor in bpos.GetBPServiceActors())
					{
						info[actor.GetNNSocketAddress().GetHostName()] = bpos.GetBlockPoolId();
					}
				}
			}
			return JSON.ToString(info);
		}

		/// <summary>
		/// Returned information is a JSON representation of a map with
		/// volume name as the key and value is a map of volume attribute
		/// keys to its values
		/// </summary>
		public virtual string GetVolumeInfo()
		{
			// DataNodeMXBean
			Preconditions.CheckNotNull(data, "Storage not yet initialized");
			return JSON.ToString(data.GetVolumeInfoMap());
		}

		public virtual string GetClusterId()
		{
			lock (this)
			{
				// DataNodeMXBean
				return clusterId;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshNamenodes(Configuration conf)
		{
			blockPoolManager.RefreshNamenodes(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RefreshNamenodes()
		{
			// ClientDatanodeProtocol
			CheckSuperuserPrivilege();
			conf = new Configuration();
			RefreshNamenodes(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DeleteBlockPool(string blockPoolId, bool force)
		{
			// ClientDatanodeProtocol
			CheckSuperuserPrivilege();
			Log.Info("deleteBlockPool command received for block pool " + blockPoolId + ", force="
				 + force);
			if (blockPoolManager.Get(blockPoolId) != null)
			{
				Log.Warn("The block pool " + blockPoolId + " is still running, cannot be deleted."
					);
				throw new IOException("The block pool is still running. First do a refreshNamenodes to "
					 + "shutdown the block pool service");
			}
			data.DeleteBlockPool(blockPoolId, force);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ShutdownDatanode(bool forUpgrade)
		{
			lock (this)
			{
				// ClientDatanodeProtocol
				CheckSuperuserPrivilege();
				Log.Info("shutdownDatanode command received (upgrade=" + forUpgrade + "). Shutting down Datanode..."
					);
				// Shutdown can be called only once.
				if (shutdownInProgress)
				{
					throw new IOException("Shutdown already in progress.");
				}
				shutdownInProgress = true;
				shutdownForUpgrade = forUpgrade;
				// Asynchronously start the shutdown process so that the rpc response can be
				// sent back.
				Sharpen.Thread shutdownThread = new _Thread_2999(this);
				// Delay the shutdown a bit if not doing for restart.
				shutdownThread.SetDaemon(true);
				shutdownThread.Start();
			}
		}

		private sealed class _Thread_2999 : Sharpen.Thread
		{
			public _Thread_2999(DataNode _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				if (!this._enclosing.shutdownForUpgrade)
				{
					try
					{
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
					}
				}
				this._enclosing.Shutdown();
			}

			private readonly DataNode _enclosing;
		}

		public virtual DatanodeLocalInfo GetDatanodeInfo()
		{
			//ClientDatanodeProtocol
			long uptime = ManagementFactory.GetRuntimeMXBean().GetUptime() / 1000;
			return new DatanodeLocalInfo(VersionInfo.GetVersion(), confVersion, uptime);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void StartReconfiguration()
		{
			// ClientDatanodeProtocol
			CheckSuperuserPrivilege();
			StartReconfigurationTask();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ReconfigurationTaskStatus GetReconfigurationStatus()
		{
			// ClientDatanodeProtocol
			CheckSuperuserPrivilege();
			return GetReconfigurationTaskStatus();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TriggerBlockReport(BlockReportOptions options)
		{
			// ClientDatanodeProtocol
			CheckSuperuserPrivilege();
			foreach (BPOfferService bpos in blockPoolManager.GetAllNamenodeThreads())
			{
				if (bpos != null)
				{
					foreach (BPServiceActor actor in bpos.GetBPServiceActors())
					{
						actor.TriggerBlockReport(options);
					}
				}
			}
		}

		/// <param name="addr">rpc address of the namenode</param>
		/// <returns>
		/// true if the datanode is connected to a NameNode at the
		/// given address
		/// </returns>
		public virtual bool IsConnectedToNN(IPEndPoint addr)
		{
			foreach (BPOfferService bpos in GetAllBpOs())
			{
				foreach (BPServiceActor bpsa in bpos.GetBPServiceActors())
				{
					if (addr.Equals(bpsa.GetNNSocketAddress()))
					{
						return bpsa.IsAlive();
					}
				}
			}
			return false;
		}

		/// <param name="bpid">block pool Id</param>
		/// <returns>true - if BPOfferService thread is alive</returns>
		public virtual bool IsBPServiceAlive(string bpid)
		{
			BPOfferService bp = blockPoolManager.Get(bpid);
			return bp != null ? bp.IsAlive() : false;
		}

		internal virtual bool IsRestarting()
		{
			return shutdownForUpgrade;
		}

		/// <summary>
		/// A datanode is considered to be fully started if all the BP threads are
		/// alive and all the block pools are initialized.
		/// </summary>
		/// <returns>true - if the data node is fully started</returns>
		public virtual bool IsDatanodeFullyStarted()
		{
			foreach (BPOfferService bp in blockPoolManager.GetAllNamenodeThreads())
			{
				if (!bp.IsInitialized() || !bp.IsAlive())
				{
					return false;
				}
			}
			return true;
		}

		[VisibleForTesting]
		public virtual DatanodeID GetDatanodeId()
		{
			return id;
		}

		[VisibleForTesting]
		public virtual void ClearAllBlockSecretKeys()
		{
			blockPoolTokenSecretManager.ClearAllKeysForTesting();
		}

		/// <summary>Get current value of the max balancer bandwidth in bytes per second.</summary>
		/// <returns>Balancer bandwidth in bytes per second for this datanode.</returns>
		public virtual long GetBalancerBandwidth()
		{
			DataXceiverServer dxcs = (DataXceiverServer)this.dataXceiverServer.GetRunnable();
			return dxcs.balanceThrottler.GetBandwidth();
		}

		public virtual DNConf GetDnConf()
		{
			return dnConf;
		}

		public virtual string GetDatanodeUuid()
		{
			return id == null ? null : id.GetDatanodeUuid();
		}

		internal virtual bool ShouldRun()
		{
			return shouldRun;
		}

		[VisibleForTesting]
		internal virtual DataStorage GetStorage()
		{
			return storage;
		}

		public virtual ShortCircuitRegistry GetShortCircuitRegistry()
		{
			return shortCircuitRegistry;
		}

		/// <summary>Check the disk error</summary>
		private void CheckDiskError()
		{
			ICollection<FilePath> unhealthyDataDirs = data.CheckDataDir();
			if (unhealthyDataDirs != null && !unhealthyDataDirs.IsEmpty())
			{
				try
				{
					// Remove all unhealthy volumes from DataNode.
					RemoveVolumes(unhealthyDataDirs, false);
				}
				catch (IOException e)
				{
					Log.Warn("Error occurred when removing unhealthy storage dirs: " + e.Message, e);
				}
				StringBuilder sb = new StringBuilder("DataNode failed volumes:");
				foreach (FilePath dataDir in unhealthyDataDirs)
				{
					sb.Append(dataDir.GetAbsolutePath() + ";");
				}
				HandleDiskError(sb.ToString());
			}
		}

		/// <summary>
		/// Starts a new thread which will check for disk error check request
		/// every 5 sec
		/// </summary>
		private void StartCheckDiskErrorThread()
		{
			checkDiskErrorThread = new Sharpen.Thread(new _Runnable_3159(this));
		}

		private sealed class _Runnable_3159 : Runnable
		{
			public _Runnable_3159(DataNode _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void Run()
			{
				while (this._enclosing.shouldRun)
				{
					bool tempFlag;
					lock (this._enclosing.checkDiskErrorMutex)
					{
						tempFlag = this._enclosing.checkDiskErrorFlag;
						this._enclosing.checkDiskErrorFlag = false;
					}
					if (tempFlag)
					{
						try
						{
							this._enclosing.CheckDiskError();
						}
						catch (Exception e)
						{
							DataNode.Log.Warn("Unexpected exception occurred while checking disk error  " + e
								);
							this._enclosing.checkDiskErrorThread = null;
							return;
						}
						lock (this._enclosing.checkDiskErrorMutex)
						{
							this._enclosing.lastDiskErrorCheck = Time.MonotonicNow();
						}
					}
					try
					{
						Sharpen.Thread.Sleep(this._enclosing.checkDiskErrorInterval);
					}
					catch (Exception e)
					{
						DataNode.Log.Debug("InterruptedException in check disk error thread", e);
						this._enclosing.checkDiskErrorThread = null;
						return;
					}
				}
			}

			private readonly DataNode _enclosing;
		}

		public virtual long GetLastDiskErrorCheck()
		{
			lock (checkDiskErrorMutex)
			{
				return lastDiskErrorCheck;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual SpanReceiverInfo[] ListSpanReceivers()
		{
			CheckSuperuserPrivilege();
			return spanReceiverHost.ListSpanReceivers();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long AddSpanReceiver(SpanReceiverInfo info)
		{
			CheckSuperuserPrivilege();
			return spanReceiverHost.AddSpanReceiver(info);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RemoveSpanReceiver(long id)
		{
			CheckSuperuserPrivilege();
			spanReceiverHost.RemoveSpanReceiver(id);
		}
	}
}
