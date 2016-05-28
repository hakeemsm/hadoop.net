using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Javax.Management;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Metrics;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Top;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.Metrics;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Top.Window;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Web.Resources;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Metrics2.Annotation;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Codehaus.Jackson.Map;
using Org.Mortbay.Util.Ajax;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// FSNamesystem does the actual bookkeeping work for the
	/// DataNode.
	/// </summary>
	/// <remarks>
	/// FSNamesystem does the actual bookkeeping work for the
	/// DataNode.
	/// It tracks several important tables.
	/// 1)  valid fsname --&gt; blocklist  (kept on disk, logged)
	/// 2)  Set of all valid blocks (inverted #1)
	/// 3)  block --&gt; machinelist (kept in memory, rebuilt dynamically from reports)
	/// 4)  machine --&gt; blocklist (inverted #2)
	/// 5)  LRU cache of updated-heartbeat machines
	/// </remarks>
	public class FSNamesystem : Namesystem, FSNamesystemMBean, NameNodeMXBean
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem
			));

		private sealed class _ThreadLocal_319 : ThreadLocal<StringBuilder>
		{
			public _ThreadLocal_319()
			{
			}

			protected override StringBuilder InitialValue()
			{
				return new StringBuilder();
			}
		}

		private static readonly ThreadLocal<StringBuilder> auditBuffer = new _ThreadLocal_319
			();

		private readonly BlockIdManager blockIdManager;

		[VisibleForTesting]
		public virtual bool IsAuditEnabled()
		{
			return !isDefaultAuditLogger || auditLog.IsInfoEnabled();
		}

		/// <exception cref="System.IO.IOException"/>
		private void LogAuditEvent(bool succeeded, string cmd, string src)
		{
			LogAuditEvent(succeeded, cmd, src, null, null);
		}

		/// <exception cref="System.IO.IOException"/>
		private void LogAuditEvent(bool succeeded, string cmd, string src, string dst, HdfsFileStatus
			 stat)
		{
			if (IsAuditEnabled() && IsExternalInvocation())
			{
				LogAuditEvent(succeeded, GetRemoteUser(), GetRemoteIp(), cmd, src, dst, stat);
			}
		}

		private void LogAuditEvent(bool succeeded, UserGroupInformation ugi, IPAddress addr
			, string cmd, string src, string dst, HdfsFileStatus stat)
		{
			FileStatus status = null;
			if (stat != null)
			{
				Path symlink = stat.IsSymlink() ? new Path(stat.GetSymlink()) : null;
				Path path = dst != null ? new Path(dst) : new Path(src);
				status = new FileStatus(stat.GetLen(), stat.IsDir(), stat.GetReplication(), stat.
					GetBlockSize(), stat.GetModificationTime(), stat.GetAccessTime(), stat.GetPermission
					(), stat.GetOwner(), stat.GetGroup(), symlink, path);
			}
			foreach (AuditLogger logger in auditLoggers)
			{
				if (logger is HdfsAuditLogger)
				{
					HdfsAuditLogger hdfsLogger = (HdfsAuditLogger)logger;
					hdfsLogger.LogAuditEvent(succeeded, ugi.ToString(), addr, cmd, src, dst, status, 
						ugi, dtSecretManager);
				}
				else
				{
					logger.LogAuditEvent(succeeded, ugi.ToString(), addr, cmd, src, dst, status);
				}
			}
		}

		/// <summary>Logger for audit events, noting successful FSNamesystem operations.</summary>
		/// <remarks>
		/// Logger for audit events, noting successful FSNamesystem operations. Emits
		/// to FSNamesystem.audit at INFO. Each event causes a set of tab-separated
		/// <code>key=value</code> pairs to be written for the following properties:
		/// <code>
		/// ugi=&lt;ugi in RPC&gt;
		/// ip=&lt;remote IP&gt;
		/// cmd=&lt;command&gt;
		/// src=&lt;src path&gt;
		/// dst=&lt;dst path (optional)&gt;
		/// perm=&lt;permissions (optional)&gt;
		/// </code>
		/// </remarks>
		public static readonly Log auditLog = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem
			).FullName + ".audit");

		internal const int DefaultMaxCorruptFileblocksReturned = 100;

		internal static int BlockDeletionIncrement = 1000;

		private readonly bool isPermissionEnabled;

		private readonly UserGroupInformation fsOwner;

		private readonly string supergroup;

		private readonly bool standbyShouldCheckpoint;

		private static readonly long DelegationTokenRemoverScanInterval = TimeUnit.Milliseconds
			.Convert(1, TimeUnit.Hours);

		internal readonly DelegationTokenSecretManager dtSecretManager;

		private readonly bool alwaysUseDelegationTokensForTests;

		private static readonly Step StepAwaitingReportedBlocks = new Step(StepType.AwaitingReportedBlocks
			);

		private readonly bool isDefaultAuditLogger;

		private readonly IList<AuditLogger> auditLoggers;

		/// <summary>The namespace tree.</summary>
		internal FSDirectory dir;

		private readonly BlockManager blockManager;

		private readonly SnapshotManager snapshotManager;

		private readonly CacheManager cacheManager;

		private readonly DatanodeStatistics datanodeStatistics;

		private string nameserviceId;

		private volatile RollingUpgradeInfo rollingUpgradeInfo = null;

		/// <summary>
		/// A flag that indicates whether the checkpointer should checkpoint a rollback
		/// fsimage.
		/// </summary>
		/// <remarks>
		/// A flag that indicates whether the checkpointer should checkpoint a rollback
		/// fsimage. The edit log tailer sets this flag. The checkpoint will create a
		/// rollback fsimage if the flag is true, and then change the flag to false.
		/// </remarks>
		private volatile bool needRollbackFsImage;

		private string blockPoolId;

		internal readonly LeaseManager leaseManager;

		internal volatile Daemon smmthread = null;

		internal Daemon nnrmthread = null;

		internal Daemon nnEditLogRoller = null;

		internal Daemon lazyPersistFileScrubber = null;

		/// <summary>When an active namenode will roll its own edit log, in # edits</summary>
		private readonly long editLogRollerThreshold;

		/// <summary>Check interval of an active namenode's edit log roller thread</summary>
		private readonly int editLogRollerInterval;

		/// <summary>How frequently we scan and unlink corrupt lazyPersist files.</summary>
		/// <remarks>
		/// How frequently we scan and unlink corrupt lazyPersist files.
		/// (In seconds)
		/// </remarks>
		private readonly int lazyPersistFileScrubIntervalSec;

		private volatile bool hasResourcesAvailable = false;

		private volatile bool fsRunning = true;

		/// <summary>The start time of the namesystem.</summary>
		private readonly long startTime = Time.Now();

		/// <summary>The interval of namenode checking for the disk space availability</summary>
		private readonly long resourceRecheckInterval;

		internal NameNodeResourceChecker nnResourceChecker;

		private readonly FsServerDefaults serverDefaults;

		private readonly bool supportAppends;

		private readonly ReplaceDatanodeOnFailure dtpReplaceDatanodeOnFailure;

		private volatile FSNamesystem.SafeModeInfo safeMode;

		private readonly long maxFsObjects;

		private readonly long minBlockSize;

		private readonly long maxBlocksPerFile;

		private readonly long accessTimePrecision;

		/// <summary>Lock to protect FSNamesystem.</summary>
		private readonly FSNamesystemLock fsLock;

		/// <summary>Checkpoint lock to protect FSNamesystem modification on standby NNs.</summary>
		/// <remarks>
		/// Checkpoint lock to protect FSNamesystem modification on standby NNs.
		/// Unlike fsLock, it does not affect block updates. On active NNs, this lock
		/// does not provide proper protection, because there are operations that
		/// modify both block and name system state.  Even on standby, fsLock is
		/// used when block state changes need to be blocked.
		/// </remarks>
		private readonly ReentrantLock cpLock;

		/// <summary>Used when this NN is in standby state to read from the shared edit log.</summary>
		private EditLogTailer editLogTailer = null;

		/// <summary>Used when this NN is in standby state to perform checkpoints.</summary>
		private StandbyCheckpointer standbyCheckpointer;

		/// <summary>Reference to the NN's HAContext object.</summary>
		/// <remarks>
		/// Reference to the NN's HAContext object. This is only set once
		/// <see cref="StartCommonServices(Org.Apache.Hadoop.Conf.Configuration, Org.Apache.Hadoop.Hdfs.Server.Namenode.HA.HAContext)
		/// 	"/>
		/// is called.
		/// </remarks>
		private HAContext haContext;

		private readonly bool haEnabled;

		/// <summary>flag indicating whether replication queues have been initialized</summary>
		internal bool initializedReplQueues = false;

		/// <summary>Whether the namenode is in the middle of starting the active service</summary>
		private volatile bool startingActiveService = false;

		private readonly RetryCache retryCache;

		private KeyProviderCryptoExtension provider = null;

		private volatile bool imageLoaded = false;

		private readonly Condition cond;

		private readonly FSImage fsImage;

		private readonly TopConf topConf;

		private TopMetrics topMetrics;

		private INodeAttributeProvider inodeAttributeProvider;

		// Scan interval is not configurable.
		// Tracks whether the default audit logger is the only configured audit
		// logger; this allows isAuditEnabled() to return false in case the
		// underlying logger is disabled, and avoid some unnecessary work.
		// Block pool ID used by this namenode
		// SafeModeMonitor thread
		// NamenodeResourceMonitor thread
		// NameNodeEditLogRoller thread
		// A daemon to periodically clean up corrupt lazyPersist files
		// from the name space.
		// The actual resource checker instance.
		// safe mode information
		// maximum number of fs objects
		// minimum block size
		// maximum # of blocks per file
		// precision of access times.
		/// <summary>
		/// Notify that loading of this FSDirectory is complete, and
		/// it is imageLoaded for use
		/// </summary>
		internal virtual void ImageLoadComplete()
		{
			Preconditions.CheckState(!imageLoaded, "FSDirectory already loaded");
			SetImageLoaded();
		}

		internal virtual void SetImageLoaded()
		{
			if (imageLoaded)
			{
				return;
			}
			WriteLock();
			try
			{
				SetImageLoaded(true);
				dir.MarkNameCacheInitialized();
				cond.SignalAll();
			}
			finally
			{
				WriteUnlock();
			}
		}

		//This is for testing purposes only
		[VisibleForTesting]
		internal virtual bool IsImageLoaded()
		{
			return imageLoaded;
		}

		// exposed for unit tests
		protected internal virtual void SetImageLoaded(bool flag)
		{
			imageLoaded = flag;
		}

		/// <summary>Block until the object is imageLoaded to be used.</summary>
		internal virtual void WaitForLoadingFSImage()
		{
			if (!imageLoaded)
			{
				WriteLock();
				try
				{
					while (!imageLoaded)
					{
						try
						{
							cond.Await(5000, TimeUnit.Milliseconds);
						}
						catch (Exception)
						{
						}
					}
				}
				finally
				{
					WriteUnlock();
				}
			}
		}

		/// <summary>Clear all loaded data</summary>
		internal virtual void Clear()
		{
			dir.Reset();
			dtSecretManager.Reset();
			blockIdManager.Clear();
			leaseManager.RemoveAllLeases();
			snapshotManager.ClearSnapshottableDirs();
			cacheManager.Clear();
			SetImageLoaded(false);
			blockManager.Clear();
		}

		[VisibleForTesting]
		internal virtual LeaseManager GetLeaseManager()
		{
			return leaseManager;
		}

		internal virtual bool IsHaEnabled()
		{
			return haEnabled;
		}

		/// <summary>Check the supplied configuration for correctness.</summary>
		/// <param name="conf">Supplies the configuration to validate.</param>
		/// <exception cref="System.IO.IOException">if the configuration could not be queried.
		/// 	</exception>
		/// <exception cref="System.ArgumentException">if the configuration is invalid.</exception>
		private static void CheckConfiguration(Configuration conf)
		{
			ICollection<URI> namespaceDirs = Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem
				.GetNamespaceDirs(conf);
			ICollection<URI> editsDirs = Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem.
				GetNamespaceEditsDirs(conf);
			ICollection<URI> requiredEditsDirs = Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem
				.GetRequiredNamespaceEditsDirs(conf);
			ICollection<URI> sharedEditsDirs = Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem
				.GetSharedEditsDirs(conf);
			foreach (URI u in requiredEditsDirs)
			{
				if (string.CompareOrdinal(u.ToString(), DFSConfigKeys.DfsNamenodeEditsDirDefault)
					 == 0)
				{
					continue;
				}
				// Each required directory must also be in editsDirs or in
				// sharedEditsDirs.
				if (!editsDirs.Contains(u) && !sharedEditsDirs.Contains(u))
				{
					throw new ArgumentException("Required edits directory " + u.ToString() + " not present in "
						 + DFSConfigKeys.DfsNamenodeEditsDirKey + ". " + DFSConfigKeys.DfsNamenodeEditsDirKey
						 + "=" + editsDirs.ToString() + "; " + DFSConfigKeys.DfsNamenodeEditsDirRequiredKey
						 + "=" + requiredEditsDirs.ToString() + ". " + DFSConfigKeys.DfsNamenodeSharedEditsDirKey
						 + "=" + sharedEditsDirs.ToString() + ".");
				}
			}
			if (namespaceDirs.Count == 1)
			{
				Log.Warn("Only one image storage directory (" + DFSConfigKeys.DfsNamenodeNameDirKey
					 + ") configured. Beware of data loss" + " due to lack of redundant storage directories!"
					);
			}
			if (editsDirs.Count == 1)
			{
				Log.Warn("Only one namespace edits storage directory (" + DFSConfigKeys.DfsNamenodeEditsDirKey
					 + ") configured. Beware of data loss" + " due to lack of redundant storage directories!"
					);
			}
		}

		/// <summary>
		/// Instantiates an FSNamesystem loaded from the image and edits
		/// directories specified in the passed Configuration.
		/// </summary>
		/// <param name="conf">
		/// the Configuration which specifies the storage directories
		/// from which to load
		/// </param>
		/// <returns>an FSNamesystem which contains the loaded namespace</returns>
		/// <exception cref="System.IO.IOException">if loading fails</exception>
		internal static Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem LoadFromDisk(
			Configuration conf)
		{
			CheckConfiguration(conf);
			FSImage fsImage = new FSImage(conf, Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem
				.GetNamespaceDirs(conf), Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem.GetNamespaceEditsDirs
				(conf));
			Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem namesystem = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSNamesystem
				(conf, fsImage, false);
			HdfsServerConstants.StartupOption startOpt = NameNode.GetStartupOption(conf);
			if (startOpt == HdfsServerConstants.StartupOption.Recover)
			{
				namesystem.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			}
			long loadStart = Time.MonotonicNow();
			try
			{
				namesystem.LoadFSImage(startOpt);
			}
			catch (IOException ioe)
			{
				Log.Warn("Encountered exception loading fsimage", ioe);
				fsImage.Close();
				throw;
			}
			long timeTakenToLoadFSImage = Time.MonotonicNow() - loadStart;
			Log.Info("Finished loading FSImage in " + timeTakenToLoadFSImage + " msecs");
			NameNodeMetrics nnMetrics = NameNode.GetNameNodeMetrics();
			if (nnMetrics != null)
			{
				nnMetrics.SetFsImageLoadTime((int)timeTakenToLoadFSImage);
			}
			return namesystem;
		}

		/// <exception cref="System.IO.IOException"/>
		internal FSNamesystem(Configuration conf, FSImage fsImage)
			: this(conf, fsImage, false)
		{
			leaseManager = new LeaseManager(this);
		}

		/// <summary>Create an FSNamesystem associated with the specified image.</summary>
		/// <remarks>
		/// Create an FSNamesystem associated with the specified image.
		/// Note that this does not load any data off of disk -- if you would
		/// like that behavior, use
		/// <see cref="LoadFromDisk(Org.Apache.Hadoop.Conf.Configuration)"/>
		/// </remarks>
		/// <param name="conf">configuration</param>
		/// <param name="fsImage">The FSImage to associate with</param>
		/// <param name="ignoreRetryCache">
		/// Whether or not should ignore the retry cache setup
		/// step. For Secondary NN this should be set to true.
		/// </param>
		/// <exception cref="System.IO.IOException">on bad configuration</exception>
		internal FSNamesystem(Configuration conf, FSImage fsImage, bool ignoreRetryCache)
		{
			leaseManager = new LeaseManager(this);
			provider = DFSUtil.CreateKeyProviderCryptoExtension(conf);
			if (provider == null)
			{
				Log.Info("No KeyProvider found.");
			}
			else
			{
				Log.Info("Found KeyProvider: " + provider.ToString());
			}
			if (conf.GetBoolean(DFSConfigKeys.DfsNamenodeAuditLogAsyncKey, DFSConfigKeys.DfsNamenodeAuditLogAsyncDefault
				))
			{
				Log.Info("Enabling async auditlog");
				EnableAsyncAuditLog();
			}
			bool fair = conf.GetBoolean("dfs.namenode.fslock.fair", true);
			Log.Info("fsLock is fair:" + fair);
			fsLock = new FSNamesystemLock(fair);
			cond = fsLock.WriteLock().NewCondition();
			cpLock = new ReentrantLock();
			this.fsImage = fsImage;
			try
			{
				resourceRecheckInterval = conf.GetLong(DFSConfigKeys.DfsNamenodeResourceCheckIntervalKey
					, DFSConfigKeys.DfsNamenodeResourceCheckIntervalDefault);
				this.blockManager = new BlockManager(this, conf);
				this.datanodeStatistics = blockManager.GetDatanodeManager().GetDatanodeStatistics
					();
				this.blockIdManager = new BlockIdManager(blockManager);
				this.fsOwner = UserGroupInformation.GetCurrentUser();
				this.supergroup = conf.Get(DFSConfigKeys.DfsPermissionsSuperusergroupKey, DFSConfigKeys
					.DfsPermissionsSuperusergroupDefault);
				this.isPermissionEnabled = conf.GetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey
					, DFSConfigKeys.DfsPermissionsEnabledDefault);
				Log.Info("fsOwner             = " + fsOwner);
				Log.Info("supergroup          = " + supergroup);
				Log.Info("isPermissionEnabled = " + isPermissionEnabled);
				// block allocation has to be persisted in HA using a shared edits directory
				// so that the standby has up-to-date namespace information
				nameserviceId = DFSUtil.GetNamenodeNameServiceId(conf);
				this.haEnabled = HAUtil.IsHAEnabled(conf, nameserviceId);
				// Sanity check the HA-related config.
				if (nameserviceId != null)
				{
					Log.Info("Determined nameservice ID: " + nameserviceId);
				}
				Log.Info("HA Enabled: " + haEnabled);
				if (!haEnabled && HAUtil.UsesSharedEditsDir(conf))
				{
					Log.Warn("Configured NNs:\n" + DFSUtil.NnAddressesAsString(conf));
					throw new IOException("Invalid configuration: a shared edits dir " + "must not be specified if HA is not enabled."
						);
				}
				// Get the checksum type from config
				string checksumTypeStr = conf.Get(DFSConfigKeys.DfsChecksumTypeKey, DFSConfigKeys
					.DfsChecksumTypeDefault);
				DataChecksum.Type checksumType;
				try
				{
					checksumType = DataChecksum.Type.ValueOf(checksumTypeStr);
				}
				catch (ArgumentException)
				{
					throw new IOException("Invalid checksum type in " + DFSConfigKeys.DfsChecksumTypeKey
						 + ": " + checksumTypeStr);
				}
				this.serverDefaults = new FsServerDefaults(conf.GetLongBytes(DFSConfigKeys.DfsBlockSizeKey
					, DFSConfigKeys.DfsBlockSizeDefault), conf.GetInt(DFSConfigKeys.DfsBytesPerChecksumKey
					, DFSConfigKeys.DfsBytesPerChecksumDefault), conf.GetInt(DFSConfigKeys.DfsClientWritePacketSizeKey
					, DFSConfigKeys.DfsClientWritePacketSizeDefault), (short)conf.GetInt(DFSConfigKeys
					.DfsReplicationKey, DFSConfigKeys.DfsReplicationDefault), conf.GetInt(CommonConfigurationKeysPublic
					.IoFileBufferSizeKey, CommonConfigurationKeysPublic.IoFileBufferSizeDefault), conf
					.GetBoolean(DFSConfigKeys.DfsEncryptDataTransferKey, DFSConfigKeys.DfsEncryptDataTransferDefault
					), conf.GetLong(CommonConfigurationKeysPublic.FsTrashIntervalKey, CommonConfigurationKeysPublic
					.FsTrashIntervalDefault), checksumType);
				this.maxFsObjects = conf.GetLong(DFSConfigKeys.DfsNamenodeMaxObjectsKey, DFSConfigKeys
					.DfsNamenodeMaxObjectsDefault);
				this.minBlockSize = conf.GetLong(DFSConfigKeys.DfsNamenodeMinBlockSizeKey, DFSConfigKeys
					.DfsNamenodeMinBlockSizeDefault);
				this.maxBlocksPerFile = conf.GetLong(DFSConfigKeys.DfsNamenodeMaxBlocksPerFileKey
					, DFSConfigKeys.DfsNamenodeMaxBlocksPerFileDefault);
				this.accessTimePrecision = conf.GetLong(DFSConfigKeys.DfsNamenodeAccesstimePrecisionKey
					, DFSConfigKeys.DfsNamenodeAccesstimePrecisionDefault);
				this.supportAppends = conf.GetBoolean(DFSConfigKeys.DfsSupportAppendKey, DFSConfigKeys
					.DfsSupportAppendDefault);
				Log.Info("Append Enabled: " + supportAppends);
				this.dtpReplaceDatanodeOnFailure = ReplaceDatanodeOnFailure.Get(conf);
				this.standbyShouldCheckpoint = conf.GetBoolean(DFSConfigKeys.DfsHaStandbyCheckpointsKey
					, DFSConfigKeys.DfsHaStandbyCheckpointsDefault);
				// # edit autoroll threshold is a multiple of the checkpoint threshold 
				this.editLogRollerThreshold = (long)(conf.GetFloat(DFSConfigKeys.DfsNamenodeEditLogAutorollMultiplierThreshold
					, DFSConfigKeys.DfsNamenodeEditLogAutorollMultiplierThresholdDefault) * conf.GetLong
					(DFSConfigKeys.DfsNamenodeCheckpointTxnsKey, DFSConfigKeys.DfsNamenodeCheckpointTxnsDefault
					));
				this.editLogRollerInterval = conf.GetInt(DFSConfigKeys.DfsNamenodeEditLogAutorollCheckIntervalMs
					, DFSConfigKeys.DfsNamenodeEditLogAutorollCheckIntervalMsDefault);
				this.lazyPersistFileScrubIntervalSec = conf.GetInt(DFSConfigKeys.DfsNamenodeLazyPersistFileScrubIntervalSec
					, DFSConfigKeys.DfsNamenodeLazyPersistFileScrubIntervalSecDefault);
				if (this.lazyPersistFileScrubIntervalSec == 0)
				{
					throw new ArgumentException(DFSConfigKeys.DfsNamenodeLazyPersistFileScrubIntervalSec
						 + " must be non-zero.");
				}
				// For testing purposes, allow the DT secret manager to be started regardless
				// of whether security is enabled.
				alwaysUseDelegationTokensForTests = conf.GetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey
					, DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseDefault);
				this.dtSecretManager = CreateDelegationTokenSecretManager(conf);
				this.dir = new FSDirectory(this, conf);
				this.snapshotManager = new SnapshotManager(dir);
				this.cacheManager = new CacheManager(this, conf, blockManager);
				this.safeMode = new FSNamesystem.SafeModeInfo(this, conf);
				this.topConf = new TopConf(conf);
				this.auditLoggers = InitAuditLoggers(conf);
				this.isDefaultAuditLogger = auditLoggers.Count == 1 && auditLoggers[0] is FSNamesystem.DefaultAuditLogger;
				this.retryCache = ignoreRetryCache ? null : InitRetryCache(conf);
				Type klass = conf.GetClass<INodeAttributeProvider>(DFSConfigKeys.DfsNamenodeInodeAttributesProviderKey
					, null);
				if (klass != null)
				{
					inodeAttributeProvider = ReflectionUtils.NewInstance(klass, conf);
					Log.Info("Using INode attribute provider: " + klass.FullName);
				}
			}
			catch (IOException e)
			{
				Log.Error(GetType().Name + " initialization failed.", e);
				Close();
				throw;
			}
			catch (RuntimeException re)
			{
				Log.Error(GetType().Name + " initialization failed.", re);
				Close();
				throw;
			}
		}

		[VisibleForTesting]
		public virtual IList<AuditLogger> GetAuditLoggers()
		{
			return auditLoggers;
		}

		[VisibleForTesting]
		public virtual RetryCache GetRetryCache()
		{
			return retryCache;
		}

		internal virtual void LockRetryCache()
		{
			if (retryCache != null)
			{
				retryCache.Lock();
			}
		}

		internal virtual void UnlockRetryCache()
		{
			if (retryCache != null)
			{
				retryCache.Unlock();
			}
		}

		/// <summary>Whether or not retry cache is enabled</summary>
		internal virtual bool HasRetryCache()
		{
			return retryCache != null;
		}

		internal virtual void AddCacheEntryWithPayload(byte[] clientId, int callId, object
			 payload)
		{
			if (retryCache != null)
			{
				retryCache.AddCacheEntryWithPayload(clientId, callId, payload);
			}
		}

		internal virtual void AddCacheEntry(byte[] clientId, int callId)
		{
			if (retryCache != null)
			{
				retryCache.AddCacheEntry(clientId, callId);
			}
		}

		[VisibleForTesting]
		public virtual KeyProviderCryptoExtension GetProvider()
		{
			return provider;
		}

		[VisibleForTesting]
		internal static RetryCache InitRetryCache(Configuration conf)
		{
			bool enable = conf.GetBoolean(DFSConfigKeys.DfsNamenodeEnableRetryCacheKey, DFSConfigKeys
				.DfsNamenodeEnableRetryCacheDefault);
			Log.Info("Retry cache on namenode is " + (enable ? "enabled" : "disabled"));
			if (enable)
			{
				float heapPercent = conf.GetFloat(DFSConfigKeys.DfsNamenodeRetryCacheHeapPercentKey
					, DFSConfigKeys.DfsNamenodeRetryCacheHeapPercentDefault);
				long entryExpiryMillis = conf.GetLong(DFSConfigKeys.DfsNamenodeRetryCacheExpirytimeMillisKey
					, DFSConfigKeys.DfsNamenodeRetryCacheExpirytimeMillisDefault);
				Log.Info("Retry cache will use " + heapPercent + " of total heap and retry cache entry expiry time is "
					 + entryExpiryMillis + " millis");
				long entryExpiryNanos = entryExpiryMillis * 1000 * 1000;
				return new RetryCache("NameNodeRetryCache", heapPercent, entryExpiryNanos);
			}
			return null;
		}

		private IList<AuditLogger> InitAuditLoggers(Configuration conf)
		{
			// Initialize the custom access loggers if configured.
			ICollection<string> alClasses = conf.GetStringCollection(DFSConfigKeys.DfsNamenodeAuditLoggersKey
				);
			IList<AuditLogger> auditLoggers = Lists.NewArrayList();
			if (alClasses != null && !alClasses.IsEmpty())
			{
				foreach (string className in alClasses)
				{
					try
					{
						AuditLogger logger;
						if (DFSConfigKeys.DfsNamenodeDefaultAuditLoggerName.Equals(className))
						{
							logger = new FSNamesystem.DefaultAuditLogger();
						}
						else
						{
							logger = (AuditLogger)System.Activator.CreateInstance(Sharpen.Runtime.GetType(className
								));
						}
						logger.Initialize(conf);
						auditLoggers.AddItem(logger);
					}
					catch (RuntimeException re)
					{
						throw;
					}
					catch (Exception e)
					{
						throw new RuntimeException(e);
					}
				}
			}
			// Make sure there is at least one logger installed.
			if (auditLoggers.IsEmpty())
			{
				auditLoggers.AddItem(new FSNamesystem.DefaultAuditLogger());
			}
			// Add audit logger to calculate top users
			if (topConf.isEnabled)
			{
				topMetrics = new TopMetrics(conf, topConf.nntopReportingPeriodsMs);
				auditLoggers.AddItem(new TopAuditLogger(topMetrics));
			}
			return Sharpen.Collections.UnmodifiableList(auditLoggers);
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadFSImage(HdfsServerConstants.StartupOption startOpt)
		{
			FSImage fsImage = GetFSImage();
			// format before starting up if requested
			if (startOpt == HdfsServerConstants.StartupOption.Format)
			{
				fsImage.Format(this, fsImage.GetStorage().DetermineClusterId());
				// reuse current id
				startOpt = HdfsServerConstants.StartupOption.Regular;
			}
			bool success = false;
			WriteLock();
			try
			{
				// We shouldn't be calling saveNamespace if we've come up in standby state.
				MetaRecoveryContext recovery = startOpt.CreateRecoveryContext();
				bool staleImage = fsImage.RecoverTransitionRead(startOpt, this, recovery);
				if (HdfsServerConstants.RollingUpgradeStartupOption.Rollback.Matches(startOpt) ||
					 HdfsServerConstants.RollingUpgradeStartupOption.Downgrade.Matches(startOpt))
				{
					rollingUpgradeInfo = null;
				}
				bool needToSave = staleImage && !haEnabled && !IsRollingUpgrade();
				Log.Info("Need to save fs image? " + needToSave + " (staleImage=" + staleImage + 
					", haEnabled=" + haEnabled + ", isRollingUpgrade=" + IsRollingUpgrade() + ")");
				if (needToSave)
				{
					fsImage.SaveNamespace(this);
				}
				else
				{
					UpdateStorageVersionForRollingUpgrade(fsImage.GetLayoutVersion(), startOpt);
					// No need to save, so mark the phase done.
					StartupProgress prog = NameNode.GetStartupProgress();
					prog.BeginPhase(Phase.SavingCheckpoint);
					prog.EndPhase(Phase.SavingCheckpoint);
				}
				// This will start a new log segment and write to the seen_txid file, so
				// we shouldn't do it when coming up in standby state
				if (!haEnabled || (haEnabled && startOpt == HdfsServerConstants.StartupOption.Upgrade
					) || (haEnabled && startOpt == HdfsServerConstants.StartupOption.Upgradeonly))
				{
					fsImage.OpenEditLogForWrite();
				}
				success = true;
			}
			finally
			{
				if (!success)
				{
					fsImage.Close();
				}
				WriteUnlock();
			}
			ImageLoadComplete();
		}

		/// <exception cref="System.IO.IOException"/>
		private void UpdateStorageVersionForRollingUpgrade(long layoutVersion, HdfsServerConstants.StartupOption
			 startOpt)
		{
			bool rollingStarted = HdfsServerConstants.RollingUpgradeStartupOption.Started.Matches
				(startOpt) && layoutVersion > HdfsConstants.NamenodeLayoutVersion;
			bool rollingRollback = HdfsServerConstants.RollingUpgradeStartupOption.Rollback.Matches
				(startOpt);
			if (rollingRollback || rollingStarted)
			{
				fsImage.UpdateStorageVersion();
			}
		}

		private void StartSecretManager()
		{
			if (dtSecretManager != null)
			{
				try
				{
					dtSecretManager.StartThreads();
				}
				catch (IOException e)
				{
					// Inability to start secret manager
					// can't be recovered from.
					throw new RuntimeException(e);
				}
			}
		}

		private void StartSecretManagerIfNecessary()
		{
			bool shouldRun = ShouldUseDelegationTokens() && !IsInSafeMode() && GetEditLog().IsOpenForWrite
				();
			bool running = dtSecretManager.IsRunning();
			if (shouldRun && !running)
			{
				StartSecretManager();
			}
		}

		private void StopSecretManager()
		{
			if (dtSecretManager != null)
			{
				dtSecretManager.StopThreads();
			}
		}

		/// <summary>Start services common to both active and standby states</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void StartCommonServices(Configuration conf, HAContext haContext
			)
		{
			this.RegisterMBean();
			// register the MBean for the FSNamesystemState
			WriteLock();
			this.haContext = haContext;
			try
			{
				nnResourceChecker = new NameNodeResourceChecker(conf);
				CheckAvailableResources();
				System.Diagnostics.Debug.Assert(safeMode != null && !IsPopulatingReplQueues());
				StartupProgress prog = NameNode.GetStartupProgress();
				prog.BeginPhase(Phase.Safemode);
				prog.SetTotal(Phase.Safemode, StepAwaitingReportedBlocks, GetCompleteBlocksTotal(
					));
				SetBlockTotal();
				blockManager.Activate(conf);
			}
			finally
			{
				WriteUnlock();
			}
			RegisterMXBean();
			DefaultMetricsSystem.Instance().Register(this);
			if (inodeAttributeProvider != null)
			{
				inodeAttributeProvider.Start();
				dir.SetINodeAttributeProvider(inodeAttributeProvider);
			}
			snapshotManager.RegisterMXBean();
		}

		/// <summary>Stop services common to both active and standby states</summary>
		internal virtual void StopCommonServices()
		{
			WriteLock();
			if (inodeAttributeProvider != null)
			{
				dir.SetINodeAttributeProvider(null);
				inodeAttributeProvider.Stop();
			}
			try
			{
				if (blockManager != null)
				{
					blockManager.Close();
				}
			}
			finally
			{
				WriteUnlock();
			}
			RetryCache.Clear(retryCache);
		}

		/// <summary>Start services required in active state</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void StartActiveServices()
		{
			startingActiveService = true;
			Log.Info("Starting services required for active state");
			WriteLock();
			try
			{
				FSEditLog editLog = GetFSImage().GetEditLog();
				if (!editLog.IsOpenForWrite())
				{
					// During startup, we're already open for write during initialization.
					editLog.InitJournalsForWrite();
					// May need to recover
					editLog.RecoverUnclosedStreams();
					Log.Info("Catching up to latest edits from old active before " + "taking over writer role in edits logs"
						);
					editLogTailer.CatchupDuringFailover();
					blockManager.SetPostponeBlocksFromFuture(false);
					blockManager.GetDatanodeManager().MarkAllDatanodesStale();
					blockManager.ClearQueues();
					blockManager.ProcessAllPendingDNMessages();
					// Only need to re-process the queue, If not in SafeMode.
					if (!IsInSafeMode())
					{
						Log.Info("Reprocessing replication and invalidation queues");
						InitializeReplQueues();
					}
					if (Log.IsDebugEnabled())
					{
						Log.Debug("NameNode metadata after re-processing " + "replication and invalidation queues during failover:\n"
							 + MetaSaveAsString());
					}
					long nextTxId = GetFSImage().GetLastAppliedTxId() + 1;
					Log.Info("Will take over writing edit logs at txnid " + nextTxId);
					editLog.SetNextTxId(nextTxId);
					GetFSImage().editLog.OpenForWrite();
				}
				// Enable quota checks.
				dir.EnableQuotaChecks();
				if (haEnabled)
				{
					// Renew all of the leases before becoming active.
					// This is because, while we were in standby mode,
					// the leases weren't getting renewed on this NN.
					// Give them all a fresh start here.
					leaseManager.RenewAllLeases();
				}
				leaseManager.StartMonitor();
				StartSecretManagerIfNecessary();
				//ResourceMonitor required only at ActiveNN. See HDFS-2914
				this.nnrmthread = new Daemon(new FSNamesystem.NameNodeResourceMonitor(this));
				nnrmthread.Start();
				nnEditLogRoller = new Daemon(new FSNamesystem.NameNodeEditLogRoller(this, editLogRollerThreshold
					, editLogRollerInterval));
				nnEditLogRoller.Start();
				if (lazyPersistFileScrubIntervalSec > 0)
				{
					lazyPersistFileScrubber = new Daemon(new FSNamesystem.LazyPersistFileScrubber(this
						, lazyPersistFileScrubIntervalSec));
					lazyPersistFileScrubber.Start();
				}
				cacheManager.StartMonitorThread();
				blockManager.GetDatanodeManager().SetShouldSendCachingCommands(true);
			}
			finally
			{
				startingActiveService = false;
				CheckSafeMode();
				WriteUnlock();
			}
		}

		/// <summary>Initialize replication queues.</summary>
		private void InitializeReplQueues()
		{
			Log.Info("initializing replication queues");
			blockManager.ProcessMisReplicatedBlocks();
			initializedReplQueues = true;
		}

		private bool InActiveState()
		{
			return haContext != null && haContext.GetState().GetServiceState() == HAServiceProtocol.HAServiceState
				.Active;
		}

		/// <returns>
		/// Whether the namenode is transitioning to active state and is in the
		/// middle of the
		/// <see cref="StartActiveServices()"/>
		/// </returns>
		public virtual bool InTransitionToActive()
		{
			return haEnabled && InActiveState() && startingActiveService;
		}

		private bool ShouldUseDelegationTokens()
		{
			return UserGroupInformation.IsSecurityEnabled() || alwaysUseDelegationTokensForTests;
		}

		/// <summary>Stop services required in active state</summary>
		internal virtual void StopActiveServices()
		{
			Log.Info("Stopping services started for active state");
			WriteLock();
			try
			{
				StopSecretManager();
				leaseManager.StopMonitor();
				if (nnrmthread != null)
				{
					((FSNamesystem.NameNodeResourceMonitor)nnrmthread.GetRunnable()).StopMonitor();
					nnrmthread.Interrupt();
				}
				if (nnEditLogRoller != null)
				{
					((FSNamesystem.NameNodeEditLogRoller)nnEditLogRoller.GetRunnable()).Stop();
					nnEditLogRoller.Interrupt();
				}
				if (lazyPersistFileScrubber != null)
				{
					((FSNamesystem.LazyPersistFileScrubber)lazyPersistFileScrubber.GetRunnable()).Stop
						();
					lazyPersistFileScrubber.Interrupt();
				}
				if (dir != null && GetFSImage() != null)
				{
					if (GetFSImage().editLog != null)
					{
						GetFSImage().editLog.Close();
					}
					// Update the fsimage with the last txid that we wrote
					// so that the tailer starts from the right spot.
					GetFSImage().UpdateLastAppliedTxIdFromWritten();
				}
				if (cacheManager != null)
				{
					cacheManager.StopMonitorThread();
					cacheManager.ClearDirectiveStats();
				}
				blockManager.GetDatanodeManager().ClearPendingCachingCommands();
				blockManager.GetDatanodeManager().SetShouldSendCachingCommands(false);
				// Don't want to keep replication queues when not in Active.
				blockManager.ClearQueues();
				initializedReplQueues = false;
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <summary>Start services required in standby state</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void StartStandbyServices(Configuration conf)
		{
			Log.Info("Starting services required for standby state");
			if (!GetFSImage().editLog.IsOpenForRead())
			{
				// During startup, we're already open for read.
				GetFSImage().editLog.InitSharedJournalsForRead();
			}
			blockManager.SetPostponeBlocksFromFuture(true);
			// Disable quota checks while in standby.
			dir.DisableQuotaChecks();
			editLogTailer = new EditLogTailer(this, conf);
			editLogTailer.Start();
			if (standbyShouldCheckpoint)
			{
				standbyCheckpointer = new StandbyCheckpointer(conf, this);
				standbyCheckpointer.Start();
			}
		}

		/// <summary>
		/// Called when the NN is in Standby state and the editlog tailer tails the
		/// OP_ROLLING_UPGRADE_START.
		/// </summary>
		internal virtual void TriggerRollbackCheckpoint()
		{
			SetNeedRollbackFsImage(true);
			if (standbyCheckpointer != null)
			{
				standbyCheckpointer.TriggerRollbackCheckpoint();
			}
		}

		/// <summary>
		/// Called while the NN is in Standby state, but just about to be
		/// asked to enter Active state.
		/// </summary>
		/// <remarks>
		/// Called while the NN is in Standby state, but just about to be
		/// asked to enter Active state. This cancels any checkpoints
		/// currently being taken.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		internal virtual void PrepareToStopStandbyServices()
		{
			if (standbyCheckpointer != null)
			{
				standbyCheckpointer.CancelAndPreventCheckpoints("About to leave standby state");
			}
		}

		/// <summary>Stop services required in standby state</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void StopStandbyServices()
		{
			Log.Info("Stopping services started for standby state");
			if (standbyCheckpointer != null)
			{
				standbyCheckpointer.Stop();
			}
			if (editLogTailer != null)
			{
				editLogTailer.Stop();
			}
			if (dir != null && GetFSImage() != null && GetFSImage().editLog != null)
			{
				GetFSImage().editLog.Close();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		public virtual void CheckOperation(NameNode.OperationCategory op)
		{
			if (haContext != null)
			{
				// null in some unit tests
				haContext.CheckOperation(op);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Ipc.RetriableException">
		/// If 1) The NameNode is in SafeMode, 2) HA is enabled, and 3)
		/// NameNode is in active state
		/// </exception>
		/// <exception cref="SafeModeException">Otherwise if NameNode is in SafeMode.</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		internal virtual void CheckNameNodeSafeMode(string errorMsg)
		{
			if (IsInSafeMode())
			{
				SafeModeException se = new SafeModeException(errorMsg, safeMode);
				if (haEnabled && haContext != null && haContext.GetState().GetServiceState() == HAServiceProtocol.HAServiceState
					.Active && ShouldRetrySafeMode(this.safeMode))
				{
					throw new RetriableException(se);
				}
				else
				{
					throw se;
				}
			}
		}

		internal virtual bool IsPermissionEnabled()
		{
			return isPermissionEnabled;
		}

		/// <summary>We already know that the safemode is on.</summary>
		/// <remarks>
		/// We already know that the safemode is on. We will throw a RetriableException
		/// if the safemode is not manual or caused by low resource.
		/// </remarks>
		private bool ShouldRetrySafeMode(FSNamesystem.SafeModeInfo safeMode)
		{
			if (safeMode == null)
			{
				return false;
			}
			else
			{
				return !safeMode.IsManual() && !safeMode.AreResourcesLow();
			}
		}

		public static ICollection<URI> GetNamespaceDirs(Configuration conf)
		{
			return GetStorageDirs(conf, DFSConfigKeys.DfsNamenodeNameDirKey);
		}

		/// <summary>Get all edits dirs which are required.</summary>
		/// <remarks>
		/// Get all edits dirs which are required. If any shared edits dirs are
		/// configured, these are also included in the set of required dirs.
		/// </remarks>
		/// <param name="conf">the HDFS configuration.</param>
		/// <returns>all required dirs.</returns>
		public static ICollection<URI> GetRequiredNamespaceEditsDirs(Configuration conf)
		{
			ICollection<URI> ret = new HashSet<URI>();
			Sharpen.Collections.AddAll(ret, GetStorageDirs(conf, DFSConfigKeys.DfsNamenodeEditsDirRequiredKey
				));
			Sharpen.Collections.AddAll(ret, GetSharedEditsDirs(conf));
			return ret;
		}

		private static ICollection<URI> GetStorageDirs(Configuration conf, string propertyName
			)
		{
			ICollection<string> dirNames = conf.GetTrimmedStringCollection(propertyName);
			HdfsServerConstants.StartupOption startOpt = NameNode.GetStartupOption(conf);
			if (startOpt == HdfsServerConstants.StartupOption.Import)
			{
				// In case of IMPORT this will get rid of default directories 
				// but will retain directories specified in hdfs-site.xml
				// When importing image from a checkpoint, the name-node can
				// start with empty set of storage directories.
				Configuration cE = new HdfsConfiguration(false);
				cE.AddResource("core-default.xml");
				cE.AddResource("core-site.xml");
				cE.AddResource("hdfs-default.xml");
				ICollection<string> dirNames2 = cE.GetTrimmedStringCollection(propertyName);
				dirNames.RemoveAll(dirNames2);
				if (dirNames.IsEmpty())
				{
					Log.Warn("!!! WARNING !!!" + "\n\tThe NameNode currently runs without persistent storage."
						 + "\n\tAny changes to the file system meta-data may be lost." + "\n\tRecommended actions:"
						 + "\n\t\t- shutdown and restart NameNode with configured \"" + propertyName + "\" in hdfs-site.xml;"
						 + "\n\t\t- use Backup Node as a persistent and up-to-date storage " + "of the file system meta-data."
						);
				}
			}
			else
			{
				if (dirNames.IsEmpty())
				{
					dirNames = Sharpen.Collections.SingletonList(DFSConfigKeys.DfsNamenodeEditsDirDefault
						);
				}
			}
			return Org.Apache.Hadoop.Hdfs.Server.Common.Util.StringCollectionAsURIs(dirNames);
		}

		/// <summary>Return an ordered list of edits directories to write to.</summary>
		/// <remarks>
		/// Return an ordered list of edits directories to write to.
		/// The list is ordered such that all shared edits directories
		/// are ordered before non-shared directories, and any duplicates
		/// are removed. The order they are specified in the configuration
		/// is retained.
		/// </remarks>
		/// <returns>Collection of shared edits directories.</returns>
		/// <exception cref="System.IO.IOException">if multiple shared edits directories are configured
		/// 	</exception>
		public static IList<URI> GetNamespaceEditsDirs(Configuration conf)
		{
			return GetNamespaceEditsDirs(conf, true);
		}

		/// <exception cref="System.IO.IOException"/>
		public static IList<URI> GetNamespaceEditsDirs(Configuration conf, bool includeShared
			)
		{
			// Use a LinkedHashSet so that order is maintained while we de-dup
			// the entries.
			LinkedHashSet<URI> editsDirs = new LinkedHashSet<URI>();
			if (includeShared)
			{
				IList<URI> sharedDirs = GetSharedEditsDirs(conf);
				// Fail until multiple shared edits directories are supported (HDFS-2782)
				if (sharedDirs.Count > 1)
				{
					throw new IOException("Multiple shared edits directories are not yet supported");
				}
				// First add the shared edits dirs. It's critical that the shared dirs
				// are added first, since JournalSet syncs them in the order they are listed,
				// and we need to make sure all edits are in place in the shared storage
				// before they are replicated locally. See HDFS-2874.
				foreach (URI dir in sharedDirs)
				{
					if (!editsDirs.AddItem(dir))
					{
						Log.Warn("Edits URI " + dir + " listed multiple times in " + DFSConfigKeys.DfsNamenodeSharedEditsDirKey
							 + ". Ignoring duplicates.");
					}
				}
			}
			// Now add the non-shared dirs.
			foreach (URI dir_1 in GetStorageDirs(conf, DFSConfigKeys.DfsNamenodeEditsDirKey))
			{
				if (!editsDirs.AddItem(dir_1))
				{
					Log.Warn("Edits URI " + dir_1 + " listed multiple times in " + DFSConfigKeys.DfsNamenodeSharedEditsDirKey
						 + " and " + DFSConfigKeys.DfsNamenodeEditsDirKey + ". Ignoring duplicates.");
				}
			}
			if (editsDirs.IsEmpty())
			{
				// If this is the case, no edit dirs have been explicitly configured.
				// Image dirs are to be used for edits too.
				return Lists.NewArrayList(GetNamespaceDirs(conf));
			}
			else
			{
				return Lists.NewArrayList(editsDirs);
			}
		}

		/// <summary>Returns edit directories that are shared between primary and secondary.</summary>
		/// <param name="conf">configuration</param>
		/// <returns>
		/// collection of edit directories from
		/// <paramref name="conf"/>
		/// </returns>
		public static IList<URI> GetSharedEditsDirs(Configuration conf)
		{
			// don't use getStorageDirs here, because we want an empty default
			// rather than the dir in /tmp
			ICollection<string> dirNames = conf.GetTrimmedStringCollection(DFSConfigKeys.DfsNamenodeSharedEditsDirKey
				);
			return Org.Apache.Hadoop.Hdfs.Server.Common.Util.StringCollectionAsURIs(dirNames);
		}

		public virtual void ReadLock()
		{
			this.fsLock.ReadLock().Lock();
		}

		public virtual void ReadUnlock()
		{
			this.fsLock.ReadLock().Unlock();
		}

		public virtual void WriteLock()
		{
			this.fsLock.WriteLock().Lock();
		}

		/// <exception cref="System.Exception"/>
		public virtual void WriteLockInterruptibly()
		{
			this.fsLock.WriteLock().LockInterruptibly();
		}

		public virtual void WriteUnlock()
		{
			this.fsLock.WriteLock().Unlock();
		}

		public virtual bool HasWriteLock()
		{
			return this.fsLock.IsWriteLockedByCurrentThread();
		}

		public virtual bool HasReadLock()
		{
			return this.fsLock.GetReadHoldCount() > 0 || HasWriteLock();
		}

		public virtual int GetReadHoldCount()
		{
			return this.fsLock.GetReadHoldCount();
		}

		public virtual int GetWriteHoldCount()
		{
			return this.fsLock.GetWriteHoldCount();
		}

		/// <summary>Lock the checkpoint lock</summary>
		public virtual void CpLock()
		{
			this.cpLock.Lock();
		}

		/// <summary>Lock the checkpoint lock interrupibly</summary>
		/// <exception cref="System.Exception"/>
		public virtual void CpLockInterruptibly()
		{
			this.cpLock.LockInterruptibly();
		}

		/// <summary>Unlock the checkpoint lock</summary>
		public virtual void CpUnlock()
		{
			this.cpLock.Unlock();
		}

		internal virtual NamespaceInfo GetNamespaceInfo()
		{
			ReadLock();
			try
			{
				return UnprotectedGetNamespaceInfo();
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <summary>Version of @see #getNamespaceInfo() that is not protected by a lock.</summary>
		internal virtual NamespaceInfo UnprotectedGetNamespaceInfo()
		{
			return new NamespaceInfo(GetFSImage().GetStorage().GetNamespaceID(), GetClusterId
				(), GetBlockPoolId(), GetFSImage().GetStorage().GetCTime());
		}

		/// <summary>Close down this file system manager.</summary>
		/// <remarks>
		/// Close down this file system manager.
		/// Causes heartbeat and lease daemons to stop; waits briefly for
		/// them to finish, but a short timeout returns control back to caller.
		/// </remarks>
		internal virtual void Close()
		{
			fsRunning = false;
			try
			{
				StopCommonServices();
				if (smmthread != null)
				{
					smmthread.Interrupt();
				}
			}
			finally
			{
				// using finally to ensure we also wait for lease daemon
				try
				{
					StopActiveServices();
					StopStandbyServices();
				}
				catch (IOException)
				{
				}
				finally
				{
					IOUtils.Cleanup(Log, dir);
					IOUtils.Cleanup(Log, fsImage);
				}
			}
		}

		public virtual bool IsRunning()
		{
			return fsRunning;
		}

		public virtual bool IsInStandbyState()
		{
			if (haContext == null || haContext.GetState() == null)
			{
				// We're still starting up. In this case, if HA is
				// on for the cluster, we always start in standby. Otherwise
				// start in active.
				return haEnabled;
			}
			return HAServiceProtocol.HAServiceState.Standby == haContext.GetState().GetServiceState
				();
		}

		/// <summary>Dump all metadata into specified file</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void MetaSave(string filename)
		{
			CheckSuperuserPrivilege();
			CheckOperation(NameNode.OperationCategory.Unchecked);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Unchecked);
				FilePath file = new FilePath(Runtime.GetProperty("hadoop.log.dir"), filename);
				PrintWriter @out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new 
					FileOutputStream(file), Charsets.Utf8)));
				MetaSave(@out);
				@out.Flush();
				@out.Close();
			}
			finally
			{
				WriteUnlock();
			}
		}

		private void MetaSave(PrintWriter @out)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			long totalInodes = this.dir.TotalInodes();
			long totalBlocks = this.GetBlocksTotal();
			@out.WriteLine(totalInodes + " files and directories, " + totalBlocks + " blocks = "
				 + (totalInodes + totalBlocks) + " total");
			blockManager.MetaSave(@out);
		}

		private string MetaSaveAsString()
		{
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			MetaSave(pw);
			pw.Flush();
			return sw.ToString();
		}

		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		internal virtual FsServerDefaults GetServerDefaults()
		{
			CheckOperation(NameNode.OperationCategory.Read);
			return serverDefaults;
		}

		internal virtual long GetAccessTimePrecision()
		{
			return accessTimePrecision;
		}

		private bool IsAccessTimeSupported()
		{
			return accessTimePrecision > 0;
		}

		/////////////////////////////////////////////////////////
		//
		// These methods are called by HadoopFS clients
		//
		/////////////////////////////////////////////////////////
		/// <summary>Set permissions for an existing file.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetPermission(string src, FsPermission permission)
		{
			HdfsFileStatus auditStat;
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot set permission for " + src);
				auditStat = FSDirAttrOp.SetPermission(dir, src, permission);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "setPermission", src);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(true, "setPermission", src, null, auditStat);
		}

		/// <summary>Set owner for an existing file.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetOwner(string src, string username, string group)
		{
			HdfsFileStatus auditStat;
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot set owner for " + src);
				auditStat = FSDirAttrOp.SetOwner(dir, src, username, group);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "setOwner", src);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(true, "setOwner", src, null, auditStat);
		}

		internal class GetBlockLocationsResult
		{
			internal readonly bool updateAccessTime;

			internal readonly LocatedBlocks blocks;

			internal virtual bool UpdateAccessTime()
			{
				return updateAccessTime;
			}

			private GetBlockLocationsResult(bool updateAccessTime, LocatedBlocks blocks)
			{
				this.updateAccessTime = updateAccessTime;
				this.blocks = blocks;
			}
		}

		/// <summary>Get block locations within the specified range.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetBlockLocations(string, long, long)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual LocatedBlocks GetBlockLocations(string clientMachine, string srcArg
			, long offset, long length)
		{
			CheckOperation(NameNode.OperationCategory.Read);
			FSNamesystem.GetBlockLocationsResult res = null;
			FSPermissionChecker pc = GetPermissionChecker();
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				res = GetBlockLocations(pc, srcArg, offset, length, true, true);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "open", srcArg);
				throw;
			}
			finally
			{
				ReadUnlock();
			}
			LogAuditEvent(true, "open", srcArg);
			if (res.UpdateAccessTime())
			{
				byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(srcArg);
				string src = srcArg;
				WriteLock();
				long now = Time.Now();
				try
				{
					CheckOperation(NameNode.OperationCategory.Write);
					src = dir.ResolvePath(pc, srcArg, pathComponents);
					INodesInPath iip = dir.GetINodesInPath(src, true);
					INode inode = iip.GetLastINode();
					bool updateAccessTime = inode != null && now > inode.GetAccessTime() + GetAccessTimePrecision
						();
					if (!IsInSafeMode() && updateAccessTime)
					{
						bool changed = FSDirAttrOp.SetTimes(dir, inode, -1, now, false, iip.GetLatestSnapshotId
							());
						if (changed)
						{
							GetEditLog().LogTimes(src, -1, now);
						}
					}
				}
				catch (Exception e)
				{
					Log.Warn("Failed to update the access time of " + src, e);
				}
				finally
				{
					WriteUnlock();
				}
			}
			LocatedBlocks blocks = res.blocks;
			if (blocks != null)
			{
				blockManager.GetDatanodeManager().SortLocatedBlocks(clientMachine, blocks.GetLocatedBlocks
					());
				// lastBlock is not part of getLocatedBlocks(), might need to sort it too
				LocatedBlock lastBlock = blocks.GetLastLocatedBlock();
				if (lastBlock != null)
				{
					AList<LocatedBlock> lastBlockList = Lists.NewArrayList(lastBlock);
					blockManager.GetDatanodeManager().SortLocatedBlocks(clientMachine, lastBlockList);
				}
			}
			return blocks;
		}

		/// <summary>Get block locations within the specified range.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetBlockLocations(string, long, long)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FSNamesystem.GetBlockLocationsResult GetBlockLocations(FSPermissionChecker
			 pc, string src, long offset, long length, bool needBlockToken, bool checkSafeMode
			)
		{
			if (offset < 0)
			{
				throw new HadoopIllegalArgumentException("Negative offset is not supported. File: "
					 + src);
			}
			if (length < 0)
			{
				throw new HadoopIllegalArgumentException("Negative length is not supported. File: "
					 + src);
			}
			FSNamesystem.GetBlockLocationsResult ret = GetBlockLocationsInt(pc, src, offset, 
				length, needBlockToken);
			if (checkSafeMode && IsInSafeMode())
			{
				foreach (LocatedBlock b in ret.blocks.GetLocatedBlocks())
				{
					// if safemode & no block locations yet then throw safemodeException
					if ((b.GetLocations() == null) || (b.GetLocations().Length == 0))
					{
						SafeModeException se = new SafeModeException("Zero blocklocations for " + src, safeMode
							);
						if (haEnabled && haContext != null && haContext.GetState().GetServiceState() == HAServiceProtocol.HAServiceState
							.Active)
						{
							throw new RetriableException(se);
						}
						else
						{
							throw se;
						}
					}
				}
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		private FSNamesystem.GetBlockLocationsResult GetBlockLocationsInt(FSPermissionChecker
			 pc, string srcArg, long offset, long length, bool needBlockToken)
		{
			string src = srcArg;
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			src = dir.ResolvePath(pc, srcArg, pathComponents);
			INodesInPath iip = dir.GetINodesInPath(src, true);
			INodeFile inode = INodeFile.ValueOf(iip.GetLastINode(), src);
			if (isPermissionEnabled)
			{
				dir.CheckPathAccess(pc, iip, FsAction.Read);
				CheckUnreadableBySuperuser(pc, inode, iip.GetPathSnapshotId());
			}
			long fileSize = iip.IsSnapshot() ? inode.ComputeFileSize(iip.GetPathSnapshotId())
				 : inode.ComputeFileSizeNotIncludingLastUcBlock();
			bool isUc = inode.IsUnderConstruction();
			if (iip.IsSnapshot())
			{
				// if src indicates a snapshot file, we need to make sure the returned
				// blocks do not exceed the size of the snapshot file.
				length = Math.Min(length, fileSize - offset);
				isUc = false;
			}
			FileEncryptionInfo feInfo = FSDirectory.IsReservedRawName(srcArg) ? null : dir.GetFileEncryptionInfo
				(inode, iip.GetPathSnapshotId(), iip);
			LocatedBlocks blocks = blockManager.CreateLocatedBlocks(inode.GetBlocks(iip.GetPathSnapshotId
				()), fileSize, isUc, offset, length, needBlockToken, iip.IsSnapshot(), feInfo);
			// Set caching information for the located blocks.
			foreach (LocatedBlock lb in blocks.GetLocatedBlocks())
			{
				cacheManager.SetCachedLocations(lb);
			}
			long now = Time.Now();
			bool updateAccessTime = IsAccessTimeSupported() && !IsInSafeMode() && !iip.IsSnapshot
				() && now > inode.GetAccessTime() + GetAccessTimePrecision();
			return new FSNamesystem.GetBlockLocationsResult(updateAccessTime, blocks);
		}

		/// <summary>
		/// Moves all the blocks from
		/// <paramref name="srcs"/>
		/// and appends them to
		/// <paramref name="target"/>
		/// To avoid rollbacks we will verify validity of ALL of the args
		/// before we start actual move.
		/// This does not support ".inodes" relative path
		/// </summary>
		/// <param name="target">target to concat into</param>
		/// <param name="srcs">file that will be concatenated</param>
		/// <exception cref="System.IO.IOException">on error</exception>
		internal virtual void Concat(string target, string[] srcs, bool logRetryCache)
		{
			WaitForLoadingFSImage();
			HdfsFileStatus stat = null;
			bool success = false;
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot concat " + target);
				stat = FSDirConcatOp.Concat(dir, target, srcs, logRetryCache);
				success = true;
			}
			finally
			{
				WriteUnlock();
				if (success)
				{
					GetEditLog().LogSync();
				}
				LogAuditEvent(success, "concat", Arrays.ToString(srcs), target, stat);
			}
		}

		/// <summary>stores the modification and access time for this inode.</summary>
		/// <remarks>
		/// stores the modification and access time for this inode.
		/// The access time is precise up to an hour. The transaction, if needed, is
		/// written to the edits log but is not flushed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetTimes(string src, long mtime, long atime)
		{
			HdfsFileStatus auditStat;
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot set times " + src);
				auditStat = FSDirAttrOp.SetTimes(dir, src, mtime, atime);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "setTimes", src);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(true, "setTimes", src, null, auditStat);
		}

		/// <summary>Create a symbolic link.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CreateSymlink(string target, string link, PermissionStatus 
			dirPerms, bool createParent, bool logRetryCache)
		{
			if (!FileSystem.AreSymlinksEnabled())
			{
				throw new NotSupportedException("Symlinks not supported");
			}
			HdfsFileStatus auditStat = null;
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot create symlink " + link);
				auditStat = FSDirSymlinkOp.CreateSymlinkInt(this, target, link, dirPerms, createParent
					, logRetryCache);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "createSymlink", link, target, null);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(true, "createSymlink", link, target, auditStat);
		}

		/// <summary>Set replication for an existing file.</summary>
		/// <remarks>
		/// Set replication for an existing file.
		/// The NameNode sets new replication and schedules either replication of
		/// under-replicated data blocks or removal of the excessive block copies
		/// if the blocks are over-replicated.
		/// </remarks>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetReplication(string, short)
		/// 	"/>
		/// <param name="src">file name</param>
		/// <param name="replication">new replication</param>
		/// <returns>
		/// true if successful;
		/// false if file does not exist or is a directory
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool SetReplication(string src, short replication)
		{
			bool success = false;
			WaitForLoadingFSImage();
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot set replication for " + src);
				success = FSDirAttrOp.SetReplication(dir, blockManager, src, replication);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "setReplication", src);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			if (success)
			{
				GetEditLog().LogSync();
				LogAuditEvent(true, "setReplication", src);
			}
			return success;
		}

		/// <summary>Truncate file to a lower length.</summary>
		/// <remarks>
		/// Truncate file to a lower length.
		/// Truncate cannot be reverted / recovered from as it causes data loss.
		/// Truncation at block boundary is atomic, otherwise it requires
		/// block recovery to truncate the last block of the file.
		/// </remarks>
		/// <returns>
		/// true if client does not need to wait for block recovery,
		/// false if client needs to wait for block recovery.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		internal virtual bool Truncate(string src, long newLength, string clientName, string
			 clientMachine, long mtime)
		{
			bool ret;
			try
			{
				ret = TruncateInt(src, newLength, clientName, clientMachine, mtime);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "truncate", src);
				throw;
			}
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		internal virtual bool TruncateInt(string srcArg, long newLength, string clientName
			, string clientMachine, long mtime)
		{
			string src = srcArg;
			NameNode.stateChangeLog.Debug("DIR* NameSystem.truncate: src={} newLength={}", src
				, newLength);
			if (newLength < 0)
			{
				throw new HadoopIllegalArgumentException("Cannot truncate to a negative file size: "
					 + newLength + ".");
			}
			HdfsFileStatus stat = null;
			FSPermissionChecker pc = GetPermissionChecker();
			CheckOperation(NameNode.OperationCategory.Write);
			bool res;
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			WriteLock();
			INode.BlocksMapUpdateInfo toRemoveBlocks = new INode.BlocksMapUpdateInfo();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot truncate for " + src);
				src = dir.ResolvePath(pc, src, pathComponents);
				res = TruncateInternal(src, newLength, clientName, clientMachine, mtime, pc, toRemoveBlocks
					);
				stat = dir.GetAuditFileInfo(dir.GetINodesInPath4Write(src, false));
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			if (!toRemoveBlocks.GetToDeleteList().IsEmpty())
			{
				RemoveBlocks(toRemoveBlocks);
				toRemoveBlocks.Clear();
			}
			LogAuditEvent(true, "truncate", src, null, stat);
			return res;
		}

		/// <summary>
		/// Truncate a file to a given size
		/// Update the count at each ancestor directory with quota
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		internal virtual bool TruncateInternal(string src, long newLength, string clientName
			, string clientMachine, long mtime, FSPermissionChecker pc, INode.BlocksMapUpdateInfo
			 toRemoveBlocks)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			INodesInPath iip = dir.GetINodesInPath4Write(src, true);
			if (isPermissionEnabled)
			{
				dir.CheckPathAccess(pc, iip, FsAction.Write);
			}
			INodeFile file = INodeFile.ValueOf(iip.GetLastINode(), src);
			BlockStoragePolicy lpPolicy = blockManager.GetStoragePolicy("LAZY_PERSIST");
			if (lpPolicy != null && lpPolicy.GetId() == file.GetStoragePolicyID())
			{
				throw new NotSupportedException("Cannot truncate lazy persist file " + src);
			}
			// Check if the file is already being truncated with the same length
			BlockInfoContiguous last = file.GetLastBlock();
			if (last != null && last.GetBlockUCState() == HdfsServerConstants.BlockUCState.UnderRecovery)
			{
				Block truncateBlock = ((BlockInfoContiguousUnderConstruction)last).GetTruncateBlock
					();
				if (truncateBlock != null)
				{
					long truncateLength = file.ComputeFileSize(false, false) + truncateBlock.GetNumBytes
						();
					if (newLength == truncateLength)
					{
						return false;
					}
				}
			}
			// Opening an existing file for truncate. May need lease recovery.
			RecoverLeaseInternal(FSNamesystem.RecoverLeaseOp.TruncateFile, iip, src, clientName
				, clientMachine, false);
			// Truncate length check.
			long oldLength = file.ComputeFileSize();
			if (oldLength == newLength)
			{
				return true;
			}
			if (oldLength < newLength)
			{
				throw new HadoopIllegalArgumentException("Cannot truncate to a larger file size. Current size: "
					 + oldLength + ", truncate size: " + newLength + ".");
			}
			// Perform INodeFile truncation.
			QuotaCounts delta = new QuotaCounts.Builder().Build();
			bool onBlockBoundary = dir.Truncate(iip, newLength, toRemoveBlocks, mtime, delta);
			Block truncateBlock_1 = null;
			if (!onBlockBoundary)
			{
				// Open file for write, but don't log into edits
				long lastBlockDelta = file.ComputeFileSize() - newLength;
				System.Diagnostics.Debug.Assert(lastBlockDelta > 0, "delta is 0 only if on block bounday"
					);
				truncateBlock_1 = PrepareFileForTruncate(iip, clientName, clientMachine, lastBlockDelta
					, null);
			}
			// update the quota: use the preferred block size for UC block
			dir.WriteLock();
			try
			{
				dir.UpdateCountNoQuotaCheck(iip, iip.Length() - 1, delta);
			}
			finally
			{
				dir.WriteUnlock();
			}
			GetEditLog().LogTruncate(src, clientName, clientMachine, newLength, mtime, truncateBlock_1
				);
			return onBlockBoundary;
		}

		/// <summary>Convert current INode to UnderConstruction.</summary>
		/// <remarks>
		/// Convert current INode to UnderConstruction.
		/// Recreate lease.
		/// Create new block for the truncated copy.
		/// Schedule truncation of the replicas.
		/// </remarks>
		/// <returns>
		/// the returned block will be written to editLog and passed back into
		/// this method upon loading.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual Block PrepareFileForTruncate(INodesInPath iip, string leaseHolder
			, string clientMachine, long lastBlockDelta, Block newBlock)
		{
			INodeFile file = iip.GetLastINode().AsFile();
			string src = iip.GetPath();
			file.RecordModification(iip.GetLatestSnapshotId());
			file.ToUnderConstruction(leaseHolder, clientMachine);
			System.Diagnostics.Debug.Assert(file.IsUnderConstruction(), "inode should be under construction."
				);
			leaseManager.AddLease(file.GetFileUnderConstructionFeature().GetClientName(), src
				);
			bool shouldRecoverNow = (newBlock == null);
			BlockInfoContiguous oldBlock = file.GetLastBlock();
			bool shouldCopyOnTruncate = ShouldCopyOnTruncate(file, oldBlock);
			if (newBlock == null)
			{
				newBlock = (shouldCopyOnTruncate) ? CreateNewBlock() : new Block(oldBlock.GetBlockId
					(), oldBlock.GetNumBytes(), NextGenerationStamp(blockIdManager.IsLegacyBlock(oldBlock
					)));
			}
			BlockInfoContiguousUnderConstruction truncatedBlockUC;
			if (shouldCopyOnTruncate)
			{
				// Add new truncateBlock into blocksMap and
				// use oldBlock as a source for copy-on-truncate recovery
				truncatedBlockUC = new BlockInfoContiguousUnderConstruction(newBlock, file.GetBlockReplication
					());
				truncatedBlockUC.SetNumBytes(oldBlock.GetNumBytes() - lastBlockDelta);
				truncatedBlockUC.SetTruncateBlock(oldBlock);
				file.SetLastBlock(truncatedBlockUC, blockManager.GetStorages(oldBlock));
				GetBlockManager().AddBlockCollection(truncatedBlockUC, file);
				NameNode.stateChangeLog.Debug("BLOCK* prepareFileForTruncate: Scheduling copy-on-truncate to new"
					 + " size {}  new block {} old block {}", truncatedBlockUC.GetNumBytes(), newBlock
					, truncatedBlockUC.GetTruncateBlock());
			}
			else
			{
				// Use new generation stamp for in-place truncate recovery
				blockManager.ConvertLastBlockToUnderConstruction(file, lastBlockDelta);
				oldBlock = file.GetLastBlock();
				System.Diagnostics.Debug.Assert(!oldBlock.IsComplete(), "oldBlock should be under construction"
					);
				truncatedBlockUC = (BlockInfoContiguousUnderConstruction)oldBlock;
				truncatedBlockUC.SetTruncateBlock(new Block(oldBlock));
				truncatedBlockUC.GetTruncateBlock().SetNumBytes(oldBlock.GetNumBytes() - lastBlockDelta
					);
				truncatedBlockUC.GetTruncateBlock().SetGenerationStamp(newBlock.GetGenerationStamp
					());
				NameNode.stateChangeLog.Debug("BLOCK* prepareFileForTruncate: {} Scheduling in-place block "
					 + "truncate to new size {}", truncatedBlockUC.GetTruncateBlock().GetNumBytes(), 
					truncatedBlockUC);
			}
			if (shouldRecoverNow)
			{
				truncatedBlockUC.InitializeBlockRecovery(newBlock.GetGenerationStamp());
			}
			return newBlock;
		}

		/// <summary>
		/// Defines if a replica needs to be copied on truncate or
		/// can be truncated in place.
		/// </summary>
		internal virtual bool ShouldCopyOnTruncate(INodeFile file, BlockInfoContiguous blk
			)
		{
			if (!IsUpgradeFinalized())
			{
				return true;
			}
			if (IsRollingUpgrade())
			{
				return true;
			}
			return file.IsBlockInLatestSnapshot(blk);
		}

		/// <summary>Set the storage policy for a file or a directory.</summary>
		/// <param name="src">file/directory path</param>
		/// <param name="policyName">storage policy name</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetStoragePolicy(string src, string policyName)
		{
			HdfsFileStatus auditStat;
			WaitForLoadingFSImage();
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot set storage policy for " + src);
				auditStat = FSDirAttrOp.SetStoragePolicy(dir, blockManager, src, policyName);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "setStoragePolicy", src);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(true, "setStoragePolicy", src, null, auditStat);
		}

		/// <returns>All the existing block storage policies</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual BlockStoragePolicy[] GetStoragePolicies()
		{
			CheckOperation(NameNode.OperationCategory.Read);
			WaitForLoadingFSImage();
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				return FSDirAttrOp.GetStoragePolicies(blockManager);
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long GetPreferredBlockSize(string src)
		{
			CheckOperation(NameNode.OperationCategory.Read);
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				return FSDirAttrOp.GetPreferredBlockSize(dir, src);
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <summary>
		/// If the file is within an encryption zone, select the appropriate
		/// CryptoProtocolVersion from the list provided by the client.
		/// </summary>
		/// <remarks>
		/// If the file is within an encryption zone, select the appropriate
		/// CryptoProtocolVersion from the list provided by the client. Since the
		/// client may be newer, we need to handle unknown versions.
		/// </remarks>
		/// <param name="zone">EncryptionZone of the file</param>
		/// <param name="supportedVersions">List of supported protocol versions</param>
		/// <returns>chosen protocol version</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.UnknownCryptoProtocolVersionException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.SnapshotAccessControlException"/
		/// 	>
		private CryptoProtocolVersion ChooseProtocolVersion(EncryptionZone zone, CryptoProtocolVersion
			[] supportedVersions)
		{
			Preconditions.CheckNotNull(zone);
			Preconditions.CheckNotNull(supportedVersions);
			// Right now, we only support a single protocol version,
			// so simply look for it in the list of provided options
			CryptoProtocolVersion required = zone.GetVersion();
			foreach (CryptoProtocolVersion c in supportedVersions)
			{
				if (c.Equals(CryptoProtocolVersion.Unknown))
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Ignoring unknown CryptoProtocolVersion provided by " + "client: " + c.
							GetUnknownValue());
					}
					continue;
				}
				if (c.Equals(required))
				{
					return c;
				}
			}
			throw new UnknownCryptoProtocolVersionException("No crypto protocol versions provided by the client are supported."
				 + " Client provided: " + Arrays.ToString(supportedVersions) + " NameNode supports: "
				 + Arrays.ToString(CryptoProtocolVersion.Values()));
		}

		/// <summary>
		/// Invoke KeyProvider APIs to generate an encrypted data encryption key for an
		/// encryption zone.
		/// </summary>
		/// <remarks>
		/// Invoke KeyProvider APIs to generate an encrypted data encryption key for an
		/// encryption zone. Should not be called with any locks held.
		/// </remarks>
		/// <param name="ezKeyName">key name of an encryption zone</param>
		/// <returns>New EDEK, or null if ezKeyName is null</returns>
		/// <exception cref="System.IO.IOException"/>
		private KeyProviderCryptoExtension.EncryptedKeyVersion GenerateEncryptedDataEncryptionKey
			(string ezKeyName)
		{
			if (ezKeyName == null)
			{
				return null;
			}
			KeyProviderCryptoExtension.EncryptedKeyVersion edek = null;
			try
			{
				edek = provider.GenerateEncryptedKey(ezKeyName);
			}
			catch (GeneralSecurityException e)
			{
				throw new IOException(e);
			}
			Preconditions.CheckNotNull(edek);
			return edek;
		}

		/// <summary>Create a new file entry in the namespace.</summary>
		/// <remarks>
		/// Create a new file entry in the namespace.
		/// For description of parameters and exceptions thrown see
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Create(string, Org.Apache.Hadoop.FS.Permission.FsPermission, string, Org.Apache.Hadoop.IO.EnumSetWritable{E}, bool, short, long, Org.Apache.Hadoop.Crypto.CryptoProtocolVersion[])
		/// 	"/>
		/// , except it returns valid file status upon
		/// success
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual HdfsFileStatus StartFile(string src, PermissionStatus permissions
			, string holder, string clientMachine, EnumSet<CreateFlag> flag, bool createParent
			, short replication, long blockSize, CryptoProtocolVersion[] supportedVersions, 
			bool logRetryCache)
		{
			HdfsFileStatus status = null;
			try
			{
				status = StartFileInt(src, permissions, holder, clientMachine, flag, createParent
					, replication, blockSize, supportedVersions, logRetryCache);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "create", src);
				throw;
			}
			return status;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.FileAlreadyExistsException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.ParentNotDirectoryException"/>
		/// <exception cref="System.IO.IOException"/>
		private HdfsFileStatus StartFileInt(string srcArg, PermissionStatus permissions, 
			string holder, string clientMachine, EnumSet<CreateFlag> flag, bool createParent
			, short replication, long blockSize, CryptoProtocolVersion[] supportedVersions, 
			bool logRetryCache)
		{
			string src = srcArg;
			if (NameNode.stateChangeLog.IsDebugEnabled())
			{
				StringBuilder builder = new StringBuilder();
				builder.Append("DIR* NameSystem.startFile: src=" + src + ", holder=" + holder + ", clientMachine="
					 + clientMachine + ", createParent=" + createParent + ", replication=" + replication
					 + ", createFlag=" + flag.ToString() + ", blockSize=" + blockSize);
				builder.Append(", supportedVersions=");
				if (supportedVersions != null)
				{
					builder.Append(Arrays.ToString(supportedVersions));
				}
				else
				{
					builder.Append("null");
				}
				NameNode.stateChangeLog.Debug(builder.ToString());
			}
			if (!DFSUtil.IsValidName(src))
			{
				throw new InvalidPathException(src);
			}
			blockManager.VerifyReplication(src, replication, clientMachine);
			bool skipSync = false;
			HdfsFileStatus stat = null;
			FSPermissionChecker pc = GetPermissionChecker();
			if (blockSize < minBlockSize)
			{
				throw new IOException("Specified block size is less than configured" + " minimum value ("
					 + DFSConfigKeys.DfsNamenodeMinBlockSizeKey + "): " + blockSize + " < " + minBlockSize
					);
			}
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			bool create = flag.Contains(CreateFlag.Create);
			bool overwrite = flag.Contains(CreateFlag.Overwrite);
			bool isLazyPersist = flag.Contains(CreateFlag.LazyPersist);
			WaitForLoadingFSImage();
			CryptoProtocolVersion protocolVersion = null;
			CipherSuite suite = null;
			string ezKeyName = null;
			KeyProviderCryptoExtension.EncryptedKeyVersion edek = null;
			if (provider != null)
			{
				ReadLock();
				try
				{
					src = dir.ResolvePath(pc, src, pathComponents);
					INodesInPath iip = dir.GetINodesInPath4Write(src);
					// Nothing to do if the path is not within an EZ
					EncryptionZone zone = dir.GetEZForPath(iip);
					if (zone != null)
					{
						protocolVersion = ChooseProtocolVersion(zone, supportedVersions);
						suite = zone.GetSuite();
						ezKeyName = zone.GetKeyName();
						Preconditions.CheckNotNull(protocolVersion);
						Preconditions.CheckNotNull(suite);
						Preconditions.CheckArgument(!suite.Equals(CipherSuite.Unknown), "Chose an UNKNOWN CipherSuite!"
							);
						Preconditions.CheckNotNull(ezKeyName);
					}
				}
				finally
				{
					ReadUnlock();
				}
				Preconditions.CheckState((suite == null && ezKeyName == null) || (suite != null &&
					 ezKeyName != null), "Both suite and ezKeyName should both be null or not null");
				// Generate EDEK if necessary while not holding the lock
				edek = GenerateEncryptedDataEncryptionKey(ezKeyName);
				EncryptionFaultInjector.GetInstance().StartFileAfterGenerateKey();
			}
			// Proceed with the create, using the computed cipher suite and 
			// generated EDEK
			INode.BlocksMapUpdateInfo toRemoveBlocks = null;
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot create file" + src);
				dir.WriteLock();
				try
				{
					src = dir.ResolvePath(pc, src, pathComponents);
					INodesInPath iip = dir.GetINodesInPath4Write(src);
					toRemoveBlocks = StartFileInternal(pc, iip, permissions, holder, clientMachine, create
						, overwrite, createParent, replication, blockSize, isLazyPersist, suite, protocolVersion
						, edek, logRetryCache);
					stat = FSDirStatAndListingOp.GetFileInfo(dir, src, false, FSDirectory.IsReservedRawName
						(srcArg), true);
				}
				finally
				{
					dir.WriteUnlock();
				}
			}
			catch (StandbyException se)
			{
				skipSync = true;
				throw;
			}
			finally
			{
				WriteUnlock();
				// There might be transactions logged while trying to recover the lease.
				// They need to be sync'ed even when an exception was thrown.
				if (!skipSync)
				{
					GetEditLog().LogSync();
					if (toRemoveBlocks != null)
					{
						RemoveBlocks(toRemoveBlocks);
						toRemoveBlocks.Clear();
					}
				}
			}
			LogAuditEvent(true, "create", srcArg, null, stat);
			return stat;
		}

		/// <summary>
		/// Create a new file or overwrite an existing file<br />
		/// Once the file is create the client then allocates a new block with the next
		/// call using
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.AddBlock(string, string, Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, Org.Apache.Hadoop.Hdfs.Protocol.DatanodeInfo[], long, string[])
		/// 	"/>
		/// .
		/// <p>
		/// For description of parameters and exceptions thrown see
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Create(string, Org.Apache.Hadoop.FS.Permission.FsPermission, string, Org.Apache.Hadoop.IO.EnumSetWritable{E}, bool, short, long, Org.Apache.Hadoop.Crypto.CryptoProtocolVersion[])
		/// 	"/>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private INode.BlocksMapUpdateInfo StartFileInternal(FSPermissionChecker pc, INodesInPath
			 iip, PermissionStatus permissions, string holder, string clientMachine, bool create
			, bool overwrite, bool createParent, short replication, long blockSize, bool isLazyPersist
			, CipherSuite suite, CryptoProtocolVersion version, KeyProviderCryptoExtension.EncryptedKeyVersion
			 edek, bool logRetryEntry)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			// Verify that the destination does not exist as a directory already.
			INode inode = iip.GetLastINode();
			string src = iip.GetPath();
			if (inode != null && inode.IsDirectory())
			{
				throw new FileAlreadyExistsException(src + " already exists as a directory");
			}
			INodeFile myFile = INodeFile.ValueOf(inode, src, true);
			if (isPermissionEnabled)
			{
				if (overwrite && myFile != null)
				{
					dir.CheckPathAccess(pc, iip, FsAction.Write);
				}
				/*
				* To overwrite existing file, need to check 'w' permission
				* of parent (equals to ancestor in this case)
				*/
				dir.CheckAncestorAccess(pc, iip, FsAction.Write);
			}
			if (!createParent)
			{
				dir.VerifyParentDir(iip, src);
			}
			FileEncryptionInfo feInfo = null;
			EncryptionZone zone = dir.GetEZForPath(iip);
			if (zone != null)
			{
				// The path is now within an EZ, but we're missing encryption parameters
				if (suite == null || edek == null)
				{
					throw new RetryStartFileException();
				}
				// Path is within an EZ and we have provided encryption parameters.
				// Make sure that the generated EDEK matches the settings of the EZ.
				string ezKeyName = zone.GetKeyName();
				if (!ezKeyName.Equals(edek.GetEncryptionKeyName()))
				{
					throw new RetryStartFileException();
				}
				feInfo = new FileEncryptionInfo(suite, version, edek.GetEncryptedKeyVersion().GetMaterial
					(), edek.GetEncryptedKeyIv(), ezKeyName, edek.GetEncryptionKeyVersionName());
			}
			try
			{
				INode.BlocksMapUpdateInfo toRemoveBlocks = null;
				if (myFile == null)
				{
					if (!create)
					{
						throw new FileNotFoundException("Can't overwrite non-existent " + src + " for client "
							 + clientMachine);
					}
				}
				else
				{
					if (overwrite)
					{
						toRemoveBlocks = new INode.BlocksMapUpdateInfo();
						IList<INode> toRemoveINodes = new ChunkedArrayList<INode>();
						long ret = FSDirDeleteOp.Delete(dir, iip, toRemoveBlocks, toRemoveINodes, Time.Now
							());
						if (ret >= 0)
						{
							iip = INodesInPath.Replace(iip, iip.Length() - 1, null);
							FSDirDeleteOp.IncrDeletedFileCount(ret);
							RemoveLeasesAndINodes(src, toRemoveINodes, true);
						}
					}
					else
					{
						// If lease soft limit time is expired, recover the lease
						RecoverLeaseInternal(FSNamesystem.RecoverLeaseOp.CreateFile, iip, src, holder, clientMachine
							, false);
						throw new FileAlreadyExistsException(src + " for client " + clientMachine + " already exists"
							);
					}
				}
				CheckFsObjectLimit();
				INodeFile newNode = null;
				// Always do an implicit mkdirs for parent directory tree.
				KeyValuePair<INodesInPath, string> parent = FSDirMkdirOp.CreateAncestorDirectories
					(dir, iip, permissions);
				if (parent != null)
				{
					iip = dir.AddFile(parent.Key, parent.Value, permissions, replication, blockSize, 
						holder, clientMachine);
					newNode = iip != null ? iip.GetLastINode().AsFile() : null;
				}
				if (newNode == null)
				{
					throw new IOException("Unable to add " + src + " to namespace");
				}
				leaseManager.AddLease(newNode.GetFileUnderConstructionFeature().GetClientName(), 
					src);
				// Set encryption attributes if necessary
				if (feInfo != null)
				{
					dir.SetFileEncryptionInfo(src, feInfo);
					newNode = dir.GetInode(newNode.GetId()).AsFile();
				}
				SetNewINodeStoragePolicy(newNode, iip, isLazyPersist);
				// record file record in log, record new generation stamp
				GetEditLog().LogOpenFile(src, newNode, overwrite, logRetryEntry);
				NameNode.stateChangeLog.Debug("DIR* NameSystem.startFile: added {}" + " inode {} holder {}"
					, src, newNode.GetId(), holder);
				return toRemoveBlocks;
			}
			catch (IOException ie)
			{
				NameNode.stateChangeLog.Warn("DIR* NameSystem.startFile: " + src + " " + ie.Message
					);
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void SetNewINodeStoragePolicy(INodeFile inode, INodesInPath iip, bool isLazyPersist
			)
		{
			if (isLazyPersist)
			{
				BlockStoragePolicy lpPolicy = blockManager.GetStoragePolicy("LAZY_PERSIST");
				// Set LAZY_PERSIST storage policy if the flag was passed to
				// CreateFile.
				if (lpPolicy == null)
				{
					throw new HadoopIllegalArgumentException("The LAZY_PERSIST storage policy has been disabled "
						 + "by the administrator.");
				}
				inode.SetStoragePolicyID(lpPolicy.GetId(), iip.GetLatestSnapshotId());
			}
			else
			{
				BlockStoragePolicy effectivePolicy = blockManager.GetStoragePolicy(inode.GetStoragePolicyID
					());
				if (effectivePolicy != null && effectivePolicy.IsCopyOnCreateFile())
				{
					// Copy effective policy from ancestor directory to current file.
					inode.SetStoragePolicyID(effectivePolicy.GetId(), iip.GetLatestSnapshotId());
				}
			}
		}

		/// <summary>Append to an existing file for append.</summary>
		/// <remarks>
		/// Append to an existing file for append.
		/// <p>
		/// The method returns the last block of the file if this is a partial block,
		/// which can still be used for writing more data. The client uses the returned
		/// block locations to form the data pipeline for this block.<br />
		/// The method returns null if the last block is full. The client then
		/// allocates a new block with the next call using
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.AddBlock(string, string, Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, Org.Apache.Hadoop.Hdfs.Protocol.DatanodeInfo[], long, string[])
		/// 	"/>
		/// .
		/// <p>
		/// For description of parameters and exceptions thrown see
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Append(string, string, Org.Apache.Hadoop.IO.EnumSetWritable{E})
		/// 	"/>
		/// </remarks>
		/// <returns>the last block locations if the block is partial or null otherwise</returns>
		/// <exception cref="System.IO.IOException"/>
		private LocatedBlock AppendFileInternal(FSPermissionChecker pc, INodesInPath iip, 
			string holder, string clientMachine, bool newBlock, bool logRetryCache)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			// Verify that the destination does not exist as a directory already.
			INode inode = iip.GetLastINode();
			string src = iip.GetPath();
			if (inode != null && inode.IsDirectory())
			{
				throw new FileAlreadyExistsException("Cannot append to directory " + src + "; already exists as a directory."
					);
			}
			if (isPermissionEnabled)
			{
				dir.CheckPathAccess(pc, iip, FsAction.Write);
			}
			try
			{
				if (inode == null)
				{
					throw new FileNotFoundException("failed to append to non-existent file " + src + 
						" for client " + clientMachine);
				}
				INodeFile myFile = INodeFile.ValueOf(inode, src, true);
				BlockStoragePolicy lpPolicy = blockManager.GetStoragePolicy("LAZY_PERSIST");
				if (lpPolicy != null && lpPolicy.GetId() == myFile.GetStoragePolicyID())
				{
					throw new NotSupportedException("Cannot append to lazy persist file " + src);
				}
				// Opening an existing file for append - may need to recover lease.
				RecoverLeaseInternal(FSNamesystem.RecoverLeaseOp.AppendFile, iip, src, holder, clientMachine
					, false);
				BlockInfoContiguous lastBlock = myFile.GetLastBlock();
				// Check that the block has at least minimum replication.
				if (lastBlock != null && lastBlock.IsComplete() && !GetBlockManager().IsSufficientlyReplicated
					(lastBlock))
				{
					throw new IOException("append: lastBlock=" + lastBlock + " of src=" + src + " is not sufficiently replicated yet."
						);
				}
				return PrepareFileForAppend(src, iip, holder, clientMachine, newBlock, true, logRetryCache
					);
			}
			catch (IOException ie)
			{
				NameNode.stateChangeLog.Warn("DIR* NameSystem.append: " + ie.Message);
				throw;
			}
		}

		/// <summary>Convert current node to under construction.</summary>
		/// <remarks>
		/// Convert current node to under construction.
		/// Recreate in-memory lease record.
		/// </remarks>
		/// <param name="src">path to the file</param>
		/// <param name="leaseHolder">identifier of the lease holder on this file</param>
		/// <param name="clientMachine">identifier of the client machine</param>
		/// <param name="newBlock">if the data is appended to a new block</param>
		/// <param name="writeToEditLog">whether to persist this change to the edit log</param>
		/// <param name="logRetryCache">
		/// whether to record RPC ids in editlog for retry cache
		/// rebuilding
		/// </param>
		/// <returns>the last block locations if the block is partial or null otherwise</returns>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual LocatedBlock PrepareFileForAppend(string src, INodesInPath iip, 
			string leaseHolder, string clientMachine, bool newBlock, bool writeToEditLog, bool
			 logRetryCache)
		{
			INodeFile file = iip.GetLastINode().AsFile();
			QuotaCounts delta = VerifyQuotaForUCBlock(file, iip);
			file.RecordModification(iip.GetLatestSnapshotId());
			file.ToUnderConstruction(leaseHolder, clientMachine);
			leaseManager.AddLease(file.GetFileUnderConstructionFeature().GetClientName(), src
				);
			LocatedBlock ret = null;
			if (!newBlock)
			{
				ret = blockManager.ConvertLastBlockToUnderConstruction(file, 0);
				if (ret != null && delta != null)
				{
					Preconditions.CheckState(delta.GetStorageSpace() >= 0, "appending to a block with size larger than the preferred block size"
						);
					dir.WriteLock();
					try
					{
						dir.UpdateCountNoQuotaCheck(iip, iip.Length() - 1, delta);
					}
					finally
					{
						dir.WriteUnlock();
					}
				}
			}
			else
			{
				BlockInfoContiguous lastBlock = file.GetLastBlock();
				if (lastBlock != null)
				{
					ExtendedBlock blk = new ExtendedBlock(this.GetBlockPoolId(), lastBlock);
					ret = new LocatedBlock(blk, new DatanodeInfo[0]);
				}
			}
			if (writeToEditLog)
			{
				GetEditLog().LogAppendFile(src, file, newBlock, logRetryCache);
			}
			return ret;
		}

		/// <summary>Verify quota when using the preferred block size for UC block.</summary>
		/// <remarks>
		/// Verify quota when using the preferred block size for UC block. This is
		/// usually used by append and truncate
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException">when violating the storage quota
		/// 	</exception>
		/// <returns>
		/// expected quota usage update. null means no change or no need to
		/// update quota usage later
		/// </returns>
		private QuotaCounts VerifyQuotaForUCBlock(INodeFile file, INodesInPath iip)
		{
			if (!IsImageLoaded() || dir.ShouldSkipQuotaChecks())
			{
				// Do not check quota if editlog is still being processed
				return null;
			}
			if (file.GetLastBlock() != null)
			{
				QuotaCounts delta = ComputeQuotaDeltaForUCBlock(file);
				dir.ReadLock();
				try
				{
					FSDirectory.VerifyQuota(iip, iip.Length() - 1, delta, null);
					return delta;
				}
				finally
				{
					dir.ReadUnlock();
				}
			}
			return null;
		}

		/// <summary>Compute quota change for converting a complete block to a UC block</summary>
		private QuotaCounts ComputeQuotaDeltaForUCBlock(INodeFile file)
		{
			QuotaCounts delta = new QuotaCounts.Builder().Build();
			BlockInfoContiguous lastBlock = file.GetLastBlock();
			if (lastBlock != null)
			{
				long diff = file.GetPreferredBlockSize() - lastBlock.GetNumBytes();
				short repl = file.GetBlockReplication();
				delta.AddStorageSpace(diff * repl);
				BlockStoragePolicy policy = dir.GetBlockStoragePolicySuite().GetPolicy(file.GetStoragePolicyID
					());
				IList<StorageType> types = policy.ChooseStorageTypes(repl);
				foreach (StorageType t in types)
				{
					if (t.SupportTypeQuota())
					{
						delta.AddTypeSpace(t, diff);
					}
				}
			}
			return delta;
		}

		/// <summary>
		/// Recover lease;
		/// Immediately revoke the lease of the current lease holder and start lease
		/// recovery so that the file can be forced to be closed.
		/// </summary>
		/// <param name="src">the path of the file to start lease recovery</param>
		/// <param name="holder">the lease holder's name</param>
		/// <param name="clientMachine">the client machine's name</param>
		/// <returns>
		/// true if the file is already closed or
		/// if the lease can be released and the file can be closed.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool RecoverLease(string src, string holder, string clientMachine
			)
		{
			if (!DFSUtil.IsValidName(src))
			{
				throw new IOException("Invalid file name: " + src);
			}
			bool skipSync = false;
			FSPermissionChecker pc = GetPermissionChecker();
			CheckOperation(NameNode.OperationCategory.Write);
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot recover the lease of " + src);
				src = dir.ResolvePath(pc, src, pathComponents);
				INodesInPath iip = dir.GetINodesInPath4Write(src);
				INodeFile inode = INodeFile.ValueOf(iip.GetLastINode(), src);
				if (!inode.IsUnderConstruction())
				{
					return true;
				}
				if (isPermissionEnabled)
				{
					dir.CheckPathAccess(pc, iip, FsAction.Write);
				}
				return RecoverLeaseInternal(FSNamesystem.RecoverLeaseOp.RecoverLease, iip, src, holder
					, clientMachine, true);
			}
			catch (StandbyException se)
			{
				skipSync = true;
				throw;
			}
			finally
			{
				WriteUnlock();
				// There might be transactions logged while trying to recover the lease.
				// They need to be sync'ed even when an exception was thrown.
				if (!skipSync)
				{
					GetEditLog().LogSync();
				}
			}
		}

		[System.Serializable]
		private sealed class RecoverLeaseOp
		{
			public static readonly FSNamesystem.RecoverLeaseOp CreateFile = new FSNamesystem.RecoverLeaseOp
				();

			public static readonly FSNamesystem.RecoverLeaseOp AppendFile = new FSNamesystem.RecoverLeaseOp
				();

			public static readonly FSNamesystem.RecoverLeaseOp TruncateFile = new FSNamesystem.RecoverLeaseOp
				();

			public static readonly FSNamesystem.RecoverLeaseOp RecoverLease = new FSNamesystem.RecoverLeaseOp
				();

			private string GetExceptionMessage(string src, string holder, string clientMachine
				, string reason)
			{
				return "Failed to " + this + " " + src + " for " + holder + " on " + clientMachine
					 + " because " + reason;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual bool RecoverLeaseInternal(FSNamesystem.RecoverLeaseOp op, INodesInPath
			 iip, string src, string holder, string clientMachine, bool force)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			INodeFile file = iip.GetLastINode().AsFile();
			if (file.IsUnderConstruction())
			{
				//
				// If the file is under construction , then it must be in our
				// leases. Find the appropriate lease record.
				//
				LeaseManager.Lease lease = leaseManager.GetLease(holder);
				if (!force && lease != null)
				{
					LeaseManager.Lease leaseFile = leaseManager.GetLeaseByPath(src);
					if (leaseFile != null && leaseFile.Equals(lease))
					{
						// We found the lease for this file but the original
						// holder is trying to obtain it again.
						throw new AlreadyBeingCreatedException(op.GetExceptionMessage(src, holder, clientMachine
							, holder + " is already the current lease holder."));
					}
				}
				//
				// Find the original holder.
				//
				FileUnderConstructionFeature uc = file.GetFileUnderConstructionFeature();
				string clientName = uc.GetClientName();
				lease = leaseManager.GetLease(clientName);
				if (lease == null)
				{
					throw new AlreadyBeingCreatedException(op.GetExceptionMessage(src, holder, clientMachine
						, "the file is under construction but no leases found."));
				}
				if (force)
				{
					// close now: no need to wait for soft lease expiration and 
					// close only the file src
					Log.Info("recoverLease: " + lease + ", src=" + src + " from client " + clientName
						);
					return InternalReleaseLease(lease, src, iip, holder);
				}
				else
				{
					System.Diagnostics.Debug.Assert(lease.GetHolder().Equals(clientName), "Current lease holder "
						 + lease.GetHolder() + " does not match file creator " + clientName);
					//
					// If the original holder has not renewed in the last SOFTLIMIT 
					// period, then start lease recovery.
					//
					if (lease.ExpiredSoftLimit())
					{
						Log.Info("startFile: recover " + lease + ", src=" + src + " client " + clientName
							);
						if (InternalReleaseLease(lease, src, iip, null))
						{
							return true;
						}
						else
						{
							throw new RecoveryInProgressException(op.GetExceptionMessage(src, holder, clientMachine
								, "lease recovery is in progress. Try again later."));
						}
					}
					else
					{
						BlockInfoContiguous lastBlock = file.GetLastBlock();
						if (lastBlock != null && lastBlock.GetBlockUCState() == HdfsServerConstants.BlockUCState
							.UnderRecovery)
						{
							throw new RecoveryInProgressException(op.GetExceptionMessage(src, holder, clientMachine
								, "another recovery is in progress by " + clientName + " on " + uc.GetClientMachine
								()));
						}
						else
						{
							throw new AlreadyBeingCreatedException(op.GetExceptionMessage(src, holder, clientMachine
								, "this file lease is currently owned by " + clientName + " on " + uc.GetClientMachine
								()));
						}
					}
				}
			}
			else
			{
				return true;
			}
		}

		/// <summary>Append to an existing file in the namespace.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual LastBlockWithStatus AppendFile(string src, string holder, string
			 clientMachine, EnumSet<CreateFlag> flag, bool logRetryCache)
		{
			try
			{
				return AppendFileInt(src, holder, clientMachine, flag.Contains(CreateFlag.NewBlock
					), logRetryCache);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "append", src);
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private LastBlockWithStatus AppendFileInt(string srcArg, string holder, string clientMachine
			, bool newBlock, bool logRetryCache)
		{
			string src = srcArg;
			NameNode.stateChangeLog.Debug("DIR* NameSystem.appendFile: src={}, holder={}, clientMachine={}"
				, src, holder, clientMachine);
			bool skipSync = false;
			if (!supportAppends)
			{
				throw new NotSupportedException("Append is not enabled on this NameNode. Use the "
					 + DFSConfigKeys.DfsSupportAppendKey + " configuration option to enable it.");
			}
			LocatedBlock lb = null;
			HdfsFileStatus stat = null;
			FSPermissionChecker pc = GetPermissionChecker();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot append to file" + src);
				src = dir.ResolvePath(pc, src, pathComponents);
				INodesInPath iip = dir.GetINodesInPath4Write(src);
				lb = AppendFileInternal(pc, iip, holder, clientMachine, newBlock, logRetryCache);
				stat = FSDirStatAndListingOp.GetFileInfo(dir, src, false, FSDirectory.IsReservedRawName
					(srcArg), true);
			}
			catch (StandbyException se)
			{
				skipSync = true;
				throw;
			}
			finally
			{
				WriteUnlock();
				// There might be transactions logged while trying to recover the lease.
				// They need to be sync'ed even when an exception was thrown.
				if (!skipSync)
				{
					GetEditLog().LogSync();
				}
			}
			if (lb != null)
			{
				NameNode.stateChangeLog.Debug("DIR* NameSystem.appendFile: file {} for {} at {} block {} block"
					 + " size {}", src, holder, clientMachine, lb.GetBlock(), lb.GetBlock().GetNumBytes
					());
			}
			LogAuditEvent(true, "append", srcArg);
			return new LastBlockWithStatus(lb, stat);
		}

		internal virtual ExtendedBlock GetExtendedBlock(Block blk)
		{
			return new ExtendedBlock(blockPoolId, blk);
		}

		internal virtual void SetBlockPoolId(string bpid)
		{
			blockPoolId = bpid;
			blockManager.SetBlockPoolId(blockPoolId);
		}

		/// <summary>
		/// The client would like to obtain an additional block for the indicated
		/// filename (which is being written-to).
		/// </summary>
		/// <remarks>
		/// The client would like to obtain an additional block for the indicated
		/// filename (which is being written-to).  Return an array that consists
		/// of the block, plus a set of machines.  The first on this list should
		/// be where the client writes data.  Subsequent items in the list must
		/// be provided in the connection to the first datanode.
		/// Make sure the previous blocks have been reported by datanodes and
		/// are replicated.  Will return an empty 2-elt array if we want the
		/// client to "try again later".
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual LocatedBlock GetAdditionalBlock(string src, long fileId, string 
			clientName, ExtendedBlock previous, ICollection<Node> excludedNodes, IList<string
			> favoredNodes)
		{
			LocatedBlock[] onRetryBlock = new LocatedBlock[1];
			DatanodeStorageInfo[] targets = GetNewBlockTargets(src, fileId, clientName, previous
				, excludedNodes, favoredNodes, onRetryBlock);
			if (targets == null)
			{
				System.Diagnostics.Debug.Assert(onRetryBlock[0] != null, "Retry block is null");
				// This is a retry. Just return the last block.
				return onRetryBlock[0];
			}
			LocatedBlock newBlock = StoreAllocatedBlock(src, fileId, clientName, previous, targets
				);
			return newBlock;
		}

		/// <summary>Part I of getAdditionalBlock().</summary>
		/// <remarks>
		/// Part I of getAdditionalBlock().
		/// Analyze the state of the file under read lock to determine if the client
		/// can add a new block, detect potential retries, lease mismatches,
		/// and minimal replication of the penultimate block.
		/// Generate target DataNode locations for the new block,
		/// but do not create the new block yet.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual DatanodeStorageInfo[] GetNewBlockTargets(string src, long fileId
			, string clientName, ExtendedBlock previous, ICollection<Node> excludedNodes, IList
			<string> favoredNodes, LocatedBlock[] onRetryBlock)
		{
			long blockSize;
			int replication;
			byte storagePolicyID;
			Node clientNode = null;
			string clientMachine = null;
			NameNode.stateChangeLog.Debug("BLOCK* getAdditionalBlock: {}  inodeId {}" + " for {}"
				, src, fileId, clientName);
			CheckOperation(NameNode.OperationCategory.Read);
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			FSPermissionChecker pc = GetPermissionChecker();
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				src = dir.ResolvePath(pc, src, pathComponents);
				FSNamesystem.FileState fileState = AnalyzeFileState(src, fileId, clientName, previous
					, onRetryBlock);
				INodeFile pendingFile = fileState.inode;
				// Check if the penultimate block is minimally replicated
				if (!CheckFileProgress(src, pendingFile, false))
				{
					throw new NotReplicatedYetException("Not replicated yet: " + src);
				}
				src = fileState.path;
				if (onRetryBlock[0] != null && onRetryBlock[0].GetLocations().Length > 0)
				{
					// This is a retry. No need to generate new locations.
					// Use the last block if it has locations.
					return null;
				}
				if (pendingFile.GetBlocks().Length >= maxBlocksPerFile)
				{
					throw new IOException("File has reached the limit on maximum number of" + " blocks ("
						 + DFSConfigKeys.DfsNamenodeMaxBlocksPerFileKey + "): " + pendingFile.GetBlocks(
						).Length + " >= " + maxBlocksPerFile);
				}
				blockSize = pendingFile.GetPreferredBlockSize();
				clientMachine = pendingFile.GetFileUnderConstructionFeature().GetClientMachine();
				clientNode = blockManager.GetDatanodeManager().GetDatanodeByHost(clientMachine);
				replication = pendingFile.GetFileReplication();
				storagePolicyID = pendingFile.GetStoragePolicyID();
			}
			finally
			{
				ReadUnlock();
			}
			if (clientNode == null)
			{
				clientNode = GetClientNode(clientMachine);
			}
			// choose targets for the new block to be allocated.
			return GetBlockManager().ChooseTarget4NewBlock(src, replication, clientNode, excludedNodes
				, blockSize, favoredNodes, storagePolicyID);
		}

		/// <summary>Part II of getAdditionalBlock().</summary>
		/// <remarks>
		/// Part II of getAdditionalBlock().
		/// Should repeat the same analysis of the file state as in Part 1,
		/// but under the write lock.
		/// If the conditions still hold, then allocate a new block with
		/// the new targets, add it to the INode and to the BlocksMap.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual LocatedBlock StoreAllocatedBlock(string src, long fileId, string
			 clientName, ExtendedBlock previous, DatanodeStorageInfo[] targets)
		{
			Block newBlock = null;
			long offset;
			CheckOperation(NameNode.OperationCategory.Write);
			WaitForLoadingFSImage();
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				// Run the full analysis again, since things could have changed
				// while chooseTarget() was executing.
				LocatedBlock[] onRetryBlock = new LocatedBlock[1];
				FSNamesystem.FileState fileState = AnalyzeFileState(src, fileId, clientName, previous
					, onRetryBlock);
				INodeFile pendingFile = fileState.inode;
				src = fileState.path;
				if (onRetryBlock[0] != null)
				{
					if (onRetryBlock[0].GetLocations().Length > 0)
					{
						// This is a retry. Just return the last block if having locations.
						return onRetryBlock[0];
					}
					else
					{
						// add new chosen targets to already allocated block and return
						BlockInfoContiguous lastBlockInFile = pendingFile.GetLastBlock();
						((BlockInfoContiguousUnderConstruction)lastBlockInFile).SetExpectedLocations(targets
							);
						offset = pendingFile.ComputeFileSize();
						return MakeLocatedBlock(lastBlockInFile, targets, offset);
					}
				}
				// commit the last block and complete it if it has minimum replicas
				CommitOrCompleteLastBlock(pendingFile, fileState.iip, ExtendedBlock.GetLocalBlock
					(previous));
				// allocate new block, record block locations in INode.
				newBlock = CreateNewBlock();
				INodesInPath inodesInPath = INodesInPath.FromINode(pendingFile);
				SaveAllocatedBlock(src, inodesInPath, newBlock, targets);
				PersistNewBlock(src, pendingFile);
				offset = pendingFile.ComputeFileSize();
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			// Return located block
			return MakeLocatedBlock(newBlock, targets, offset);
		}

		/*
		* Resolve clientmachine address to get a network location path
		*/
		private Node GetClientNode(string clientMachine)
		{
			IList<string> hosts = new AList<string>(1);
			hosts.AddItem(clientMachine);
			IList<string> rName = GetBlockManager().GetDatanodeManager().ResolveNetworkLocation
				(hosts);
			Node clientNode = null;
			if (rName != null)
			{
				// Able to resolve clientMachine mapping.
				// Create a temp node to findout the rack local nodes
				clientNode = new NodeBase(rName[0] + NodeBase.PathSeparatorStr + clientMachine);
			}
			return clientNode;
		}

		internal class FileState
		{
			public readonly INodeFile inode;

			public readonly string path;

			public readonly INodesInPath iip;

			public FileState(INodeFile inode, string fullPath, INodesInPath iip)
			{
				this.inode = inode;
				this.path = fullPath;
				this.iip = iip;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual FSNamesystem.FileState AnalyzeFileState(string src, long fileId, 
			string clientName, ExtendedBlock previous, LocatedBlock[] onRetryBlock)
		{
			System.Diagnostics.Debug.Assert(HasReadLock());
			CheckBlock(previous);
			onRetryBlock[0] = null;
			CheckNameNodeSafeMode("Cannot add block to " + src);
			// have we exceeded the configured limit of fs objects.
			CheckFsObjectLimit();
			Block previousBlock = ExtendedBlock.GetLocalBlock(previous);
			INode inode;
			INodesInPath iip;
			if (fileId == INodeId.GrandfatherInodeId)
			{
				// Older clients may not have given us an inode ID to work with.
				// In this case, we have to try to resolve the path and hope it
				// hasn't changed or been deleted since the file was opened for write.
				iip = dir.GetINodesInPath4Write(src);
				inode = iip.GetLastINode();
			}
			else
			{
				// Newer clients pass the inode ID, so we can just get the inode
				// directly.
				inode = dir.GetInode(fileId);
				iip = INodesInPath.FromINode(inode);
				if (inode != null)
				{
					src = iip.GetPath();
				}
			}
			INodeFile pendingFile = CheckLease(src, clientName, inode, fileId);
			BlockInfoContiguous lastBlockInFile = pendingFile.GetLastBlock();
			if (!Block.MatchingIdAndGenStamp(previousBlock, lastBlockInFile))
			{
				// The block that the client claims is the current last block
				// doesn't match up with what we think is the last block. There are
				// four possibilities:
				// 1) This is the first block allocation of an append() pipeline
				//    which started appending exactly at or exceeding the block boundary.
				//    In this case, the client isn't passed the previous block,
				//    so it makes the allocateBlock() call with previous=null.
				//    We can distinguish this since the last block of the file
				//    will be exactly a full block.
				// 2) This is a retry from a client that missed the response of a
				//    prior getAdditionalBlock() call, perhaps because of a network
				//    timeout, or because of an HA failover. In that case, we know
				//    by the fact that the client is re-issuing the RPC that it
				//    never began to write to the old block. Hence it is safe to
				//    to return the existing block.
				// 3) This is an entirely bogus request/bug -- we should error out
				//    rather than potentially appending a new block with an empty
				//    one in the middle, etc
				// 4) This is a retry from a client that timed out while
				//    the prior getAdditionalBlock() is still being processed,
				//    currently working on chooseTarget(). 
				//    There are no means to distinguish between the first and 
				//    the second attempts in Part I, because the first one hasn't
				//    changed the namesystem state yet.
				//    We run this analysis again in Part II where case 4 is impossible.
				BlockInfoContiguous penultimateBlock = pendingFile.GetPenultimateBlock();
				if (previous == null && lastBlockInFile != null && lastBlockInFile.GetNumBytes() 
					>= pendingFile.GetPreferredBlockSize() && lastBlockInFile.IsComplete())
				{
					// Case 1
					NameNode.stateChangeLog.Debug("BLOCK* NameSystem.allocateBlock: handling block allocation"
						 + " writing to a file with a complete previous block: src={}" + " lastBlock={}"
						, src, lastBlockInFile);
				}
				else
				{
					if (Block.MatchingIdAndGenStamp(penultimateBlock, previousBlock))
					{
						if (lastBlockInFile.GetNumBytes() != 0)
						{
							throw new IOException("Request looked like a retry to allocate block " + lastBlockInFile
								 + " but it already contains " + lastBlockInFile.GetNumBytes() + " bytes");
						}
						// Case 2
						// Return the last block.
						NameNode.stateChangeLog.Info("BLOCK* allocateBlock: " + "caught retry for allocation of a new block in "
							 + src + ". Returning previously allocated block " + lastBlockInFile);
						long offset = pendingFile.ComputeFileSize();
						onRetryBlock[0] = MakeLocatedBlock(lastBlockInFile, ((BlockInfoContiguousUnderConstruction
							)lastBlockInFile).GetExpectedStorageLocations(), offset);
						return new FSNamesystem.FileState(pendingFile, src, iip);
					}
					else
					{
						// Case 3
						throw new IOException("Cannot allocate block in " + src + ": " + "passed 'previous' block "
							 + previous + " does not match actual " + "last block in file " + lastBlockInFile
							);
					}
				}
			}
			return new FSNamesystem.FileState(pendingFile, src, iip);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual LocatedBlock MakeLocatedBlock(Block blk, DatanodeStorageInfo[] locs
			, long offset)
		{
			LocatedBlock lBlk = new LocatedBlock(GetExtendedBlock(blk), locs, offset, false);
			GetBlockManager().SetBlockToken(lBlk, BlockTokenSecretManager.AccessMode.Write);
			return lBlk;
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetAdditionalDatanode(string, long, Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, Org.Apache.Hadoop.Hdfs.Protocol.DatanodeInfo[], string[], Org.Apache.Hadoop.Hdfs.Protocol.DatanodeInfo[], int, string)
		/// 	"></seealso>
		/// <exception cref="System.IO.IOException"/>
		internal virtual LocatedBlock GetAdditionalDatanode(string src, long fileId, ExtendedBlock
			 blk, DatanodeInfo[] existings, string[] storageIDs, ICollection<Node> excludes, 
			int numAdditionalNodes, string clientName)
		{
			//check if the feature is enabled
			dtpReplaceDatanodeOnFailure.CheckEnabled();
			Node clientnode = null;
			string clientMachine;
			long preferredblocksize;
			byte storagePolicyID;
			IList<DatanodeStorageInfo> chosen;
			CheckOperation(NameNode.OperationCategory.Read);
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			FSPermissionChecker pc = GetPermissionChecker();
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				//check safe mode
				CheckNameNodeSafeMode("Cannot add datanode; src=" + src + ", blk=" + blk);
				src = dir.ResolvePath(pc, src, pathComponents);
				//check lease
				INode inode;
				if (fileId == INodeId.GrandfatherInodeId)
				{
					// Older clients may not have given us an inode ID to work with.
					// In this case, we have to try to resolve the path and hope it
					// hasn't changed or been deleted since the file was opened for write.
					inode = dir.GetINode(src);
				}
				else
				{
					inode = dir.GetInode(fileId);
					if (inode != null)
					{
						src = inode.GetFullPathName();
					}
				}
				INodeFile file = CheckLease(src, clientName, inode, fileId);
				clientMachine = file.GetFileUnderConstructionFeature().GetClientMachine();
				clientnode = blockManager.GetDatanodeManager().GetDatanodeByHost(clientMachine);
				preferredblocksize = file.GetPreferredBlockSize();
				storagePolicyID = file.GetStoragePolicyID();
				//find datanode storages
				DatanodeManager dm = blockManager.GetDatanodeManager();
				chosen = Arrays.AsList(dm.GetDatanodeStorageInfos(existings, storageIDs));
			}
			finally
			{
				ReadUnlock();
			}
			if (clientnode == null)
			{
				clientnode = GetClientNode(clientMachine);
			}
			// choose new datanodes.
			DatanodeStorageInfo[] targets = blockManager.ChooseTarget4AdditionalDatanode(src, 
				numAdditionalNodes, clientnode, chosen, excludes, preferredblocksize, storagePolicyID
				);
			LocatedBlock lb = new LocatedBlock(blk, targets);
			blockManager.SetBlockToken(lb, BlockTokenSecretManager.AccessMode.Copy);
			return lb;
		}

		/// <summary>The client would like to let go of the given block</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool AbandonBlock(ExtendedBlock b, long fileId, string src, string
			 holder)
		{
			NameNode.stateChangeLog.Debug("BLOCK* NameSystem.abandonBlock: {} of file {}", b, 
				src);
			CheckOperation(NameNode.OperationCategory.Write);
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			FSPermissionChecker pc = GetPermissionChecker();
			WaitForLoadingFSImage();
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot abandon block " + b + " for file" + src);
				src = dir.ResolvePath(pc, src, pathComponents);
				INode inode;
				INodesInPath iip;
				if (fileId == INodeId.GrandfatherInodeId)
				{
					// Older clients may not have given us an inode ID to work with.
					// In this case, we have to try to resolve the path and hope it
					// hasn't changed or been deleted since the file was opened for write.
					iip = dir.GetINodesInPath(src, true);
					inode = iip.GetLastINode();
				}
				else
				{
					inode = dir.GetInode(fileId);
					iip = INodesInPath.FromINode(inode);
					if (inode != null)
					{
						src = iip.GetPath();
					}
				}
				INodeFile file = CheckLease(src, holder, inode, fileId);
				// Remove the block from the pending creates list
				bool removed = dir.RemoveBlock(src, iip, file, ExtendedBlock.GetLocalBlock(b));
				if (!removed)
				{
					return true;
				}
				NameNode.stateChangeLog.Debug("BLOCK* NameSystem.abandonBlock: {} is " + "removed from pendingCreates"
					, b);
				PersistBlocks(src, file, false);
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			return true;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.LeaseExpiredException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		private INodeFile CheckLease(string src, string holder, INode inode, long fileId)
		{
			System.Diagnostics.Debug.Assert(HasReadLock());
			string ident = src + " (inode " + fileId + ")";
			if (inode == null)
			{
				LeaseManager.Lease lease = leaseManager.GetLease(holder);
				throw new LeaseExpiredException("No lease on " + ident + ": File does not exist. "
					 + (lease != null ? lease.ToString() : "Holder " + holder + " does not have any open files."
					));
			}
			if (!inode.IsFile())
			{
				LeaseManager.Lease lease = leaseManager.GetLease(holder);
				throw new LeaseExpiredException("No lease on " + ident + ": INode is not a regular file. "
					 + (lease != null ? lease.ToString() : "Holder " + holder + " does not have any open files."
					));
			}
			INodeFile file = inode.AsFile();
			if (!file.IsUnderConstruction())
			{
				LeaseManager.Lease lease = leaseManager.GetLease(holder);
				throw new LeaseExpiredException("No lease on " + ident + ": File is not open for writing. "
					 + (lease != null ? lease.ToString() : "Holder " + holder + " does not have any open files."
					));
			}
			// No further modification is allowed on a deleted file.
			// A file is considered deleted, if it is not in the inodeMap or is marked
			// as deleted in the snapshot feature.
			if (IsFileDeleted(file))
			{
				throw new FileNotFoundException(src);
			}
			string clientName = file.GetFileUnderConstructionFeature().GetClientName();
			if (holder != null && !clientName.Equals(holder))
			{
				throw new LeaseExpiredException("Lease mismatch on " + ident + " owned by " + clientName
					 + " but is accessed by " + holder);
			}
			return file;
		}

		/// <summary>Complete in-progress write to the given file.</summary>
		/// <returns>
		/// true if successful, false if the client should continue to retry
		/// (e.g if not all blocks have reached minimum replication yet)
		/// </returns>
		/// <exception cref="System.IO.IOException">on error (eg lease mismatch, file not open, file deleted)
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		internal virtual bool CompleteFile(string srcArg, string holder, ExtendedBlock last
			, long fileId)
		{
			string src = srcArg;
			NameNode.stateChangeLog.Debug("DIR* NameSystem.completeFile: {} for {}", src, holder
				);
			CheckBlock(last);
			bool success = false;
			CheckOperation(NameNode.OperationCategory.Write);
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			FSPermissionChecker pc = GetPermissionChecker();
			WaitForLoadingFSImage();
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot complete file " + src);
				src = dir.ResolvePath(pc, src, pathComponents);
				success = CompleteFileInternal(src, holder, ExtendedBlock.GetLocalBlock(last), fileId
					);
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			if (success)
			{
				NameNode.stateChangeLog.Info("DIR* completeFile: " + srcArg + " is closed by " + 
					holder);
			}
			return success;
		}

		/// <exception cref="System.IO.IOException"/>
		private bool CompleteFileInternal(string src, string holder, Org.Apache.Hadoop.Hdfs.Protocol.Block
			 last, long fileId)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			INodeFile pendingFile;
			INodesInPath iip;
			INode inode = null;
			try
			{
				if (fileId == INodeId.GrandfatherInodeId)
				{
					// Older clients may not have given us an inode ID to work with.
					// In this case, we have to try to resolve the path and hope it
					// hasn't changed or been deleted since the file was opened for write.
					iip = dir.GetINodesInPath(src, true);
					inode = iip.GetLastINode();
				}
				else
				{
					inode = dir.GetInode(fileId);
					iip = INodesInPath.FromINode(inode);
					if (inode != null)
					{
						src = iip.GetPath();
					}
				}
				pendingFile = CheckLease(src, holder, inode, fileId);
			}
			catch (LeaseExpiredException lee)
			{
				if (inode != null && inode.IsFile() && !inode.AsFile().IsUnderConstruction())
				{
					// This could be a retry RPC - i.e the client tried to close
					// the file, but missed the RPC response. Thus, it is trying
					// again to close the file. If the file still exists and
					// the client's view of the last block matches the actual
					// last block, then we'll treat it as a successful close.
					// See HDFS-3031.
					Org.Apache.Hadoop.Hdfs.Protocol.Block realLastBlock = inode.AsFile().GetLastBlock
						();
					if (Org.Apache.Hadoop.Hdfs.Protocol.Block.MatchingIdAndGenStamp(last, realLastBlock
						))
					{
						NameNode.stateChangeLog.Info("DIR* completeFile: " + "request from " + holder + " to complete inode "
							 + fileId + "(" + src + ") which is already closed. But, it appears to be " + "an RPC retry. Returning success"
							);
						return true;
					}
				}
				throw;
			}
			// Check the state of the penultimate block. It should be completed
			// before attempting to complete the last one.
			if (!CheckFileProgress(src, pendingFile, false))
			{
				return false;
			}
			// commit the last block and complete it if it has minimum replicas
			CommitOrCompleteLastBlock(pendingFile, iip, last);
			if (!CheckFileProgress(src, pendingFile, true))
			{
				return false;
			}
			FinalizeINodeFileUnderConstruction(src, pendingFile, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId);
			return true;
		}

		/// <summary>Save allocated block at the given pending filename</summary>
		/// <param name="src">path to the file</param>
		/// <param name="inodesInPath">
		/// representing each of the components of src.
		/// The last INode is the INode for
		/// <paramref name="src"/>
		/// file.
		/// </param>
		/// <param name="newBlock">newly allocated block to be save</param>
		/// <param name="targets">target datanodes where replicas of the new block is placed</param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.QuotaExceededException">If addition of block exceeds space quota
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		internal virtual BlockInfoContiguous SaveAllocatedBlock(string src, INodesInPath 
			inodesInPath, Org.Apache.Hadoop.Hdfs.Protocol.Block newBlock, DatanodeStorageInfo
			[] targets)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			BlockInfoContiguous b = dir.AddBlock(src, inodesInPath, newBlock, targets);
			NameNode.stateChangeLog.Info("BLOCK* allocate " + b + " for " + src);
			DatanodeStorageInfo.IncrementBlocksScheduled(targets);
			return b;
		}

		/// <summary>Create new block with a unique block id and a new generation stamp.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual Org.Apache.Hadoop.Hdfs.Protocol.Block CreateNewBlock()
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			Org.Apache.Hadoop.Hdfs.Protocol.Block b = new Org.Apache.Hadoop.Hdfs.Protocol.Block
				(NextBlockId(), 0, 0);
			// Increment the generation stamp for every new block.
			b.SetGenerationStamp(NextGenerationStamp(false));
			return b;
		}

		/// <summary>
		/// Check that the indicated file's blocks are present and
		/// replicated.
		/// </summary>
		/// <remarks>
		/// Check that the indicated file's blocks are present and
		/// replicated.  If not, return false. If checkall is true, then check
		/// all blocks, otherwise check only penultimate block.
		/// </remarks>
		internal virtual bool CheckFileProgress(string src, INodeFile v, bool checkall)
		{
			if (checkall)
			{
				// check all blocks of the file.
				foreach (BlockInfoContiguous block in v.GetBlocks())
				{
					if (!IsCompleteBlock(src, block, blockManager.minReplication))
					{
						return false;
					}
				}
			}
			else
			{
				// check the penultimate block of this file
				BlockInfoContiguous b = v.GetPenultimateBlock();
				if (b != null && !IsCompleteBlock(src, b, blockManager.minReplication))
				{
					return false;
				}
			}
			return true;
		}

		private static bool IsCompleteBlock(string src, BlockInfoContiguous b, int minRepl
			)
		{
			if (!b.IsComplete())
			{
				BlockInfoContiguousUnderConstruction uc = (BlockInfoContiguousUnderConstruction)b;
				int numNodes = b.NumNodes();
				Log.Info("BLOCK* " + b + " is not COMPLETE (ucState = " + uc.GetBlockUCState() + 
					", replication# = " + numNodes + (numNodes < minRepl ? " < " : " >= ") + " minimum = "
					 + minRepl + ") in file " + src);
				return false;
			}
			return true;
		}

		////////////////////////////////////////////////////////////////
		// Here's how to handle block-copy failure during client write:
		// -- As usual, the client's write should result in a streaming
		// backup write to a k-machine sequence.
		// -- If one of the backup machines fails, no worries.  Fail silently.
		// -- Before client is allowed to close and finalize file, make sure
		// that the blocks are backed up.  Namenode may have to issue specific backup
		// commands to make up for earlier datanode failures.  Once all copies
		// are made, edit namespace and return to client.
		////////////////////////////////////////////////////////////////
		/// <summary>Change the indicated filename.</summary>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use RenameTo(string, string, bool, Org.Apache.Hadoop.FS.Options.Rename[]) instead."
			)]
		internal virtual bool RenameTo(string src, string dst, bool logRetryCache)
		{
			WaitForLoadingFSImage();
			FSDirRenameOp.RenameOldResult ret = null;
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot rename " + src);
				ret = FSDirRenameOp.RenameToInt(dir, src, dst, logRetryCache);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "rename", src, dst, null);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			bool success = ret != null && ret.success;
			if (success)
			{
				GetEditLog().LogSync();
			}
			LogAuditEvent(success, "rename", src, dst, ret == null ? null : ret.auditStat);
			return success;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void RenameTo(string src, string dst, bool logRetryCache, params 
			Options.Rename[] options)
		{
			WaitForLoadingFSImage();
			KeyValuePair<INode.BlocksMapUpdateInfo, HdfsFileStatus> res = null;
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot rename " + src);
				res = FSDirRenameOp.RenameToInt(dir, src, dst, logRetryCache, options);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "rename (options=" + Arrays.ToString(options) + ")", src, dst
					, null);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			INode.BlocksMapUpdateInfo collectedBlocks = res.Key;
			HdfsFileStatus auditStat = res.Value;
			if (!collectedBlocks.GetToDeleteList().IsEmpty())
			{
				RemoveBlocks(collectedBlocks);
				collectedBlocks.Clear();
			}
			LogAuditEvent(true, "rename (options=" + Arrays.ToString(options) + ")", src, dst
				, auditStat);
		}

		/// <summary>Remove the indicated file from namespace.</summary>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.Delete(string, bool)
		/// 	">
		/// for detailed description and
		/// description of exceptions
		/// </seealso>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool Delete(string src, bool recursive, bool logRetryCache)
		{
			WaitForLoadingFSImage();
			INode.BlocksMapUpdateInfo toRemovedBlocks = null;
			WriteLock();
			bool ret = false;
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot delete " + src);
				toRemovedBlocks = FSDirDeleteOp.Delete(this, src, recursive, logRetryCache);
				ret = toRemovedBlocks != null;
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "delete", src);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			if (toRemovedBlocks != null)
			{
				RemoveBlocks(toRemovedBlocks);
			}
			// Incremental deletion of blocks
			LogAuditEvent(true, "delete", src);
			return ret;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		internal virtual FSPermissionChecker GetPermissionChecker()
		{
			return dir.GetPermissionChecker();
		}

		/// <summary>
		/// From the given list, incrementally remove the blocks from blockManager
		/// Writelock is dropped and reacquired every BLOCK_DELETION_INCREMENT to
		/// ensure that other waiters on the lock can get in.
		/// </summary>
		/// <remarks>
		/// From the given list, incrementally remove the blocks from blockManager
		/// Writelock is dropped and reacquired every BLOCK_DELETION_INCREMENT to
		/// ensure that other waiters on the lock can get in. See HDFS-2938
		/// </remarks>
		/// <param name="blocks">
		/// An instance of
		/// <see cref="BlocksMapUpdateInfo"/>
		/// which contains a list
		/// of blocks that need to be removed from blocksMap
		/// </param>
		internal virtual void RemoveBlocks(INode.BlocksMapUpdateInfo blocks)
		{
			IList<Org.Apache.Hadoop.Hdfs.Protocol.Block> toDeleteList = blocks.GetToDeleteList
				();
			IEnumerator<Org.Apache.Hadoop.Hdfs.Protocol.Block> iter = toDeleteList.GetEnumerator
				();
			while (iter.HasNext())
			{
				WriteLock();
				try
				{
					for (int i = 0; i < BlockDeletionIncrement && iter.HasNext(); i++)
					{
						blockManager.RemoveBlock(iter.Next());
					}
				}
				finally
				{
					WriteUnlock();
				}
			}
		}

		/// <summary>Remove leases and inodes related to a given path</summary>
		/// <param name="src">The given path</param>
		/// <param name="removedINodes">
		/// Containing the list of inodes to be removed from
		/// inodesMap
		/// </param>
		/// <param name="acquireINodeMapLock">Whether to acquire the lock for inode removal</param>
		internal virtual void RemoveLeasesAndINodes(string src, IList<INode> removedINodes
			, bool acquireINodeMapLock)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			leaseManager.RemoveLeaseWithPrefixPath(src);
			// remove inodes from inodesMap
			if (removedINodes != null)
			{
				if (acquireINodeMapLock)
				{
					dir.WriteLock();
				}
				try
				{
					dir.RemoveFromInodeMap(removedINodes);
				}
				finally
				{
					if (acquireINodeMapLock)
					{
						dir.WriteUnlock();
					}
				}
				removedINodes.Clear();
			}
		}

		/// <summary>Removes the blocks from blocksmap and updates the safemode blocks total</summary>
		/// <param name="blocks">
		/// An instance of
		/// <see cref="BlocksMapUpdateInfo"/>
		/// which contains a list
		/// of blocks that need to be removed from blocksMap
		/// </param>
		internal virtual void RemoveBlocksAndUpdateSafemodeTotal(INode.BlocksMapUpdateInfo
			 blocks)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			// In the case that we are a Standby tailing edits from the
			// active while in safe-mode, we need to track the total number
			// of blocks and safe blocks in the system.
			bool trackBlockCounts = IsSafeModeTrackingBlocks();
			int numRemovedComplete = 0;
			int numRemovedSafe = 0;
			foreach (Org.Apache.Hadoop.Hdfs.Protocol.Block b in blocks.GetToDeleteList())
			{
				if (trackBlockCounts)
				{
					BlockInfoContiguous bi = GetStoredBlock(b);
					if (bi.IsComplete())
					{
						numRemovedComplete++;
						if (bi.NumNodes() >= blockManager.minReplication)
						{
							numRemovedSafe++;
						}
					}
				}
				blockManager.RemoveBlock(b);
			}
			if (trackBlockCounts)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Adjusting safe-mode totals for deletion." + "decreasing safeBlocks by "
						 + numRemovedSafe + ", totalBlocks by " + numRemovedComplete);
				}
				AdjustSafeModeBlockTotals(-numRemovedSafe, -numRemovedComplete);
			}
		}

		/// <seealso cref="SafeModeInfo.shouldIncrementallyTrackBlocks"/>
		private bool IsSafeModeTrackingBlocks()
		{
			if (!haEnabled)
			{
				// Never track blocks incrementally in non-HA code.
				return false;
			}
			FSNamesystem.SafeModeInfo sm = this.safeMode;
			return sm != null && sm.ShouldIncrementallyTrackBlocks();
		}

		/// <summary>Get the file info for a specific file.</summary>
		/// <param name="src">The string representation of the path to the file</param>
		/// <param name="resolveLink">
		/// whether to throw UnresolvedLinkException
		/// if src refers to a symlink
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if access is denied
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if a symlink is encountered.
		/// 	</exception>
		/// <returns>
		/// object containing information regarding the file
		/// or null if file not found
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual HdfsFileStatus GetFileInfo(string src, bool resolveLink)
		{
			CheckOperation(NameNode.OperationCategory.Read);
			HdfsFileStatus stat = null;
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				stat = FSDirStatAndListingOp.GetFileInfo(dir, src, resolveLink);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "getfileinfo", src);
				throw;
			}
			finally
			{
				ReadUnlock();
			}
			LogAuditEvent(true, "getfileinfo", src);
			return stat;
		}

		/// <summary>Returns true if the file is closed</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool IsFileClosed(string src)
		{
			CheckOperation(NameNode.OperationCategory.Read);
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				return FSDirStatAndListingOp.IsFileClosed(dir, src);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "isFileClosed", src);
				throw;
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <summary>Create all the necessary directories</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool Mkdirs(string src, PermissionStatus permissions, bool createParent
			)
		{
			HdfsFileStatus auditStat = null;
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot create directory " + src);
				auditStat = FSDirMkdirOp.Mkdirs(this, src, permissions, createParent);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "mkdirs", src);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(true, "mkdirs", src, null, auditStat);
			return true;
		}

		/// <summary>Get the content summary for a specific file/dir.</summary>
		/// <param name="src">The string representation of the path to the file</param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if access is denied
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if a symlink is encountered.
		/// 	</exception>
		/// <exception cref="System.IO.FileNotFoundException">if no file exists</exception>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException">for issues with writing to the audit log</exception>
		/// <returns>
		/// object containing information regarding the file
		/// or null if file not found
		/// </returns>
		internal virtual ContentSummary GetContentSummary(string src)
		{
			CheckOperation(NameNode.OperationCategory.Read);
			ReadLock();
			bool success = true;
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				return FSDirStatAndListingOp.GetContentSummary(dir, src);
			}
			catch (AccessControlException ace)
			{
				success = false;
				throw;
			}
			finally
			{
				ReadUnlock();
				LogAuditEvent(success, "contentSummary", src);
			}
		}

		/// <summary>Set the namespace quota and storage space quota for a directory.</summary>
		/// <remarks>
		/// Set the namespace quota and storage space quota for a directory.
		/// See
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetQuota(string, long, long, Org.Apache.Hadoop.FS.StorageType)
		/// 	"/>
		/// for the
		/// contract.
		/// Note: This does not support ".inodes" relative path.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetQuota(string src, long nsQuota, long ssQuota, StorageType
			 type)
		{
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			bool success = false;
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot set quota on " + src);
				FSDirAttrOp.SetQuota(dir, src, nsQuota, ssQuota, type);
				success = true;
			}
			finally
			{
				WriteUnlock();
				if (success)
				{
					GetEditLog().LogSync();
				}
				LogAuditEvent(success, "setQuota", src);
			}
		}

		/// <summary>Persist all metadata about this file.</summary>
		/// <param name="src">The string representation of the path</param>
		/// <param name="fileId">
		/// The inode ID that we're fsyncing.  Older clients will pass
		/// INodeId.GRANDFATHER_INODE_ID here.
		/// </param>
		/// <param name="clientName">The string representation of the client</param>
		/// <param name="lastBlockLength">
		/// The length of the last block
		/// under construction reported from client.
		/// </param>
		/// <exception cref="System.IO.IOException">if path does not exist</exception>
		internal virtual void Fsync(string src, long fileId, string clientName, long lastBlockLength
			)
		{
			NameNode.stateChangeLog.Info("BLOCK* fsync: " + src + " for " + clientName);
			CheckOperation(NameNode.OperationCategory.Write);
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			FSPermissionChecker pc = GetPermissionChecker();
			WaitForLoadingFSImage();
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot fsync file " + src);
				src = dir.ResolvePath(pc, src, pathComponents);
				INode inode;
				if (fileId == INodeId.GrandfatherInodeId)
				{
					// Older clients may not have given us an inode ID to work with.
					// In this case, we have to try to resolve the path and hope it
					// hasn't changed or been deleted since the file was opened for write.
					inode = dir.GetINode(src);
				}
				else
				{
					inode = dir.GetInode(fileId);
					if (inode != null)
					{
						src = inode.GetFullPathName();
					}
				}
				INodeFile pendingFile = CheckLease(src, clientName, inode, fileId);
				if (lastBlockLength > 0)
				{
					pendingFile.GetFileUnderConstructionFeature().UpdateLengthOfLastBlock(pendingFile
						, lastBlockLength);
				}
				PersistBlocks(src, pendingFile, false);
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
		}

		/// <summary>Move a file that is being written to be immutable.</summary>
		/// <param name="src">The filename</param>
		/// <param name="lease">The lease for the client creating the file</param>
		/// <param name="recoveryLeaseHolder">
		/// reassign lease to this holder if the last block
		/// needs recovery; keep current holder if null.
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.AlreadyBeingCreatedException">
		/// if file is waiting to achieve minimal
		/// replication;<br />
		/// RecoveryInProgressException if lease recovery is in progress.<br />
		/// IOException in case of an error.
		/// </exception>
		/// <returns>
		/// true  if file has been successfully finalized and closed or
		/// false if block recovery has been initiated. Since the lease owner
		/// has been changed and logged, caller should call logSync().
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool InternalReleaseLease(LeaseManager.Lease lease, string src, 
			INodesInPath iip, string recoveryLeaseHolder)
		{
			Log.Info("Recovering " + lease + ", src=" + src);
			System.Diagnostics.Debug.Assert(!IsInSafeMode());
			System.Diagnostics.Debug.Assert(HasWriteLock());
			INodeFile pendingFile = iip.GetLastINode().AsFile();
			int nrBlocks = pendingFile.NumBlocks();
			BlockInfoContiguous[] blocks = pendingFile.GetBlocks();
			int nrCompleteBlocks;
			BlockInfoContiguous curBlock = null;
			for (nrCompleteBlocks = 0; nrCompleteBlocks < nrBlocks; nrCompleteBlocks++)
			{
				curBlock = blocks[nrCompleteBlocks];
				if (!curBlock.IsComplete())
				{
					break;
				}
				System.Diagnostics.Debug.Assert(blockManager.CheckMinReplication(curBlock), "A COMPLETE block is not minimally replicated in "
					 + src);
			}
			// If there are no incomplete blocks associated with this file,
			// then reap lease immediately and close the file.
			if (nrCompleteBlocks == nrBlocks)
			{
				FinalizeINodeFileUnderConstruction(src, pendingFile, iip.GetLatestSnapshotId());
				NameNode.stateChangeLog.Warn("BLOCK*" + " internalReleaseLease: All existing blocks are COMPLETE,"
					 + " lease removed, file closed.");
				return true;
			}
			// closed!
			// Only the last and the penultimate blocks may be in non COMPLETE state.
			// If the penultimate block is not COMPLETE, then it must be COMMITTED.
			if (nrCompleteBlocks < nrBlocks - 2 || nrCompleteBlocks == nrBlocks - 2 && curBlock
				 != null && curBlock.GetBlockUCState() != HdfsServerConstants.BlockUCState.Committed)
			{
				string message = "DIR* NameSystem.internalReleaseLease: " + "attempt to release a create lock on "
					 + src + " but file is already closed.";
				NameNode.stateChangeLog.Warn(message);
				throw new IOException(message);
			}
			// The last block is not COMPLETE, and
			// that the penultimate block if exists is either COMPLETE or COMMITTED
			BlockInfoContiguous lastBlock = pendingFile.GetLastBlock();
			HdfsServerConstants.BlockUCState lastBlockState = lastBlock.GetBlockUCState();
			BlockInfoContiguous penultimateBlock = pendingFile.GetPenultimateBlock();
			// If penultimate block doesn't exist then its minReplication is met
			bool penultimateBlockMinReplication = penultimateBlock == null ? true : blockManager
				.CheckMinReplication(penultimateBlock);
			switch (lastBlockState)
			{
				case HdfsServerConstants.BlockUCState.Complete:
				{
					System.Diagnostics.Debug.Assert(false, "Already checked that the last block is incomplete"
						);
					break;
				}

				case HdfsServerConstants.BlockUCState.Committed:
				{
					// Close file if committed blocks are minimally replicated
					if (penultimateBlockMinReplication && blockManager.CheckMinReplication(lastBlock))
					{
						FinalizeINodeFileUnderConstruction(src, pendingFile, iip.GetLatestSnapshotId());
						NameNode.stateChangeLog.Warn("BLOCK*" + " internalReleaseLease: Committed blocks are minimally replicated,"
							 + " lease removed, file closed.");
						return true;
					}
					// closed!
					// Cannot close file right now, since some blocks 
					// are not yet minimally replicated.
					// This may potentially cause infinite loop in lease recovery
					// if there are no valid replicas on data-nodes.
					string message_1 = "DIR* NameSystem.internalReleaseLease: " + "Failed to release lease for file "
						 + src + ". Committed blocks are waiting to be minimally replicated." + " Try again later.";
					NameNode.stateChangeLog.Warn(message_1);
					throw new AlreadyBeingCreatedException(message_1);
				}

				case HdfsServerConstants.BlockUCState.UnderConstruction:
				case HdfsServerConstants.BlockUCState.UnderRecovery:
				{
					BlockInfoContiguousUnderConstruction uc = (BlockInfoContiguousUnderConstruction)lastBlock;
					// determine if last block was intended to be truncated
					Org.Apache.Hadoop.Hdfs.Protocol.Block recoveryBlock = uc.GetTruncateBlock();
					bool truncateRecovery = recoveryBlock != null;
					bool copyOnTruncate = truncateRecovery && recoveryBlock.GetBlockId() != uc.GetBlockId
						();
					System.Diagnostics.Debug.Assert(!copyOnTruncate || recoveryBlock.GetBlockId() < uc
						.GetBlockId() && recoveryBlock.GetGenerationStamp() < uc.GetGenerationStamp() &&
						 recoveryBlock.GetNumBytes() > uc.GetNumBytes(), "wrong recoveryBlock");
					// setup the last block locations from the blockManager if not known
					if (uc.GetNumExpectedLocations() == 0)
					{
						uc.SetExpectedLocations(blockManager.GetStorages(lastBlock));
					}
					if (uc.GetNumExpectedLocations() == 0 && uc.GetNumBytes() == 0)
					{
						// There is no datanode reported to this block.
						// may be client have crashed before writing data to pipeline.
						// This blocks doesn't need any recovery.
						// We can remove this block and close the file.
						pendingFile.RemoveLastBlock(lastBlock);
						FinalizeINodeFileUnderConstruction(src, pendingFile, iip.GetLatestSnapshotId());
						NameNode.stateChangeLog.Warn("BLOCK* internalReleaseLease: " + "Removed empty last block and closed file."
							);
						return true;
					}
					// start recovery of the last block for this file
					long blockRecoveryId = NextGenerationStamp(blockIdManager.IsLegacyBlock(uc));
					lease = ReassignLease(lease, src, recoveryLeaseHolder, pendingFile);
					if (copyOnTruncate)
					{
						uc.SetGenerationStamp(blockRecoveryId);
					}
					else
					{
						if (truncateRecovery)
						{
							recoveryBlock.SetGenerationStamp(blockRecoveryId);
						}
					}
					uc.InitializeBlockRecovery(blockRecoveryId);
					leaseManager.RenewLease(lease);
					// Cannot close file right now, since the last block requires recovery.
					// This may potentially cause infinite loop in lease recovery
					// if there are no valid replicas on data-nodes.
					NameNode.stateChangeLog.Warn("DIR* NameSystem.internalReleaseLease: " + "File " +
						 src + " has not been closed." + " Lease recovery is in progress. " + "RecoveryId = "
						 + blockRecoveryId + " for block " + lastBlock);
					break;
				}
			}
			return false;
		}

		private LeaseManager.Lease ReassignLease(LeaseManager.Lease lease, string src, string
			 newHolder, INodeFile pendingFile)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			if (newHolder == null)
			{
				return lease;
			}
			// The following transaction is not synced. Make sure it's sync'ed later.
			LogReassignLease(lease.GetHolder(), src, newHolder);
			return ReassignLeaseInternal(lease, src, newHolder, pendingFile);
		}

		internal virtual LeaseManager.Lease ReassignLeaseInternal(LeaseManager.Lease lease
			, string src, string newHolder, INodeFile pendingFile)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			pendingFile.GetFileUnderConstructionFeature().SetClientName(newHolder);
			return leaseManager.ReassignLease(lease, src, newHolder);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CommitOrCompleteLastBlock(INodeFile fileINode, INodesInPath iip, Org.Apache.Hadoop.Hdfs.Protocol.Block
			 commitBlock)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			Preconditions.CheckArgument(fileINode.IsUnderConstruction());
			if (!blockManager.CommitOrCompleteLastBlock(fileINode, commitBlock))
			{
				return;
			}
			// Adjust disk space consumption if required
			long diff = fileINode.GetPreferredBlockSize() - commitBlock.GetNumBytes();
			if (diff > 0)
			{
				try
				{
					dir.UpdateSpaceConsumed(iip, 0, -diff, fileINode.GetFileReplication());
				}
				catch (IOException e)
				{
					Log.Warn("Unexpected exception while updating disk space.", e);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void FinalizeINodeFileUnderConstruction(string src, INodeFile pendingFile
			, int latestSnapshot)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			FileUnderConstructionFeature uc = pendingFile.GetFileUnderConstructionFeature();
			if (uc == null)
			{
				throw new IOException("Cannot finalize file " + src + " because it is not under construction"
					);
			}
			leaseManager.RemoveLease(uc.GetClientName(), src);
			pendingFile.RecordModification(latestSnapshot);
			// The file is no longer pending.
			// Create permanent INode, update blocks. No need to replace the inode here
			// since we just remove the uc feature from pendingFile
			pendingFile.ToCompleteFile(Time.Now());
			WaitForLoadingFSImage();
			// close file and persist block allocations for this file
			CloseFile(src, pendingFile);
			blockManager.CheckReplication(pendingFile);
		}

		[VisibleForTesting]
		internal virtual BlockInfoContiguous GetStoredBlock(Org.Apache.Hadoop.Hdfs.Protocol.Block
			 block)
		{
			return blockManager.GetStoredBlock(block);
		}

		public virtual bool IsInSnapshot(BlockInfoContiguousUnderConstruction blockUC)
		{
			System.Diagnostics.Debug.Assert(HasReadLock());
			BlockCollection bc = blockUC.GetBlockCollection();
			if (bc == null || !(bc is INodeFile) || !bc.IsUnderConstruction())
			{
				return false;
			}
			string fullName = bc.GetName();
			try
			{
				if (fullName != null && fullName.StartsWith(Path.Separator) && dir.GetINode(fullName
					) == bc)
				{
					// If file exists in normal path then no need to look in snapshot
					return false;
				}
			}
			catch (UnresolvedLinkException e)
			{
				Log.Error("Error while resolving the link : " + fullName, e);
				return false;
			}
			/*
			* 1. if bc is under construction and also with snapshot, and
			* bc is not in the current fsdirectory tree, bc must represent a snapshot
			* file.
			* 2. if fullName is not an absolute path, bc cannot be existent in the
			* current fsdirectory tree.
			* 3. if bc is not the current node associated with fullName, bc must be a
			* snapshot inode.
			*/
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void CommitBlockSynchronization(ExtendedBlock oldBlock, long newgenerationstamp
			, long newlength, bool closeFile, bool deleteblock, DatanodeID[] newtargets, string
			[] newtargetstorages)
		{
			Log.Info("commitBlockSynchronization(oldBlock=" + oldBlock + ", newgenerationstamp="
				 + newgenerationstamp + ", newlength=" + newlength + ", newtargets=" + Arrays.AsList
				(newtargets) + ", closeFile=" + closeFile + ", deleteBlock=" + deleteblock + ")"
				);
			CheckOperation(NameNode.OperationCategory.Write);
			string src = string.Empty;
			WaitForLoadingFSImage();
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				// If a DN tries to commit to the standby, the recovery will
				// fail, and the next retry will succeed on the new NN.
				CheckNameNodeSafeMode("Cannot commitBlockSynchronization while in safe mode");
				BlockInfoContiguous storedBlock = GetStoredBlock(ExtendedBlock.GetLocalBlock(oldBlock
					));
				if (storedBlock == null)
				{
					if (deleteblock)
					{
						// This may be a retry attempt so ignore the failure
						// to locate the block.
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Block (=" + oldBlock + ") not found");
						}
						return;
					}
					else
					{
						throw new IOException("Block (=" + oldBlock + ") not found");
					}
				}
				long oldGenerationStamp = storedBlock.GetGenerationStamp();
				long oldNumBytes = storedBlock.GetNumBytes();
				//
				// The implementation of delete operation (see @deleteInternal method)
				// first removes the file paths from namespace, and delays the removal
				// of blocks to later time for better performance. When
				// commitBlockSynchronization (this method) is called in between, the
				// blockCollection of storedBlock could have been assigned to null by
				// the delete operation, throw IOException here instead of NPE; if the
				// file path is already removed from namespace by the delete operation,
				// throw FileNotFoundException here, so not to proceed to the end of
				// this method to add a CloseOp to the edit log for an already deleted
				// file (See HDFS-6825).
				//
				BlockCollection blockCollection = storedBlock.GetBlockCollection();
				if (blockCollection == null)
				{
					throw new IOException("The blockCollection of " + storedBlock + " is null, likely because the file owning this block was"
						 + " deleted and the block removal is delayed");
				}
				INodeFile iFile = ((INode)blockCollection).AsFile();
				if (IsFileDeleted(iFile))
				{
					throw new FileNotFoundException("File not found: " + iFile.GetFullPathName() + ", likely due to delayed block"
						 + " removal");
				}
				if ((!iFile.IsUnderConstruction() || storedBlock.IsComplete()) && iFile.GetLastBlock
					().IsComplete())
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Unexpected block (=" + oldBlock + ") since the file (=" + iFile.GetLocalName
							() + ") is not under construction");
					}
					return;
				}
				BlockInfoContiguousUnderConstruction truncatedBlock = (BlockInfoContiguousUnderConstruction
					)iFile.GetLastBlock();
				long recoveryId = truncatedBlock.GetBlockRecoveryId();
				bool copyTruncate = truncatedBlock.GetBlockId() != storedBlock.GetBlockId();
				if (recoveryId != newgenerationstamp)
				{
					throw new IOException("The recovery id " + newgenerationstamp + " does not match current recovery id "
						 + recoveryId + " for block " + oldBlock);
				}
				if (deleteblock)
				{
					Org.Apache.Hadoop.Hdfs.Protocol.Block blockToDel = ExtendedBlock.GetLocalBlock(oldBlock
						);
					bool remove = iFile.RemoveLastBlock(blockToDel);
					if (remove)
					{
						blockManager.RemoveBlock(storedBlock);
					}
				}
				else
				{
					// update last block
					if (!copyTruncate)
					{
						storedBlock.SetGenerationStamp(newgenerationstamp);
						storedBlock.SetNumBytes(newlength);
					}
					// find the DatanodeDescriptor objects
					AList<DatanodeDescriptor> trimmedTargets = new AList<DatanodeDescriptor>(newtargets
						.Length);
					AList<string> trimmedStorages = new AList<string>(newtargets.Length);
					if (newtargets.Length > 0)
					{
						for (int i = 0; i < newtargets.Length; ++i)
						{
							// try to get targetNode
							DatanodeDescriptor targetNode = blockManager.GetDatanodeManager().GetDatanode(newtargets
								[i]);
							if (targetNode != null)
							{
								trimmedTargets.AddItem(targetNode);
								trimmedStorages.AddItem(newtargetstorages[i]);
							}
							else
							{
								if (Log.IsDebugEnabled())
								{
									Log.Debug("DatanodeDescriptor (=" + newtargets[i] + ") not found");
								}
							}
						}
					}
					if ((closeFile) && !trimmedTargets.IsEmpty())
					{
						// the file is getting closed. Insert block locations into blockManager.
						// Otherwise fsck will report these blocks as MISSING, especially if the
						// blocksReceived from Datanodes take a long time to arrive.
						for (int i = 0; i < trimmedTargets.Count; i++)
						{
							DatanodeStorageInfo storageInfo = trimmedTargets[i].GetStorageInfo(trimmedStorages
								[i]);
							if (storageInfo != null)
							{
								if (copyTruncate)
								{
									storageInfo.AddBlock(truncatedBlock);
								}
								else
								{
									storageInfo.AddBlock(storedBlock);
								}
							}
						}
					}
					// add pipeline locations into the INodeUnderConstruction
					DatanodeStorageInfo[] trimmedStorageInfos = blockManager.GetDatanodeManager().GetDatanodeStorageInfos
						(Sharpen.Collections.ToArray(trimmedTargets, new DatanodeID[trimmedTargets.Count
						]), Sharpen.Collections.ToArray(trimmedStorages, new string[trimmedStorages.Count
						]));
					if (copyTruncate)
					{
						iFile.SetLastBlock(truncatedBlock, trimmedStorageInfos);
					}
					else
					{
						iFile.SetLastBlock(storedBlock, trimmedStorageInfos);
						if (closeFile)
						{
							blockManager.MarkBlockReplicasAsCorrupt(storedBlock, oldGenerationStamp, oldNumBytes
								, trimmedStorageInfos);
						}
					}
				}
				if (closeFile)
				{
					if (copyTruncate)
					{
						src = CloseFileCommitBlocks(iFile, truncatedBlock);
						if (!iFile.IsBlockInLatestSnapshot(storedBlock))
						{
							blockManager.RemoveBlock(storedBlock);
						}
					}
					else
					{
						src = CloseFileCommitBlocks(iFile, storedBlock);
					}
				}
				else
				{
					// If this commit does not want to close the file, persist blocks
					src = iFile.GetFullPathName();
					PersistBlocks(src, iFile, false);
				}
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			if (closeFile)
			{
				Log.Info("commitBlockSynchronization(oldBlock=" + oldBlock + ", file=" + src + ", newgenerationstamp="
					 + newgenerationstamp + ", newlength=" + newlength + ", newtargets=" + Arrays.AsList
					(newtargets) + ") successful");
			}
			else
			{
				Log.Info("commitBlockSynchronization(" + oldBlock + ") successful");
			}
		}

		/// <param name="pendingFile">open file that needs to be closed</param>
		/// <param name="storedBlock">last block</param>
		/// <returns>Path of the file that was closed.</returns>
		/// <exception cref="System.IO.IOException">on error</exception>
		[VisibleForTesting]
		internal virtual string CloseFileCommitBlocks(INodeFile pendingFile, BlockInfoContiguous
			 storedBlock)
		{
			INodesInPath iip = INodesInPath.FromINode(pendingFile);
			string src = iip.GetPath();
			// commit the last block and complete it if it has minimum replicas
			CommitOrCompleteLastBlock(pendingFile, iip, storedBlock);
			//remove lease, close file
			FinalizeINodeFileUnderConstruction(src, pendingFile, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.FindLatestSnapshot(pendingFile, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId));
			return src;
		}

		/// <summary>Renew the lease(s) held by the given client</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void RenewLease(string holder)
		{
			CheckOperation(NameNode.OperationCategory.Write);
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot renew lease for " + holder);
				leaseManager.RenewLease(holder);
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <summary>Get a partial listing of the indicated directory</summary>
		/// <param name="src">the directory name</param>
		/// <param name="startAfter">the name to start after</param>
		/// <param name="needLocation">if blockLocations need to be returned</param>
		/// <returns>a partial listing starting after startAfter</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if access is denied
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if symbolic link is encountered
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if other I/O error occurred</exception>
		internal virtual DirectoryListing GetListing(string src, byte[] startAfter, bool 
			needLocation)
		{
			CheckOperation(NameNode.OperationCategory.Read);
			DirectoryListing dl = null;
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				dl = FSDirStatAndListingOp.GetListingInt(dir, src, startAfter, needLocation);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "listStatus", src);
				throw;
			}
			finally
			{
				ReadUnlock();
			}
			LogAuditEvent(true, "listStatus", src);
			return dl;
		}

		/////////////////////////////////////////////////////////
		//
		// These methods are called by datanodes
		//
		/////////////////////////////////////////////////////////
		/// <summary>Register Datanode.</summary>
		/// <remarks>
		/// Register Datanode.
		/// <p>
		/// The purpose of registration is to identify whether the new datanode
		/// serves a new data storage, and will report new data block copies,
		/// which the namenode was not aware of; or the datanode is a replacement
		/// node for the data storage that was previously served by a different
		/// or the same (in terms of host:port) datanode.
		/// The data storages are distinguished by their storageIDs. When a new
		/// data storage is reported the namenode issues a new unique storageID.
		/// <p>
		/// Finally, the namenode returns its namespaceID as the registrationID
		/// for the datanodes.
		/// namespaceID is a persistent attribute of the name space.
		/// The registrationID is checked every time the datanode is communicating
		/// with the namenode.
		/// Datanodes with inappropriate registrationID are rejected.
		/// If the namenode stops, and then restarts it can restore its
		/// namespaceID and will continue serving the datanodes that has previously
		/// registered with the namenode without restarting the whole cluster.
		/// </remarks>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Datanode.DataNode"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void RegisterDatanode(DatanodeRegistration nodeReg)
		{
			WriteLock();
			try
			{
				GetBlockManager().GetDatanodeManager().RegisterDatanode(nodeReg);
				CheckSafeMode();
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <summary>Get registrationID for datanodes based on the namespaceID.</summary>
		/// <seealso cref="RegisterDatanode(Org.Apache.Hadoop.Hdfs.Server.Protocol.DatanodeRegistration)
		/// 	"/>
		/// <returns>registration ID</returns>
		internal virtual string GetRegistrationID()
		{
			return Storage.GetRegistrationID(GetFSImage().GetStorage());
		}

		/// <summary>The given node has reported in.</summary>
		/// <remarks>
		/// The given node has reported in.  This method should:
		/// 1) Record the heartbeat, so the datanode isn't timed out
		/// 2) Adjust usage stats for future block allocation
		/// If a substantial amount of time passed since the last datanode
		/// heartbeat then request an immediate block report.
		/// </remarks>
		/// <returns>an array of datanode commands</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual HeartbeatResponse HandleHeartbeat(DatanodeRegistration nodeReg, 
			StorageReport[] reports, long cacheCapacity, long cacheUsed, int xceiverCount, int
			 xmitsInProgress, int failedVolumes, VolumeFailureSummary volumeFailureSummary)
		{
			ReadLock();
			try
			{
				//get datanode commands
				int maxTransfer = blockManager.GetMaxReplicationStreams() - xmitsInProgress;
				DatanodeCommand[] cmds = blockManager.GetDatanodeManager().HandleHeartbeat(nodeReg
					, reports, blockPoolId, cacheCapacity, cacheUsed, xceiverCount, maxTransfer, failedVolumes
					, volumeFailureSummary);
				//create ha status
				NNHAStatusHeartbeat haState = new NNHAStatusHeartbeat(haContext.GetState().GetServiceState
					(), GetFSImage().GetLastAppliedOrWrittenTxId());
				return new HeartbeatResponse(cmds, haState, rollingUpgradeInfo);
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <summary>
		/// Returns whether or not there were available resources at the last check of
		/// resources.
		/// </summary>
		/// <returns>true if there were sufficient resources available, false otherwise.</returns>
		internal virtual bool NameNodeHasResourcesAvailable()
		{
			return hasResourcesAvailable;
		}

		/// <summary>Perform resource checks and cache the results.</summary>
		internal virtual void CheckAvailableResources()
		{
			Preconditions.CheckState(nnResourceChecker != null, "nnResourceChecker not initialized"
				);
			hasResourcesAvailable = nnResourceChecker.HasAvailableDiskSpace();
		}

		/// <summary>Persist the block list for the inode.</summary>
		/// <param name="path"/>
		/// <param name="file"/>
		/// <param name="logRetryCache"/>
		private void PersistBlocks(string path, INodeFile file, bool logRetryCache)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			Preconditions.CheckArgument(file.IsUnderConstruction());
			GetEditLog().LogUpdateBlocks(path, file, logRetryCache);
			NameNode.stateChangeLog.Debug("persistBlocks: {} with {} blocks is" + " peristed to the file system"
				, path, file.GetBlocks().Length);
		}

		/// <summary>Close file.</summary>
		/// <param name="path"/>
		/// <param name="file"/>
		private void CloseFile(string path, INodeFile file)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			WaitForLoadingFSImage();
			// file is closed
			GetEditLog().LogCloseFile(path, file);
			NameNode.stateChangeLog.Debug("closeFile: {} with {} blocks is persisted" + " to the file system"
				, path, file.GetBlocks().Length);
		}

		/// <summary>
		/// Periodically calls hasAvailableResources of NameNodeResourceChecker, and if
		/// there are found to be insufficient resources available, causes the NN to
		/// enter safe mode.
		/// </summary>
		/// <remarks>
		/// Periodically calls hasAvailableResources of NameNodeResourceChecker, and if
		/// there are found to be insufficient resources available, causes the NN to
		/// enter safe mode. If resources are later found to have returned to
		/// acceptable levels, this daemon will cause the NN to exit safe mode.
		/// </remarks>
		internal class NameNodeResourceMonitor : Runnable
		{
			internal bool shouldNNRmRun = true;

			public virtual void Run()
			{
				try
				{
					while (this._enclosing.fsRunning && this.shouldNNRmRun)
					{
						this._enclosing.CheckAvailableResources();
						if (!this._enclosing.NameNodeHasResourcesAvailable())
						{
							string lowResourcesMsg = "NameNode low on available disk space. ";
							if (!this._enclosing.IsInSafeMode())
							{
								FSNamesystem.Log.Warn(lowResourcesMsg + "Entering safe mode.");
							}
							else
							{
								FSNamesystem.Log.Warn(lowResourcesMsg + "Already in safe mode.");
							}
							this._enclosing.EnterSafeMode(true);
						}
						try
						{
							Sharpen.Thread.Sleep(this._enclosing.resourceRecheckInterval);
						}
						catch (Exception)
						{
						}
					}
				}
				catch (Exception e)
				{
					// Deliberately ignore
					FSNamesystem.Log.Error("Exception in NameNodeResourceMonitor: ", e);
				}
			}

			public virtual void StopMonitor()
			{
				this.shouldNNRmRun = false;
			}

			internal NameNodeResourceMonitor(FSNamesystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly FSNamesystem _enclosing;
		}

		internal class NameNodeEditLogRoller : Runnable
		{
			private bool shouldRun = true;

			private readonly long rollThreshold;

			private readonly long sleepIntervalMs;

			public NameNodeEditLogRoller(FSNamesystem _enclosing, long rollThreshold, int sleepIntervalMs
				)
			{
				this._enclosing = _enclosing;
				this.rollThreshold = rollThreshold;
				this.sleepIntervalMs = sleepIntervalMs;
			}

			public virtual void Run()
			{
				while (this._enclosing.fsRunning && this.shouldRun)
				{
					try
					{
						FSEditLog editLog = this._enclosing.GetFSImage().GetEditLog();
						long numEdits = editLog.GetLastWrittenTxId() - editLog.GetCurSegmentTxId();
						if (numEdits > this.rollThreshold)
						{
							FSNamesystem.Log.Info("NameNode rolling its own edit log because" + " number of edits in open segment exceeds threshold of "
								 + this.rollThreshold);
							this._enclosing.RollEditLog();
						}
					}
					catch (Exception e)
					{
						FSNamesystem.Log.Error("Swallowing exception in " + typeof(FSNamesystem.NameNodeEditLogRoller
							).Name + ":", e);
					}
					try
					{
						Sharpen.Thread.Sleep(this.sleepIntervalMs);
					}
					catch (Exception)
					{
						FSNamesystem.Log.Info(typeof(FSNamesystem.NameNodeEditLogRoller).Name + " was interrupted, exiting"
							);
						break;
					}
				}
			}

			public virtual void Stop()
			{
				this.shouldRun = false;
			}

			private readonly FSNamesystem _enclosing;
		}

		/// <summary>
		/// Daemon to periodically scan the namespace for lazyPersist files
		/// with missing blocks and unlink them.
		/// </summary>
		internal class LazyPersistFileScrubber : Runnable
		{
			private volatile bool shouldRun = true;

			internal readonly int scrubIntervalSec;

			public LazyPersistFileScrubber(FSNamesystem _enclosing, int scrubIntervalSec)
			{
				this._enclosing = _enclosing;
				this.scrubIntervalSec = scrubIntervalSec;
			}

			/// <summary>
			/// Periodically go over the list of lazyPersist files with missing
			/// blocks and unlink them from the namespace.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			private void ClearCorruptLazyPersistFiles()
			{
				BlockStoragePolicy lpPolicy = this._enclosing.blockManager.GetStoragePolicy("LAZY_PERSIST"
					);
				IList<BlockCollection> filesToDelete = new AList<BlockCollection>();
				bool changed = false;
				this._enclosing.WriteLock();
				try
				{
					IEnumerator<Org.Apache.Hadoop.Hdfs.Protocol.Block> it = this._enclosing.blockManager
						.GetCorruptReplicaBlockIterator();
					while (it.HasNext())
					{
						Org.Apache.Hadoop.Hdfs.Protocol.Block b = it.Next();
						BlockInfoContiguous blockInfo = this._enclosing.blockManager.GetStoredBlock(b);
						if (blockInfo.GetBlockCollection().GetStoragePolicyID() == lpPolicy.GetId())
						{
							filesToDelete.AddItem(blockInfo.GetBlockCollection());
						}
					}
					foreach (BlockCollection bc in filesToDelete)
					{
						FSNamesystem.Log.Warn("Removing lazyPersist file " + bc.GetName() + " with no replicas."
							);
						INode.BlocksMapUpdateInfo toRemoveBlocks = FSDirDeleteOp.DeleteInternal(this._enclosing
							, bc.GetName(), INodesInPath.FromINode((INodeFile)bc), false);
						changed |= toRemoveBlocks != null;
						if (toRemoveBlocks != null)
						{
							this._enclosing.RemoveBlocks(toRemoveBlocks);
						}
					}
				}
				finally
				{
					// Incremental deletion of blocks
					this._enclosing.WriteUnlock();
				}
				if (changed)
				{
					this._enclosing.GetEditLog().LogSync();
				}
			}

			public virtual void Run()
			{
				while (this._enclosing.fsRunning && this.shouldRun)
				{
					try
					{
						this.ClearCorruptLazyPersistFiles();
						Sharpen.Thread.Sleep(this.scrubIntervalSec * 1000);
					}
					catch (Exception)
					{
						FSNamesystem.Log.Info("LazyPersistFileScrubber was interrupted, exiting");
						break;
					}
					catch (Exception e)
					{
						FSNamesystem.Log.Error("Ignoring exception in LazyPersistFileScrubber:", e);
					}
				}
			}

			public virtual void Stop()
			{
				this.shouldRun = false;
			}

			private readonly FSNamesystem _enclosing;
		}

		public virtual FSImage GetFSImage()
		{
			return fsImage;
		}

		public virtual FSEditLog GetEditLog()
		{
			return GetFSImage().GetEditLog();
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckBlock(ExtendedBlock block)
		{
			if (block != null && !this.blockPoolId.Equals(block.GetBlockPoolId()))
			{
				throw new IOException("Unexpected BlockPoolId " + block.GetBlockPoolId() + " - expected "
					 + blockPoolId);
			}
		}

		public virtual long GetMissingBlocksCount()
		{
			// not locking
			return blockManager.GetMissingBlocksCount();
		}

		public virtual long GetMissingReplOneBlocksCount()
		{
			// not locking
			return blockManager.GetMissingReplOneBlocksCount();
		}

		public virtual int GetExpiredHeartbeats()
		{
			return datanodeStatistics.GetExpiredHeartbeats();
		}

		public virtual long GetTransactionsSinceLastCheckpoint()
		{
			return GetEditLog().GetLastWrittenTxId() - GetFSImage().GetStorage().GetMostRecentCheckpointTxId
				();
		}

		public virtual long GetTransactionsSinceLastLogRoll()
		{
			if (IsInStandbyState() || !GetEditLog().IsSegmentOpen())
			{
				return 0;
			}
			else
			{
				return GetEditLog().GetLastWrittenTxId() - GetEditLog().GetCurSegmentTxId() + 1;
			}
		}

		public virtual long GetLastWrittenTransactionId()
		{
			return GetEditLog().GetLastWrittenTxId();
		}

		public virtual long GetLastCheckpointTime()
		{
			return GetFSImage().GetStorage().GetMostRecentCheckpointTime();
		}

		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.GetStats()"></seealso>
		internal virtual long[] GetStats()
		{
			long[] stats = datanodeStatistics.GetStats();
			stats[ClientProtocol.GetStatsUnderReplicatedIdx] = GetUnderReplicatedBlocks();
			stats[ClientProtocol.GetStatsCorruptBlocksIdx] = GetCorruptReplicaBlocks();
			stats[ClientProtocol.GetStatsMissingBlocksIdx] = GetMissingBlocksCount();
			stats[ClientProtocol.GetStatsMissingReplOneBlocksIdx] = GetMissingReplOneBlocksCount
				();
			return stats;
		}

		public virtual long GetCapacityTotal()
		{
			// FSNamesystemMBean
			return datanodeStatistics.GetCapacityTotal();
		}

		public virtual float GetCapacityTotalGB()
		{
			return DFSUtil.RoundBytesToGB(GetCapacityTotal());
		}

		public virtual long GetCapacityUsed()
		{
			// FSNamesystemMBean
			return datanodeStatistics.GetCapacityUsed();
		}

		public virtual float GetCapacityUsedGB()
		{
			return DFSUtil.RoundBytesToGB(GetCapacityUsed());
		}

		public virtual long GetCapacityRemaining()
		{
			// FSNamesystemMBean
			return datanodeStatistics.GetCapacityRemaining();
		}

		public virtual float GetCapacityRemainingGB()
		{
			return DFSUtil.RoundBytesToGB(GetCapacityRemaining());
		}

		public virtual long GetCapacityUsedNonDFS()
		{
			return datanodeStatistics.GetCapacityUsedNonDFS();
		}

		/// <summary>Total number of connections.</summary>
		[Metric]
		public virtual int GetTotalLoad()
		{
			// FSNamesystemMBean
			return datanodeStatistics.GetXceiverCount();
		}

		public virtual int GetNumSnapshottableDirs()
		{
			return this.snapshotManager.GetNumSnapshottableDirs();
		}

		public virtual int GetNumSnapshots()
		{
			return this.snapshotManager.GetNumSnapshots();
		}

		public virtual string GetSnapshotStats()
		{
			IDictionary<string, object> info = new Dictionary<string, object>();
			info["SnapshottableDirectories"] = this.GetNumSnapshottableDirs();
			info["Snapshots"] = this.GetNumSnapshots();
			return JSON.ToString(info);
		}

		internal virtual int GetNumberOfDatanodes(HdfsConstants.DatanodeReportType type)
		{
			ReadLock();
			try
			{
				return GetBlockManager().GetDatanodeManager().GetDatanodeListForReport(type).Count;
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		internal virtual DatanodeInfo[] DatanodeReport(HdfsConstants.DatanodeReportType type
			)
		{
			CheckSuperuserPrivilege();
			CheckOperation(NameNode.OperationCategory.Unchecked);
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Unchecked);
				DatanodeManager dm = GetBlockManager().GetDatanodeManager();
				IList<DatanodeDescriptor> results = dm.GetDatanodeListForReport(type);
				DatanodeInfo[] arr = new DatanodeInfo[results.Count];
				for (int i = 0; i < arr.Length; i++)
				{
					arr[i] = new DatanodeInfo(results[i]);
				}
				return arr;
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		internal virtual DatanodeStorageReport[] GetDatanodeStorageReport(HdfsConstants.DatanodeReportType
			 type)
		{
			CheckSuperuserPrivilege();
			CheckOperation(NameNode.OperationCategory.Unchecked);
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Unchecked);
				DatanodeManager dm = GetBlockManager().GetDatanodeManager();
				IList<DatanodeDescriptor> datanodes = dm.GetDatanodeListForReport(type);
				DatanodeStorageReport[] reports = new DatanodeStorageReport[datanodes.Count];
				for (int i = 0; i < reports.Length; i++)
				{
					DatanodeDescriptor d = datanodes[i];
					reports[i] = new DatanodeStorageReport(new DatanodeInfo(d), d.GetStorageReports()
						);
				}
				return reports;
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <summary>Save namespace image.</summary>
		/// <remarks>
		/// Save namespace image.
		/// This will save current namespace into fsimage file and empty edits file.
		/// Requires superuser privilege and safe mode.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if superuser privilege is violated.
		/// 	</exception>
		/// <exception cref="System.IO.IOException">if</exception>
		internal virtual void SaveNamespace()
		{
			CheckOperation(NameNode.OperationCategory.Unchecked);
			CheckSuperuserPrivilege();
			CpLock();
			// Block if a checkpointing is in progress on standby.
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Unchecked);
				if (!IsInSafeMode())
				{
					throw new IOException("Safe mode should be turned ON " + "in order to create namespace image."
						);
				}
				GetFSImage().SaveNamespace(this);
			}
			finally
			{
				ReadUnlock();
				CpUnlock();
			}
			Log.Info("New namespace image has been created");
		}

		/// <summary>Enables/Disables/Checks restoring failed storage replicas if the storage becomes available again.
		/// 	</summary>
		/// <remarks>
		/// Enables/Disables/Checks restoring failed storage replicas if the storage becomes available again.
		/// Requires superuser privilege.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if superuser privilege is violated.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		internal virtual bool RestoreFailedStorage(string arg)
		{
			CheckSuperuserPrivilege();
			CheckOperation(NameNode.OperationCategory.Unchecked);
			CpLock();
			// Block if a checkpointing is in progress on standby.
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Unchecked);
				// if it is disabled - enable it and vice versa.
				if (arg.Equals("check"))
				{
					return GetFSImage().GetStorage().GetRestoreFailedStorage();
				}
				bool val = arg.Equals("true");
				// false if not
				GetFSImage().GetStorage().SetRestoreFailedStorage(val);
				return val;
			}
			finally
			{
				WriteUnlock();
				CpUnlock();
			}
		}

		internal virtual DateTime GetStartTime()
		{
			return Sharpen.Extensions.CreateDate(startTime);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void FinalizeUpgrade()
		{
			CheckSuperuserPrivilege();
			CheckOperation(NameNode.OperationCategory.Unchecked);
			CpLock();
			// Block if a checkpointing is in progress on standby.
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Unchecked);
				GetFSImage().FinalizeUpgrade(this.IsHaEnabled() && InActiveState());
			}
			finally
			{
				WriteUnlock();
				CpUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void RefreshNodes()
		{
			CheckOperation(NameNode.OperationCategory.Unchecked);
			CheckSuperuserPrivilege();
			GetBlockManager().GetDatanodeManager().RefreshNodes(new HdfsConfiguration());
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetBalancerBandwidth(long bandwidth)
		{
			CheckOperation(NameNode.OperationCategory.Unchecked);
			CheckSuperuserPrivilege();
			GetBlockManager().GetDatanodeManager().SetBalancerBandwidth(bandwidth);
		}

		/// <summary>Persist the new block (the last block of the given file).</summary>
		/// <param name="path"/>
		/// <param name="file"/>
		private void PersistNewBlock(string path, INodeFile file)
		{
			Preconditions.CheckArgument(file.IsUnderConstruction());
			GetEditLog().LogAddBlock(path, file);
			NameNode.stateChangeLog.Debug("persistNewBlock: {} with new block {}," + " current total block count is {}"
				, path, file.GetLastBlock().ToString(), file.GetBlocks().Length);
		}

		/// <summary>SafeModeInfo contains information related to the safe mode.</summary>
		/// <remarks>
		/// SafeModeInfo contains information related to the safe mode.
		/// <p>
		/// An instance of
		/// <see cref="SafeModeInfo"/>
		/// is created when the name node
		/// enters safe mode.
		/// <p>
		/// During name node startup
		/// <see cref="SafeModeInfo"/>
		/// counts the number of
		/// <em>safe blocks</em>, those that have at least the minimal number of
		/// replicas, and calculates the ratio of safe blocks to the total number
		/// of blocks in the system, which is the size of blocks in
		/// <see cref="FSNamesystem.blockManager"/>
		/// . When the ratio reaches the
		/// <see cref="threshold"/>
		/// it starts the SafeModeMonitor daemon in order
		/// to monitor whether the safe mode
		/// <see cref="extension"/>
		/// is passed.
		/// Then it leaves safe mode and destroys itself.
		/// <p>
		/// If safe mode is turned on manually then the number of safe blocks is
		/// not tracked because the name node is not intended to leave safe mode
		/// automatically in the case.
		/// </remarks>
		/// <seealso cref="Org.Apache.Hadoop.Hdfs.Protocol.ClientProtocol.SetSafeMode(Org.Apache.Hadoop.Hdfs.Protocol.HdfsConstants.SafeModeAction, bool)
		/// 	"/>
		public class SafeModeInfo
		{
			/// <summary>Safe mode threshold condition %.</summary>
			private readonly double threshold;

			/// <summary>Safe mode minimum number of datanodes alive</summary>
			private readonly int datanodeThreshold;

			/// <summary>Safe mode extension after the threshold.</summary>
			/// <remarks>
			/// Safe mode extension after the threshold.
			/// Make it volatile so that getSafeModeTip can read the latest value
			/// without taking a lock.
			/// </remarks>
			private volatile int extension;

			/// <summary>Min replication required by safe mode.</summary>
			private readonly int safeReplication;

			/// <summary>threshold for populating needed replication queues</summary>
			private readonly double replQueueThreshold;

			/// <summary>Time when threshold was reached.</summary>
			/// <remarks>
			/// Time when threshold was reached.
			/// <br /> -1 safe mode is off
			/// <br /> 0 safe mode is on, and threshold is not reached yet
			/// <br /> &gt;0 safe mode is on, but we are in extension period
			/// </remarks>
			private long reached = -1;

			private long reachedTimestamp = -1;

			/// <summary>Total number of blocks.</summary>
			internal int blockTotal;

			/// <summary>Number of safe blocks.</summary>
			internal int blockSafe;

			/// <summary>Number of blocks needed to satisfy safe mode threshold condition</summary>
			private int blockThreshold;

			/// <summary>Number of blocks needed before populating replication queues</summary>
			private int blockReplQueueThreshold;

			/// <summary>time of the last status printout</summary>
			private long lastStatusReport = 0;

			/// <summary>Was safemode entered automatically because available resources were low.
			/// 	</summary>
			/// <remarks>
			/// Was safemode entered automatically because available resources were low.
			/// Make it volatile so that getSafeModeTip can read the latest value
			/// without taking a lock.
			/// </remarks>
			private volatile bool resourcesLow = false;

			/// <summary>Should safemode adjust its block totals as blocks come in</summary>
			private bool shouldIncrementallyTrackBlocks = false;

			/// <summary>counter for tracking startup progress of reported blocks</summary>
			private StartupProgress.Counter awaitingReportedBlocksCounter;

			/// <summary>
			/// Creates SafeModeInfo when the name node enters
			/// automatic safe mode at startup.
			/// </summary>
			/// <param name="conf">configuration</param>
			private SafeModeInfo(FSNamesystem _enclosing, Configuration conf)
			{
				this._enclosing = _enclosing;
				// configuration fields
				// internal fields
				this.threshold = conf.GetFloat(DFSConfigKeys.DfsNamenodeSafemodeThresholdPctKey, 
					DFSConfigKeys.DfsNamenodeSafemodeThresholdPctDefault);
				if (this.threshold > 1.0)
				{
					FSNamesystem.Log.Warn("The threshold value should't be greater than 1, threshold: "
						 + this.threshold);
				}
				this.datanodeThreshold = conf.GetInt(DFSConfigKeys.DfsNamenodeSafemodeMinDatanodesKey
					, DFSConfigKeys.DfsNamenodeSafemodeMinDatanodesDefault);
				this.extension = conf.GetInt(DFSConfigKeys.DfsNamenodeSafemodeExtensionKey, 0);
				this.safeReplication = conf.GetInt(DFSConfigKeys.DfsNamenodeReplicationMinKey, DFSConfigKeys
					.DfsNamenodeReplicationMinDefault);
				FSNamesystem.Log.Info(DFSConfigKeys.DfsNamenodeSafemodeThresholdPctKey + " = " + 
					this.threshold);
				FSNamesystem.Log.Info(DFSConfigKeys.DfsNamenodeSafemodeMinDatanodesKey + " = " + 
					this.datanodeThreshold);
				FSNamesystem.Log.Info(DFSConfigKeys.DfsNamenodeSafemodeExtensionKey + "     = " +
					 this.extension);
				// default to safe mode threshold (i.e., don't populate queues before leaving safe mode)
				this.replQueueThreshold = conf.GetFloat(DFSConfigKeys.DfsNamenodeReplQueueThresholdPctKey
					, (float)this.threshold);
				this.blockTotal = 0;
				this.blockSafe = 0;
			}

			/// <summary>
			/// In the HA case, the StandbyNode can be in safemode while the namespace
			/// is modified by the edit log tailer.
			/// </summary>
			/// <remarks>
			/// In the HA case, the StandbyNode can be in safemode while the namespace
			/// is modified by the edit log tailer. In this case, the number of total
			/// blocks changes as edits are processed (eg blocks are added and deleted).
			/// However, we don't want to do the incremental tracking during the
			/// startup-time loading process -- only once the initial total has been
			/// set after the image has been loaded.
			/// </remarks>
			private bool ShouldIncrementallyTrackBlocks()
			{
				return this.shouldIncrementallyTrackBlocks;
			}

			/// <summary>
			/// Creates SafeModeInfo when safe mode is entered manually, or because
			/// available resources are low.
			/// </summary>
			/// <remarks>
			/// Creates SafeModeInfo when safe mode is entered manually, or because
			/// available resources are low.
			/// The
			/// <see cref="threshold"/>
			/// is set to 1.5 so that it could never be reached.
			/// <see cref="blockTotal"/>
			/// is set to -1 to indicate that safe mode is manual.
			/// </remarks>
			/// <seealso cref="SafeModeInfo"/>
			private SafeModeInfo(FSNamesystem _enclosing, bool resourcesLow)
			{
				this._enclosing = _enclosing;
				this.threshold = 1.5f;
				// this threshold can never be reached
				this.datanodeThreshold = int.MaxValue;
				this.extension = int.MaxValue;
				this.safeReplication = short.MaxValue + 1;
				// more than maxReplication
				this.replQueueThreshold = 1.5f;
				// can never be reached
				this.blockTotal = -1;
				this.blockSafe = -1;
				this.resourcesLow = resourcesLow;
				this.Enter();
				this.ReportStatus("STATE* Safe mode is ON.", true);
			}

			/// <summary>Check if safe mode is on.</summary>
			/// <returns>true if in safe mode</returns>
			private bool IsOn()
			{
				lock (this)
				{
					this.DoConsistencyCheck();
					return this.reached >= 0;
				}
			}

			/// <summary>Enter safe mode.</summary>
			private void Enter()
			{
				this.reached = 0;
				this.reachedTimestamp = 0;
			}

			/// <summary>Leave safe mode.</summary>
			/// <remarks>
			/// Leave safe mode.
			/// <p>
			/// Check for invalid, under- & over-replicated blocks in the end of startup.
			/// </remarks>
			private void Leave()
			{
				lock (this)
				{
					// if not done yet, initialize replication queues.
					// In the standby, do not populate repl queues
					if (!this._enclosing.IsPopulatingReplQueues() && this._enclosing.ShouldPopulateReplQueues
						())
					{
						this._enclosing.InitializeReplQueues();
					}
					long timeInSafemode = Time.Now() - this._enclosing.startTime;
					NameNode.stateChangeLog.Info("STATE* Leaving safe mode after " + timeInSafemode /
						 1000 + " secs");
					NameNode.GetNameNodeMetrics().SetSafeModeTime((int)timeInSafemode);
					//Log the following only once (when transitioning from ON -> OFF)
					if (this.reached >= 0)
					{
						NameNode.stateChangeLog.Info("STATE* Safe mode is OFF");
					}
					this.reached = -1;
					this.reachedTimestamp = -1;
					this._enclosing.safeMode = null;
					NetworkTopology nt = this._enclosing.blockManager.GetDatanodeManager().GetNetworkTopology
						();
					NameNode.stateChangeLog.Info("STATE* Network topology has " + nt.GetNumOfRacks() 
						+ " racks and " + nt.GetNumOfLeaves() + " datanodes");
					NameNode.stateChangeLog.Info("STATE* UnderReplicatedBlocks has " + this._enclosing
						.blockManager.NumOfUnderReplicatedBlocks() + " blocks");
					this._enclosing.StartSecretManagerIfNecessary();
					// If startup has not yet completed, end safemode phase.
					StartupProgress prog = NameNode.GetStartupProgress();
					if (prog.GetStatus(Phase.Safemode) != Status.Complete)
					{
						prog.EndStep(Phase.Safemode, FSNamesystem.StepAwaitingReportedBlocks);
						prog.EndPhase(Phase.Safemode);
					}
				}
			}

			/// <summary>
			/// Check whether we have reached the threshold for
			/// initializing replication queues.
			/// </summary>
			private bool CanInitializeReplQueues()
			{
				lock (this)
				{
					return this._enclosing.ShouldPopulateReplQueues() && this.blockSafe >= this.blockReplQueueThreshold;
				}
			}

			/// <summary>
			/// Safe mode can be turned off iff
			/// the threshold is reached and
			/// the extension time have passed.
			/// </summary>
			/// <returns>true if can leave or false otherwise.</returns>
			private bool CanLeave()
			{
				lock (this)
				{
					if (this.reached == 0)
					{
						return false;
					}
					if (Time.MonotonicNow() - this.reached < this.extension)
					{
						this.ReportStatus("STATE* Safe mode ON, in safe mode extension.", false);
						return false;
					}
					if (this.NeedEnter())
					{
						this.ReportStatus("STATE* Safe mode ON, thresholds not met.", false);
						return false;
					}
					return true;
				}
			}

			/// <summary>
			/// There is no need to enter safe mode
			/// if DFS is empty or
			/// <see cref="threshold"/>
			/// == 0
			/// </summary>
			private bool NeedEnter()
			{
				return (this.threshold != 0 && this.blockSafe < this.blockThreshold) || (this.datanodeThreshold
					 != 0 && this._enclosing.GetNumLiveDataNodes() < this.datanodeThreshold) || (!this
					._enclosing.NameNodeHasResourcesAvailable());
			}

			/// <summary>Check and trigger safe mode if needed.</summary>
			private void CheckMode()
			{
				// Have to have write-lock since leaving safemode initializes
				// repl queues, which requires write lock
				System.Diagnostics.Debug.Assert(this._enclosing.HasWriteLock());
				if (this._enclosing.InTransitionToActive())
				{
					return;
				}
				// if smmthread is already running, the block threshold must have been 
				// reached before, there is no need to enter the safe mode again
				if (this._enclosing.smmthread == null && this.NeedEnter())
				{
					this.Enter();
					// check if we are ready to initialize replication queues
					if (this.CanInitializeReplQueues() && !this._enclosing.IsPopulatingReplQueues() &&
						 !this._enclosing.haEnabled)
					{
						this._enclosing.InitializeReplQueues();
					}
					this.ReportStatus("STATE* Safe mode ON.", false);
					return;
				}
				// the threshold is reached or was reached before
				if (!this.IsOn() || this.extension <= 0 || this.threshold <= 0)
				{
					// safe mode is off
					// don't need to wait
					this.Leave();
					// leave safe mode
					return;
				}
				if (this.reached > 0)
				{
					// threshold has already been reached before
					this.ReportStatus("STATE* Safe mode ON.", false);
					return;
				}
				// start monitor
				this.reached = Time.MonotonicNow();
				this.reachedTimestamp = Time.Now();
				if (this._enclosing.smmthread == null)
				{
					this._enclosing.smmthread = new Daemon(new FSNamesystem.SafeModeMonitor(this));
					this._enclosing.smmthread.Start();
					this.ReportStatus("STATE* Safe mode extension entered.", true);
				}
				// check if we are ready to initialize replication queues
				if (this.CanInitializeReplQueues() && !this._enclosing.IsPopulatingReplQueues() &&
					 !this._enclosing.haEnabled)
				{
					this._enclosing.InitializeReplQueues();
				}
			}

			/// <summary>Set total number of blocks.</summary>
			private void SetBlockTotal(int total)
			{
				lock (this)
				{
					this.blockTotal = total;
					this.blockThreshold = (int)(this.blockTotal * this.threshold);
					this.blockReplQueueThreshold = (int)(this.blockTotal * this.replQueueThreshold);
					if (this._enclosing.haEnabled)
					{
						// After we initialize the block count, any further namespace
						// modifications done while in safe mode need to keep track
						// of the number of total blocks in the system.
						this.shouldIncrementallyTrackBlocks = true;
					}
					if (this.blockSafe < 0)
					{
						this.blockSafe = 0;
					}
					this.CheckMode();
				}
			}

			/// <summary>
			/// Increment number of safe blocks if current block has
			/// reached minimal replication.
			/// </summary>
			/// <param name="replication">current replication</param>
			private void IncrementSafeBlockCount(short replication)
			{
				lock (this)
				{
					if (replication == this.safeReplication)
					{
						this.blockSafe++;
						// Report startup progress only if we haven't completed startup yet.
						StartupProgress prog = NameNode.GetStartupProgress();
						if (prog.GetStatus(Phase.Safemode) != Status.Complete)
						{
							if (this.awaitingReportedBlocksCounter == null)
							{
								this.awaitingReportedBlocksCounter = prog.GetCounter(Phase.Safemode, FSNamesystem
									.StepAwaitingReportedBlocks);
							}
							this.awaitingReportedBlocksCounter.Increment();
						}
						this.CheckMode();
					}
				}
			}

			/// <summary>
			/// Decrement number of safe blocks if current block has
			/// fallen below minimal replication.
			/// </summary>
			/// <param name="replication">current replication</param>
			private void DecrementSafeBlockCount(short replication)
			{
				lock (this)
				{
					if (replication == this.safeReplication - 1)
					{
						this.blockSafe--;
						//blockSafe is set to -1 in manual / low resources safemode
						System.Diagnostics.Debug.Assert(this.blockSafe >= 0 || this.IsManual() || this.AreResourcesLow
							());
						this.CheckMode();
					}
				}
			}

			/// <summary>Check if safe mode was entered manually</summary>
			private bool IsManual()
			{
				return this.extension == int.MaxValue;
			}

			/// <summary>Set manual safe mode.</summary>
			private void SetManual()
			{
				lock (this)
				{
					this.extension = int.MaxValue;
				}
			}

			/// <summary>Check if safe mode was entered due to resources being low.</summary>
			private bool AreResourcesLow()
			{
				return this.resourcesLow;
			}

			/// <summary>Set that resources are low for this instance of safe mode.</summary>
			private void SetResourcesLow()
			{
				this.resourcesLow = true;
			}

			/// <summary>A tip on how safe mode is to be turned off: manually or automatically.</summary>
			internal virtual string GetTurnOffTip()
			{
				if (!this.IsOn())
				{
					return "Safe mode is OFF.";
				}
				//Manual OR low-resource safemode. (Admin intervention required)
				string adminMsg = "It was turned on manually. ";
				if (this.AreResourcesLow())
				{
					adminMsg = "Resources are low on NN. Please add or free up more " + "resources then turn off safe mode manually. NOTE:  If you turn off"
						 + " safe mode before adding resources, " + "the NN will immediately return to safe mode. ";
				}
				if (this.IsManual() || this.AreResourcesLow())
				{
					return adminMsg + "Use \"hdfs dfsadmin -safemode leave\" to turn safe mode off.";
				}
				bool thresholdsMet = true;
				int numLive = this._enclosing.GetNumLiveDataNodes();
				string msg = string.Empty;
				if (this.blockSafe < this.blockThreshold)
				{
					msg += string.Format("The reported blocks %d needs additional %d" + " blocks to reach the threshold %.4f of total blocks %d.%n"
						, this.blockSafe, (this.blockThreshold - this.blockSafe) + 1, this.threshold, this
						.blockTotal);
					thresholdsMet = false;
				}
				else
				{
					msg += string.Format("The reported blocks %d has reached the threshold" + " %.4f of total blocks %d. "
						, this.blockSafe, this.threshold, this.blockTotal);
				}
				if (numLive < this.datanodeThreshold)
				{
					msg += string.Format("The number of live datanodes %d needs an additional %d live "
						 + "datanodes to reach the minimum number %d.%n", numLive, (this.datanodeThreshold
						 - numLive), this.datanodeThreshold);
					thresholdsMet = false;
				}
				else
				{
					msg += string.Format("The number of live datanodes %d has reached " + "the minimum number %d. "
						, numLive, this.datanodeThreshold);
				}
				msg += (this.reached > 0) ? "In safe mode extension. " : string.Empty;
				msg += "Safe mode will be turned off automatically ";
				if (!thresholdsMet)
				{
					msg += "once the thresholds have been reached.";
				}
				else
				{
					if (this.reached + this.extension - Time.MonotonicNow() > 0)
					{
						msg += ("in " + (this.reached + this.extension - Time.MonotonicNow()) / 1000 + " seconds."
							);
					}
					else
					{
						msg += "soon.";
					}
				}
				return msg;
			}

			/// <summary>Print status every 20 seconds.</summary>
			private void ReportStatus(string msg, bool rightNow)
			{
				long curTime = Time.Now();
				if (!rightNow && (curTime - this.lastStatusReport < 20 * 1000))
				{
					return;
				}
				NameNode.stateChangeLog.Info(msg + " \n" + this.GetTurnOffTip());
				this.lastStatusReport = curTime;
			}

			public override string ToString()
			{
				string resText = "Current safe blocks = " + this.blockSafe + ". Target blocks = "
					 + this.blockThreshold + " for threshold = %" + this.threshold + ". Minimal replication = "
					 + this.safeReplication + ".";
				if (this.reached > 0)
				{
					resText += " Threshold was reached " + Sharpen.Extensions.CreateDate(this.reachedTimestamp
						) + ".";
				}
				return resText;
			}

			/// <summary>Checks consistency of the class state.</summary>
			/// <remarks>
			/// Checks consistency of the class state.
			/// This is costly so only runs if asserts are enabled.
			/// </remarks>
			private void DoConsistencyCheck()
			{
				bool assertsOn = false;
				System.Diagnostics.Debug.Assert(assertsOn = true);
				// set to true if asserts are on
				if (!assertsOn)
				{
					return;
				}
				if (this.blockTotal == -1 && this.blockSafe == -1)
				{
					return;
				}
				// manual safe mode
				int activeBlocks = this._enclosing.blockManager.GetActiveBlockCount();
				if ((this.blockTotal != activeBlocks) && !(this.blockSafe >= 0 && this.blockSafe 
					<= this.blockTotal))
				{
					throw new Exception(" SafeMode: Inconsistent filesystem state: " + "SafeMode data: blockTotal="
						 + this.blockTotal + " blockSafe=" + this.blockSafe + "; " + "BlockManager data: active="
						 + activeBlocks);
				}
			}

			private void AdjustBlockTotals(int deltaSafe, int deltaTotal)
			{
				lock (this)
				{
					if (!this.shouldIncrementallyTrackBlocks)
					{
						return;
					}
					System.Diagnostics.Debug.Assert(this._enclosing.haEnabled);
					if (FSNamesystem.Log.IsDebugEnabled())
					{
						FSNamesystem.Log.Debug("Adjusting block totals from " + this.blockSafe + "/" + this
							.blockTotal + " to " + (this.blockSafe + deltaSafe) + "/" + (this.blockTotal + deltaTotal
							));
					}
					System.Diagnostics.Debug.Assert(this.blockSafe + deltaSafe >= 0, "Can't reduce blockSafe "
						 + this.blockSafe + " by " + deltaSafe + ": would be negative");
					System.Diagnostics.Debug.Assert(this.blockTotal + deltaTotal >= 0, "Can't reduce blockTotal "
						 + this.blockTotal + " by " + deltaTotal + ": would be negative");
					this.blockSafe += deltaSafe;
					this.SetBlockTotal(this.blockTotal + deltaTotal);
				}
			}

			private readonly FSNamesystem _enclosing;
		}

		/// <summary>Periodically check whether it is time to leave safe mode.</summary>
		/// <remarks>
		/// Periodically check whether it is time to leave safe mode.
		/// This thread starts when the threshold level is reached.
		/// </remarks>
		internal class SafeModeMonitor : Runnable
		{
			/// <summary>
			/// interval in msec for checking safe mode:
			/// <value/>
			/// 
			/// </summary>
			private const long recheckInterval = 1000;

			public virtual void Run()
			{
				while (this._enclosing.fsRunning)
				{
					this._enclosing.WriteLock();
					try
					{
						if (this._enclosing.safeMode == null)
						{
							// Not in safe mode.
							break;
						}
						if (this._enclosing.safeMode.CanLeave())
						{
							// Leave safe mode.
							this._enclosing.safeMode.Leave();
							this._enclosing.smmthread = null;
							break;
						}
					}
					finally
					{
						this._enclosing.WriteUnlock();
					}
					try
					{
						Sharpen.Thread.Sleep(FSNamesystem.SafeModeMonitor.recheckInterval);
					}
					catch (Exception)
					{
					}
				}
				// Ignored
				if (!this._enclosing.fsRunning)
				{
					FSNamesystem.Log.Info("NameNode is being shutdown, exit SafeModeMonitor thread");
				}
			}

			internal SafeModeMonitor(FSNamesystem _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly FSNamesystem _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual bool SetSafeMode(HdfsConstants.SafeModeAction action)
		{
			if (action != HdfsConstants.SafeModeAction.SafemodeGet)
			{
				CheckSuperuserPrivilege();
				switch (action)
				{
					case HdfsConstants.SafeModeAction.SafemodeLeave:
					{
						// leave safe mode
						LeaveSafeMode();
						break;
					}

					case HdfsConstants.SafeModeAction.SafemodeEnter:
					{
						// enter safe mode
						EnterSafeMode(false);
						break;
					}

					default:
					{
						Log.Error("Unexpected safe mode action");
						break;
					}
				}
			}
			return IsInSafeMode();
		}

		public virtual void CheckSafeMode()
		{
			// safeMode is volatile, and may be set to null at any time
			FSNamesystem.SafeModeInfo safeMode = this.safeMode;
			if (safeMode != null)
			{
				safeMode.CheckMode();
			}
		}

		public virtual bool IsInSafeMode()
		{
			// safeMode is volatile, and may be set to null at any time
			FSNamesystem.SafeModeInfo safeMode = this.safeMode;
			if (safeMode == null)
			{
				return false;
			}
			return safeMode.IsOn();
		}

		public virtual bool IsInStartupSafeMode()
		{
			// safeMode is volatile, and may be set to null at any time
			FSNamesystem.SafeModeInfo safeMode = this.safeMode;
			if (safeMode == null)
			{
				return false;
			}
			// If the NN is in safemode, and not due to manual / low resources, we
			// assume it must be because of startup. If the NN had low resources during
			// startup, we assume it came out of startup safemode and it is now in low
			// resources safemode
			return !safeMode.IsManual() && !safeMode.AreResourcesLow() && safeMode.IsOn();
		}

		/// <summary>Check if replication queues are to be populated</summary>
		/// <returns>true when node is HAState.Active and not in the very first safemode</returns>
		public virtual bool IsPopulatingReplQueues()
		{
			if (!ShouldPopulateReplQueues())
			{
				return false;
			}
			return initializedReplQueues;
		}

		private bool ShouldPopulateReplQueues()
		{
			if (haContext == null || haContext.GetState() == null)
			{
				return false;
			}
			return haContext.GetState().ShouldPopulateReplQueues();
		}

		public virtual void IncrementSafeBlockCount(int replication)
		{
			// safeMode is volatile, and may be set to null at any time
			FSNamesystem.SafeModeInfo safeMode = this.safeMode;
			if (safeMode == null)
			{
				return;
			}
			safeMode.IncrementSafeBlockCount((short)replication);
		}

		public virtual void DecrementSafeBlockCount(Org.Apache.Hadoop.Hdfs.Protocol.Block
			 b)
		{
			// safeMode is volatile, and may be set to null at any time
			FSNamesystem.SafeModeInfo safeMode = this.safeMode;
			if (safeMode == null)
			{
				// mostly true
				return;
			}
			BlockInfoContiguous storedBlock = GetStoredBlock(b);
			if (storedBlock.IsComplete())
			{
				safeMode.DecrementSafeBlockCount((short)blockManager.CountNodes(b).LiveReplicas()
					);
			}
		}

		/// <summary>Adjust the total number of blocks safe and expected during safe mode.</summary>
		/// <remarks>
		/// Adjust the total number of blocks safe and expected during safe mode.
		/// If safe mode is not currently on, this is a no-op.
		/// </remarks>
		/// <param name="deltaSafe">the change in number of safe blocks</param>
		/// <param name="deltaTotal">the change i nnumber of total blocks expected</param>
		public virtual void AdjustSafeModeBlockTotals(int deltaSafe, int deltaTotal)
		{
			// safeMode is volatile, and may be set to null at any time
			FSNamesystem.SafeModeInfo safeMode = this.safeMode;
			if (safeMode == null)
			{
				return;
			}
			safeMode.AdjustBlockTotals(deltaSafe, deltaTotal);
		}

		/// <summary>Set the total number of blocks in the system.</summary>
		public virtual void SetBlockTotal()
		{
			// safeMode is volatile, and may be set to null at any time
			FSNamesystem.SafeModeInfo safeMode = this.safeMode;
			if (safeMode == null)
			{
				return;
			}
			safeMode.SetBlockTotal((int)GetCompleteBlocksTotal());
		}

		/// <summary>Get the total number of blocks in the system.</summary>
		[Metric]
		public virtual long GetBlocksTotal()
		{
			// FSNamesystemMBean
			return blockManager.GetTotalBlocks();
		}

		/// <summary>Get the total number of COMPLETE blocks in the system.</summary>
		/// <remarks>
		/// Get the total number of COMPLETE blocks in the system.
		/// For safe mode only complete blocks are counted.
		/// </remarks>
		private long GetCompleteBlocksTotal()
		{
			// Calculate number of blocks under construction
			long numUCBlocks = 0;
			ReadLock();
			numUCBlocks = leaseManager.GetNumUnderConstructionBlocks();
			try
			{
				return GetBlocksTotal() - numUCBlocks;
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <summary>Enter safe mode.</summary>
		/// <remarks>Enter safe mode. If resourcesLow is false, then we assume it is manual</remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void EnterSafeMode(bool resourcesLow)
		{
			WriteLock();
			try
			{
				// Stop the secret manager, since rolling the master key would
				// try to write to the edit log
				StopSecretManager();
				// Ensure that any concurrent operations have been fully synced
				// before entering safe mode. This ensures that the FSImage
				// is entirely stable on disk as soon as we're in safe mode.
				bool isEditlogOpenForWrite = GetEditLog().IsOpenForWrite();
				// Before Editlog is in OpenForWrite mode, editLogStream will be null. So,
				// logSyncAll call can be called only when Edlitlog is in OpenForWrite mode
				if (isEditlogOpenForWrite)
				{
					GetEditLog().LogSyncAll();
				}
				if (!IsInSafeMode())
				{
					safeMode = new FSNamesystem.SafeModeInfo(this, resourcesLow);
					return;
				}
				if (resourcesLow)
				{
					safeMode.SetResourcesLow();
				}
				else
				{
					safeMode.SetManual();
				}
				if (isEditlogOpenForWrite)
				{
					GetEditLog().LogSyncAll();
				}
				NameNode.stateChangeLog.Info("STATE* Safe mode is ON" + safeMode.GetTurnOffTip());
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <summary>Leave safe mode.</summary>
		internal virtual void LeaveSafeMode()
		{
			WriteLock();
			try
			{
				if (!IsInSafeMode())
				{
					NameNode.stateChangeLog.Info("STATE* Safe mode is already OFF");
					return;
				}
				safeMode.Leave();
			}
			finally
			{
				WriteUnlock();
			}
		}

		internal virtual string GetSafeModeTip()
		{
			// There is no need to take readLock.
			// Don't use isInSafeMode as this.safeMode might be set to null.
			// after isInSafeMode returns.
			bool inSafeMode;
			FSNamesystem.SafeModeInfo safeMode = this.safeMode;
			if (safeMode == null)
			{
				inSafeMode = false;
			}
			else
			{
				inSafeMode = safeMode.IsOn();
			}
			if (!inSafeMode)
			{
				return string.Empty;
			}
			else
			{
				return safeMode.GetTurnOffTip();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual CheckpointSignature RollEditLog()
		{
			CheckSuperuserPrivilege();
			CheckOperation(NameNode.OperationCategory.Journal);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Journal);
				CheckNameNodeSafeMode("Log not rolled");
				if (Org.Apache.Hadoop.Ipc.Server.IsRpcInvocation())
				{
					Log.Info("Roll Edit Log from " + Org.Apache.Hadoop.Ipc.Server.GetRemoteAddress());
				}
				return GetFSImage().RollEditLog();
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual NamenodeCommand StartCheckpoint(NamenodeRegistration backupNode, 
			NamenodeRegistration activeNamenode)
		{
			CheckOperation(NameNode.OperationCategory.Checkpoint);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Checkpoint);
				CheckNameNodeSafeMode("Checkpoint not started");
				Log.Info("Start checkpoint for " + backupNode.GetAddress());
				NamenodeCommand cmd = GetFSImage().StartCheckpoint(backupNode, activeNamenode);
				GetEditLog().LogSync();
				return cmd;
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ProcessIncrementalBlockReport(DatanodeID nodeID, StorageReceivedDeletedBlocks
			 srdb)
		{
			WriteLock();
			try
			{
				blockManager.ProcessIncrementalBlockReport(nodeID, srdb);
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void EndCheckpoint(NamenodeRegistration registration, CheckpointSignature
			 sig)
		{
			CheckOperation(NameNode.OperationCategory.Checkpoint);
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Checkpoint);
				CheckNameNodeSafeMode("Checkpoint not ended");
				Log.Info("End checkpoint for " + registration.GetAddress());
				GetFSImage().EndCheckpoint(sig);
			}
			finally
			{
				ReadUnlock();
			}
		}

		internal virtual PermissionStatus CreateFsOwnerPermissions(FsPermission permission
			)
		{
			return new PermissionStatus(fsOwner.GetShortUserName(), supergroup, permission);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckUnreadableBySuperuser(FSPermissionChecker pc, INode inode, int 
			snapshotId)
		{
			if (pc.IsSuperUser())
			{
				foreach (XAttr xattr in FSDirXAttrOp.GetXAttrs(dir, inode, snapshotId))
				{
					if (XAttrHelper.GetPrefixName(xattr).Equals(HdfsServerConstants.SecurityXattrUnreadableBySuperuser
						))
					{
						throw new AccessControlException("Access is denied for " + pc.GetUser() + " since the superuser is not allowed to "
							 + "perform this operation.");
					}
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		public virtual void CheckSuperuserPrivilege()
		{
			if (isPermissionEnabled)
			{
				FSPermissionChecker pc = GetPermissionChecker();
				pc.CheckSuperuserPrivilege();
			}
		}

		/// <summary>
		/// Check to see if we have exceeded the limit on the number
		/// of inodes.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckFsObjectLimit()
		{
			if (maxFsObjects != 0 && maxFsObjects <= dir.TotalInodes() + GetBlocksTotal())
			{
				throw new IOException("Exceeded the configured number of objects " + maxFsObjects
					 + " in the filesystem.");
			}
		}

		/// <summary>Get the total number of objects in the system.</summary>
		public virtual long GetMaxObjects()
		{
			// FSNamesystemMBean
			return maxFsObjects;
		}

		[Metric]
		public virtual long GetFilesTotal()
		{
			// FSNamesystemMBean
			// There is no need to take fSNamesystem's lock as
			// FSDirectory has its own lock.
			return this.dir.TotalInodes();
		}

		[Metric]
		public virtual long GetPendingReplicationBlocks()
		{
			// FSNamesystemMBean
			return blockManager.GetPendingReplicationBlocksCount();
		}

		[Metric]
		public virtual long GetUnderReplicatedBlocks()
		{
			// FSNamesystemMBean
			return blockManager.GetUnderReplicatedBlocksCount();
		}

		/// <summary>Returns number of blocks with corrupt replicas</summary>
		public virtual long GetCorruptReplicaBlocks()
		{
			return blockManager.GetCorruptReplicaBlocksCount();
		}

		[Metric]
		public virtual long GetScheduledReplicationBlocks()
		{
			// FSNamesystemMBean
			return blockManager.GetScheduledReplicationBlocksCount();
		}

		[Metric]
		public virtual long GetPendingDeletionBlocks()
		{
			return blockManager.GetPendingDeletionBlocksCount();
		}

		public virtual long GetBlockDeletionStartTime()
		{
			return startTime + blockManager.GetStartupDelayBlockDeletionInMs();
		}

		[Metric]
		public virtual long GetExcessBlocks()
		{
			return blockManager.GetExcessBlocksCount();
		}

		// HA-only metric
		[Metric]
		public virtual long GetPostponedMisreplicatedBlocks()
		{
			return blockManager.GetPostponedMisreplicatedBlocksCount();
		}

		// HA-only metric
		[Metric]
		public virtual int GetPendingDataNodeMessageCount()
		{
			return blockManager.GetPendingDataNodeMessageCount();
		}

		// HA-only metric
		[Metric]
		public virtual string GetHAState()
		{
			return haContext.GetState().ToString();
		}

		// HA-only metric
		[Metric]
		public virtual long GetMillisSinceLastLoadedEdits()
		{
			if (IsInStandbyState() && editLogTailer != null)
			{
				return Time.MonotonicNow() - editLogTailer.GetLastLoadTimeMs();
			}
			else
			{
				return 0;
			}
		}

		[Metric]
		public virtual int GetBlockCapacity()
		{
			return blockManager.GetCapacity();
		}

		public virtual string GetFSState()
		{
			// FSNamesystemMBean
			return IsInSafeMode() ? "safeMode" : "Operational";
		}

		private ObjectName mbeanName;

		private ObjectName mxbeanName;

		/// <summary>
		/// Register the FSNamesystem MBean using the name
		/// "hadoop:service=NameNode,name=FSNamesystemState"
		/// </summary>
		private void RegisterMBean()
		{
			// We can only implement one MXBean interface, so we keep the old one.
			try
			{
				StandardMBean bean = new StandardMBean(this, typeof(FSNamesystemMBean));
				mbeanName = MBeans.Register("NameNode", "FSNamesystemState", bean);
			}
			catch (NotCompliantMBeanException e)
			{
				throw new RuntimeException("Bad MBean setup", e);
			}
			Log.Info("Registered FSNamesystemState MBean");
		}

		/// <summary>shutdown FSNamesystem</summary>
		internal virtual void Shutdown()
		{
			if (snapshotManager != null)
			{
				snapshotManager.Shutdown();
			}
			if (mbeanName != null)
			{
				MBeans.Unregister(mbeanName);
				mbeanName = null;
			}
			if (mxbeanName != null)
			{
				MBeans.Unregister(mxbeanName);
				mxbeanName = null;
			}
			if (dir != null)
			{
				dir.Shutdown();
			}
			if (blockManager != null)
			{
				blockManager.Shutdown();
			}
		}

		public virtual int GetNumLiveDataNodes()
		{
			// FSNamesystemMBean
			return GetBlockManager().GetDatanodeManager().GetNumLiveDataNodes();
		}

		public virtual int GetNumDeadDataNodes()
		{
			// FSNamesystemMBean
			return GetBlockManager().GetDatanodeManager().GetNumDeadDataNodes();
		}

		public virtual int GetNumDecomLiveDataNodes()
		{
			// FSNamesystemMBean
			IList<DatanodeDescriptor> live = new AList<DatanodeDescriptor>();
			GetBlockManager().GetDatanodeManager().FetchDatanodes(live, null, true);
			int liveDecommissioned = 0;
			foreach (DatanodeDescriptor node in live)
			{
				liveDecommissioned += node.IsDecommissioned() ? 1 : 0;
			}
			return liveDecommissioned;
		}

		public virtual int GetNumDecomDeadDataNodes()
		{
			// FSNamesystemMBean
			IList<DatanodeDescriptor> dead = new AList<DatanodeDescriptor>();
			GetBlockManager().GetDatanodeManager().FetchDatanodes(null, dead, true);
			int deadDecommissioned = 0;
			foreach (DatanodeDescriptor node in dead)
			{
				deadDecommissioned += node.IsDecommissioned() ? 1 : 0;
			}
			return deadDecommissioned;
		}

		public virtual int GetVolumeFailuresTotal()
		{
			// FSNamesystemMBean
			IList<DatanodeDescriptor> live = new AList<DatanodeDescriptor>();
			GetBlockManager().GetDatanodeManager().FetchDatanodes(live, null, true);
			int volumeFailuresTotal = 0;
			foreach (DatanodeDescriptor node in live)
			{
				volumeFailuresTotal += node.GetVolumeFailures();
			}
			return volumeFailuresTotal;
		}

		public virtual long GetEstimatedCapacityLostTotal()
		{
			// FSNamesystemMBean
			IList<DatanodeDescriptor> live = new AList<DatanodeDescriptor>();
			GetBlockManager().GetDatanodeManager().FetchDatanodes(live, null, true);
			long estimatedCapacityLostTotal = 0;
			foreach (DatanodeDescriptor node in live)
			{
				VolumeFailureSummary volumeFailureSummary = node.GetVolumeFailureSummary();
				if (volumeFailureSummary != null)
				{
					estimatedCapacityLostTotal += volumeFailureSummary.GetEstimatedCapacityLostTotal(
						);
				}
			}
			return estimatedCapacityLostTotal;
		}

		public virtual int GetNumDecommissioningDataNodes()
		{
			// FSNamesystemMBean
			return GetBlockManager().GetDatanodeManager().GetDecommissioningNodes().Count;
		}

		public virtual int GetNumStaleDataNodes()
		{
			// FSNamesystemMBean
			return GetBlockManager().GetDatanodeManager().GetNumStaleNodes();
		}

		/// <summary>
		/// Storages are marked as "content stale" after NN restart or fails over and
		/// before NN receives the first Heartbeat followed by the first Blockreport.
		/// </summary>
		public virtual int GetNumStaleStorages()
		{
			// FSNamesystemMBean
			return GetBlockManager().GetDatanodeManager().GetNumStaleStorages();
		}

		public virtual string GetTopUserOpCounts()
		{
			// FSNamesystemMBean
			if (!topConf.isEnabled)
			{
				return null;
			}
			DateTime now = new DateTime();
			IList<RollingWindowManager.TopWindow> topWindows = topMetrics.GetTopWindows();
			IDictionary<string, object> topMap = new SortedDictionary<string, object>();
			topMap["windows"] = topWindows;
			topMap["timestamp"] = DFSUtil.DateToIso8601String(now);
			ObjectMapper mapper = new ObjectMapper();
			try
			{
				return mapper.WriteValueAsString(topMap);
			}
			catch (IOException e)
			{
				Log.Warn("Failed to fetch TopUser metrics", e);
			}
			return null;
		}

		/// <summary>Increments, logs and then returns the stamp</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		internal virtual long NextGenerationStamp(bool legacyBlock)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			CheckNameNodeSafeMode("Cannot get next generation stamp");
			long gs = blockIdManager.NextGenerationStamp(legacyBlock);
			if (legacyBlock)
			{
				GetEditLog().LogGenerationStampV1(gs);
			}
			else
			{
				GetEditLog().LogGenerationStampV2(gs);
			}
			// NB: callers sync the log
			return gs;
		}

		/// <summary>Increments, logs and then returns the block ID</summary>
		/// <exception cref="System.IO.IOException"/>
		private long NextBlockId()
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			CheckNameNodeSafeMode("Cannot get next block ID");
			long blockId = blockIdManager.NextBlockId();
			GetEditLog().LogAllocateBlockId(blockId);
			// NB: callers sync the log
			return blockId;
		}

		private bool IsFileDeleted(INodeFile file)
		{
			// Not in the inodeMap or in the snapshot but marked deleted.
			if (dir.GetInode(file.GetId()) == null)
			{
				return true;
			}
			// look at the path hierarchy to see if one parent is deleted by recursive
			// deletion
			INode tmpChild = file;
			INodeDirectory tmpParent = file.GetParent();
			while (true)
			{
				if (tmpParent == null)
				{
					return true;
				}
				INode childINode = tmpParent.GetChild(tmpChild.GetLocalNameBytes(), Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
					.CurrentStateId);
				if (childINode == null || !childINode.Equals(tmpChild))
				{
					// a newly created INode with the same name as an already deleted one
					// would be a different INode than the deleted one
					return true;
				}
				if (tmpParent.IsRoot())
				{
					break;
				}
				tmpChild = tmpParent;
				tmpParent = tmpParent.GetParent();
			}
			if (file.IsWithSnapshot() && file.GetFileWithSnapshotFeature().IsCurrentFileDeleted
				())
			{
				return true;
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		private INodeFile CheckUCBlock(ExtendedBlock block, string clientName)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			CheckNameNodeSafeMode("Cannot get a new generation stamp and an " + "access token for block "
				 + block);
			// check stored block state
			BlockInfoContiguous storedBlock = GetStoredBlock(ExtendedBlock.GetLocalBlock(block
				));
			if (storedBlock == null || storedBlock.GetBlockUCState() != HdfsServerConstants.BlockUCState
				.UnderConstruction)
			{
				throw new IOException(block + " does not exist or is not under Construction" + storedBlock
					);
			}
			// check file inode
			INodeFile file = ((INode)storedBlock.GetBlockCollection()).AsFile();
			if (file == null || !file.IsUnderConstruction() || IsFileDeleted(file))
			{
				throw new IOException("The file " + storedBlock + " belonged to does not exist or it is not under construction."
					);
			}
			// check lease
			if (clientName == null || !clientName.Equals(file.GetFileUnderConstructionFeature
				().GetClientName()))
			{
				throw new LeaseExpiredException("Lease mismatch: " + block + " is accessed by a non lease holder "
					 + clientName);
			}
			return file;
		}

		/// <summary>Client is reporting some bad block locations.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void ReportBadBlocks(LocatedBlock[] blocks)
		{
			CheckOperation(NameNode.OperationCategory.Write);
			NameNode.stateChangeLog.Info("*DIR* reportBadBlocks");
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				for (int i = 0; i < blocks.Length; i++)
				{
					ExtendedBlock blk = blocks[i].GetBlock();
					DatanodeInfo[] nodes = blocks[i].GetLocations();
					string[] storageIDs = blocks[i].GetStorageIDs();
					for (int j = 0; j < nodes.Length; j++)
					{
						blockManager.FindAndMarkBlockAsCorrupt(blk, nodes[j], storageIDs == null ? null : 
							storageIDs[j], "client machine reported it");
					}
				}
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <summary>
		/// Get a new generation stamp together with an access token for
		/// a block under construction
		/// This method is called for recovering a failed pipeline or setting up
		/// a pipeline to append to a block.
		/// </summary>
		/// <param name="block">a block</param>
		/// <param name="clientName">the name of a client</param>
		/// <returns>a located block with a new generation stamp and an access token</returns>
		/// <exception cref="System.IO.IOException">if any error occurs</exception>
		internal virtual LocatedBlock UpdateBlockForPipeline(ExtendedBlock block, string 
			clientName)
		{
			LocatedBlock locatedBlock;
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				// check vadility of parameters
				CheckUCBlock(block, clientName);
				// get a new generation stamp and an access token
				block.SetGenerationStamp(NextGenerationStamp(blockIdManager.IsLegacyBlock(block.GetLocalBlock
					())));
				locatedBlock = new LocatedBlock(block, new DatanodeInfo[0]);
				blockManager.SetBlockToken(locatedBlock, BlockTokenSecretManager.AccessMode.Write
					);
			}
			finally
			{
				WriteUnlock();
			}
			// Ensure we record the new generation stamp
			GetEditLog().LogSync();
			return locatedBlock;
		}

		/// <summary>Update a pipeline for a block under construction</summary>
		/// <param name="clientName">the name of the client</param>
		/// <param name="oldBlock">and old block</param>
		/// <param name="newBlock">a new block with a new generation stamp and length</param>
		/// <param name="newNodes">datanodes in the pipeline</param>
		/// <exception cref="System.IO.IOException">if any error occurs</exception>
		internal virtual void UpdatePipeline(string clientName, ExtendedBlock oldBlock, ExtendedBlock
			 newBlock, DatanodeID[] newNodes, string[] newStorageIDs, bool logRetryCache)
		{
			Log.Info("updatePipeline(" + oldBlock.GetLocalBlock() + ", newGS=" + newBlock.GetGenerationStamp
				() + ", newLength=" + newBlock.GetNumBytes() + ", newNodes=" + Arrays.AsList(newNodes
				) + ", client=" + clientName + ")");
			WaitForLoadingFSImage();
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Pipeline not updated");
				System.Diagnostics.Debug.Assert(newBlock.GetBlockId() == oldBlock.GetBlockId(), newBlock
					 + " and " + oldBlock + " has different block identifier");
				UpdatePipelineInternal(clientName, oldBlock, newBlock, newNodes, newStorageIDs, logRetryCache
					);
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			Log.Info("updatePipeline(" + oldBlock.GetLocalBlock() + " => " + newBlock.GetLocalBlock
				() + ") success");
		}

		/// <exception cref="System.IO.IOException"/>
		private void UpdatePipelineInternal(string clientName, ExtendedBlock oldBlock, ExtendedBlock
			 newBlock, DatanodeID[] newNodes, string[] newStorageIDs, bool logRetryCache)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			// check the vadility of the block and lease holder name
			INodeFile pendingFile = CheckUCBlock(oldBlock, clientName);
			BlockInfoContiguousUnderConstruction blockinfo = (BlockInfoContiguousUnderConstruction
				)pendingFile.GetLastBlock();
			// check new GS & length: this is not expected
			if (newBlock.GetGenerationStamp() <= blockinfo.GetGenerationStamp() || newBlock.GetNumBytes
				() < blockinfo.GetNumBytes())
			{
				string msg = "Update " + oldBlock + " (len = " + blockinfo.GetNumBytes() + ") to an older state: "
					 + newBlock + " (len = " + newBlock.GetNumBytes() + ")";
				Log.Warn(msg);
				throw new IOException(msg);
			}
			// Update old block with the new generation stamp and new length
			blockinfo.SetNumBytes(newBlock.GetNumBytes());
			blockinfo.SetGenerationStampAndVerifyReplicas(newBlock.GetGenerationStamp());
			// find the DatanodeDescriptor objects
			DatanodeStorageInfo[] storages = blockManager.GetDatanodeManager().GetDatanodeStorageInfos
				(newNodes, newStorageIDs);
			blockinfo.SetExpectedLocations(storages);
			string src = pendingFile.GetFullPathName();
			PersistBlocks(src, pendingFile, logRetryCache);
		}

		// rename was successful. If any part of the renamed subtree had
		// files that were being written to, update with new filename.
		internal virtual void UnprotectedChangeLease(string src, string dst)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			leaseManager.ChangeLease(src, dst);
		}

		/// <summary>Serializes leases.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SaveFilesUnderConstruction(DataOutputStream @out, IDictionary
			<long, INodeFile> snapshotUCMap)
		{
			// This is run by an inferior thread of saveNamespace, which holds a read
			// lock on our behalf. If we took the read lock here, we could block
			// for fairness if a writer is waiting on the lock.
			lock (leaseManager)
			{
				IDictionary<string, INodeFile> nodes = leaseManager.GetINodesUnderConstruction();
				foreach (KeyValuePair<string, INodeFile> entry in nodes)
				{
					// TODO: for HDFS-5428, because of rename operations, some
					// under-construction files that are
					// in the current fs directory can also be captured in the
					// snapshotUCMap. We should remove them from the snapshotUCMap.
					Sharpen.Collections.Remove(snapshotUCMap, entry.Value.GetId());
				}
				@out.WriteInt(nodes.Count + snapshotUCMap.Count);
				// write the size
				foreach (KeyValuePair<string, INodeFile> entry_1 in nodes)
				{
					FSImageSerialization.WriteINodeUnderConstruction(@out, entry_1.Value, entry_1.Key
						);
				}
				foreach (KeyValuePair<long, INodeFile> entry_2 in snapshotUCMap)
				{
					// for those snapshot INodeFileUC, we use "/.reserved/.inodes/<inodeid>"
					// as their paths
					StringBuilder b = new StringBuilder();
					b.Append(FSDirectory.DotReservedPathPrefix).Append(Path.Separator).Append(FSDirectory
						.DotInodesString).Append(Path.Separator).Append(entry_2.Value.GetId());
					FSImageSerialization.WriteINodeUnderConstruction(@out, entry_2.Value, b.ToString(
						));
				}
			}
		}

		/// <returns>all the under-construction files in the lease map</returns>
		internal virtual IDictionary<string, INodeFile> GetFilesUnderConstruction()
		{
			lock (leaseManager)
			{
				return leaseManager.GetINodesUnderConstruction();
			}
		}

		/// <summary>
		/// Register a Backup name-node, verifying that it belongs
		/// to the correct namespace, and adding it to the set of
		/// active journals if necessary.
		/// </summary>
		/// <param name="bnReg">registration of the new BackupNode</param>
		/// <param name="nnReg">registration of this NameNode</param>
		/// <exception cref="System.IO.IOException">if the namespace IDs do not match</exception>
		internal virtual void RegisterBackupNode(NamenodeRegistration bnReg, NamenodeRegistration
			 nnReg)
		{
			WriteLock();
			try
			{
				if (GetFSImage().GetStorage().GetNamespaceID() != bnReg.GetNamespaceID())
				{
					throw new IOException("Incompatible namespaceIDs: " + " Namenode namespaceID = " 
						+ GetFSImage().GetStorage().GetNamespaceID() + "; " + bnReg.GetRole() + " node namespaceID = "
						 + bnReg.GetNamespaceID());
				}
				if (bnReg.GetRole() == HdfsServerConstants.NamenodeRole.Backup)
				{
					GetFSImage().GetEditLog().RegisterBackupNode(bnReg, nnReg);
				}
			}
			finally
			{
				WriteUnlock();
			}
		}

		/// <summary>Release (unregister) backup node.</summary>
		/// <remarks>
		/// Release (unregister) backup node.
		/// <p>
		/// Find and remove the backup stream corresponding to the node.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void ReleaseBackupNode(NamenodeRegistration registration)
		{
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				if (GetFSImage().GetStorage().GetNamespaceID() != registration.GetNamespaceID())
				{
					throw new IOException("Incompatible namespaceIDs: " + " Namenode namespaceID = " 
						+ GetFSImage().GetStorage().GetNamespaceID() + "; " + registration.GetRole() + " node namespaceID = "
						 + registration.GetNamespaceID());
				}
				GetEditLog().ReleaseBackupStream(registration);
			}
			finally
			{
				WriteUnlock();
			}
		}

		internal class CorruptFileBlockInfo
		{
			internal readonly string path;

			internal readonly Org.Apache.Hadoop.Hdfs.Protocol.Block block;

			public CorruptFileBlockInfo(string p, Org.Apache.Hadoop.Hdfs.Protocol.Block b)
			{
				path = p;
				block = b;
			}

			public override string ToString()
			{
				return block.GetBlockName() + "\t" + path;
			}
		}

		/// <param name="path">Restrict corrupt files to this portion of namespace.</param>
		/// <param name="cookieTab">
		/// Support for continuation; cookieTab  tells where
		/// to start from
		/// </param>
		/// <returns>a list in which each entry describes a corrupt file/block</returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual ICollection<FSNamesystem.CorruptFileBlockInfo> ListCorruptFileBlocks
			(string path, string[] cookieTab)
		{
			CheckSuperuserPrivilege();
			CheckOperation(NameNode.OperationCategory.Read);
			int count = 0;
			AList<FSNamesystem.CorruptFileBlockInfo> corruptFiles = new AList<FSNamesystem.CorruptFileBlockInfo
				>();
			if (cookieTab == null)
			{
				cookieTab = new string[] { null };
			}
			// Do a quick check if there are any corrupt files without taking the lock
			if (blockManager.GetMissingBlocksCount() == 0)
			{
				if (cookieTab[0] == null)
				{
					cookieTab[0] = GetIntCookie(cookieTab[0]).ToString();
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("there are no corrupt file blocks.");
				}
				return corruptFiles;
			}
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				if (!IsPopulatingReplQueues())
				{
					throw new IOException("Cannot run listCorruptFileBlocks because " + "replication queues have not been initialized."
						);
				}
				// print a limited # of corrupt files per call
				IEnumerator<Org.Apache.Hadoop.Hdfs.Protocol.Block> blkIterator = blockManager.GetCorruptReplicaBlockIterator
					();
				int skip = GetIntCookie(cookieTab[0]);
				for (int i = 0; i < skip && blkIterator.HasNext(); i++)
				{
					blkIterator.Next();
				}
				while (blkIterator.HasNext())
				{
					Org.Apache.Hadoop.Hdfs.Protocol.Block blk = blkIterator.Next();
					INode inode = (INode)blockManager.GetBlockCollection(blk);
					skip++;
					if (inode != null && blockManager.CountNodes(blk).LiveReplicas() == 0)
					{
						string src = FSDirectory.GetFullPathName(inode);
						if (src.StartsWith(path))
						{
							corruptFiles.AddItem(new FSNamesystem.CorruptFileBlockInfo(src, blk));
							count++;
							if (count >= DefaultMaxCorruptFileblocksReturned)
							{
								break;
							}
						}
					}
				}
				cookieTab[0] = skip.ToString();
				if (Log.IsDebugEnabled())
				{
					Log.Debug("list corrupt file blocks returned: " + count);
				}
				return corruptFiles;
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <summary>Convert string cookie to integer.</summary>
		private static int GetIntCookie(string cookie)
		{
			int c;
			if (cookie == null)
			{
				c = 0;
			}
			else
			{
				try
				{
					c = System.Convert.ToInt32(cookie);
				}
				catch (FormatException)
				{
					c = 0;
				}
			}
			c = Math.Max(0, c);
			return c;
		}

		/// <summary>Create delegation token secret manager</summary>
		private DelegationTokenSecretManager CreateDelegationTokenSecretManager(Configuration
			 conf)
		{
			return new DelegationTokenSecretManager(conf.GetLong(DFSConfigKeys.DfsNamenodeDelegationKeyUpdateIntervalKey
				, DFSConfigKeys.DfsNamenodeDelegationKeyUpdateIntervalDefault), conf.GetLong(DFSConfigKeys
				.DfsNamenodeDelegationTokenMaxLifetimeKey, DFSConfigKeys.DfsNamenodeDelegationTokenMaxLifetimeDefault
				), conf.GetLong(DFSConfigKeys.DfsNamenodeDelegationTokenRenewIntervalKey, DFSConfigKeys
				.DfsNamenodeDelegationTokenRenewIntervalDefault), DelegationTokenRemoverScanInterval
				, conf.GetBoolean(DFSConfigKeys.DfsNamenodeAuditLogTokenTrackingIdKey, DFSConfigKeys
				.DfsNamenodeAuditLogTokenTrackingIdDefault), this);
		}

		/// <summary>Returns the DelegationTokenSecretManager instance in the namesystem.</summary>
		/// <returns>delegation token secret manager object</returns>
		internal virtual DelegationTokenSecretManager GetDelegationTokenSecretManager()
		{
			return dtSecretManager;
		}

		/// <param name="renewer">Renewer information</param>
		/// <returns>delegation toek</returns>
		/// <exception cref="System.IO.IOException">on error</exception>
		internal virtual Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
			> GetDelegationToken(Org.Apache.Hadoop.IO.Text renewer)
		{
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token;
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot issue delegation token");
				if (!IsAllowedDelegationTokenOp())
				{
					throw new IOException("Delegation Token can be issued only with kerberos or web authentication"
						);
				}
				if (dtSecretManager == null || !dtSecretManager.IsRunning())
				{
					Log.Warn("trying to get DT with no secret manager running");
					return null;
				}
				UserGroupInformation ugi = GetRemoteUser();
				string user = ugi.GetUserName();
				Org.Apache.Hadoop.IO.Text owner = new Org.Apache.Hadoop.IO.Text(user);
				Org.Apache.Hadoop.IO.Text realUser = null;
				if (ugi.GetRealUser() != null)
				{
					realUser = new Org.Apache.Hadoop.IO.Text(ugi.GetRealUser().GetUserName());
				}
				DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(owner, renewer, realUser
					);
				token = new Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>(dtId
					, dtSecretManager);
				long expiryTime = dtSecretManager.GetTokenExpiryTime(dtId);
				GetEditLog().LogGetDelegationToken(dtId, expiryTime);
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			return token;
		}

		/// <param name="token">token to renew</param>
		/// <returns>new expiryTime of the token</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken">
		/// if
		/// <paramref name="token"/>
		/// is invalid
		/// </exception>
		/// <exception cref="System.IO.IOException">on other errors</exception>
		internal virtual long RenewDelegationToken(Org.Apache.Hadoop.Security.Token.Token
			<DelegationTokenIdentifier> token)
		{
			long expiryTime;
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot renew delegation token");
				if (!IsAllowedDelegationTokenOp())
				{
					throw new IOException("Delegation Token can be renewed only with kerberos or web authentication"
						);
				}
				string renewer = GetRemoteUser().GetShortUserName();
				expiryTime = dtSecretManager.RenewToken(token, renewer);
				DelegationTokenIdentifier id = new DelegationTokenIdentifier();
				ByteArrayInputStream buf = new ByteArrayInputStream(token.GetIdentifier());
				DataInputStream @in = new DataInputStream(buf);
				id.ReadFields(@in);
				GetEditLog().LogRenewDelegationToken(id, expiryTime);
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			return expiryTime;
		}

		/// <param name="token">token to cancel</param>
		/// <exception cref="System.IO.IOException">on error</exception>
		internal virtual void CancelDelegationToken(Org.Apache.Hadoop.Security.Token.Token
			<DelegationTokenIdentifier> token)
		{
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot cancel delegation token");
				string canceller = GetRemoteUser().GetUserName();
				DelegationTokenIdentifier id = dtSecretManager.CancelToken(token, canceller);
				GetEditLog().LogCancelDelegationToken(id);
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
		}

		/// <param name="out">save state of the secret manager</param>
		/// <param name="sdPath">String storage directory path</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SaveSecretManagerStateCompat(DataOutputStream @out, string 
			sdPath)
		{
			dtSecretManager.SaveSecretManagerStateCompat(@out, sdPath);
		}

		internal virtual DelegationTokenSecretManager.SecretManagerState SaveSecretManagerState
			()
		{
			return dtSecretManager.SaveSecretManagerState();
		}

		/// <param name="in">load the state of secret manager from input stream</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void LoadSecretManagerStateCompat(DataInput @in)
		{
			dtSecretManager.LoadSecretManagerStateCompat(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void LoadSecretManagerState(FsImageProto.SecretManagerSection s, 
			IList<FsImageProto.SecretManagerSection.DelegationKey> keys, IList<FsImageProto.SecretManagerSection.PersistToken
			> tokens)
		{
			dtSecretManager.LoadSecretManagerState(new DelegationTokenSecretManager.SecretManagerState
				(s, keys, tokens));
		}

		/// <summary>Log the updateMasterKey operation to edit logs</summary>
		/// <param name="key">new delegation key.</param>
		public virtual void LogUpdateMasterKey(DelegationKey key)
		{
			System.Diagnostics.Debug.Assert(!IsInSafeMode(), "this should never be called while in safemode, since we stop "
				 + "the DT manager before entering safemode!");
			// No need to hold FSN lock since we don't access any internal
			// structures, and this is stopped before the FSN shuts itself
			// down, etc.
			GetEditLog().LogUpdateMasterKey(key);
			GetEditLog().LogSync();
		}

		/// <summary>Log the cancellation of expired tokens to edit logs</summary>
		/// <param name="id">token identifier to cancel</param>
		public virtual void LogExpireDelegationToken(DelegationTokenIdentifier id)
		{
			System.Diagnostics.Debug.Assert(!IsInSafeMode(), "this should never be called while in safemode, since we stop "
				 + "the DT manager before entering safemode!");
			// No need to hold FSN lock since we don't access any internal
			// structures, and this is stopped before the FSN shuts itself
			// down, etc.
			GetEditLog().LogCancelDelegationToken(id);
		}

		private void LogReassignLease(string leaseHolder, string src, string newHolder)
		{
			System.Diagnostics.Debug.Assert(HasWriteLock());
			GetEditLog().LogReassignLease(leaseHolder, src, newHolder);
		}

		/// <returns>true if delegation token operation is allowed</returns>
		/// <exception cref="System.IO.IOException"/>
		private bool IsAllowedDelegationTokenOp()
		{
			UserGroupInformation.AuthenticationMethod authMethod = GetConnectionAuthenticationMethod
				();
			if (UserGroupInformation.IsSecurityEnabled() && (authMethod != UserGroupInformation.AuthenticationMethod
				.Kerberos) && (authMethod != UserGroupInformation.AuthenticationMethod.KerberosSsl
				) && (authMethod != UserGroupInformation.AuthenticationMethod.Certificate))
			{
				return false;
			}
			return true;
		}

		/// <summary>Returns authentication method used to establish the connection</summary>
		/// <returns>AuthenticationMethod used to establish connection</returns>
		/// <exception cref="System.IO.IOException"/>
		private UserGroupInformation.AuthenticationMethod GetConnectionAuthenticationMethod
			()
		{
			UserGroupInformation ugi = GetRemoteUser();
			UserGroupInformation.AuthenticationMethod authMethod = ugi.GetAuthenticationMethod
				();
			if (authMethod == UserGroupInformation.AuthenticationMethod.Proxy)
			{
				authMethod = ugi.GetRealUser().GetAuthenticationMethod();
			}
			return authMethod;
		}

		/// <summary>
		/// Client invoked methods are invoked over RPC and will be in
		/// RPC call context even if the client exits.
		/// </summary>
		internal virtual bool IsExternalInvocation()
		{
			return Org.Apache.Hadoop.Ipc.Server.IsRpcInvocation() || NamenodeWebHdfsMethods.IsWebHdfsInvocation
				();
		}

		private static IPAddress GetRemoteIp()
		{
			IPAddress ip = Org.Apache.Hadoop.Ipc.Server.GetRemoteIp();
			if (ip != null)
			{
				return ip;
			}
			return NamenodeWebHdfsMethods.GetRemoteIp();
		}

		// optimize ugi lookup for RPC operations to avoid a trip through
		// UGI.getCurrentUser which is synch'ed
		/// <exception cref="System.IO.IOException"/>
		private static UserGroupInformation GetRemoteUser()
		{
			return NameNode.GetRemoteUser();
		}

		/// <summary>Log fsck event in the audit log</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void LogFsckEvent(string src, IPAddress remoteAddress)
		{
			if (IsAuditEnabled())
			{
				LogAuditEvent(true, GetRemoteUser(), remoteAddress, "fsck", src, null, null);
			}
		}

		/// <summary>Register NameNodeMXBean</summary>
		private void RegisterMXBean()
		{
			mxbeanName = MBeans.Register("NameNode", "NameNodeInfo", this);
		}

		/// <summary>Class representing Namenode information for JMX interfaces</summary>
		public virtual string GetVersion()
		{
			// NameNodeMXBean
			return VersionInfo.GetVersion() + ", r" + VersionInfo.GetRevision();
		}

		public virtual long GetUsed()
		{
			// NameNodeMXBean
			return this.GetCapacityUsed();
		}

		public virtual long GetFree()
		{
			// NameNodeMXBean
			return this.GetCapacityRemaining();
		}

		public virtual long GetTotal()
		{
			// NameNodeMXBean
			return this.GetCapacityTotal();
		}

		public virtual string GetSafemode()
		{
			// NameNodeMXBean
			if (!this.IsInSafeMode())
			{
				return string.Empty;
			}
			return "Safe mode is ON. " + this.GetSafeModeTip();
		}

		public virtual bool IsUpgradeFinalized()
		{
			// NameNodeMXBean
			return this.GetFSImage().IsUpgradeFinalized();
		}

		public virtual long GetNonDfsUsedSpace()
		{
			// NameNodeMXBean
			return datanodeStatistics.GetCapacityUsedNonDFS();
		}

		public virtual float GetPercentUsed()
		{
			// NameNodeMXBean
			return datanodeStatistics.GetCapacityUsedPercent();
		}

		public virtual long GetBlockPoolUsedSpace()
		{
			// NameNodeMXBean
			return datanodeStatistics.GetBlockPoolUsed();
		}

		public virtual float GetPercentBlockPoolUsed()
		{
			// NameNodeMXBean
			return datanodeStatistics.GetPercentBlockPoolUsed();
		}

		public virtual float GetPercentRemaining()
		{
			// NameNodeMXBean
			return datanodeStatistics.GetCapacityRemainingPercent();
		}

		public virtual long GetCacheCapacity()
		{
			// NameNodeMXBean
			return datanodeStatistics.GetCacheCapacity();
		}

		public virtual long GetCacheUsed()
		{
			// NameNodeMXBean
			return datanodeStatistics.GetCacheUsed();
		}

		public virtual long GetTotalBlocks()
		{
			// NameNodeMXBean
			return GetBlocksTotal();
		}

		[Metric]
		public virtual long GetTotalFiles()
		{
			// NameNodeMXBean
			return GetFilesTotal();
		}

		public virtual long GetNumberOfMissingBlocks()
		{
			// NameNodeMXBean
			return GetMissingBlocksCount();
		}

		public virtual long GetNumberOfMissingBlocksWithReplicationFactorOne()
		{
			// NameNodeMXBean
			return GetMissingReplOneBlocksCount();
		}

		public virtual int GetThreads()
		{
			// NameNodeMXBean
			return ManagementFactory.GetThreadMXBean().GetThreadCount();
		}

		/// <summary>
		/// Returned information is a JSON representation of map with host name as the
		/// key and value is a map of live node attribute keys to its values
		/// </summary>
		public virtual string GetLiveNodes()
		{
			// NameNodeMXBean
			IDictionary<string, IDictionary<string, object>> info = new Dictionary<string, IDictionary
				<string, object>>();
			IList<DatanodeDescriptor> live = new AList<DatanodeDescriptor>();
			blockManager.GetDatanodeManager().FetchDatanodes(live, null, true);
			foreach (DatanodeDescriptor node in live)
			{
				ImmutableMap.Builder<string, object> innerinfo = ImmutableMap.Builder<string, object
					>();
				innerinfo.Put("infoAddr", node.GetInfoAddr()).Put("infoSecureAddr", node.GetInfoSecureAddr
					()).Put("xferaddr", node.GetXferAddr()).Put("lastContact", GetLastContact(node))
					.Put("usedSpace", GetDfsUsed(node)).Put("adminState", node.GetAdminState().ToString
					()).Put("nonDfsUsedSpace", node.GetNonDfsUsed()).Put("capacity", node.GetCapacity
					()).Put("numBlocks", node.NumBlocks()).Put("version", node.GetSoftwareVersion())
					.Put("used", node.GetDfsUsed()).Put("remaining", node.GetRemaining()).Put("blockScheduled"
					, node.GetBlocksScheduled()).Put("blockPoolUsed", node.GetBlockPoolUsed()).Put("blockPoolUsedPercent"
					, node.GetBlockPoolUsedPercent()).Put("volfails", node.GetVolumeFailures());
				VolumeFailureSummary volumeFailureSummary = node.GetVolumeFailureSummary();
				if (volumeFailureSummary != null)
				{
					innerinfo.Put("failedStorageLocations", volumeFailureSummary.GetFailedStorageLocations
						()).Put("lastVolumeFailureDate", volumeFailureSummary.GetLastVolumeFailureDate()
						).Put("estimatedCapacityLostTotal", volumeFailureSummary.GetEstimatedCapacityLostTotal
						());
				}
				info[node.GetHostName() + ":" + node.GetXferPort()] = innerinfo.Build();
			}
			return JSON.ToString(info);
		}

		/// <summary>
		/// Returned information is a JSON representation of map with host name as the
		/// key and value is a map of dead node attribute keys to its values
		/// </summary>
		public virtual string GetDeadNodes()
		{
			// NameNodeMXBean
			IDictionary<string, IDictionary<string, object>> info = new Dictionary<string, IDictionary
				<string, object>>();
			IList<DatanodeDescriptor> dead = new AList<DatanodeDescriptor>();
			blockManager.GetDatanodeManager().FetchDatanodes(null, dead, true);
			foreach (DatanodeDescriptor node in dead)
			{
				IDictionary<string, object> innerinfo = ImmutableMap.Builder<string, object>().Put
					("lastContact", GetLastContact(node)).Put("decommissioned", node.IsDecommissioned
					()).Put("xferaddr", node.GetXferAddr()).Build();
				info[node.GetHostName() + ":" + node.GetXferPort()] = innerinfo;
			}
			return JSON.ToString(info);
		}

		/// <summary>
		/// Returned information is a JSON representation of map with host name as the
		/// key and value is a map of decommissioning node attribute keys to its
		/// values
		/// </summary>
		public virtual string GetDecomNodes()
		{
			// NameNodeMXBean
			IDictionary<string, IDictionary<string, object>> info = new Dictionary<string, IDictionary
				<string, object>>();
			IList<DatanodeDescriptor> decomNodeList = blockManager.GetDatanodeManager().GetDecommissioningNodes
				();
			foreach (DatanodeDescriptor node in decomNodeList)
			{
				IDictionary<string, object> innerinfo = ImmutableMap.Builder<string, object>().Put
					("xferaddr", node.GetXferAddr()).Put("underReplicatedBlocks", node.decommissioningStatus
					.GetUnderReplicatedBlocks()).Put("decommissionOnlyReplicas", node.decommissioningStatus
					.GetDecommissionOnlyReplicas()).Put("underReplicateInOpenFiles", node.decommissioningStatus
					.GetUnderReplicatedInOpenFiles()).Build();
				info[node.GetHostName() + ":" + node.GetXferPort()] = innerinfo;
			}
			return JSON.ToString(info);
		}

		private long GetLastContact(DatanodeDescriptor alivenode)
		{
			return (Time.MonotonicNow() - alivenode.GetLastUpdateMonotonic()) / 1000;
		}

		private long GetDfsUsed(DatanodeDescriptor alivenode)
		{
			return alivenode.GetDfsUsed();
		}

		public virtual string GetClusterId()
		{
			// NameNodeMXBean
			return GetFSImage().GetStorage().GetClusterID();
		}

		public virtual string GetBlockPoolId()
		{
			// NameNodeMXBean
			return blockPoolId;
		}

		public virtual string GetNameDirStatuses()
		{
			// NameNodeMXBean
			IDictionary<string, IDictionary<FilePath, Storage.StorageDirType>> statusMap = new 
				Dictionary<string, IDictionary<FilePath, Storage.StorageDirType>>();
			IDictionary<FilePath, Storage.StorageDirType> activeDirs = new Dictionary<FilePath
				, Storage.StorageDirType>();
			for (IEnumerator<Storage.StorageDirectory> it = GetFSImage().GetStorage().DirIterator
				(); it.HasNext(); )
			{
				Storage.StorageDirectory st = it.Next();
				activeDirs[st.GetRoot()] = st.GetStorageDirType();
			}
			statusMap["active"] = activeDirs;
			IList<Storage.StorageDirectory> removedStorageDirs = GetFSImage().GetStorage().GetRemovedStorageDirs
				();
			IDictionary<FilePath, Storage.StorageDirType> failedDirs = new Dictionary<FilePath
				, Storage.StorageDirType>();
			foreach (Storage.StorageDirectory st_1 in removedStorageDirs)
			{
				failedDirs[st_1.GetRoot()] = st_1.GetStorageDirType();
			}
			statusMap["failed"] = failedDirs;
			return JSON.ToString(statusMap);
		}

		public virtual string GetNodeUsage()
		{
			// NameNodeMXBean
			float median = 0;
			float max = 0;
			float min = 0;
			float dev = 0;
			IDictionary<string, IDictionary<string, object>> info = new Dictionary<string, IDictionary
				<string, object>>();
			IList<DatanodeDescriptor> live = new AList<DatanodeDescriptor>();
			blockManager.GetDatanodeManager().FetchDatanodes(live, null, true);
			if (live.Count > 0)
			{
				float totalDfsUsed = 0;
				float[] usages = new float[live.Count];
				int i = 0;
				foreach (DatanodeDescriptor dn in live)
				{
					usages[i++] = dn.GetDfsUsedPercent();
					totalDfsUsed += dn.GetDfsUsedPercent();
				}
				totalDfsUsed /= live.Count;
				Arrays.Sort(usages);
				median = usages[usages.Length / 2];
				max = usages[usages.Length - 1];
				min = usages[0];
				for (i = 0; i < usages.Length; i++)
				{
					dev += (usages[i] - totalDfsUsed) * (usages[i] - totalDfsUsed);
				}
				dev = (float)Math.Sqrt(dev / usages.Length);
			}
			IDictionary<string, object> innerInfo = new Dictionary<string, object>();
			innerInfo["min"] = StringUtils.Format("%.2f%%", min);
			innerInfo["median"] = StringUtils.Format("%.2f%%", median);
			innerInfo["max"] = StringUtils.Format("%.2f%%", max);
			innerInfo["stdDev"] = StringUtils.Format("%.2f%%", dev);
			info["nodeUsage"] = innerInfo;
			return JSON.ToString(info);
		}

		public virtual string GetNameJournalStatus()
		{
			// NameNodeMXBean
			IList<IDictionary<string, string>> jasList = new AList<IDictionary<string, string
				>>();
			FSEditLog log = GetFSImage().GetEditLog();
			if (log != null)
			{
				bool openForWrite = log.IsOpenForWrite();
				foreach (JournalSet.JournalAndStream jas in log.GetJournals())
				{
					IDictionary<string, string> jasMap = new Dictionary<string, string>();
					string manager = jas.GetManager().ToString();
					jasMap["required"] = jas.IsRequired().ToString();
					jasMap["disabled"] = jas.IsDisabled().ToString();
					jasMap["manager"] = manager;
					if (jas.IsDisabled())
					{
						jasMap["stream"] = "Failed";
					}
					else
					{
						if (openForWrite)
						{
							EditLogOutputStream elos = jas.GetCurrentStream();
							if (elos != null)
							{
								jasMap["stream"] = elos.GenerateReport();
							}
							else
							{
								jasMap["stream"] = "not currently writing";
							}
						}
						else
						{
							jasMap["stream"] = "open for read";
						}
					}
					jasList.AddItem(jasMap);
				}
			}
			return JSON.ToString(jasList);
		}

		public virtual string GetJournalTransactionInfo()
		{
			// NameNodeMxBean
			IDictionary<string, string> txnIdMap = new Dictionary<string, string>();
			txnIdMap["LastAppliedOrWrittenTxId"] = System.Convert.ToString(this.GetFSImage().
				GetLastAppliedOrWrittenTxId());
			txnIdMap["MostRecentCheckpointTxId"] = System.Convert.ToString(this.GetFSImage().
				GetMostRecentCheckpointTxId());
			return JSON.ToString(txnIdMap);
		}

		public virtual string GetNNStarted()
		{
			// NameNodeMXBean
			return GetStartTime().ToString();
		}

		public virtual string GetCompileInfo()
		{
			// NameNodeMXBean
			return VersionInfo.GetDate() + " by " + VersionInfo.GetUser() + " from " + VersionInfo
				.GetBranch();
		}

		/// <returns>the block manager.</returns>
		public virtual BlockManager GetBlockManager()
		{
			return blockManager;
		}

		public virtual BlockIdManager GetBlockIdManager()
		{
			return blockIdManager;
		}

		/// <returns>the FSDirectory.</returns>
		public virtual FSDirectory GetFSDirectory()
		{
			return dir;
		}

		/// <summary>Set the FSDirectory.</summary>
		[VisibleForTesting]
		public virtual void SetFSDirectory(FSDirectory dir)
		{
			this.dir = dir;
		}

		/// <returns>the cache manager.</returns>
		public virtual CacheManager GetCacheManager()
		{
			return cacheManager;
		}

		public virtual string GetCorruptFiles()
		{
			// NameNodeMXBean
			IList<string> list = new AList<string>();
			ICollection<FSNamesystem.CorruptFileBlockInfo> corruptFileBlocks;
			try
			{
				corruptFileBlocks = ListCorruptFileBlocks("/", null);
				int corruptFileCount = corruptFileBlocks.Count;
				if (corruptFileCount != 0)
				{
					foreach (FSNamesystem.CorruptFileBlockInfo c in corruptFileBlocks)
					{
						list.AddItem(c.ToString());
					}
				}
			}
			catch (IOException e)
			{
				Log.Warn("Get corrupt file blocks returned error: " + e.Message);
			}
			return JSON.ToString(list);
		}

		public virtual int GetDistinctVersionCount()
		{
			//NameNodeMXBean
			return blockManager.GetDatanodeManager().GetDatanodesSoftwareVersions().Count;
		}

		public virtual IDictionary<string, int> GetDistinctVersions()
		{
			//NameNodeMXBean
			return blockManager.GetDatanodeManager().GetDatanodesSoftwareVersions();
		}

		public virtual string GetSoftwareVersion()
		{
			//NameNodeMXBean
			return VersionInfo.GetVersion();
		}

		/// <summary>Verifies that the given identifier and password are valid and match.</summary>
		/// <param name="identifier">Token identifier.</param>
		/// <param name="password">Password in the token.</param>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.RetriableException"/>
		public virtual void VerifyToken(DelegationTokenIdentifier identifier, byte[] password
			)
		{
			lock (this)
			{
				try
				{
					GetDelegationTokenSecretManager().VerifyToken(identifier, password);
				}
				catch (SecretManager.InvalidToken it)
				{
					if (InTransitionToActive())
					{
						throw new RetriableException(it);
					}
					throw;
				}
			}
		}

		public virtual bool IsGenStampInFuture(Org.Apache.Hadoop.Hdfs.Protocol.Block block
			)
		{
			return blockIdManager.IsGenStampInFuture(block);
		}

		[VisibleForTesting]
		public virtual EditLogTailer GetEditLogTailer()
		{
			return editLogTailer;
		}

		[VisibleForTesting]
		public virtual void SetEditLogTailerForTests(EditLogTailer tailer)
		{
			this.editLogTailer = tailer;
		}

		[VisibleForTesting]
		internal virtual void SetFsLockForTests(ReentrantReadWriteLock Lock)
		{
			this.fsLock.coarseLock = Lock;
		}

		[VisibleForTesting]
		public virtual ReentrantReadWriteLock GetFsLockForTests()
		{
			return fsLock.coarseLock;
		}

		[VisibleForTesting]
		public virtual ReentrantLock GetCpLockForTests()
		{
			return cpLock;
		}

		[VisibleForTesting]
		public virtual FSNamesystem.SafeModeInfo GetSafeModeInfoForTests()
		{
			return safeMode;
		}

		[VisibleForTesting]
		public virtual void SetNNResourceChecker(NameNodeResourceChecker nnResourceChecker
			)
		{
			this.nnResourceChecker = nnResourceChecker;
		}

		public virtual SnapshotManager GetSnapshotManager()
		{
			return snapshotManager;
		}

		/// <summary>Allow snapshot on a directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void AllowSnapshot(string path)
		{
			CheckOperation(NameNode.OperationCategory.Write);
			bool success = false;
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot allow snapshot for " + path);
				CheckSuperuserPrivilege();
				FSDirSnapshotOp.AllowSnapshot(dir, snapshotManager, path);
				success = true;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(success, "allowSnapshot", path, null, null);
		}

		/// <summary>Disallow snapshot on a directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void DisallowSnapshot(string path)
		{
			CheckOperation(NameNode.OperationCategory.Write);
			bool success = false;
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot disallow snapshot for " + path);
				CheckSuperuserPrivilege();
				FSDirSnapshotOp.DisallowSnapshot(dir, snapshotManager, path);
				success = true;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(success, "disallowSnapshot", path, null, null);
		}

		/// <summary>Create a snapshot</summary>
		/// <param name="snapshotRoot">The directory path where the snapshot is taken</param>
		/// <param name="snapshotName">The name of the snapshot</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual string CreateSnapshot(string snapshotRoot, string snapshotName, 
			bool logRetryCache)
		{
			string snapshotPath = null;
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot create snapshot for " + snapshotRoot);
				snapshotPath = FSDirSnapshotOp.CreateSnapshot(dir, snapshotManager, snapshotRoot, 
					snapshotName, logRetryCache);
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(snapshotPath != null, "createSnapshot", snapshotRoot, snapshotPath, 
				null);
			return snapshotPath;
		}

		/// <summary>Rename a snapshot</summary>
		/// <param name="path">The directory path where the snapshot was taken</param>
		/// <param name="snapshotOldName">Old snapshot name</param>
		/// <param name="snapshotNewName">New snapshot name</param>
		/// <exception cref="SafeModeException"/>
		/// <exception cref="System.IO.IOException"></exception>
		internal virtual void RenameSnapshot(string path, string snapshotOldName, string 
			snapshotNewName, bool logRetryCache)
		{
			bool success = false;
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot rename snapshot for " + path);
				FSDirSnapshotOp.RenameSnapshot(dir, snapshotManager, path, snapshotOldName, snapshotNewName
					, logRetryCache);
				success = true;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			string oldSnapshotRoot = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.GetSnapshotPath(path, snapshotOldName);
			string newSnapshotRoot = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.GetSnapshotPath(path, snapshotNewName);
			LogAuditEvent(success, "renameSnapshot", oldSnapshotRoot, newSnapshotRoot, null);
		}

		/// <summary>
		/// Get the list of snapshottable directories that are owned
		/// by the current user.
		/// </summary>
		/// <remarks>
		/// Get the list of snapshottable directories that are owned
		/// by the current user. Return all the snapshottable directories if the
		/// current user is a super user.
		/// </remarks>
		/// <returns>The list of all the current snapshottable directories</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual SnapshottableDirectoryStatus[] GetSnapshottableDirListing()
		{
			SnapshottableDirectoryStatus[] status = null;
			CheckOperation(NameNode.OperationCategory.Read);
			bool success = false;
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				status = FSDirSnapshotOp.GetSnapshottableDirListing(dir, snapshotManager);
				success = true;
			}
			finally
			{
				ReadUnlock();
			}
			LogAuditEvent(success, "listSnapshottableDirectory", null, null, null);
			return status;
		}

		/// <summary>
		/// Get the difference between two snapshots (or between a snapshot and the
		/// current status) of a snapshottable directory.
		/// </summary>
		/// <param name="path">The full path of the snapshottable directory.</param>
		/// <param name="fromSnapshot">
		/// Name of the snapshot to calculate the diff from. Null
		/// or empty string indicates the current tree.
		/// </param>
		/// <param name="toSnapshot">
		/// Name of the snapshot to calculated the diff to. Null or
		/// empty string indicates the current tree.
		/// </param>
		/// <returns>
		/// A report about the difference between
		/// <paramref name="fromSnapshot"/>
		/// and
		/// <paramref name="toSnapshot"/>
		/// . Modified/deleted/created/renamed files and
		/// directories belonging to the snapshottable directories are listed
		/// and labeled as M/-/+/R respectively.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual SnapshotDiffReport GetSnapshotDiffReport(string path, string fromSnapshot
			, string toSnapshot)
		{
			SnapshotDiffReport diffs = null;
			CheckOperation(NameNode.OperationCategory.Read);
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				diffs = FSDirSnapshotOp.GetSnapshotDiffReport(dir, snapshotManager, path, fromSnapshot
					, toSnapshot);
			}
			finally
			{
				ReadUnlock();
			}
			LogAuditEvent(diffs != null, "computeSnapshotDiff", null, null, null);
			return diffs;
		}

		/// <summary>Delete a snapshot of a snapshottable directory</summary>
		/// <param name="snapshotRoot">The snapshottable directory</param>
		/// <param name="snapshotName">The name of the to-be-deleted snapshot</param>
		/// <exception cref="SafeModeException"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void DeleteSnapshot(string snapshotRoot, string snapshotName, bool
			 logRetryCache)
		{
			bool success = false;
			WriteLock();
			INode.BlocksMapUpdateInfo blocksToBeDeleted = null;
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot delete snapshot for " + snapshotRoot);
				blocksToBeDeleted = FSDirSnapshotOp.DeleteSnapshot(dir, snapshotManager, snapshotRoot
					, snapshotName, logRetryCache);
				success = true;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			// Breaking the pattern as removing blocks have to happen outside of the
			// global lock
			if (blocksToBeDeleted != null)
			{
				RemoveBlocks(blocksToBeDeleted);
			}
			string rootPath = Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.GetSnapshotPath
				(snapshotRoot, snapshotName);
			LogAuditEvent(success, "deleteSnapshot", rootPath, null, null);
		}

		/// <summary>Remove a list of INodeDirectorySnapshottable from the SnapshotManager</summary>
		/// <param name="toRemove">the list of INodeDirectorySnapshottable to be removed</param>
		internal virtual void RemoveSnapshottableDirs(IList<INodeDirectory> toRemove)
		{
			if (snapshotManager != null)
			{
				snapshotManager.RemoveSnapshottable(toRemove);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual RollingUpgradeInfo QueryRollingUpgrade()
		{
			CheckSuperuserPrivilege();
			CheckOperation(NameNode.OperationCategory.Read);
			ReadLock();
			try
			{
				if (!IsRollingUpgrade())
				{
					return null;
				}
				Preconditions.CheckNotNull(rollingUpgradeInfo);
				bool hasRollbackImage = this.GetFSImage().HasRollbackFSImage();
				rollingUpgradeInfo.SetCreatedRollbackImages(hasRollbackImage);
				return rollingUpgradeInfo;
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual RollingUpgradeInfo StartRollingUpgrade()
		{
			CheckSuperuserPrivilege();
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				if (IsRollingUpgrade())
				{
					return rollingUpgradeInfo;
				}
				long startTime = Time.Now();
				if (!haEnabled)
				{
					// for non-HA, we require NN to be in safemode
					StartRollingUpgradeInternalForNonHA(startTime);
				}
				else
				{
					// for HA, NN cannot be in safemode
					CheckNameNodeSafeMode("Failed to start rolling upgrade");
					StartRollingUpgradeInternal(startTime);
				}
				GetEditLog().LogStartRollingUpgrade(rollingUpgradeInfo.GetStartTime());
				if (haEnabled)
				{
					// roll the edit log to make sure the standby NameNode can tail
					GetFSImage().RollEditLog();
				}
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			if (auditLog.IsInfoEnabled() && IsExternalInvocation())
			{
				LogAuditEvent(true, "startRollingUpgrade", null, null, null);
			}
			return rollingUpgradeInfo;
		}

		/// <summary>Update internal state to indicate that a rolling upgrade is in progress.
		/// 	</summary>
		/// <param name="startTime">rolling upgrade start time</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void StartRollingUpgradeInternal(long startTime)
		{
			CheckRollingUpgrade("start rolling upgrade");
			GetFSImage().CheckUpgrade();
			SetRollingUpgradeInfo(false, startTime);
		}

		/// <summary>
		/// Update internal state to indicate that a rolling upgrade is in progress for
		/// non-HA setup.
		/// </summary>
		/// <remarks>
		/// Update internal state to indicate that a rolling upgrade is in progress for
		/// non-HA setup. This requires the namesystem is in SafeMode and after doing a
		/// checkpoint for rollback the namesystem will quit the safemode automatically
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void StartRollingUpgradeInternalForNonHA(long startTime)
		{
			Preconditions.CheckState(!haEnabled);
			if (!IsInSafeMode())
			{
				throw new IOException("Safe mode should be turned ON " + "in order to create namespace image."
					);
			}
			CheckRollingUpgrade("start rolling upgrade");
			GetFSImage().CheckUpgrade();
			// in non-HA setup, we do an extra checkpoint to generate a rollback image
			GetFSImage().SaveNamespace(this, NNStorage.NameNodeFile.ImageRollback, null);
			Log.Info("Successfully saved namespace for preparing rolling upgrade.");
			// leave SafeMode automatically
			SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			SetRollingUpgradeInfo(true, startTime);
		}

		internal virtual void SetRollingUpgradeInfo(bool createdRollbackImages, long startTime
			)
		{
			rollingUpgradeInfo = new RollingUpgradeInfo(blockPoolId, createdRollbackImages, startTime
				, 0L);
		}

		public virtual void SetCreatedRollbackImages(bool created)
		{
			if (rollingUpgradeInfo != null)
			{
				rollingUpgradeInfo.SetCreatedRollbackImages(created);
			}
		}

		public virtual RollingUpgradeInfo GetRollingUpgradeInfo()
		{
			return rollingUpgradeInfo;
		}

		public virtual bool IsNeedRollbackFsImage()
		{
			return needRollbackFsImage;
		}

		public virtual void SetNeedRollbackFsImage(bool needRollbackFsImage)
		{
			this.needRollbackFsImage = needRollbackFsImage;
		}

		public virtual RollingUpgradeInfo.Bean GetRollingUpgradeStatus()
		{
			// NameNodeMXBean
			if (!IsRollingUpgrade())
			{
				return null;
			}
			RollingUpgradeInfo upgradeInfo = GetRollingUpgradeInfo();
			if (upgradeInfo.CreatedRollbackImages())
			{
				return new RollingUpgradeInfo.Bean(upgradeInfo);
			}
			ReadLock();
			try
			{
				// check again after acquiring the read lock.
				upgradeInfo = GetRollingUpgradeInfo();
				if (upgradeInfo == null)
				{
					return null;
				}
				if (!upgradeInfo.CreatedRollbackImages())
				{
					bool hasRollbackImage = this.GetFSImage().HasRollbackFSImage();
					upgradeInfo.SetCreatedRollbackImages(hasRollbackImage);
				}
			}
			catch (IOException ioe)
			{
				Log.Warn("Encountered exception setting Rollback Image", ioe);
			}
			finally
			{
				ReadUnlock();
			}
			return new RollingUpgradeInfo.Bean(upgradeInfo);
		}

		/// <summary>Is rolling upgrade in progress?</summary>
		public virtual bool IsRollingUpgrade()
		{
			return rollingUpgradeInfo != null && !rollingUpgradeInfo.IsFinalized();
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.RollingUpgradeException"/>
		internal virtual void CheckRollingUpgrade(string action)
		{
			if (IsRollingUpgrade())
			{
				throw new RollingUpgradeException("Failed to " + action + " since a rolling upgrade is already in progress."
					 + " Existing rolling upgrade info:\n" + rollingUpgradeInfo);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual RollingUpgradeInfo FinalizeRollingUpgrade()
		{
			CheckSuperuserPrivilege();
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				if (!IsRollingUpgrade())
				{
					return null;
				}
				CheckNameNodeSafeMode("Failed to finalize rolling upgrade");
				FinalizeRollingUpgradeInternal(Time.Now());
				GetEditLog().LogFinalizeRollingUpgrade(rollingUpgradeInfo.GetFinalizeTime());
				if (haEnabled)
				{
					// roll the edit log to make sure the standby NameNode can tail
					GetFSImage().RollEditLog();
				}
				GetFSImage().UpdateStorageVersion();
				GetFSImage().RenameCheckpoint(NNStorage.NameNodeFile.ImageRollback, NNStorage.NameNodeFile
					.Image);
			}
			finally
			{
				WriteUnlock();
			}
			if (!haEnabled)
			{
				// Sync not needed for ha since the edit was rolled after logging.
				GetEditLog().LogSync();
			}
			if (auditLog.IsInfoEnabled() && IsExternalInvocation())
			{
				LogAuditEvent(true, "finalizeRollingUpgrade", null, null, null);
			}
			return rollingUpgradeInfo;
		}

		internal virtual void FinalizeRollingUpgradeInternal(long finalizeTime)
		{
			// Set the finalize time
			rollingUpgradeInfo.Finalize(finalizeTime);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual long AddCacheDirective(CacheDirectiveInfo directive, EnumSet<CacheFlag
			> flags, bool logRetryCache)
		{
			CacheDirectiveInfo effectiveDirective = null;
			if (!flags.Contains(CacheFlag.Force))
			{
				cacheManager.WaitForRescanIfNeeded();
			}
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				if (IsInSafeMode())
				{
					throw new SafeModeException("Cannot add cache directive", safeMode);
				}
				effectiveDirective = FSNDNCacheOp.AddCacheDirective(this, cacheManager, directive
					, flags, logRetryCache);
			}
			finally
			{
				WriteUnlock();
				bool success = effectiveDirective != null;
				if (success)
				{
					GetEditLog().LogSync();
				}
				string effectiveDirectiveStr = effectiveDirective != null ? effectiveDirective.ToString
					() : null;
				LogAuditEvent(success, "addCacheDirective", effectiveDirectiveStr, null, null);
			}
			return effectiveDirective != null ? effectiveDirective.GetId() : 0;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void ModifyCacheDirective(CacheDirectiveInfo directive, EnumSet<
			CacheFlag> flags, bool logRetryCache)
		{
			bool success = false;
			if (!flags.Contains(CacheFlag.Force))
			{
				cacheManager.WaitForRescanIfNeeded();
			}
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				if (IsInSafeMode())
				{
					throw new SafeModeException("Cannot add cache directive", safeMode);
				}
				FSNDNCacheOp.ModifyCacheDirective(this, cacheManager, directive, flags, logRetryCache
					);
				success = true;
			}
			finally
			{
				WriteUnlock();
				if (success)
				{
					GetEditLog().LogSync();
				}
				string idStr = "{id: " + directive.GetId().ToString() + "}";
				LogAuditEvent(success, "modifyCacheDirective", idStr, directive.ToString(), null);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void RemoveCacheDirective(long id, bool logRetryCache)
		{
			bool success = false;
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				if (IsInSafeMode())
				{
					throw new SafeModeException("Cannot remove cache directives", safeMode);
				}
				FSNDNCacheOp.RemoveCacheDirective(this, cacheManager, id, logRetryCache);
				success = true;
			}
			finally
			{
				WriteUnlock();
				string idStr = "{id: " + System.Convert.ToString(id) + "}";
				LogAuditEvent(success, "removeCacheDirective", idStr, null, null);
			}
			GetEditLog().LogSync();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual BatchedRemoteIterator.BatchedListEntries<CacheDirectiveEntry> ListCacheDirectives
			(long startId, CacheDirectiveInfo filter)
		{
			CheckOperation(NameNode.OperationCategory.Read);
			BatchedRemoteIterator.BatchedListEntries<CacheDirectiveEntry> results;
			cacheManager.WaitForRescanIfNeeded();
			ReadLock();
			bool success = false;
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				results = FSNDNCacheOp.ListCacheDirectives(this, cacheManager, startId, filter);
				success = true;
			}
			finally
			{
				ReadUnlock();
				LogAuditEvent(success, "listCacheDirectives", filter.ToString(), null, null);
			}
			return results;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void AddCachePool(CachePoolInfo req, bool logRetryCache)
		{
			WriteLock();
			bool success = false;
			string poolInfoStr = null;
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				if (IsInSafeMode())
				{
					throw new SafeModeException("Cannot add cache pool " + req.GetPoolName(), safeMode
						);
				}
				CachePoolInfo info = FSNDNCacheOp.AddCachePool(this, cacheManager, req, logRetryCache
					);
				poolInfoStr = info.ToString();
				success = true;
			}
			finally
			{
				WriteUnlock();
				LogAuditEvent(success, "addCachePool", poolInfoStr, null, null);
			}
			GetEditLog().LogSync();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void ModifyCachePool(CachePoolInfo req, bool logRetryCache)
		{
			WriteLock();
			bool success = false;
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				if (IsInSafeMode())
				{
					throw new SafeModeException("Cannot modify cache pool " + req.GetPoolName(), safeMode
						);
				}
				FSNDNCacheOp.ModifyCachePool(this, cacheManager, req, logRetryCache);
				success = true;
			}
			finally
			{
				WriteUnlock();
				string poolNameStr = "{poolName: " + (req == null ? null : req.GetPoolName()) + "}";
				LogAuditEvent(success, "modifyCachePool", poolNameStr, req == null ? null : req.ToString
					(), null);
			}
			GetEditLog().LogSync();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void RemoveCachePool(string cachePoolName, bool logRetryCache)
		{
			WriteLock();
			bool success = false;
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				if (IsInSafeMode())
				{
					throw new SafeModeException("Cannot remove cache pool " + cachePoolName, safeMode
						);
				}
				FSNDNCacheOp.RemoveCachePool(this, cacheManager, cachePoolName, logRetryCache);
				success = true;
			}
			finally
			{
				WriteUnlock();
				string poolNameStr = "{poolName: " + cachePoolName + "}";
				LogAuditEvent(success, "removeCachePool", poolNameStr, null, null);
			}
			GetEditLog().LogSync();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual BatchedRemoteIterator.BatchedListEntries<CachePoolEntry> ListCachePools
			(string prevKey)
		{
			BatchedRemoteIterator.BatchedListEntries<CachePoolEntry> results;
			CheckOperation(NameNode.OperationCategory.Read);
			bool success = false;
			cacheManager.WaitForRescanIfNeeded();
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				results = FSNDNCacheOp.ListCachePools(this, cacheManager, prevKey);
				success = true;
			}
			finally
			{
				ReadUnlock();
				LogAuditEvent(success, "listCachePools", null, null, null);
			}
			return results;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void ModifyAclEntries(string src, IList<AclEntry> aclSpec)
		{
			HdfsFileStatus auditStat = null;
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot modify ACL entries on " + src);
				auditStat = FSDirAclOp.ModifyAclEntries(dir, src, aclSpec);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "modifyAclEntries", src);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(true, "modifyAclEntries", src, null, auditStat);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void RemoveAclEntries(string src, IList<AclEntry> aclSpec)
		{
			CheckOperation(NameNode.OperationCategory.Write);
			HdfsFileStatus auditStat = null;
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot remove ACL entries on " + src);
				auditStat = FSDirAclOp.RemoveAclEntries(dir, src, aclSpec);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "removeAclEntries", src);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(true, "removeAclEntries", src, null, auditStat);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void RemoveDefaultAcl(string src)
		{
			HdfsFileStatus auditStat = null;
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot remove default ACL entries on " + src);
				auditStat = FSDirAclOp.RemoveDefaultAcl(dir, src);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "removeDefaultAcl", src);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(true, "removeDefaultAcl", src, null, auditStat);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void RemoveAcl(string src)
		{
			HdfsFileStatus auditStat = null;
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot remove ACL on " + src);
				auditStat = FSDirAclOp.RemoveAcl(dir, src);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "removeAcl", src);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(true, "removeAcl", src, null, auditStat);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetAcl(string src, IList<AclEntry> aclSpec)
		{
			HdfsFileStatus auditStat = null;
			CheckOperation(NameNode.OperationCategory.Write);
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot set ACL on " + src);
				auditStat = FSDirAclOp.SetAcl(dir, src, aclSpec);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "setAcl", src);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(true, "setAcl", src, null, auditStat);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual AclStatus GetAclStatus(string src)
		{
			CheckOperation(NameNode.OperationCategory.Read);
			bool success = false;
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				AclStatus ret = FSDirAclOp.GetAclStatus(dir, src);
				success = true;
				return ret;
			}
			finally
			{
				ReadUnlock();
				LogAuditEvent(success, "getAclStatus", src);
			}
		}

		/// <summary>Create an encryption zone on directory src using the specified key.</summary>
		/// <param name="src">
		/// the path of a directory which will be the root of the
		/// encryption zone. The directory must be empty.
		/// </param>
		/// <param name="keyName">
		/// name of a key which must be present in the configured
		/// KeyProvider.
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if the caller is not the superuser.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if the path can't be resolved.
		/// 	</exception>
		/// <exception cref="SafeModeException">if the Namenode is in safe mode.</exception>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.SafeModeException"/>
		internal virtual void CreateEncryptionZone(string src, string keyName, bool logRetryCache
			)
		{
			try
			{
				if (provider == null)
				{
					throw new IOException("Can't create an encryption zone for " + src + " since no key provider is available."
						);
				}
				if (keyName == null || keyName.IsEmpty())
				{
					throw new IOException("Must specify a key name when creating an " + "encryption zone"
						);
				}
				KeyProvider.Metadata metadata = provider.GetMetadata(keyName);
				if (metadata == null)
				{
					/*
					* It would be nice if we threw something more specific than
					* IOException when the key is not found, but the KeyProvider API
					* doesn't provide for that. If that API is ever changed to throw
					* something more specific (e.g. UnknownKeyException) then we can
					* update this to match it, or better yet, just rethrow the
					* KeyProvider's exception.
					*/
					throw new IOException("Key " + keyName + " doesn't exist.");
				}
				// If the provider supports pool for EDEKs, this will fill in the pool
				GenerateEncryptedDataEncryptionKey(keyName);
				CreateEncryptionZoneInt(src, metadata.GetCipher(), keyName, logRetryCache);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "createEncryptionZone", src);
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateEncryptionZoneInt(string srcArg, string cipher, string keyName
			, bool logRetryCache)
		{
			string src = srcArg;
			HdfsFileStatus resultingStat = null;
			CheckSuperuserPrivilege();
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			FSPermissionChecker pc = GetPermissionChecker();
			WriteLock();
			try
			{
				CheckSuperuserPrivilege();
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot create encryption zone on " + src);
				src = dir.ResolvePath(pc, src, pathComponents);
				CipherSuite suite = CipherSuite.Convert(cipher);
				// For now this is hardcoded, as we only support one method.
				CryptoProtocolVersion version = CryptoProtocolVersion.EncryptionZones;
				XAttr ezXAttr = dir.CreateEncryptionZone(src, suite, version, keyName);
				IList<XAttr> xAttrs = Lists.NewArrayListWithCapacity(1);
				xAttrs.AddItem(ezXAttr);
				GetEditLog().LogSetXAttrs(src, xAttrs, logRetryCache);
				INodesInPath iip = dir.GetINodesInPath4Write(src, false);
				resultingStat = dir.GetAuditFileInfo(iip);
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(true, "createEncryptionZone", srcArg, null, resultingStat);
		}

		/// <summary>Get the encryption zone for the specified path.</summary>
		/// <param name="srcArg">the path of a file or directory to get the EZ for.</param>
		/// <returns>the EZ of the of the path or null if none.</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException">if the caller is not the superuser.
		/// 	</exception>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException">if the path can't be resolved.
		/// 	</exception>
		/// <exception cref="System.IO.IOException"/>
		internal virtual EncryptionZone GetEZForPath(string srcArg)
		{
			string src = srcArg;
			HdfsFileStatus resultingStat = null;
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			bool success = false;
			FSPermissionChecker pc = GetPermissionChecker();
			CheckOperation(NameNode.OperationCategory.Read);
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				src = dir.ResolvePath(pc, src, pathComponents);
				INodesInPath iip = dir.GetINodesInPath(src, true);
				if (isPermissionEnabled)
				{
					dir.CheckPathAccess(pc, iip, FsAction.Read);
				}
				EncryptionZone ret = dir.GetEZForPath(iip);
				resultingStat = dir.GetAuditFileInfo(iip);
				success = true;
				return ret;
			}
			finally
			{
				ReadUnlock();
				LogAuditEvent(success, "getEZForPath", srcArg, null, resultingStat);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual BatchedRemoteIterator.BatchedListEntries<EncryptionZone> ListEncryptionZones
			(long prevId)
		{
			bool success = false;
			CheckSuperuserPrivilege();
			CheckOperation(NameNode.OperationCategory.Read);
			ReadLock();
			try
			{
				CheckSuperuserPrivilege();
				CheckOperation(NameNode.OperationCategory.Read);
				BatchedRemoteIterator.BatchedListEntries<EncryptionZone> ret = dir.ListEncryptionZones
					(prevId);
				success = true;
				return ret;
			}
			finally
			{
				ReadUnlock();
				LogAuditEvent(success, "listEncryptionZones", null);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetXAttr(string src, XAttr xAttr, EnumSet<XAttrSetFlag> flag
			, bool logRetryCache)
		{
			HdfsFileStatus auditStat = null;
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot set XAttr on " + src);
				auditStat = FSDirXAttrOp.SetXAttr(dir, src, xAttr, flag, logRetryCache);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "setXAttr", src);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(true, "setXAttr", src, null, auditStat);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual IList<XAttr> GetXAttrs(string src, IList<XAttr> xAttrs)
		{
			CheckOperation(NameNode.OperationCategory.Read);
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				return FSDirXAttrOp.GetXAttrs(dir, src, xAttrs);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "getXAttrs", src);
				throw;
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual IList<XAttr> ListXAttrs(string src)
		{
			CheckOperation(NameNode.OperationCategory.Read);
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				return FSDirXAttrOp.ListXAttrs(dir, src);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "listXAttrs", src);
				throw;
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void RemoveXAttr(string src, XAttr xAttr, bool logRetryCache)
		{
			HdfsFileStatus auditStat = null;
			WriteLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Write);
				CheckNameNodeSafeMode("Cannot remove XAttr entry on " + src);
				auditStat = FSDirXAttrOp.RemoveXAttr(dir, src, xAttr, logRetryCache);
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "removeXAttr", src);
				throw;
			}
			finally
			{
				WriteUnlock();
			}
			GetEditLog().LogSync();
			LogAuditEvent(true, "removeXAttr", src, null, auditStat);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckAccess(string src, FsAction mode)
		{
			CheckOperation(NameNode.OperationCategory.Read);
			byte[][] pathComponents = FSDirectory.GetPathComponentsForReservedPath(src);
			ReadLock();
			try
			{
				CheckOperation(NameNode.OperationCategory.Read);
				src = FSDirectory.ResolvePath(src, pathComponents, dir);
				INodesInPath iip = dir.GetINodesInPath(src, true);
				INode inode = iip.GetLastINode();
				if (inode == null)
				{
					throw new FileNotFoundException("Path not found");
				}
				if (isPermissionEnabled)
				{
					FSPermissionChecker pc = GetPermissionChecker();
					dir.CheckPathAccess(pc, iip, mode);
				}
			}
			catch (AccessControlException e)
			{
				LogAuditEvent(false, "checkAccess", src);
				throw;
			}
			finally
			{
				ReadUnlock();
			}
		}

		/// <summary>
		/// Default AuditLogger implementation; used when no access logger is
		/// defined in the config file.
		/// </summary>
		/// <remarks>
		/// Default AuditLogger implementation; used when no access logger is
		/// defined in the config file. It can also be explicitly listed in the
		/// config file.
		/// </remarks>
		private class DefaultAuditLogger : HdfsAuditLogger
		{
			private bool logTokenTrackingId;

			public override void Initialize(Configuration conf)
			{
				logTokenTrackingId = conf.GetBoolean(DFSConfigKeys.DfsNamenodeAuditLogTokenTrackingIdKey
					, DFSConfigKeys.DfsNamenodeAuditLogTokenTrackingIdDefault);
			}

			public override void LogAuditEvent(bool succeeded, string userName, IPAddress addr
				, string cmd, string src, string dst, FileStatus status, UserGroupInformation ugi
				, DelegationTokenSecretManager dtSecretManager)
			{
				if (auditLog.IsInfoEnabled())
				{
					StringBuilder sb = auditBuffer.Get();
					sb.Length = 0;
					sb.Append("allowed=").Append(succeeded).Append("\t");
					sb.Append("ugi=").Append(userName).Append("\t");
					sb.Append("ip=").Append(addr).Append("\t");
					sb.Append("cmd=").Append(cmd).Append("\t");
					sb.Append("src=").Append(src).Append("\t");
					sb.Append("dst=").Append(dst).Append("\t");
					if (null == status)
					{
						sb.Append("perm=null");
					}
					else
					{
						sb.Append("perm=");
						sb.Append(status.GetOwner()).Append(":");
						sb.Append(status.GetGroup()).Append(":");
						sb.Append(status.GetPermission());
					}
					if (logTokenTrackingId)
					{
						sb.Append("\t").Append("trackingId=");
						string trackingId = null;
						if (ugi != null && dtSecretManager != null && ugi.GetAuthenticationMethod() == UserGroupInformation.AuthenticationMethod
							.Token)
						{
							foreach (TokenIdentifier tid in ugi.GetTokenIdentifiers())
							{
								if (tid is DelegationTokenIdentifier)
								{
									DelegationTokenIdentifier dtid = (DelegationTokenIdentifier)tid;
									trackingId = dtSecretManager.GetTokenTrackingId(dtid);
									break;
								}
							}
						}
						sb.Append(trackingId);
					}
					sb.Append("\t").Append("proto=");
					sb.Append(NamenodeWebHdfsMethods.IsWebHdfsInvocation() ? "webhdfs" : "rpc");
					LogAuditMessage(sb.ToString());
				}
			}

			public virtual void LogAuditMessage(string message)
			{
				auditLog.Info(message);
			}
		}

		private static void EnableAsyncAuditLog()
		{
			if (!(auditLog is Log4JLogger))
			{
				Log.Warn("Log4j is required to enable async auditlog");
				return;
			}
			Logger logger = ((Log4JLogger)auditLog).GetLogger();
			IList<Appender> appenders = Sharpen.Collections.List(logger.GetAllAppenders());
			// failsafe against trying to async it more than once
			if (!appenders.IsEmpty() && !(appenders[0] is AsyncAppender))
			{
				AsyncAppender asyncAppender = new AsyncAppender();
				// change logger to have an async appender containing all the
				// previously configured appenders
				foreach (Appender appender in appenders)
				{
					logger.RemoveAppender(appender);
					asyncAppender.AddAppender(appender);
				}
				logger.AddAppender(asyncAppender);
			}
		}
	}
}
