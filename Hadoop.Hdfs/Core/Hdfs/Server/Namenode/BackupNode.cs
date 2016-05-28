using System;
using System.IO;
using System.Net;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>BackupNode.</summary>
	/// <remarks>
	/// BackupNode.
	/// <p>
	/// Backup node can play two roles.
	/// <ol>
	/// <li>
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.NamenodeRole.Checkpoint
	/// 	"/>
	/// node periodically creates checkpoints,
	/// that is downloads image and edits from the active node, merges them, and
	/// uploads the new image back to the active.</li>
	/// <li>
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.HdfsServerConstants.NamenodeRole.Backup
	/// 	"/>
	/// node keeps its namespace in sync with the
	/// active node, and periodically creates checkpoints by simply saving the
	/// namespace image to local disk(s).</li>
	/// </ol>
	/// </remarks>
	public class BackupNode : NameNode
	{
		private const string BnAddressNameKey = DFSConfigKeys.DfsNamenodeBackupAddressKey;

		private const string BnAddressDefault = DFSConfigKeys.DfsNamenodeBackupAddressDefault;

		private const string BnHttpAddressNameKey = DFSConfigKeys.DfsNamenodeBackupHttpAddressKey;

		private const string BnHttpAddressDefault = DFSConfigKeys.DfsNamenodeBackupHttpAddressDefault;

		private const string BnServiceRpcAddressKey = DFSConfigKeys.DfsNamenodeBackupServiceRpcAddressKey;

		private const float BnSafemodeThresholdPctDefault = 1.5f;

		private const int BnSafemodeExtensionDefault = int.MaxValue;

		/// <summary>Name-node proxy</summary>
		internal NamenodeProtocol namenode;

		/// <summary>Name-node RPC address</summary>
		internal string nnRpcAddress;

		/// <summary>Name-node HTTP address</summary>
		internal Uri nnHttpAddress;

		/// <summary>Checkpoint manager</summary>
		internal Checkpointer checkpointManager;

		/// <exception cref="System.IO.IOException"/>
		internal BackupNode(Configuration conf, HdfsServerConstants.NamenodeRole role)
			: base(conf, role)
		{
		}

		/////////////////////////////////////////////////////
		// Common NameNode methods implementation for backup node.
		/////////////////////////////////////////////////////
		protected internal override IPEndPoint GetRpcServerAddress(Configuration conf)
		{
			// NameNode
			string addr = conf.GetTrimmed(BnAddressNameKey, BnAddressDefault);
			return NetUtils.CreateSocketAddr(addr);
		}

		protected internal override IPEndPoint GetServiceRpcServerAddress(Configuration conf
			)
		{
			string addr = conf.GetTrimmed(BnServiceRpcAddressKey);
			if (addr == null || addr.IsEmpty())
			{
				return null;
			}
			return NetUtils.CreateSocketAddr(addr);
		}

		protected internal override void SetRpcServerAddress(Configuration conf, IPEndPoint
			 addr)
		{
			// NameNode
			conf.Set(BnAddressNameKey, NetUtils.GetHostPortString(addr));
		}

		protected internal override void SetRpcServiceServerAddress(Configuration conf, IPEndPoint
			 addr)
		{
			// Namenode
			conf.Set(BnServiceRpcAddressKey, NetUtils.GetHostPortString(addr));
		}

		protected internal override IPEndPoint GetHttpServerAddress(Configuration conf)
		{
			// NameNode
			System.Diagnostics.Debug.Assert(GetNameNodeAddress() != null, "rpcAddress should be calculated first"
				);
			string addr = conf.GetTrimmed(BnHttpAddressNameKey, BnHttpAddressDefault);
			return NetUtils.CreateSocketAddr(addr);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void LoadNamesystem(Configuration conf)
		{
			// NameNode
			conf.SetFloat(DFSConfigKeys.DfsNamenodeSafemodeThresholdPctKey, BnSafemodeThresholdPctDefault
				);
			conf.SetInt(DFSConfigKeys.DfsNamenodeSafemodeExtensionKey, BnSafemodeExtensionDefault
				);
			BackupImage bnImage = new BackupImage(conf);
			this.namesystem = new FSNamesystem(conf, bnImage);
			namesystem.dir.DisableQuotaChecks();
			bnImage.SetNamesystem(namesystem);
			bnImage.RecoverCreateRead();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void Initialize(Configuration conf)
		{
			// NameNode
			// Trash is disabled in BackupNameNode,
			// but should be turned back on if it ever becomes active.
			conf.SetLong(CommonConfigurationKeys.FsTrashIntervalKey, CommonConfigurationKeys.
				FsTrashIntervalDefault);
			NamespaceInfo nsInfo = Handshake(conf);
			base.Initialize(conf);
			namesystem.SetBlockPoolId(nsInfo.GetBlockPoolID());
			if (false == namesystem.IsInSafeMode())
			{
				namesystem.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			}
			// Backup node should never do lease recovery,
			// therefore lease hard limit should never expire.
			namesystem.leaseManager.SetLeasePeriod(HdfsConstants.LeaseSoftlimitPeriod, long.MaxValue
				);
			// register with the active name-node 
			RegisterWith(nsInfo);
			// Checkpoint daemon should start after the rpc server started
			RunCheckpointDaemon(conf);
			IPEndPoint addr = GetHttpAddress();
			if (addr != null)
			{
				conf.Set(BnHttpAddressNameKey, NetUtils.GetHostPortString(GetHttpAddress()));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override NameNodeRpcServer CreateRpcServer(Configuration conf)
		{
			return new BackupNode.BackupNodeRpcServer(conf, this);
		}

		public override void Stop()
		{
			// NameNode
			if (checkpointManager != null)
			{
				// Prevent from starting a new checkpoint.
				// Checkpoints that has already been started may proceed until 
				// the error reporting to the name-node is complete.
				// Checkpoint manager should not be interrupted yet because it will
				// close storage file channels and the checkpoint may fail with 
				// ClosedByInterruptException.
				checkpointManager.shouldRun = false;
			}
			if (namenode != null && GetRegistration() != null)
			{
				// Exclude this node from the list of backup streams on the name-node
				try
				{
					namenode.ErrorReport(GetRegistration(), NamenodeProtocol.Fatal, "Shutting down.");
				}
				catch (IOException e)
				{
					Log.Error("Failed to report to name-node.", e);
				}
			}
			// Stop the RPC client
			if (namenode != null)
			{
				RPC.StopProxy(namenode);
			}
			namenode = null;
			// Stop the checkpoint manager
			if (checkpointManager != null)
			{
				checkpointManager.Interrupt();
				checkpointManager = null;
			}
			// Abort current log segment - otherwise the NN shutdown code
			// will close it gracefully, which is incorrect.
			GetFSImage().GetEditLog().AbortCurrentLogSegment();
			// Stop name-node threads
			base.Stop();
		}

		/* @Override */
		// NameNode
		/// <exception cref="System.IO.IOException"/>
		public virtual bool SetSafeMode(HdfsConstants.SafeModeAction action)
		{
			throw new UnsupportedActionException("setSafeMode");
		}

		internal class BackupNodeRpcServer : NameNodeRpcServer, JournalProtocol
		{
			/// <exception cref="System.IO.IOException"/>
			private BackupNodeRpcServer(Configuration conf, BackupNode nn)
				: base(conf, nn)
			{
				JournalProtocolServerSideTranslatorPB journalProtocolTranslator = new JournalProtocolServerSideTranslatorPB
					(this);
				BlockingService service = JournalProtocolProtos.JournalProtocolService.NewReflectiveBlockingService
					(journalProtocolTranslator);
				DFSUtil.AddPBProtocol(conf, typeof(JournalProtocolPB), service, this.clientRpcServer
					);
			}

			/// <summary>Verifies a journal request</summary>
			/// <exception cref="System.IO.IOException"/>
			private void VerifyJournalRequest(JournalInfo journalInfo)
			{
				VerifyLayoutVersion(journalInfo.GetLayoutVersion());
				string errorMsg = null;
				int expectedNamespaceID = namesystem.GetNamespaceInfo().GetNamespaceID();
				if (journalInfo.GetNamespaceId() != expectedNamespaceID)
				{
					errorMsg = "Invalid namespaceID in journal request - expected " + expectedNamespaceID
						 + " actual " + journalInfo.GetNamespaceId();
					Log.Warn(errorMsg);
					throw new UnregisteredNodeException(journalInfo);
				}
				if (!journalInfo.GetClusterId().Equals(namesystem.GetClusterId()))
				{
					errorMsg = "Invalid clusterId in journal request - expected " + journalInfo.GetClusterId
						() + " actual " + namesystem.GetClusterId();
					Log.Warn(errorMsg);
					throw new UnregisteredNodeException(journalInfo);
				}
			}

			/////////////////////////////////////////////////////
			// BackupNodeProtocol implementation for backup node.
			/////////////////////////////////////////////////////
			/// <exception cref="System.IO.IOException"/>
			public override void StartLogSegment(JournalInfo journalInfo, long epoch, long txid
				)
			{
				namesystem.CheckOperation(NameNode.OperationCategory.Journal);
				VerifyJournalRequest(journalInfo);
				GetBNImage().NamenodeStartedLogSegment(txid);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Journal(JournalInfo journalInfo, long epoch, long firstTxId, 
				int numTxns, byte[] records)
			{
				namesystem.CheckOperation(NameNode.OperationCategory.Journal);
				VerifyJournalRequest(journalInfo);
				GetBNImage().Journal(firstTxId, numTxns, records);
			}

			private BackupImage GetBNImage()
			{
				return (BackupImage)nn.GetFSImage();
			}

			/// <exception cref="System.IO.IOException"/>
			public override FenceResponse Fence(JournalInfo journalInfo, long epoch, string fencerInfo
				)
			{
				Log.Info("Fenced by " + fencerInfo + " with epoch " + epoch);
				throw new NotSupportedException("BackupNode does not support fence");
			}
		}

		//////////////////////////////////////////////////////
		internal virtual bool ShouldCheckpointAtStartup()
		{
			FSImage fsImage = GetFSImage();
			if (IsRole(HdfsServerConstants.NamenodeRole.Checkpoint))
			{
				System.Diagnostics.Debug.Assert(fsImage.GetStorage().GetNumStorageDirs() > 0);
				return !fsImage.GetStorage().GetStorageDir(0).GetVersionFile().Exists();
			}
			// BN always checkpoints on startup in order to get in sync with namespace
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		private NamespaceInfo Handshake(Configuration conf)
		{
			// connect to name node
			IPEndPoint nnAddress = NameNode.GetServiceAddress(conf, true);
			this.namenode = NameNodeProxies.CreateNonHAProxy<NamenodeProtocol>(conf, nnAddress
				, UserGroupInformation.GetCurrentUser(), true).GetProxy();
			this.nnRpcAddress = NetUtils.GetHostPortString(nnAddress);
			this.nnHttpAddress = DFSUtil.GetInfoServer(nnAddress, conf, DFSUtil.GetHttpClientScheme
				(conf)).ToURL();
			// get version and id info from the name-node
			NamespaceInfo nsInfo = null;
			while (!IsStopRequested())
			{
				try
				{
					nsInfo = Handshake(namenode);
					break;
				}
				catch (SocketTimeoutException e)
				{
					// name-node is busy
					Log.Info("Problem connecting to server: " + nnAddress);
					try
					{
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
						Log.Warn("Encountered exception ", e);
					}
				}
			}
			return nsInfo;
		}

		/// <summary>Start a backup node daemon.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void RunCheckpointDaemon(Configuration conf)
		{
			checkpointManager = new Checkpointer(conf, this);
			checkpointManager.Start();
		}

		/// <summary>
		/// Checkpoint.<br />
		/// Tests may use it to initiate a checkpoint process.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void DoCheckpoint()
		{
			checkpointManager.DoCheckpoint();
		}

		/// <summary>Register this backup node with the active name-node.</summary>
		/// <param name="nsInfo">namespace information</param>
		/// <exception cref="System.IO.IOException"/>
		private void RegisterWith(NamespaceInfo nsInfo)
		{
			BackupImage bnImage = (BackupImage)GetFSImage();
			NNStorage storage = bnImage.GetStorage();
			// verify namespaceID
			if (storage.GetNamespaceID() == 0)
			{
				// new backup storage
				storage.SetStorageInfo(nsInfo);
				storage.SetBlockPoolID(nsInfo.GetBlockPoolID());
				storage.SetClusterID(nsInfo.GetClusterID());
			}
			else
			{
				nsInfo.ValidateStorage(storage);
			}
			bnImage.InitEditLog(HdfsServerConstants.StartupOption.Regular);
			SetRegistration();
			NamenodeRegistration nnReg = null;
			while (!IsStopRequested())
			{
				try
				{
					nnReg = namenode.RegisterSubordinateNamenode(GetRegistration());
					break;
				}
				catch (SocketTimeoutException e)
				{
					// name-node is busy
					Log.Info("Problem connecting to name-node: " + nnRpcAddress);
					try
					{
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
						Log.Warn("Encountered exception ", e);
					}
				}
			}
			string msg = null;
			if (nnReg == null)
			{
				// consider as a rejection
				msg = "Registration rejected by " + nnRpcAddress;
			}
			else
			{
				if (!nnReg.IsRole(HdfsServerConstants.NamenodeRole.Namenode))
				{
					msg = "Name-node " + nnRpcAddress + " is not active";
				}
			}
			if (msg != null)
			{
				msg += ". Shutting down.";
				Log.Error(msg);
				throw new IOException(msg);
			}
			// stop the node
			nnRpcAddress = nnReg.GetAddress();
		}

		// TODO: move to a common with DataNode util class
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.SocketTimeoutException"/>
		private static NamespaceInfo Handshake(NamenodeProtocol namenode)
		{
			NamespaceInfo nsInfo;
			nsInfo = namenode.VersionRequest();
			// throws SocketTimeoutException 
			string errorMsg = null;
			// verify build version
			if (!nsInfo.GetBuildVersion().Equals(Storage.GetBuildVersion()))
			{
				errorMsg = "Incompatible build versions: active name-node BV = " + nsInfo.GetBuildVersion
					() + "; backup node BV = " + Storage.GetBuildVersion();
				Log.Error(errorMsg);
				throw new IOException(errorMsg);
			}
			System.Diagnostics.Debug.Assert(HdfsConstants.NamenodeLayoutVersion == nsInfo.GetLayoutVersion
				(), "Active and backup node layout versions must be the same. Expected: " + HdfsConstants
				.NamenodeLayoutVersion + " actual " + nsInfo.GetLayoutVersion());
			return nsInfo;
		}

		protected internal override string GetNameServiceId(Configuration conf)
		{
			return DFSUtil.GetBackupNameServiceId(conf);
		}

		protected internal override HAState CreateHAState(HdfsServerConstants.StartupOption
			 startOpt)
		{
			return new BackupState();
		}

		protected internal override HAContext CreateHAContext()
		{
			// NameNode
			return new BackupNode.BNHAContext(this);
		}

		private class BNHAContext : NameNode.NameNodeHAContext
		{
			/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
			public override void CheckOperation(NameNode.OperationCategory op)
			{
				// NameNodeHAContext
				if (op == NameNode.OperationCategory.Unchecked || op == NameNode.OperationCategory
					.Checkpoint)
				{
					return;
				}
				if (NameNode.OperationCategory.Journal != op && !(NameNode.OperationCategory.Read
					 == op && !this._enclosing.IsRole(HdfsServerConstants.NamenodeRole.Checkpoint)))
				{
					string msg = "Operation category " + op + " is not supported at " + this._enclosing
						.GetRole();
					throw new StandbyException(msg);
				}
			}

			/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
			public override void PrepareToStopStandbyServices()
			{
			}

			// NameNodeHAContext
			/// <summary>Start services for BackupNode.</summary>
			/// <remarks>
			/// Start services for BackupNode.
			/// <p>
			/// The following services should be muted
			/// (not run or not pass any control commands to DataNodes)
			/// on BackupNode:
			/// <see cref="Monitor"/>
			/// protected by SafeMode.
			/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Blockmanagement.BlockManager.ReplicationMonitor
			/// 	"/>
			/// protected by SafeMode.
			/// <see cref="HeartbeatManager.Monitor"/>
			/// protected by SafeMode.
			/// <see cref="DecommissionManager.Monitor"/>
			/// need to prohibit refreshNodes().
			/// <see cref="PendingReplicationBlocks.PendingReplicationMonitor"/>
			/// harmless,
			/// because ReplicationMonitor is muted.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public override void StartActiveServices()
			{
				try
				{
					this._enclosing.namesystem.StartActiveServices();
				}
				catch (Exception t)
				{
					this._enclosing.DoImmediateShutdown(t);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void StopActiveServices()
			{
				try
				{
					if (this._enclosing.namesystem != null)
					{
						this._enclosing.namesystem.StopActiveServices();
					}
				}
				catch (Exception t)
				{
					this._enclosing.DoImmediateShutdown(t);
				}
			}

			internal BNHAContext(BackupNode _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly BackupNode _enclosing;
		}
	}
}
