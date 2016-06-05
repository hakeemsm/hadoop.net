using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Data;
using Org.Apache.Zookeeper.Server.Auth;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	/// <summary>
	/// Changes from 1.1 to 1.2, AMRMTokenSecretManager state has been saved
	/// separately.
	/// </summary>
	/// <remarks>
	/// Changes from 1.1 to 1.2, AMRMTokenSecretManager state has been saved
	/// separately. The currentMasterkey and nextMasterkey have been stored.
	/// Also, AMRMToken has been removed from ApplicationAttemptState.
	/// </remarks>
	public class ZKRMStateStore : RMStateStore
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(ZKRMStateStore));

		private readonly SecureRandom random = new SecureRandom();

		protected internal const string RootZnodeName = "ZKRMStateRoot";

		protected internal static readonly Version CurrentVersionInfo = Version.NewInstance
			(1, 2);

		private const string RmDelegationTokensRootZnodeName = "RMDelegationTokensRoot";

		private const string RmDtSequentialNumberZnodeName = "RMDTSequentialNumber";

		private const string RmDtMasterKeysRootZnodeName = "RMDTMasterKeysRoot";

		private int numRetries;

		private string zkHostPort = null;

		private int zkSessionTimeout;

		private long zkResyncWaitTime;

		[VisibleForTesting]
		internal long zkRetryInterval;

		private IList<ACL> zkAcl;

		private IList<ZKUtil.ZKAuthInfo> zkAuths;

		internal class ZKSyncOperationCallback : AsyncCallback.VoidCallback
		{
			// wait time for zkClient to re-establish connection with zk-server.
			public virtual void ProcessResult(int rc, string path, object ctx)
			{
				if (rc == KeeperException.Code.Ok.IntValue())
				{
					ZKRMStateStore.Log.Info("ZooKeeper sync operation succeeded. path: " + path);
				}
				else
				{
					ZKRMStateStore.Log.Fatal("ZooKeeper sync operation failed. Waiting for session " 
						+ "timeout. path: " + path);
				}
			}

			internal ZKSyncOperationCallback(ZKRMStateStore _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ZKRMStateStore _enclosing;
		}

		/// <summary>
		/// ROOT_DIR_PATH
		/// |--- VERSION_INFO
		/// |--- EPOCH_NODE
		/// |--- RM_ZK_FENCING_LOCK
		/// |--- RM_APP_ROOT
		/// |     |----- (#ApplicationId1)
		/// |     |        |----- (#ApplicationAttemptIds)
		/// |     |
		/// |     |----- (#ApplicationId2)
		/// |     |       |----- (#ApplicationAttemptIds)
		/// |     ....
		/// </summary>
		/// <remarks>
		/// ROOT_DIR_PATH
		/// |--- VERSION_INFO
		/// |--- EPOCH_NODE
		/// |--- RM_ZK_FENCING_LOCK
		/// |--- RM_APP_ROOT
		/// |     |----- (#ApplicationId1)
		/// |     |        |----- (#ApplicationAttemptIds)
		/// |     |
		/// |     |----- (#ApplicationId2)
		/// |     |       |----- (#ApplicationAttemptIds)
		/// |     ....
		/// |
		/// |--- RM_DT_SECRET_MANAGER_ROOT
		/// |----- RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME
		/// |----- RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME
		/// |       |----- Token_1
		/// |       |----- Token_2
		/// |       ....
		/// |
		/// |----- RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME
		/// |      |----- Key_1
		/// |      |----- Key_2
		/// ....
		/// |--- AMRMTOKEN_SECRET_MANAGER_ROOT
		/// |----- currentMasterKey
		/// |----- nextMasterKey
		/// </remarks>
		private string zkRootNodePath;

		private string rmAppRoot;

		private string rmDTSecretManagerRoot;

		private string dtMasterKeysRootPath;

		private string delegationTokensRootPath;

		private string dtSequenceNumberPath;

		private string amrmTokenSecretManagerRoot;

		[VisibleForTesting]
		protected internal string znodeWorkingPath;

		[VisibleForTesting]
		protected internal ZooKeeper zkClient;

		[VisibleForTesting]
		internal ZooKeeper activeZkClient;

		/// <summary>Fencing related variables</summary>
		private const string FencingLock = "RM_ZK_FENCING_LOCK";

		private string fencingNodePath;

		private OP createFencingNodePathOp;

		private OP deleteFencingNodePathOp;

		private Sharpen.Thread verifyActiveStatusThread;

		private string zkRootNodeUsername;

		private readonly string zkRootNodePassword = System.Convert.ToString(random.NextLong
			());

		[VisibleForTesting]
		internal IList<ACL> zkRootNodeAcl;

		private bool useDefaultFencingScheme = false;

		public const int CreateDeletePerms = ZooDefs.Perms.Create | ZooDefs.Perms.Delete;

		private readonly string zkRootNodeAuthScheme = new DigestAuthenticationProvider()
			.GetScheme();

		/* activeZkClient is not used to do actual operations,
		* it is only used to verify client session for watched events and
		* it gets activated into zkClient on connection event.
		*/
		/// <summary>
		/// Given the
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// and
		/// <see cref="Org.Apache.Zookeeper.Data.ACL"/>
		/// s used (zkAcl) for
		/// ZooKeeper access, construct the
		/// <see cref="Org.Apache.Zookeeper.Data.ACL"/>
		/// s for the store's root node.
		/// In the constructed
		/// <see cref="Org.Apache.Zookeeper.Data.ACL"/>
		/// , all the users allowed by zkAcl are given
		/// rwa access, while the current RM has exclude create-delete access.
		/// To be called only when HA is enabled and the configuration doesn't set ACL
		/// for the root node.
		/// </summary>
		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		[VisibleForTesting]
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		protected internal virtual IList<ACL> ConstructZkRootNodeACL(Configuration conf, 
			IList<ACL> sourceACLs)
		{
			IList<ACL> zkRootNodeAcl = new AList<ACL>();
			foreach (ACL acl in sourceACLs)
			{
				zkRootNodeAcl.AddItem(new ACL(ZKUtil.RemoveSpecificPerms(acl.GetPerms(), CreateDeletePerms
					), acl.GetId()));
			}
			zkRootNodeUsername = HAUtil.GetConfValueForRMInstance(YarnConfiguration.RmAddress
				, YarnConfiguration.DefaultRmAddress, conf);
			ID rmId = new ID(zkRootNodeAuthScheme, DigestAuthenticationProvider.GenerateDigest
				(zkRootNodeUsername + ":" + zkRootNodePassword));
			zkRootNodeAcl.AddItem(new ACL(CreateDeletePerms, rmId));
			return zkRootNodeAcl;
		}

		/// <exception cref="System.Exception"/>
		protected internal override void InitInternal(Configuration conf)
		{
			lock (this)
			{
				zkHostPort = conf.Get(YarnConfiguration.RmZkAddress);
				if (zkHostPort == null)
				{
					throw new YarnRuntimeException("No server address specified for " + "zookeeper state store for Resource Manager recovery. "
						 + YarnConfiguration.RmZkAddress + " is not configured.");
				}
				numRetries = conf.GetInt(YarnConfiguration.RmZkNumRetries, YarnConfiguration.DefaultZkRmNumRetries
					);
				znodeWorkingPath = conf.Get(YarnConfiguration.ZkRmStateStoreParentPath, YarnConfiguration
					.DefaultZkRmStateStoreParentPath);
				zkSessionTimeout = conf.GetInt(YarnConfiguration.RmZkTimeoutMs, YarnConfiguration
					.DefaultRmZkTimeoutMs);
				if (HAUtil.IsHAEnabled(conf))
				{
					zkRetryInterval = zkSessionTimeout / numRetries;
				}
				else
				{
					zkRetryInterval = conf.GetLong(YarnConfiguration.RmZkRetryIntervalMs, YarnConfiguration
						.DefaultRmZkRetryIntervalMs);
				}
				zkResyncWaitTime = zkRetryInterval * numRetries;
				zkAcl = RMZKUtils.GetZKAcls(conf);
				zkAuths = RMZKUtils.GetZKAuths(conf);
				zkRootNodePath = GetNodePath(znodeWorkingPath, RootZnodeName);
				rmAppRoot = GetNodePath(zkRootNodePath, RmAppRoot);
				/* Initialize fencing related paths, acls, and ops */
				fencingNodePath = GetNodePath(zkRootNodePath, FencingLock);
				createFencingNodePathOp = OP.Create(fencingNodePath, new byte[0], zkAcl, CreateMode
					.Persistent);
				deleteFencingNodePathOp = OP.Delete(fencingNodePath, -1);
				if (HAUtil.IsHAEnabled(conf))
				{
					string zkRootNodeAclConf = HAUtil.GetConfValueForRMInstance(YarnConfiguration.ZkRmStateStoreRootNodeAcl
						, conf);
					if (zkRootNodeAclConf != null)
					{
						zkRootNodeAclConf = ZKUtil.ResolveConfIndirection(zkRootNodeAclConf);
						try
						{
							zkRootNodeAcl = ZKUtil.ParseACLs(zkRootNodeAclConf);
						}
						catch (ZKUtil.BadAclFormatException bafe)
						{
							Log.Error("Invalid format for " + YarnConfiguration.ZkRmStateStoreRootNodeAcl);
							throw;
						}
					}
					else
					{
						useDefaultFencingScheme = true;
						zkRootNodeAcl = ConstructZkRootNodeACL(conf, zkAcl);
					}
				}
				rmDTSecretManagerRoot = GetNodePath(zkRootNodePath, RmDtSecretManagerRoot);
				dtMasterKeysRootPath = GetNodePath(rmDTSecretManagerRoot, RmDtMasterKeysRootZnodeName
					);
				delegationTokensRootPath = GetNodePath(rmDTSecretManagerRoot, RmDelegationTokensRootZnodeName
					);
				dtSequenceNumberPath = GetNodePath(rmDTSecretManagerRoot, RmDtSequentialNumberZnodeName
					);
				amrmTokenSecretManagerRoot = GetNodePath(zkRootNodePath, AmrmtokenSecretManagerRoot
					);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StartInternal()
		{
			lock (this)
			{
				// createConnection for future API calls
				CreateConnection();
				// ensure root dirs exist
				CreateRootDirRecursively(znodeWorkingPath);
				CreateRootDir(zkRootNodePath);
				SetRootNodeAcls();
				DeleteFencingNodePath();
				if (HAUtil.IsHAEnabled(GetConfig()))
				{
					verifyActiveStatusThread = new ZKRMStateStore.VerifyActiveStatusThread(this);
					verifyActiveStatusThread.Start();
				}
				CreateRootDir(rmAppRoot);
				CreateRootDir(rmDTSecretManagerRoot);
				CreateRootDir(dtMasterKeysRootPath);
				CreateRootDir(delegationTokensRootPath);
				CreateRootDir(dtSequenceNumberPath);
				CreateRootDir(amrmTokenSecretManagerRoot);
				SyncInternal(zkRootNodePath);
			}
		}

		/// <exception cref="System.Exception"/>
		private void CreateRootDir(string rootPath)
		{
			// For root dirs, we shouldn't use the doMulti helper methods
			new _ZKAction_324(this, rootPath).RunWithRetries();
		}

		private sealed class _ZKAction_324 : ZKRMStateStore.ZKAction<string>
		{
			public _ZKAction_324(ZKRMStateStore _enclosing, string rootPath)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.rootPath = rootPath;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			internal override string Run()
			{
				try
				{
					return this._enclosing.zkClient.Create(rootPath, null, this._enclosing.zkAcl, CreateMode
						.Persistent);
				}
				catch (KeeperException ke)
				{
					if (ke.Code() == KeeperException.Code.Nodeexists)
					{
						ZKRMStateStore.Log.Debug(rootPath + "znode already exists!");
						return null;
					}
					else
					{
						throw;
					}
				}
			}

			private readonly ZKRMStateStore _enclosing;

			private readonly string rootPath;
		}

		/// <exception cref="System.Exception"/>
		private void LogRootNodeAcls(string prefix)
		{
			Stat getStat = new Stat();
			IList<ACL> getAcls = GetACLWithRetries(zkRootNodePath, getStat);
			StringBuilder builder = new StringBuilder();
			builder.Append(prefix);
			foreach (ACL acl in getAcls)
			{
				builder.Append(acl.ToString());
			}
			builder.Append(getStat.ToString());
			Log.Debug(builder.ToString());
		}

		/// <exception cref="System.Exception"/>
		private void DeleteFencingNodePath()
		{
			new _ZKAction_355(this).RunWithRetries();
		}

		private sealed class _ZKAction_355 : ZKRMStateStore.ZKAction<Void>
		{
			public _ZKAction_355(ZKRMStateStore _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			internal override Void Run()
			{
				try
				{
					this._enclosing.zkClient.Multi(Sharpen.Collections.SingletonList(this._enclosing.
						deleteFencingNodePathOp));
				}
				catch (KeeperException.NoNodeException)
				{
					ZKRMStateStore.Log.Info("Fencing node " + this._enclosing.fencingNodePath + " doesn't exist to delete"
						);
				}
				return null;
			}

			private readonly ZKRMStateStore _enclosing;
		}

		/// <exception cref="System.Exception"/>
		private void SetAcl(string zkPath, IList<ACL> acl)
		{
			new _ZKAction_371(this, zkPath, acl).RunWithRetries();
		}

		private sealed class _ZKAction_371 : ZKRMStateStore.ZKAction<Void>
		{
			public _ZKAction_371(ZKRMStateStore _enclosing, string zkPath, IList<ACL> acl)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.zkPath = zkPath;
				this.acl = acl;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			internal override Void Run()
			{
				this._enclosing.zkClient.SetACL(zkPath, acl, -1);
				return null;
			}

			private readonly ZKRMStateStore _enclosing;

			private readonly string zkPath;

			private readonly IList<ACL> acl;
		}

		/// <exception cref="System.Exception"/>
		private void SetRootNodeAcls()
		{
			if (Log.IsTraceEnabled())
			{
				LogRootNodeAcls("Before fencing\n");
			}
			if (HAUtil.IsHAEnabled(GetConfig()))
			{
				SetAcl(zkRootNodePath, zkRootNodeAcl);
			}
			else
			{
				SetAcl(zkRootNodePath, zkAcl);
			}
			if (Log.IsTraceEnabled())
			{
				LogRootNodeAcls("After fencing\n");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CloseZkClients()
		{
			lock (this)
			{
				zkClient = null;
				if (activeZkClient != null)
				{
					try
					{
						activeZkClient.Close();
					}
					catch (Exception e)
					{
						throw new IOException("Interrupted while closing ZK", e);
					}
					activeZkClient = null;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void CloseInternal()
		{
			lock (this)
			{
				if (verifyActiveStatusThread != null)
				{
					verifyActiveStatusThread.Interrupt();
					verifyActiveStatusThread.Join(1000);
				}
				CloseZkClients();
			}
		}

		protected internal override Version GetCurrentVersion()
		{
			return CurrentVersionInfo;
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreVersion()
		{
			lock (this)
			{
				string versionNodePath = GetNodePath(zkRootNodePath, VersionNode);
				byte[] data = ((VersionPBImpl)CurrentVersionInfo).GetProto().ToByteArray();
				if (ExistsWithRetries(versionNodePath, false) != null)
				{
					SetDataWithRetries(versionNodePath, data, -1);
				}
				else
				{
					CreateWithRetries(versionNodePath, data, zkAcl, CreateMode.Persistent);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override Version LoadVersion()
		{
			lock (this)
			{
				string versionNodePath = GetNodePath(zkRootNodePath, VersionNode);
				if (ExistsWithRetries(versionNodePath, false) != null)
				{
					byte[] data = GetDataWithRetries(versionNodePath, false);
					Version version = new VersionPBImpl(YarnServerCommonProtos.VersionProto.ParseFrom
						(data));
					return version;
				}
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		public override long GetAndIncrementEpoch()
		{
			lock (this)
			{
				string epochNodePath = GetNodePath(zkRootNodePath, EpochNode);
				long currentEpoch = 0;
				if (ExistsWithRetries(epochNodePath, false) != null)
				{
					// load current epoch
					byte[] data = GetDataWithRetries(epochNodePath, false);
					Epoch epoch = new EpochPBImpl(YarnServerResourceManagerRecoveryProtos.EpochProto.
						ParseFrom(data));
					currentEpoch = epoch.GetEpoch();
					// increment epoch and store it
					byte[] storeData = Epoch.NewInstance(currentEpoch + 1).GetProto().ToByteArray();
					SetDataWithRetries(epochNodePath, storeData, -1);
				}
				else
				{
					// initialize epoch node with 1 for the next time.
					byte[] storeData = Epoch.NewInstance(currentEpoch + 1).GetProto().ToByteArray();
					CreateWithRetries(epochNodePath, storeData, zkAcl, CreateMode.Persistent);
				}
				return currentEpoch;
			}
		}

		/// <exception cref="System.Exception"/>
		public override RMStateStore.RMState LoadState()
		{
			lock (this)
			{
				RMStateStore.RMState rmState = new RMStateStore.RMState();
				// recover DelegationTokenSecretManager
				LoadRMDTSecretManagerState(rmState);
				// recover RM applications
				LoadRMAppState(rmState);
				// recover AMRMTokenSecretManager
				LoadAMRMTokenSecretManagerState(rmState);
				return rmState;
			}
		}

		/// <exception cref="System.Exception"/>
		private void LoadAMRMTokenSecretManagerState(RMStateStore.RMState rmState)
		{
			byte[] data = GetDataWithRetries(amrmTokenSecretManagerRoot, false);
			if (data == null)
			{
				Log.Warn("There is no data saved");
				return;
			}
			AMRMTokenSecretManagerStatePBImpl stateData = new AMRMTokenSecretManagerStatePBImpl
				(YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto.ParseFrom
				(data));
			rmState.amrmTokenSecretManagerState = AMRMTokenSecretManagerState.NewInstance(stateData
				.GetCurrentMasterKey(), stateData.GetNextMasterKey());
		}

		/// <exception cref="System.Exception"/>
		private void LoadRMDTSecretManagerState(RMStateStore.RMState rmState)
		{
			lock (this)
			{
				LoadRMDelegationKeyState(rmState);
				LoadRMSequentialNumberState(rmState);
				LoadRMDelegationTokenState(rmState);
			}
		}

		/// <exception cref="System.Exception"/>
		private void LoadRMDelegationKeyState(RMStateStore.RMState rmState)
		{
			IList<string> childNodes = GetChildrenWithRetries(dtMasterKeysRootPath, false);
			foreach (string childNodeName in childNodes)
			{
				string childNodePath = GetNodePath(dtMasterKeysRootPath, childNodeName);
				byte[] childData = GetDataWithRetries(childNodePath, false);
				if (childData == null)
				{
					Log.Warn("Content of " + childNodePath + " is broken.");
					continue;
				}
				ByteArrayInputStream @is = new ByteArrayInputStream(childData);
				DataInputStream fsIn = new DataInputStream(@is);
				try
				{
					if (childNodeName.StartsWith(DelegationKeyPrefix))
					{
						DelegationKey key = new DelegationKey();
						key.ReadFields(fsIn);
						rmState.rmSecretManagerState.masterKeyState.AddItem(key);
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Loaded delegation key: keyId=" + key.GetKeyId() + ", expirationDate=" 
								+ key.GetExpiryDate());
						}
					}
				}
				finally
				{
					@is.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void LoadRMSequentialNumberState(RMStateStore.RMState rmState)
		{
			byte[] seqData = GetDataWithRetries(dtSequenceNumberPath, false);
			if (seqData != null)
			{
				ByteArrayInputStream seqIs = new ByteArrayInputStream(seqData);
				DataInputStream seqIn = new DataInputStream(seqIs);
				try
				{
					rmState.rmSecretManagerState.dtSequenceNumber = seqIn.ReadInt();
				}
				finally
				{
					seqIn.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void LoadRMDelegationTokenState(RMStateStore.RMState rmState)
		{
			IList<string> childNodes = GetChildrenWithRetries(delegationTokensRootPath, false
				);
			foreach (string childNodeName in childNodes)
			{
				string childNodePath = GetNodePath(delegationTokensRootPath, childNodeName);
				byte[] childData = GetDataWithRetries(childNodePath, false);
				if (childData == null)
				{
					Log.Warn("Content of " + childNodePath + " is broken.");
					continue;
				}
				ByteArrayInputStream @is = new ByteArrayInputStream(childData);
				DataInputStream fsIn = new DataInputStream(@is);
				try
				{
					if (childNodeName.StartsWith(DelegationTokenPrefix))
					{
						RMDelegationTokenIdentifierData identifierData = new RMDelegationTokenIdentifierData
							();
						identifierData.ReadFields(fsIn);
						RMDelegationTokenIdentifier identifier = identifierData.GetTokenIdentifier();
						long renewDate = identifierData.GetRenewDate();
						rmState.rmSecretManagerState.delegationTokenState[identifier] = renewDate;
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Loaded RMDelegationTokenIdentifier: " + identifier + " renewDate=" + renewDate
								);
						}
					}
				}
				finally
				{
					@is.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void LoadRMAppState(RMStateStore.RMState rmState)
		{
			lock (this)
			{
				IList<string> childNodes = GetChildrenWithRetries(rmAppRoot, false);
				foreach (string childNodeName in childNodes)
				{
					string childNodePath = GetNodePath(rmAppRoot, childNodeName);
					byte[] childData = GetDataWithRetries(childNodePath, false);
					if (childNodeName.StartsWith(ApplicationId.appIdStrPrefix))
					{
						// application
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Loading application from znode: " + childNodeName);
						}
						ApplicationId appId = ConverterUtils.ToApplicationId(childNodeName);
						ApplicationStateDataPBImpl appState = new ApplicationStateDataPBImpl(YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto
							.ParseFrom(childData));
						if (!appId.Equals(appState.GetApplicationSubmissionContext().GetApplicationId()))
						{
							throw new YarnRuntimeException("The child node name is different " + "from the application id"
								);
						}
						rmState.appState[appId] = appState;
						LoadApplicationAttemptState(appState, appId);
					}
					else
					{
						Log.Info("Unknown child node with name: " + childNodeName);
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void LoadApplicationAttemptState(ApplicationStateData appState, ApplicationId
			 appId)
		{
			string appPath = GetNodePath(rmAppRoot, appId.ToString());
			IList<string> attempts = GetChildrenWithRetries(appPath, false);
			foreach (string attemptIDStr in attempts)
			{
				if (attemptIDStr.StartsWith(ApplicationAttemptId.appAttemptIdStrPrefix))
				{
					string attemptPath = GetNodePath(appPath, attemptIDStr);
					byte[] attemptData = GetDataWithRetries(attemptPath, false);
					ApplicationAttemptStateDataPBImpl attemptState = new ApplicationAttemptStateDataPBImpl
						(YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto.ParseFrom
						(attemptData));
					appState.attempts[attemptState.GetAttemptId()] = attemptState;
				}
			}
			Log.Debug("Done loading applications from ZK state store");
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreApplicationStateInternal(ApplicationId appId
			, ApplicationStateData appStateDataPB)
		{
			lock (this)
			{
				string nodeCreatePath = GetNodePath(rmAppRoot, appId.ToString());
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Storing info for app: " + appId + " at: " + nodeCreatePath);
				}
				byte[] appStateData = appStateDataPB.GetProto().ToByteArray();
				CreateWithRetries(nodeCreatePath, appStateData, zkAcl, CreateMode.Persistent);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void UpdateApplicationStateInternal(ApplicationId appId
			, ApplicationStateData appStateDataPB)
		{
			lock (this)
			{
				string nodeUpdatePath = GetNodePath(rmAppRoot, appId.ToString());
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Storing final state info for app: " + appId + " at: " + nodeUpdatePath
						);
				}
				byte[] appStateData = appStateDataPB.GetProto().ToByteArray();
				if (ExistsWithRetries(nodeUpdatePath, false) != null)
				{
					SetDataWithRetries(nodeUpdatePath, appStateData, -1);
				}
				else
				{
					CreateWithRetries(nodeUpdatePath, appStateData, zkAcl, CreateMode.Persistent);
					Log.Debug(appId + " znode didn't exist. Created a new znode to" + " update the application state."
						);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreApplicationAttemptStateInternal(ApplicationAttemptId
			 appAttemptId, ApplicationAttemptStateData attemptStateDataPB)
		{
			lock (this)
			{
				string appDirPath = GetNodePath(rmAppRoot, appAttemptId.GetApplicationId().ToString
					());
				string nodeCreatePath = GetNodePath(appDirPath, appAttemptId.ToString());
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Storing info for attempt: " + appAttemptId + " at: " + nodeCreatePath);
				}
				byte[] attemptStateData = attemptStateDataPB.GetProto().ToByteArray();
				CreateWithRetries(nodeCreatePath, attemptStateData, zkAcl, CreateMode.Persistent);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void UpdateApplicationAttemptStateInternal(ApplicationAttemptId
			 appAttemptId, ApplicationAttemptStateData attemptStateDataPB)
		{
			lock (this)
			{
				string appIdStr = appAttemptId.GetApplicationId().ToString();
				string appAttemptIdStr = appAttemptId.ToString();
				string appDirPath = GetNodePath(rmAppRoot, appIdStr);
				string nodeUpdatePath = GetNodePath(appDirPath, appAttemptIdStr);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Storing final state info for attempt: " + appAttemptIdStr + " at: " + 
						nodeUpdatePath);
				}
				byte[] attemptStateData = attemptStateDataPB.GetProto().ToByteArray();
				if (ExistsWithRetries(nodeUpdatePath, false) != null)
				{
					SetDataWithRetries(nodeUpdatePath, attemptStateData, -1);
				}
				else
				{
					CreateWithRetries(nodeUpdatePath, attemptStateData, zkAcl, CreateMode.Persistent);
					Log.Debug(appAttemptId + " znode didn't exist. Created a new znode to" + " update the application attempt state."
						);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void RemoveApplicationStateInternal(ApplicationStateData
			 appState)
		{
			lock (this)
			{
				string appId = appState.GetApplicationSubmissionContext().GetApplicationId().ToString
					();
				string appIdRemovePath = GetNodePath(rmAppRoot, appId);
				AList<OP> opList = new AList<OP>();
				foreach (ApplicationAttemptId attemptId in appState.attempts.Keys)
				{
					string attemptRemovePath = GetNodePath(appIdRemovePath, attemptId.ToString());
					opList.AddItem(OP.Delete(attemptRemovePath, -1));
				}
				opList.AddItem(OP.Delete(appIdRemovePath, -1));
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Removing info for app: " + appId + " at: " + appIdRemovePath + " and its attempts."
						);
				}
				DoDeleteMultiWithRetries(opList);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreRMDelegationTokenState(RMDelegationTokenIdentifier
			 rmDTIdentifier, long renewDate)
		{
			lock (this)
			{
				AList<OP> opList = new AList<OP>();
				AddStoreOrUpdateOps(opList, rmDTIdentifier, renewDate, false);
				DoStoreMultiWithRetries(opList);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void RemoveRMDelegationTokenState(RMDelegationTokenIdentifier
			 rmDTIdentifier)
		{
			lock (this)
			{
				string nodeRemovePath = GetNodePath(delegationTokensRootPath, DelegationTokenPrefix
					 + rmDTIdentifier.GetSequenceNumber());
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Removing RMDelegationToken_" + rmDTIdentifier.GetSequenceNumber());
				}
				if (ExistsWithRetries(nodeRemovePath, false) != null)
				{
					AList<OP> opList = new AList<OP>();
					opList.AddItem(OP.Delete(nodeRemovePath, -1));
					DoDeleteMultiWithRetries(opList);
				}
				else
				{
					Log.Debug("Attempted to delete a non-existing znode " + nodeRemovePath);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void UpdateRMDelegationTokenState(RMDelegationTokenIdentifier
			 rmDTIdentifier, long renewDate)
		{
			lock (this)
			{
				AList<OP> opList = new AList<OP>();
				string nodeRemovePath = GetNodePath(delegationTokensRootPath, DelegationTokenPrefix
					 + rmDTIdentifier.GetSequenceNumber());
				if (ExistsWithRetries(nodeRemovePath, false) == null)
				{
					// in case znode doesn't exist
					AddStoreOrUpdateOps(opList, rmDTIdentifier, renewDate, false);
					Log.Debug("Attempted to update a non-existing znode " + nodeRemovePath);
				}
				else
				{
					// in case znode exists
					AddStoreOrUpdateOps(opList, rmDTIdentifier, renewDate, true);
				}
				DoStoreMultiWithRetries(opList);
			}
		}

		/// <exception cref="System.Exception"/>
		private void AddStoreOrUpdateOps(AList<OP> opList, RMDelegationTokenIdentifier rmDTIdentifier
			, long renewDate, bool isUpdate)
		{
			// store RM delegation token
			string nodeCreatePath = GetNodePath(delegationTokensRootPath, DelegationTokenPrefix
				 + rmDTIdentifier.GetSequenceNumber());
			ByteArrayOutputStream seqOs = new ByteArrayOutputStream();
			DataOutputStream seqOut = new DataOutputStream(seqOs);
			RMDelegationTokenIdentifierData identifierData = new RMDelegationTokenIdentifierData
				(rmDTIdentifier, renewDate);
			try
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug((isUpdate ? "Storing " : "Updating ") + "RMDelegationToken_" + rmDTIdentifier
						.GetSequenceNumber());
				}
				if (isUpdate)
				{
					opList.AddItem(OP.SetData(nodeCreatePath, identifierData.ToByteArray(), -1));
				}
				else
				{
					opList.AddItem(OP.Create(nodeCreatePath, identifierData.ToByteArray(), zkAcl, CreateMode
						.Persistent));
					// Update Sequence number only while storing DT
					seqOut.WriteInt(rmDTIdentifier.GetSequenceNumber());
					if (Log.IsDebugEnabled())
					{
						Log.Debug((isUpdate ? "Storing " : "Updating ") + dtSequenceNumberPath + ". SequenceNumber: "
							 + rmDTIdentifier.GetSequenceNumber());
					}
					opList.AddItem(OP.SetData(dtSequenceNumberPath, seqOs.ToByteArray(), -1));
				}
			}
			finally
			{
				seqOs.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreRMDTMasterKeyState(DelegationKey delegationKey
			)
		{
			lock (this)
			{
				string nodeCreatePath = GetNodePath(dtMasterKeysRootPath, DelegationKeyPrefix + delegationKey
					.GetKeyId());
				ByteArrayOutputStream os = new ByteArrayOutputStream();
				DataOutputStream fsOut = new DataOutputStream(os);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Storing RMDelegationKey_" + delegationKey.GetKeyId());
				}
				delegationKey.Write(fsOut);
				try
				{
					CreateWithRetries(nodeCreatePath, os.ToByteArray(), zkAcl, CreateMode.Persistent);
				}
				finally
				{
					os.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void RemoveRMDTMasterKeyState(DelegationKey delegationKey
			)
		{
			lock (this)
			{
				string nodeRemovePath = GetNodePath(dtMasterKeysRootPath, DelegationKeyPrefix + delegationKey
					.GetKeyId());
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Removing RMDelegationKey_" + delegationKey.GetKeyId());
				}
				if (ExistsWithRetries(nodeRemovePath, false) != null)
				{
					DoDeleteMultiWithRetries(OP.Delete(nodeRemovePath, -1));
				}
				else
				{
					Log.Debug("Attempted to delete a non-existing znode " + nodeRemovePath);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public override void DeleteStore()
		{
			lock (this)
			{
				if (ExistsWithRetries(zkRootNodePath, false) != null)
				{
					DeleteWithRetries(zkRootNodePath, false);
				}
			}
		}

		/// <summary>
		/// Watcher implementation which forward events to the ZKRMStateStore This
		/// hides the ZK methods of the store from its public interface
		/// </summary>
		private sealed class ForwardingWatcher : Watcher
		{
			private ZooKeeper watchedZkClient;

			public ForwardingWatcher(ZKRMStateStore _enclosing, ZooKeeper client)
			{
				this._enclosing = _enclosing;
				// ZK related code
				this.watchedZkClient = client;
			}

			public override void Process(WatchedEvent @event)
			{
				try
				{
					this._enclosing.ProcessWatchEvent(this.watchedZkClient, @event);
				}
				catch (Exception t)
				{
					ZKRMStateStore.Log.Error("Failed to process watcher event " + @event + ": " + StringUtils
						.StringifyException(t));
				}
			}

			private readonly ZKRMStateStore _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual void ProcessWatchEvent(ZooKeeper zk, WatchedEvent @event)
		{
			lock (this)
			{
				// only process watcher event from current ZooKeeper Client session.
				if (zk != activeZkClient)
				{
					Log.Info("Ignore watcher event type: " + @event.GetType() + " with state:" + @event
						.GetState() + " for path:" + @event.GetPath() + " from old session");
					return;
				}
				Watcher.Event.EventType eventType = @event.GetType();
				Log.Info("Watcher event type: " + eventType + " with state:" + @event.GetState() 
					+ " for path:" + @event.GetPath() + " for " + this);
				if (eventType == Watcher.Event.EventType.None)
				{
					switch (@event.GetState())
					{
						case Watcher.Event.KeeperState.SyncConnected:
						{
							// the connection state has changed
							Log.Info("ZKRMStateStore Session connected");
							if (zkClient == null)
							{
								// the SyncConnected must be from the client that sent Disconnected
								zkClient = activeZkClient;
								Sharpen.Runtime.NotifyAll(this);
								Log.Info("ZKRMStateStore Session restored");
							}
							break;
						}

						case Watcher.Event.KeeperState.Disconnected:
						{
							Log.Info("ZKRMStateStore Session disconnected");
							zkClient = null;
							break;
						}

						case Watcher.Event.KeeperState.Expired:
						{
							// the connection got terminated because of session timeout
							// call listener to reconnect
							Log.Info("ZKRMStateStore Session expired");
							CreateConnection();
							SyncInternal(@event.GetPath());
							break;
						}

						default:
						{
							Log.Error("Unexpected Zookeeper" + " watch event state: " + @event.GetState());
							break;
						}
					}
				}
			}
		}

		[VisibleForTesting]
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		internal virtual string GetNodePath(string root, string nodeName)
		{
			return (root + "/" + nodeName);
		}

		/// <summary>Helper method to call ZK's sync() after calling createConnection().</summary>
		/// <remarks>
		/// Helper method to call ZK's sync() after calling createConnection().
		/// Note that sync path is meaningless for now:
		/// http://mail-archives.apache.org/mod_mbox/zookeeper-user/201102.mbox/browser
		/// </remarks>
		/// <param name="path">
		/// path to sync, nullable value. If the path is null,
		/// zkRootNodePath is used to sync.
		/// </param>
		/// <returns>true if ZK.sync() succeededs, false if ZK.sync() fails.</returns>
		/// <exception cref="System.Exception"/>
		private void SyncInternal(string path)
		{
			ZKRMStateStore.ZKSyncOperationCallback cb = new ZKRMStateStore.ZKSyncOperationCallback
				(this);
			string pathForSync = (path != null) ? path : zkRootNodePath;
			try
			{
				new _ZKAction_950(this, pathForSync, cb).RunWithRetries();
			}
			catch (Exception)
			{
				Log.Fatal("sync failed.");
			}
		}

		private sealed class _ZKAction_950 : ZKRMStateStore.ZKAction<Void>
		{
			public _ZKAction_950(ZKRMStateStore _enclosing, string pathForSync, ZKRMStateStore.ZKSyncOperationCallback
				 cb)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.pathForSync = pathForSync;
				this.cb = cb;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			internal override Void Run()
			{
				this._enclosing.zkClient.Sync(pathForSync, cb, null);
				return null;
			}

			private readonly ZKRMStateStore _enclosing;

			private readonly string pathForSync;

			private readonly ZKRMStateStore.ZKSyncOperationCallback cb;
		}

		/// <summary>
		/// Helper method that creates fencing node, executes the passed operations,
		/// and deletes the fencing node.
		/// </summary>
		/// <exception cref="System.Exception"/>
		private void DoStoreMultiWithRetries(IList<OP> opList)
		{
			lock (this)
			{
				IList<OP> execOpList = new AList<OP>(opList.Count + 2);
				execOpList.AddItem(createFencingNodePathOp);
				Sharpen.Collections.AddAll(execOpList, opList);
				execOpList.AddItem(deleteFencingNodePathOp);
				new _ZKAction_972(this, execOpList).RunWithRetries();
			}
		}

		private sealed class _ZKAction_972 : ZKRMStateStore.ZKAction<Void>
		{
			public _ZKAction_972(ZKRMStateStore _enclosing, IList<OP> execOpList)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.execOpList = execOpList;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			internal override Void Run()
			{
				this._enclosing.zkClient.Multi(execOpList);
				return null;
			}

			private readonly ZKRMStateStore _enclosing;

			private readonly IList<OP> execOpList;
		}

		/// <summary>
		/// Helper method that creates fencing node, executes the passed operation,
		/// and deletes the fencing node.
		/// </summary>
		/// <exception cref="System.Exception"/>
		private void DoStoreMultiWithRetries(OP op)
		{
			DoStoreMultiWithRetries(Sharpen.Collections.SingletonList(op));
		}

		/// <summary>
		/// Helper method that creates fencing node, executes the passed
		/// delete related operations and deletes the fencing node.
		/// </summary>
		/// <exception cref="System.Exception"/>
		private void DoDeleteMultiWithRetries(IList<OP> opList)
		{
			lock (this)
			{
				IList<OP> execOpList = new AList<OP>(opList.Count + 2);
				execOpList.AddItem(createFencingNodePathOp);
				Sharpen.Collections.AddAll(execOpList, opList);
				execOpList.AddItem(deleteFencingNodePathOp);
				new _ZKAction_999(this, execOpList).RunWithRetries();
			}
		}

		private sealed class _ZKAction_999 : ZKRMStateStore.ZKAction<Void>
		{
			public _ZKAction_999(ZKRMStateStore _enclosing, IList<OP> execOpList)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.execOpList = execOpList;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			internal override Void Run()
			{
				this.SetHasDeleteNodeOp(true);
				this._enclosing.zkClient.Multi(execOpList);
				return null;
			}

			private readonly ZKRMStateStore _enclosing;

			private readonly IList<OP> execOpList;
		}

		/// <exception cref="System.Exception"/>
		private void DoDeleteMultiWithRetries(OP op)
		{
			DoDeleteMultiWithRetries(Sharpen.Collections.SingletonList(op));
		}

		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual void CreateWithRetries(string path, byte[] data, IList<ACL> acl, CreateMode
			 mode)
		{
			DoStoreMultiWithRetries(OP.Create(path, data, acl, mode));
		}

		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual void SetDataWithRetries(string path, byte[] data, int version)
		{
			DoStoreMultiWithRetries(OP.SetData(path, data, version));
		}

		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public virtual byte[] GetDataWithRetries(string path, bool watch)
		{
			return new _ZKAction_1035(this, path, watch).RunWithRetries();
		}

		private sealed class _ZKAction_1035 : ZKRMStateStore.ZKAction<byte[]>
		{
			public _ZKAction_1035(ZKRMStateStore _enclosing, string path, bool watch)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.watch = watch;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			internal override byte[] Run()
			{
				return this._enclosing.zkClient.GetData(path, watch, null);
			}

			private readonly ZKRMStateStore _enclosing;

			private readonly string path;

			private readonly bool watch;
		}

		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		internal virtual IList<ACL> GetACLWithRetries(string path, Stat stat)
		{
			return new _ZKAction_1046(this, path, stat).RunWithRetries();
		}

		private sealed class _ZKAction_1046 : ZKRMStateStore.ZKAction<IList<ACL>>
		{
			public _ZKAction_1046(ZKRMStateStore _enclosing, string path, Stat stat)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.stat = stat;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			internal override IList<ACL> Run()
			{
				return this._enclosing.zkClient.GetACL(path, stat);
			}

			private readonly ZKRMStateStore _enclosing;

			private readonly string path;

			private readonly Stat stat;
		}

		/// <exception cref="System.Exception"/>
		private IList<string> GetChildrenWithRetries(string path, bool watch)
		{
			return new _ZKAction_1056(this, path, watch).RunWithRetries();
		}

		private sealed class _ZKAction_1056 : ZKRMStateStore.ZKAction<IList<string>>
		{
			public _ZKAction_1056(ZKRMStateStore _enclosing, string path, bool watch)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.watch = watch;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			internal override IList<string> Run()
			{
				return this._enclosing.zkClient.GetChildren(path, watch);
			}

			private readonly ZKRMStateStore _enclosing;

			private readonly string path;

			private readonly bool watch;
		}

		/// <exception cref="System.Exception"/>
		private Stat ExistsWithRetries(string path, bool watch)
		{
			return new _ZKAction_1066(this, path, watch).RunWithRetries();
		}

		private sealed class _ZKAction_1066 : ZKRMStateStore.ZKAction<Stat>
		{
			public _ZKAction_1066(ZKRMStateStore _enclosing, string path, bool watch)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.watch = watch;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			internal override Stat Run()
			{
				return this._enclosing.zkClient.Exists(path, watch);
			}

			private readonly ZKRMStateStore _enclosing;

			private readonly string path;

			private readonly bool watch;
		}

		/// <exception cref="System.Exception"/>
		private void DeleteWithRetries(string path, bool watch)
		{
			new _ZKAction_1076(this, path, watch).RunWithRetries();
		}

		private sealed class _ZKAction_1076 : ZKRMStateStore.ZKAction<Void>
		{
			public _ZKAction_1076(ZKRMStateStore _enclosing, string path, bool watch)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.watch = watch;
			}

			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			internal override Void Run()
			{
				this._enclosing.RecursiveDeleteWithRetriesHelper(path, watch);
				return null;
			}

			private readonly ZKRMStateStore _enclosing;

			private readonly string path;

			private readonly bool watch;
		}

		/// <summary>Helper method that deletes znodes recursively</summary>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		/// <exception cref="System.Exception"/>
		private void RecursiveDeleteWithRetriesHelper(string path, bool watch)
		{
			IList<string> children = zkClient.GetChildren(path, watch);
			foreach (string child in children)
			{
				RecursiveDeleteWithRetriesHelper(path + "/" + child, false);
			}
			try
			{
				zkClient.Delete(path, -1);
			}
			catch (KeeperException.NoNodeException)
			{
				Log.Info("Node " + path + " doesn't exist to delete");
			}
		}

		/// <summary>
		/// Helper class that periodically attempts creating a znode to ensure that
		/// this RM continues to be the Active.
		/// </summary>
		private class VerifyActiveStatusThread : Sharpen.Thread
		{
			private IList<OP> emptyOpList = new AList<OP>();

			internal VerifyActiveStatusThread(ZKRMStateStore _enclosing)
				: base(typeof(ZKRMStateStore.VerifyActiveStatusThread).FullName)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				try
				{
					while (true)
					{
						if (this._enclosing.IsFencedState())
						{
							break;
						}
						this._enclosing.DoStoreMultiWithRetries(this.emptyOpList);
						Sharpen.Thread.Sleep(this._enclosing.zkSessionTimeout);
					}
				}
				catch (Exception)
				{
					ZKRMStateStore.Log.Info(typeof(ZKRMStateStore.VerifyActiveStatusThread).FullName 
						+ " thread " + "interrupted! Exiting!");
				}
				catch (Exception)
				{
					this._enclosing.NotifyStoreOperationFailed(new StoreFencedException());
				}
			}

			private readonly ZKRMStateStore _enclosing;
		}

		private abstract class ZKAction<T>
		{
			private bool hasDeleteNodeOp = false;

			internal virtual void SetHasDeleteNodeOp(bool hasDeleteOp)
			{
				this.hasDeleteNodeOp = hasDeleteOp;
			}

			// run() expects synchronization on ZKRMStateStore.this
			/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
			/// <exception cref="System.Exception"/>
			internal abstract T Run();

			/// <exception cref="System.Exception"/>
			internal virtual T RunWithCheck()
			{
				long startTime = Runtime.CurrentTimeMillis();
				lock (this._enclosing)
				{
					while (this._enclosing.zkClient == null)
					{
						Sharpen.Runtime.Wait(this._enclosing, this._enclosing.zkResyncWaitTime);
						if (this._enclosing.zkClient != null)
						{
							break;
						}
						if (Runtime.CurrentTimeMillis() - startTime > this._enclosing.zkResyncWaitTime)
						{
							throw new IOException("Wait for ZKClient creation timed out");
						}
					}
					return this.Run();
				}
			}

			private bool ShouldRetry(KeeperException.Code code)
			{
				switch (code)
				{
					case KeeperException.Code.Connectionloss:
					case KeeperException.Code.Operationtimeout:
					{
						return true;
					}

					default:
					{
						break;
					}
				}
				return false;
			}

			private bool ShouldRetryWithNewConnection(KeeperException.Code code)
			{
				switch (code)
				{
					case KeeperException.Code.Sessionexpired:
					case KeeperException.Code.Sessionmoved:
					{
						// For fast recover, we choose to close current connection after
						// SESSIONMOVED occurs. Latest state of a path is assured by a following
						// zk.sync(path) operation.
						return true;
					}

					default:
					{
						break;
					}
				}
				return false;
			}

			/// <exception cref="System.Exception"/>
			internal virtual T RunWithRetries()
			{
				int retry = 0;
				while (true)
				{
					try
					{
						return this.RunWithCheck();
					}
					catch (KeeperException.NoAuthException nae)
					{
						if (HAUtil.IsHAEnabled(this._enclosing.GetConfig()))
						{
							// NoAuthException possibly means that this store is fenced due to
							// another RM becoming active. Even if not,
							// it is safer to assume we have been fenced
							throw new StoreFencedException();
						}
						else
						{
							throw;
						}
					}
					catch (KeeperException ke)
					{
						if (ke.Code() == KeeperException.Code.Nodeexists)
						{
							ZKRMStateStore.Log.Info("znode already exists!");
							return null;
						}
						if (this.hasDeleteNodeOp && ke.Code() == KeeperException.Code.Nonode)
						{
							ZKRMStateStore.Log.Info("znode has already been deleted!");
							return null;
						}
						ZKRMStateStore.Log.Info("Exception while executing a ZK operation.", ke);
						retry++;
						if (this.ShouldRetry(ke.Code()) && retry < this._enclosing.numRetries)
						{
							ZKRMStateStore.Log.Info("Retrying operation on ZK. Retry no. " + retry);
							Sharpen.Thread.Sleep(this._enclosing.zkRetryInterval);
							continue;
						}
						if (this.ShouldRetryWithNewConnection(ke.Code()) && retry < this._enclosing.numRetries)
						{
							ZKRMStateStore.Log.Info("Retrying operation on ZK with new Connection. " + "Retry no. "
								 + retry);
							Sharpen.Thread.Sleep(this._enclosing.zkRetryInterval);
							this._enclosing.CreateConnection();
							this._enclosing.SyncInternal(ke.GetPath());
							continue;
						}
						ZKRMStateStore.Log.Info("Maxed out ZK retries. Giving up!");
						throw;
					}
				}
			}

			internal ZKAction(ZKRMStateStore _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly ZKRMStateStore _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private void CreateConnection()
		{
			lock (this)
			{
				CloseZkClients();
				for (int retries = 0; retries < numRetries && zkClient == null; retries++)
				{
					try
					{
						activeZkClient = GetNewZooKeeper();
						zkClient = activeZkClient;
						foreach (ZKUtil.ZKAuthInfo zkAuth in zkAuths)
						{
							zkClient.AddAuthInfo(zkAuth.GetScheme(), zkAuth.GetAuth());
						}
						if (useDefaultFencingScheme)
						{
							zkClient.AddAuthInfo(zkRootNodeAuthScheme, Sharpen.Runtime.GetBytesForString((zkRootNodeUsername
								 + ":" + zkRootNodePassword), Sharpen.Extensions.GetEncoding("UTF-8")));
						}
					}
					catch (IOException ioe)
					{
						// Retry in case of network failures
						Log.Info("Failed to connect to the ZooKeeper on attempt - " + (retries + 1));
						Sharpen.Runtime.PrintStackTrace(ioe);
					}
				}
				if (zkClient == null)
				{
					Log.Error("Unable to connect to Zookeeper");
					throw new YarnRuntimeException("Unable to connect to Zookeeper");
				}
				Sharpen.Runtime.NotifyAll(this);
				Log.Info("Created new ZK connection");
			}
		}

		// protected to mock for testing
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		protected internal virtual ZooKeeper GetNewZooKeeper()
		{
			lock (this)
			{
				ZooKeeper zk = new ZooKeeper(zkHostPort, zkSessionTimeout, null);
				zk.Register(new ZKRMStateStore.ForwardingWatcher(this, zk));
				return zk;
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreOrUpdateAMRMTokenSecretManagerState(AMRMTokenSecretManagerState
			 amrmTokenSecretManagerState, bool isUpdate)
		{
			lock (this)
			{
				AMRMTokenSecretManagerState data = AMRMTokenSecretManagerState.NewInstance(amrmTokenSecretManagerState
					);
				byte[] stateData = data.GetProto().ToByteArray();
				SetDataWithRetries(amrmTokenSecretManagerRoot, stateData, -1);
			}
		}

		/// <summary>Utility function to ensure that the configured base znode exists.</summary>
		/// <remarks>
		/// Utility function to ensure that the configured base znode exists.
		/// This recursively creates the znode as well as all of its parents.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		private void CreateRootDirRecursively(string path)
		{
			string[] pathParts = path.Split("/");
			Preconditions.CheckArgument(pathParts.Length >= 1 && pathParts[0].IsEmpty(), "Invalid path: %s"
				, path);
			StringBuilder sb = new StringBuilder();
			for (int i = 1; i < pathParts.Length; i++)
			{
				sb.Append("/").Append(pathParts[i]);
				CreateRootDir(sb.ToString());
			}
		}
	}
}
