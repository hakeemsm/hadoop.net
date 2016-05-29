using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class FileSystemRMStateStore : RMStateStore
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(FileSystemRMStateStore)
			);

		protected internal const string RootDirName = "FSRMStateRoot";

		protected internal static readonly Version CurrentVersionInfo = Version.NewInstance
			(1, 2);

		protected internal const string AmrmtokenSecretManagerNode = "AMRMTokenSecretManagerNode";

		private const string UnreadableBySuperuserXattrib = "security.hdfs.unreadable.by.superuser";

		protected internal FileSystem fs;

		private Path rootDirPath;

		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal Path rmDTSecretManagerRoot;

		private Path rmAppRoot;

		private Path dtSequenceNumberPath = null;

		private int fsNumRetries;

		private long fsRetryInterval;

		private bool intermediateEncryptionEnabled = YarnConfiguration.DefaultYarnIntermediateDataEncryption;

		[VisibleForTesting]
		internal Path fsWorkingPath;

		internal Path amrmTokenSecretManagerRoot;

		/// <exception cref="System.Exception"/>
		protected internal override void InitInternal(Configuration conf)
		{
			lock (this)
			{
				fsWorkingPath = new Path(conf.Get(YarnConfiguration.FsRmStateStoreUri));
				rootDirPath = new Path(fsWorkingPath, RootDirName);
				rmDTSecretManagerRoot = new Path(rootDirPath, RmDtSecretManagerRoot);
				rmAppRoot = new Path(rootDirPath, RmAppRoot);
				amrmTokenSecretManagerRoot = new Path(rootDirPath, AmrmtokenSecretManagerRoot);
				fsNumRetries = conf.GetInt(YarnConfiguration.FsRmStateStoreNumRetries, YarnConfiguration
					.DefaultFsRmStateStoreNumRetries);
				fsRetryInterval = conf.GetLong(YarnConfiguration.FsRmStateStoreRetryIntervalMs, YarnConfiguration
					.DefaultFsRmStateStoreRetryIntervalMs);
				intermediateEncryptionEnabled = conf.GetBoolean(YarnConfiguration.YarnIntermediateDataEncryption
					, YarnConfiguration.DefaultYarnIntermediateDataEncryption);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StartInternal()
		{
			lock (this)
			{
				// create filesystem only now, as part of service-start. By this time, RM is
				// authenticated with kerberos so we are good to create a file-system
				// handle.
				Configuration conf = new Configuration(GetConfig());
				conf.SetBoolean("dfs.client.retry.policy.enabled", true);
				string retryPolicy = conf.Get(YarnConfiguration.FsRmStateStoreRetryPolicySpec, YarnConfiguration
					.DefaultFsRmStateStoreRetryPolicySpec);
				conf.Set("dfs.client.retry.policy.spec", retryPolicy);
				fs = fsWorkingPath.GetFileSystem(conf);
				MkdirsWithRetries(rmDTSecretManagerRoot);
				MkdirsWithRetries(rmAppRoot);
				MkdirsWithRetries(amrmTokenSecretManagerRoot);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void CloseInternal()
		{
			lock (this)
			{
				CloseWithRetries();
			}
		}

		protected internal override Version GetCurrentVersion()
		{
			return CurrentVersionInfo;
		}

		/// <exception cref="System.Exception"/>
		protected internal override Version LoadVersion()
		{
			lock (this)
			{
				Path versionNodePath = GetNodePath(rootDirPath, VersionNode);
				FileStatus status = GetFileStatusWithRetries(versionNodePath);
				if (status != null)
				{
					byte[] data = ReadFileWithRetries(versionNodePath, status.GetLen());
					Version version = new VersionPBImpl(YarnServerCommonProtos.VersionProto.ParseFrom
						(data));
					return version;
				}
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreVersion()
		{
			lock (this)
			{
				Path versionNodePath = GetNodePath(rootDirPath, VersionNode);
				byte[] data = ((VersionPBImpl)CurrentVersionInfo).GetProto().ToByteArray();
				if (ExistsWithRetries(versionNodePath))
				{
					UpdateFile(versionNodePath, data, false);
				}
				else
				{
					WriteFileWithRetries(versionNodePath, data, false);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public override long GetAndIncrementEpoch()
		{
			lock (this)
			{
				Path epochNodePath = GetNodePath(rootDirPath, EpochNode);
				long currentEpoch = 0;
				FileStatus status = GetFileStatusWithRetries(epochNodePath);
				if (status != null)
				{
					// load current epoch
					byte[] data = ReadFileWithRetries(epochNodePath, status.GetLen());
					Epoch epoch = new EpochPBImpl(YarnServerResourceManagerRecoveryProtos.EpochProto.
						ParseFrom(data));
					currentEpoch = epoch.GetEpoch();
					// increment epoch and store it
					byte[] storeData = Epoch.NewInstance(currentEpoch + 1).GetProto().ToByteArray();
					UpdateFile(epochNodePath, storeData, false);
				}
				else
				{
					// initialize epoch file with 1 for the next time.
					byte[] storeData = Epoch.NewInstance(currentEpoch + 1).GetProto().ToByteArray();
					WriteFileWithRetries(epochNodePath, storeData, false);
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
			CheckAndResumeUpdateOperation(amrmTokenSecretManagerRoot);
			Path amrmTokenSecretManagerStateDataDir = new Path(amrmTokenSecretManagerRoot, AmrmtokenSecretManagerNode
				);
			FileStatus status = GetFileStatusWithRetries(amrmTokenSecretManagerStateDataDir);
			if (status == null)
			{
				return;
			}
			System.Diagnostics.Debug.Assert(status.IsFile());
			byte[] data = ReadFileWithRetries(amrmTokenSecretManagerStateDataDir, status.GetLen
				());
			AMRMTokenSecretManagerStatePBImpl stateData = new AMRMTokenSecretManagerStatePBImpl
				(YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto.ParseFrom
				(data));
			rmState.amrmTokenSecretManagerState = AMRMTokenSecretManagerState.NewInstance(stateData
				.GetCurrentMasterKey(), stateData.GetNextMasterKey());
		}

		/// <exception cref="System.Exception"/>
		private void LoadRMAppState(RMStateStore.RMState rmState)
		{
			try
			{
				IList<ApplicationAttemptStateData> attempts = new AList<ApplicationAttemptStateData
					>();
				foreach (FileStatus appDir in ListStatusWithRetries(rmAppRoot))
				{
					CheckAndResumeUpdateOperation(appDir.GetPath());
					foreach (FileStatus childNodeStatus in ListStatusWithRetries(appDir.GetPath()))
					{
						System.Diagnostics.Debug.Assert(childNodeStatus.IsFile());
						string childNodeName = childNodeStatus.GetPath().GetName();
						if (CheckAndRemovePartialRecordWithRetries(childNodeStatus.GetPath()))
						{
							continue;
						}
						byte[] childData = ReadFileWithRetries(childNodeStatus.GetPath(), childNodeStatus
							.GetLen());
						// Set attribute if not already set
						SetUnreadableBySuperuserXattrib(childNodeStatus.GetPath());
						if (childNodeName.StartsWith(ApplicationId.appIdStrPrefix))
						{
							// application
							if (Log.IsDebugEnabled())
							{
								Log.Debug("Loading application from node: " + childNodeName);
							}
							ApplicationStateDataPBImpl appState = new ApplicationStateDataPBImpl(YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto
								.ParseFrom(childData));
							ApplicationId appId = appState.GetApplicationSubmissionContext().GetApplicationId
								();
							rmState.appState[appId] = appState;
						}
						else
						{
							if (childNodeName.StartsWith(ApplicationAttemptId.appAttemptIdStrPrefix))
							{
								// attempt
								if (Log.IsDebugEnabled())
								{
									Log.Debug("Loading application attempt from node: " + childNodeName);
								}
								ApplicationAttemptStateDataPBImpl attemptState = new ApplicationAttemptStateDataPBImpl
									(YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto.ParseFrom
									(childData));
								attempts.AddItem(attemptState);
							}
							else
							{
								Log.Info("Unknown child node with name: " + childNodeName);
							}
						}
					}
				}
				// go through all attempts and add them to their apps, Ideally, each
				// attempt node must have a corresponding app node, because remove
				// directory operation remove both at the same time
				foreach (ApplicationAttemptStateData attemptState_1 in attempts)
				{
					ApplicationId appId = attemptState_1.GetAttemptId().GetApplicationId();
					ApplicationStateData appState = rmState.appState[appId];
					System.Diagnostics.Debug.Assert(appState != null);
					appState.attempts[attemptState_1.GetAttemptId()] = attemptState_1;
				}
				Log.Info("Done loading applications from FS state store");
			}
			catch (Exception e)
			{
				Log.Error("Failed to load state.", e);
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private bool CheckAndRemovePartialRecord(Path record)
		{
			// If the file ends with .tmp then it shows that it failed
			// during saving state into state store. The file will be deleted as a
			// part of this call
			if (record.GetName().EndsWith(".tmp"))
			{
				Log.Error("incomplete rm state store entry found :" + record);
				fs.Delete(record, false);
				return true;
			}
			return false;
		}

		/// <exception cref="System.Exception"/>
		private void CheckAndResumeUpdateOperation(Path path)
		{
			// Before loading the state information, check whether .new file exists.
			// If it does, the prior updateFile is failed on half way. We need to
			// complete replacing the old file first.
			FileStatus[] newChildNodes = ListStatusWithRetries(path, new _PathFilter_319());
			foreach (FileStatus newChildNodeStatus in newChildNodes)
			{
				System.Diagnostics.Debug.Assert(newChildNodeStatus.IsFile());
				string newChildNodeName = newChildNodeStatus.GetPath().GetName();
				string childNodeName = Sharpen.Runtime.Substring(newChildNodeName, 0, newChildNodeName
					.Length - ".new".Length);
				Path childNodePath = new Path(newChildNodeStatus.GetPath().GetParent(), childNodeName
					);
				ReplaceFile(newChildNodeStatus.GetPath(), childNodePath);
			}
		}

		private sealed class _PathFilter_319 : PathFilter
		{
			public _PathFilter_319()
			{
			}

			public bool Accept(Path path)
			{
				return path.GetName().EndsWith(".new");
			}
		}

		/// <exception cref="System.Exception"/>
		private void LoadRMDTSecretManagerState(RMStateStore.RMState rmState)
		{
			CheckAndResumeUpdateOperation(rmDTSecretManagerRoot);
			FileStatus[] childNodes = ListStatusWithRetries(rmDTSecretManagerRoot);
			foreach (FileStatus childNodeStatus in childNodes)
			{
				System.Diagnostics.Debug.Assert(childNodeStatus.IsFile());
				string childNodeName = childNodeStatus.GetPath().GetName();
				if (CheckAndRemovePartialRecordWithRetries(childNodeStatus.GetPath()))
				{
					continue;
				}
				if (childNodeName.StartsWith(DelegationTokenSequenceNumberPrefix))
				{
					rmState.rmSecretManagerState.dtSequenceNumber = System.Convert.ToInt32(childNodeName
						.Split("_")[1]);
					continue;
				}
				Path childNodePath = GetNodePath(rmDTSecretManagerRoot, childNodeName);
				byte[] childData = ReadFileWithRetries(childNodePath, childNodeStatus.GetLen());
				ByteArrayInputStream @is = new ByteArrayInputStream(childData);
				using (DataInputStream fsIn = new DataInputStream(@is))
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
					else
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
						else
						{
							Log.Warn("Unknown file for recovering RMDelegationTokenSecretManager");
						}
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreApplicationStateInternal(ApplicationId appId
			, ApplicationStateData appStateDataPB)
		{
			lock (this)
			{
				Path appDirPath = GetAppDir(rmAppRoot, appId);
				MkdirsWithRetries(appDirPath);
				Path nodeCreatePath = GetNodePath(appDirPath, appId.ToString());
				Log.Info("Storing info for app: " + appId + " at: " + nodeCreatePath);
				byte[] appStateData = appStateDataPB.GetProto().ToByteArray();
				try
				{
					// currently throw all exceptions. May need to respond differently for HA
					// based on whether we have lost the right to write to FS
					WriteFileWithRetries(nodeCreatePath, appStateData, true);
				}
				catch (Exception e)
				{
					Log.Info("Error storing info for app: " + appId, e);
					throw;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void UpdateApplicationStateInternal(ApplicationId appId
			, ApplicationStateData appStateDataPB)
		{
			lock (this)
			{
				Path appDirPath = GetAppDir(rmAppRoot, appId);
				Path nodeCreatePath = GetNodePath(appDirPath, appId.ToString());
				Log.Info("Updating info for app: " + appId + " at: " + nodeCreatePath);
				byte[] appStateData = appStateDataPB.GetProto().ToByteArray();
				try
				{
					// currently throw all exceptions. May need to respond differently for HA
					// based on whether we have lost the right to write to FS
					UpdateFile(nodeCreatePath, appStateData, true);
				}
				catch (Exception e)
				{
					Log.Info("Error updating info for app: " + appId, e);
					throw;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreApplicationAttemptStateInternal(ApplicationAttemptId
			 appAttemptId, ApplicationAttemptStateData attemptStateDataPB)
		{
			lock (this)
			{
				Path appDirPath = GetAppDir(rmAppRoot, appAttemptId.GetApplicationId());
				Path nodeCreatePath = GetNodePath(appDirPath, appAttemptId.ToString());
				Log.Info("Storing info for attempt: " + appAttemptId + " at: " + nodeCreatePath);
				byte[] attemptStateData = attemptStateDataPB.GetProto().ToByteArray();
				try
				{
					// currently throw all exceptions. May need to respond differently for HA
					// based on whether we have lost the right to write to FS
					WriteFileWithRetries(nodeCreatePath, attemptStateData, true);
				}
				catch (Exception e)
				{
					Log.Info("Error storing info for attempt: " + appAttemptId, e);
					throw;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void UpdateApplicationAttemptStateInternal(ApplicationAttemptId
			 appAttemptId, ApplicationAttemptStateData attemptStateDataPB)
		{
			lock (this)
			{
				Path appDirPath = GetAppDir(rmAppRoot, appAttemptId.GetApplicationId());
				Path nodeCreatePath = GetNodePath(appDirPath, appAttemptId.ToString());
				Log.Info("Updating info for attempt: " + appAttemptId + " at: " + nodeCreatePath);
				byte[] attemptStateData = attemptStateDataPB.GetProto().ToByteArray();
				try
				{
					// currently throw all exceptions. May need to respond differently for HA
					// based on whether we have lost the right to write to FS
					UpdateFile(nodeCreatePath, attemptStateData, true);
				}
				catch (Exception e)
				{
					Log.Info("Error updating info for attempt: " + appAttemptId, e);
					throw;
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void RemoveApplicationStateInternal(ApplicationStateData
			 appState)
		{
			lock (this)
			{
				ApplicationId appId = appState.GetApplicationSubmissionContext().GetApplicationId
					();
				Path nodeRemovePath = GetAppDir(rmAppRoot, appId);
				Log.Info("Removing info for app: " + appId + " at: " + nodeRemovePath);
				DeleteFileWithRetries(nodeRemovePath);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreRMDelegationTokenState(RMDelegationTokenIdentifier
			 identifier, long renewDate)
		{
			lock (this)
			{
				StoreOrUpdateRMDelegationTokenState(identifier, renewDate, false);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void RemoveRMDelegationTokenState(RMDelegationTokenIdentifier
			 identifier)
		{
			lock (this)
			{
				Path nodeCreatePath = GetNodePath(rmDTSecretManagerRoot, DelegationTokenPrefix + 
					identifier.GetSequenceNumber());
				Log.Info("Removing RMDelegationToken_" + identifier.GetSequenceNumber());
				DeleteFileWithRetries(nodeCreatePath);
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void UpdateRMDelegationTokenState(RMDelegationTokenIdentifier
			 rmDTIdentifier, long renewDate)
		{
			lock (this)
			{
				StoreOrUpdateRMDelegationTokenState(rmDTIdentifier, renewDate, true);
			}
		}

		/// <exception cref="System.Exception"/>
		private void StoreOrUpdateRMDelegationTokenState(RMDelegationTokenIdentifier identifier
			, long renewDate, bool isUpdate)
		{
			Path nodeCreatePath = GetNodePath(rmDTSecretManagerRoot, DelegationTokenPrefix + 
				identifier.GetSequenceNumber());
			RMDelegationTokenIdentifierData identifierData = new RMDelegationTokenIdentifierData
				(identifier, renewDate);
			if (isUpdate)
			{
				Log.Info("Updating RMDelegationToken_" + identifier.GetSequenceNumber());
				UpdateFile(nodeCreatePath, identifierData.ToByteArray(), true);
			}
			else
			{
				Log.Info("Storing RMDelegationToken_" + identifier.GetSequenceNumber());
				WriteFileWithRetries(nodeCreatePath, identifierData.ToByteArray(), true);
				// store sequence number
				Path latestSequenceNumberPath = GetNodePath(rmDTSecretManagerRoot, DelegationTokenSequenceNumberPrefix
					 + identifier.GetSequenceNumber());
				Log.Info("Storing " + DelegationTokenSequenceNumberPrefix + identifier.GetSequenceNumber
					());
				if (dtSequenceNumberPath == null)
				{
					if (!CreateFileWithRetries(latestSequenceNumberPath))
					{
						throw new Exception("Failed to create " + latestSequenceNumberPath);
					}
				}
				else
				{
					if (!RenameFileWithRetries(dtSequenceNumberPath, latestSequenceNumberPath))
					{
						throw new Exception("Failed to rename " + dtSequenceNumberPath);
					}
				}
				dtSequenceNumberPath = latestSequenceNumberPath;
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreRMDTMasterKeyState(DelegationKey masterKey)
		{
			lock (this)
			{
				Path nodeCreatePath = GetNodePath(rmDTSecretManagerRoot, DelegationKeyPrefix + masterKey
					.GetKeyId());
				ByteArrayOutputStream os = new ByteArrayOutputStream();
				using (DataOutputStream fsOut = new DataOutputStream(os))
				{
					Log.Info("Storing RMDelegationKey_" + masterKey.GetKeyId());
					masterKey.Write(fsOut);
					WriteFileWithRetries(nodeCreatePath, os.ToByteArray(), true);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		protected internal override void RemoveRMDTMasterKeyState(DelegationKey masterKey
			)
		{
			lock (this)
			{
				Path nodeCreatePath = GetNodePath(rmDTSecretManagerRoot, DelegationKeyPrefix + masterKey
					.GetKeyId());
				Log.Info("Removing RMDelegationKey_" + masterKey.GetKeyId());
				DeleteFileWithRetries(nodeCreatePath);
			}
		}

		/// <exception cref="System.Exception"/>
		public override void DeleteStore()
		{
			lock (this)
			{
				if (ExistsWithRetries(rootDirPath))
				{
					DeleteFileWithRetries(rootDirPath);
				}
			}
		}

		private Path GetAppDir(Path root, ApplicationId appId)
		{
			return GetNodePath(root, appId.ToString());
		}

		[VisibleForTesting]
		protected internal virtual Path GetAppDir(ApplicationId appId)
		{
			return GetAppDir(rmAppRoot, appId);
		}

		[VisibleForTesting]
		protected internal virtual Path GetAppAttemptDir(ApplicationAttemptId appAttId)
		{
			return GetNodePath(GetAppDir(appAttId.GetApplicationId()), appAttId.ToString());
		}

		// FileSystem related code
		/// <exception cref="System.Exception"/>
		private bool CheckAndRemovePartialRecordWithRetries(Path record)
		{
			return new _FSAction_580(this, record).RunWithRetries();
		}

		private sealed class _FSAction_580 : FileSystemRMStateStore.FSAction<bool>
		{
			public _FSAction_580(FileSystemRMStateStore _enclosing, Path record)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.record = record;
			}

			/// <exception cref="System.Exception"/>
			internal override bool Run()
			{
				return this._enclosing.CheckAndRemovePartialRecord(record);
			}

			private readonly FileSystemRMStateStore _enclosing;

			private readonly Path record;
		}

		/// <exception cref="System.Exception"/>
		private void MkdirsWithRetries(Path appDirPath)
		{
			new _FSAction_589(this, appDirPath).RunWithRetries();
		}

		private sealed class _FSAction_589 : FileSystemRMStateStore.FSAction<Void>
		{
			public _FSAction_589(FileSystemRMStateStore _enclosing, Path appDirPath)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.appDirPath = appDirPath;
			}

			/// <exception cref="System.Exception"/>
			internal override Void Run()
			{
				this._enclosing.fs.Mkdirs(appDirPath);
				return null;
			}

			private readonly FileSystemRMStateStore _enclosing;

			private readonly Path appDirPath;
		}

		/// <exception cref="System.Exception"/>
		private void WriteFileWithRetries(Path outputPath, byte[] data, bool makeUnreadableByAdmin
			)
		{
			new _FSAction_601(this, outputPath, data, makeUnreadableByAdmin).RunWithRetries();
		}

		private sealed class _FSAction_601 : FileSystemRMStateStore.FSAction<Void>
		{
			public _FSAction_601(FileSystemRMStateStore _enclosing, Path outputPath, byte[] data
				, bool makeUnreadableByAdmin)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.outputPath = outputPath;
				this.data = data;
				this.makeUnreadableByAdmin = makeUnreadableByAdmin;
			}

			/// <exception cref="System.Exception"/>
			internal override Void Run()
			{
				this._enclosing.WriteFile(outputPath, data, makeUnreadableByAdmin);
				return null;
			}

			private readonly FileSystemRMStateStore _enclosing;

			private readonly Path outputPath;

			private readonly byte[] data;

			private readonly bool makeUnreadableByAdmin;
		}

		/// <exception cref="System.Exception"/>
		private void DeleteFileWithRetries(Path deletePath)
		{
			new _FSAction_611(this, deletePath).RunWithRetries();
		}

		private sealed class _FSAction_611 : FileSystemRMStateStore.FSAction<Void>
		{
			public _FSAction_611(FileSystemRMStateStore _enclosing, Path deletePath)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.deletePath = deletePath;
			}

			/// <exception cref="System.Exception"/>
			internal override Void Run()
			{
				this._enclosing.DeleteFile(deletePath);
				return null;
			}

			private readonly FileSystemRMStateStore _enclosing;

			private readonly Path deletePath;
		}

		/// <exception cref="System.Exception"/>
		private bool RenameFileWithRetries(Path src, Path dst)
		{
			return new _FSAction_622(this, src, dst).RunWithRetries();
		}

		private sealed class _FSAction_622 : FileSystemRMStateStore.FSAction<bool>
		{
			public _FSAction_622(FileSystemRMStateStore _enclosing, Path src, Path dst)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.src = src;
				this.dst = dst;
			}

			/// <exception cref="System.Exception"/>
			internal override bool Run()
			{
				return this._enclosing.RenameFile(src, dst);
			}

			private readonly FileSystemRMStateStore _enclosing;

			private readonly Path src;

			private readonly Path dst;
		}

		/// <exception cref="System.Exception"/>
		private bool CreateFileWithRetries(Path newFile)
		{
			return new _FSAction_631(this, newFile).RunWithRetries();
		}

		private sealed class _FSAction_631 : FileSystemRMStateStore.FSAction<bool>
		{
			public _FSAction_631(FileSystemRMStateStore _enclosing, Path newFile)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.newFile = newFile;
			}

			/// <exception cref="System.Exception"/>
			internal override bool Run()
			{
				return this._enclosing.CreateFile(newFile);
			}

			private readonly FileSystemRMStateStore _enclosing;

			private readonly Path newFile;
		}

		/// <exception cref="System.Exception"/>
		private FileStatus GetFileStatusWithRetries(Path path)
		{
			return new _FSAction_641(this, path).RunWithRetries();
		}

		private sealed class _FSAction_641 : FileSystemRMStateStore.FSAction<FileStatus>
		{
			public _FSAction_641(FileSystemRMStateStore _enclosing, Path path)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.path = path;
			}

			/// <exception cref="System.Exception"/>
			internal override FileStatus Run()
			{
				return this._enclosing.GetFileStatus(path);
			}

			private readonly FileSystemRMStateStore _enclosing;

			private readonly Path path;
		}

		/// <exception cref="System.Exception"/>
		private bool ExistsWithRetries(Path path)
		{
			return new _FSAction_650(this, path).RunWithRetries();
		}

		private sealed class _FSAction_650 : FileSystemRMStateStore.FSAction<bool>
		{
			public _FSAction_650(FileSystemRMStateStore _enclosing, Path path)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.path = path;
			}

			/// <exception cref="System.Exception"/>
			internal override bool Run()
			{
				return this._enclosing.fs.Exists(path);
			}

			private readonly FileSystemRMStateStore _enclosing;

			private readonly Path path;
		}

		/// <exception cref="System.Exception"/>
		private byte[] ReadFileWithRetries(Path inputPath, long len)
		{
			return new _FSAction_660(this, inputPath, len).RunWithRetries();
		}

		private sealed class _FSAction_660 : FileSystemRMStateStore.FSAction<byte[]>
		{
			public _FSAction_660(FileSystemRMStateStore _enclosing, Path inputPath, long len)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.inputPath = inputPath;
				this.len = len;
			}

			/// <exception cref="System.Exception"/>
			internal override byte[] Run()
			{
				return this._enclosing.ReadFile(inputPath, len);
			}

			private readonly FileSystemRMStateStore _enclosing;

			private readonly Path inputPath;

			private readonly long len;
		}

		/// <exception cref="System.Exception"/>
		private FileStatus[] ListStatusWithRetries(Path path)
		{
			return new _FSAction_670(this, path).RunWithRetries();
		}

		private sealed class _FSAction_670 : FileSystemRMStateStore.FSAction<FileStatus[]
			>
		{
			public _FSAction_670(FileSystemRMStateStore _enclosing, Path path)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.path = path;
			}

			/// <exception cref="System.Exception"/>
			internal override FileStatus[] Run()
			{
				return this._enclosing.fs.ListStatus(path);
			}

			private readonly FileSystemRMStateStore _enclosing;

			private readonly Path path;
		}

		/// <exception cref="System.Exception"/>
		private FileStatus[] ListStatusWithRetries(Path path, PathFilter filter)
		{
			return new _FSAction_680(this, path, filter).RunWithRetries();
		}

		private sealed class _FSAction_680 : FileSystemRMStateStore.FSAction<FileStatus[]
			>
		{
			public _FSAction_680(FileSystemRMStateStore _enclosing, Path path, PathFilter filter
				)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.path = path;
				this.filter = filter;
			}

			/// <exception cref="System.Exception"/>
			internal override FileStatus[] Run()
			{
				return this._enclosing.fs.ListStatus(path, filter);
			}

			private readonly FileSystemRMStateStore _enclosing;

			private readonly Path path;

			private readonly PathFilter filter;
		}

		/// <exception cref="System.Exception"/>
		private void CloseWithRetries()
		{
			new _FSAction_689(this).RunWithRetries();
		}

		private sealed class _FSAction_689 : FileSystemRMStateStore.FSAction<Void>
		{
			public _FSAction_689(FileSystemRMStateStore _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			internal override Void Run()
			{
				this._enclosing.fs.Close();
				return null;
			}

			private readonly FileSystemRMStateStore _enclosing;
		}

		private abstract class FSAction<T>
		{
			/// <exception cref="System.Exception"/>
			internal abstract T Run();

			/// <exception cref="System.Exception"/>
			internal virtual T RunWithRetries()
			{
				int retry = 0;
				while (true)
				{
					try
					{
						return this.Run();
					}
					catch (IOException e)
					{
						FileSystemRMStateStore.Log.Info("Exception while executing a FS operation.", e);
						if (++retry > this._enclosing.fsNumRetries)
						{
							FileSystemRMStateStore.Log.Info("Maxed out FS retries. Giving up!");
							throw;
						}
						FileSystemRMStateStore.Log.Info("Retrying operation on FS. Retry no. " + retry);
						Sharpen.Thread.Sleep(this._enclosing.fsRetryInterval);
					}
				}
			}

			internal FSAction(FileSystemRMStateStore _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly FileSystemRMStateStore _enclosing;
		}

		/// <exception cref="System.Exception"/>
		private void DeleteFile(Path deletePath)
		{
			if (!fs.Delete(deletePath, true))
			{
				throw new Exception("Failed to delete " + deletePath);
			}
		}

		/// <exception cref="System.Exception"/>
		private byte[] ReadFile(Path inputPath, long len)
		{
			FSDataInputStream fsIn = null;
			try
			{
				fsIn = fs.Open(inputPath);
				// state data will not be that "long"
				byte[] data = new byte[(int)len];
				fsIn.ReadFully(data);
				return data;
			}
			finally
			{
				IOUtils.Cleanup(Log, fsIn);
			}
		}

		/// <exception cref="System.Exception"/>
		private FileStatus GetFileStatus(Path path)
		{
			try
			{
				return fs.GetFileStatus(path);
			}
			catch (FileNotFoundException)
			{
				return null;
			}
		}

		/*
		* In order to make this write atomic as a part of write we will first write
		* data to .tmp file and then rename it. Here we are assuming that rename is
		* atomic for underlying file system.
		*/
		/// <exception cref="System.Exception"/>
		protected internal virtual void WriteFile(Path outputPath, byte[] data, bool makeUnradableByAdmin
			)
		{
			Path tempPath = new Path(outputPath.GetParent(), outputPath.GetName() + ".tmp");
			FSDataOutputStream fsOut = null;
			// This file will be overwritten when app/attempt finishes for saving the
			// final status.
			try
			{
				fsOut = fs.Create(tempPath, true);
				if (makeUnradableByAdmin)
				{
					SetUnreadableBySuperuserXattrib(tempPath);
				}
				fsOut.Write(data);
				fsOut.Close();
				fsOut = null;
				fs.Rename(tempPath, outputPath);
			}
			finally
			{
				IOUtils.Cleanup(Log, fsOut);
			}
		}

		/*
		* In order to make this update atomic as a part of write we will first write
		* data to .new file and then rename it. Here we are assuming that rename is
		* atomic for underlying file system.
		*/
		/// <exception cref="System.Exception"/>
		protected internal virtual void UpdateFile(Path outputPath, byte[] data, bool makeUnradableByAdmin
			)
		{
			Path newPath = new Path(outputPath.GetParent(), outputPath.GetName() + ".new");
			// use writeFileWithRetries to make sure .new file is created atomically
			WriteFileWithRetries(newPath, data, makeUnradableByAdmin);
			ReplaceFile(newPath, outputPath);
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void ReplaceFile(Path srcPath, Path dstPath)
		{
			if (ExistsWithRetries(dstPath))
			{
				DeleteFileWithRetries(dstPath);
			}
			else
			{
				Log.Info("File doesn't exist. Skip deleting the file " + dstPath);
			}
			RenameFileWithRetries(srcPath, dstPath);
		}

		/// <exception cref="System.Exception"/>
		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal virtual bool RenameFile(Path src, Path dst)
		{
			return fs.Rename(src, dst);
		}

		/// <exception cref="System.Exception"/>
		private bool CreateFile(Path newFile)
		{
			return fs.CreateNewFile(newFile);
		}

		[InterfaceAudience.Private]
		[VisibleForTesting]
		internal virtual Path GetNodePath(Path root, string nodeName)
		{
			return new Path(root, nodeName);
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreOrUpdateAMRMTokenSecretManagerState(AMRMTokenSecretManagerState
			 amrmTokenSecretManagerState, bool isUpdate)
		{
			lock (this)
			{
				Path nodeCreatePath = GetNodePath(amrmTokenSecretManagerRoot, AmrmtokenSecretManagerNode
					);
				AMRMTokenSecretManagerState data = AMRMTokenSecretManagerState.NewInstance(amrmTokenSecretManagerState
					);
				byte[] stateData = data.GetProto().ToByteArray();
				if (isUpdate)
				{
					UpdateFile(nodeCreatePath, stateData, true);
				}
				else
				{
					WriteFileWithRetries(nodeCreatePath, stateData, true);
				}
			}
		}

		[VisibleForTesting]
		public virtual int GetNumRetries()
		{
			return fsNumRetries;
		}

		[VisibleForTesting]
		public virtual long GetRetryInterval()
		{
			return fsRetryInterval;
		}

		/// <exception cref="System.IO.IOException"/>
		private void SetUnreadableBySuperuserXattrib(Path p)
		{
			if (fs.GetScheme().ToLower().Contains("hdfs") && intermediateEncryptionEnabled &&
				 !fs.GetXAttrs(p).Contains(UnreadableBySuperuserXattrib))
			{
				fs.SetXAttr(p, UnreadableBySuperuserXattrib, null, EnumSet.Of(XAttrSetFlag.Create
					));
			}
		}
	}
}
