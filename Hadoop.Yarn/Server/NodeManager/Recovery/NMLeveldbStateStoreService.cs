using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Fusesource.Leveldbjni;
using Org.Fusesource.Leveldbjni.Internal;
using Org.Iq80.Leveldb;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery
{
	public class NMLeveldbStateStoreService : NMStateStoreService
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery.NMLeveldbStateStoreService
			));

		private const string DbName = "yarn-nm-state";

		private const string DbSchemaVersionKey = "nm-schema-version";

		private static readonly Version CurrentVersionInfo = Version.NewInstance(1, 0);

		private const string DeletionTaskKeyPrefix = "DeletionService/deltask_";

		private const string ApplicationsKeyPrefix = "ContainerManager/applications/";

		private const string FinishedAppsKeyPrefix = "ContainerManager/finishedApps/";

		private const string LocalizationKeyPrefix = "Localization/";

		private const string LocalizationPublicKeyPrefix = LocalizationKeyPrefix + "public/";

		private const string LocalizationPrivateKeyPrefix = LocalizationKeyPrefix + "private/";

		private const string LocalizationStartedSuffix = "started/";

		private const string LocalizationCompletedSuffix = "completed/";

		private const string LocalizationFilecacheSuffix = "filecache/";

		private const string LocalizationAppcacheSuffix = "appcache/";

		private const string ContainersKeyPrefix = "ContainerManager/containers/";

		private const string ContainerRequestKeySuffix = "/request";

		private const string ContainerDiagsKeySuffix = "/diagnostics";

		private const string ContainerLaunchedKeySuffix = "/launched";

		private const string ContainerKilledKeySuffix = "/killed";

		private const string ContainerExitCodeKeySuffix = "/exitcode";

		private const string CurrentMasterKeySuffix = "CurrentMasterKey";

		private const string PrevMasterKeySuffix = "PreviousMasterKey";

		private const string NmTokensKeyPrefix = "NMTokens/";

		private const string NmTokensCurrentMasterKey = NmTokensKeyPrefix + CurrentMasterKeySuffix;

		private const string NmTokensPrevMasterKey = NmTokensKeyPrefix + PrevMasterKeySuffix;

		private const string ContainerTokensKeyPrefix = "ContainerTokens/";

		private const string ContainerTokensCurrentMasterKey = ContainerTokensKeyPrefix +
			 CurrentMasterKeySuffix;

		private const string ContainerTokensPrevMasterKey = ContainerTokensKeyPrefix + PrevMasterKeySuffix;

		private const string LogDeleterKeyPrefix = "LogDeleters/";

		private static readonly byte[] EmptyValue = new byte[0];

		private DB db;

		private bool isNewlyCreated;

		public NMLeveldbStateStoreService()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery.NMLeveldbStateStoreService
				).FullName)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void StartStorage()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void CloseStorage()
		{
			if (db != null)
			{
				db.Close();
			}
		}

		public override bool IsNewlyCreated()
		{
			return isNewlyCreated;
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<NMStateStoreService.RecoveredContainerState> LoadContainersState
			()
		{
			AList<NMStateStoreService.RecoveredContainerState> containers = new AList<NMStateStoreService.RecoveredContainerState
				>();
			AList<ContainerId> containersToRemove = new AList<ContainerId>();
			LeveldbIterator iter = null;
			try
			{
				iter = new LeveldbIterator(db);
				iter.Seek(JniDBFactory.Bytes(ContainersKeyPrefix));
				while (iter.HasNext())
				{
					KeyValuePair<byte[], byte[]> entry = iter.PeekNext();
					string key = JniDBFactory.AsString(entry.Key);
					if (!key.StartsWith(ContainersKeyPrefix))
					{
						break;
					}
					int idEndPos = key.IndexOf('/', ContainersKeyPrefix.Length);
					if (idEndPos < 0)
					{
						throw new IOException("Unable to determine container in key: " + key);
					}
					ContainerId containerId = ConverterUtils.ToContainerId(Sharpen.Runtime.Substring(
						key, ContainersKeyPrefix.Length, idEndPos));
					string keyPrefix = Sharpen.Runtime.Substring(key, 0, idEndPos + 1);
					NMStateStoreService.RecoveredContainerState rcs = LoadContainerState(containerId, 
						iter, keyPrefix);
					// Don't load container without StartContainerRequest
					if (rcs.startRequest != null)
					{
						containers.AddItem(rcs);
					}
					else
					{
						containersToRemove.AddItem(containerId);
					}
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				if (iter != null)
				{
					iter.Close();
				}
			}
			// remove container without StartContainerRequest
			foreach (ContainerId containerId_1 in containersToRemove)
			{
				Log.Warn("Remove container " + containerId_1 + " with incomplete records");
				try
				{
					RemoveContainer(containerId_1);
				}
				catch (IOException e)
				{
					// TODO: kill and cleanup the leaked container
					Log.Error("Unable to remove container " + containerId_1 + " in store", e);
				}
			}
			return containers;
		}

		/// <exception cref="System.IO.IOException"/>
		private NMStateStoreService.RecoveredContainerState LoadContainerState(ContainerId
			 containerId, LeveldbIterator iter, string keyPrefix)
		{
			NMStateStoreService.RecoveredContainerState rcs = new NMStateStoreService.RecoveredContainerState
				();
			rcs.status = NMStateStoreService.RecoveredContainerStatus.Requested;
			while (iter.HasNext())
			{
				KeyValuePair<byte[], byte[]> entry = iter.PeekNext();
				string key = JniDBFactory.AsString(entry.Key);
				if (!key.StartsWith(keyPrefix))
				{
					break;
				}
				iter.Next();
				string suffix = Sharpen.Runtime.Substring(key, keyPrefix.Length - 1);
				// start with '/'
				if (suffix.Equals(ContainerRequestKeySuffix))
				{
					rcs.startRequest = new StartContainerRequestPBImpl(YarnServiceProtos.StartContainerRequestProto
						.ParseFrom(entry.Value));
				}
				else
				{
					if (suffix.Equals(ContainerDiagsKeySuffix))
					{
						rcs.diagnostics = JniDBFactory.AsString(entry.Value);
					}
					else
					{
						if (suffix.Equals(ContainerLaunchedKeySuffix))
						{
							if (rcs.status == NMStateStoreService.RecoveredContainerStatus.Requested)
							{
								rcs.status = NMStateStoreService.RecoveredContainerStatus.Launched;
							}
						}
						else
						{
							if (suffix.Equals(ContainerKilledKeySuffix))
							{
								rcs.killed = true;
							}
							else
							{
								if (suffix.Equals(ContainerExitCodeKeySuffix))
								{
									rcs.status = NMStateStoreService.RecoveredContainerStatus.Completed;
									rcs.exitCode = System.Convert.ToInt32(JniDBFactory.AsString(entry.Value));
								}
								else
								{
									throw new IOException("Unexpected container state key: " + key);
								}
							}
						}
					}
				}
			}
			return rcs;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainer(ContainerId containerId, StartContainerRequest
			 startRequest)
		{
			string key = ContainersKeyPrefix + containerId.ToString() + ContainerRequestKeySuffix;
			try
			{
				db.Put(JniDBFactory.Bytes(key), ((StartContainerRequestPBImpl)startRequest).GetProto
					().ToByteArray());
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerDiagnostics(ContainerId containerId, StringBuilder
			 diagnostics)
		{
			string key = ContainersKeyPrefix + containerId.ToString() + ContainerDiagsKeySuffix;
			try
			{
				db.Put(JniDBFactory.Bytes(key), JniDBFactory.Bytes(diagnostics.ToString()));
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerLaunched(ContainerId containerId)
		{
			string key = ContainersKeyPrefix + containerId.ToString() + ContainerLaunchedKeySuffix;
			try
			{
				db.Put(JniDBFactory.Bytes(key), EmptyValue);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerKilled(ContainerId containerId)
		{
			string key = ContainersKeyPrefix + containerId.ToString() + ContainerKilledKeySuffix;
			try
			{
				db.Put(JniDBFactory.Bytes(key), EmptyValue);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerCompleted(ContainerId containerId, int exitCode
			)
		{
			string key = ContainersKeyPrefix + containerId.ToString() + ContainerExitCodeKeySuffix;
			try
			{
				db.Put(JniDBFactory.Bytes(key), JniDBFactory.Bytes(Sharpen.Extensions.ToString(exitCode
					)));
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveContainer(ContainerId containerId)
		{
			string keyPrefix = ContainersKeyPrefix + containerId.ToString();
			try
			{
				WriteBatch batch = db.CreateWriteBatch();
				try
				{
					batch.Delete(JniDBFactory.Bytes(keyPrefix + ContainerRequestKeySuffix));
					batch.Delete(JniDBFactory.Bytes(keyPrefix + ContainerDiagsKeySuffix));
					batch.Delete(JniDBFactory.Bytes(keyPrefix + ContainerLaunchedKeySuffix));
					batch.Delete(JniDBFactory.Bytes(keyPrefix + ContainerKilledKeySuffix));
					batch.Delete(JniDBFactory.Bytes(keyPrefix + ContainerExitCodeKeySuffix));
					db.Write(batch);
				}
				finally
				{
					batch.Close();
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredApplicationsState LoadApplicationsState
			()
		{
			NMStateStoreService.RecoveredApplicationsState state = new NMStateStoreService.RecoveredApplicationsState
				();
			state.applications = new AList<YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto
				>();
			string keyPrefix = ApplicationsKeyPrefix;
			LeveldbIterator iter = null;
			try
			{
				iter = new LeveldbIterator(db);
				iter.Seek(JniDBFactory.Bytes(keyPrefix));
				while (iter.HasNext())
				{
					KeyValuePair<byte[], byte[]> entry = iter.Next();
					string key = JniDBFactory.AsString(entry.Key);
					if (!key.StartsWith(keyPrefix))
					{
						break;
					}
					state.applications.AddItem(YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto
						.ParseFrom(entry.Value));
				}
				state.finishedApplications = new AList<ApplicationId>();
				keyPrefix = FinishedAppsKeyPrefix;
				iter.Seek(JniDBFactory.Bytes(keyPrefix));
				while (iter.HasNext())
				{
					KeyValuePair<byte[], byte[]> entry = iter.Next();
					string key = JniDBFactory.AsString(entry.Key);
					if (!key.StartsWith(keyPrefix))
					{
						break;
					}
					ApplicationId appId = ConverterUtils.ToApplicationId(Sharpen.Runtime.Substring(key
						, keyPrefix.Length));
					state.finishedApplications.AddItem(appId);
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				if (iter != null)
				{
					iter.Close();
				}
			}
			return state;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreApplication(ApplicationId appId, YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto
			 p)
		{
			string key = ApplicationsKeyPrefix + appId;
			try
			{
				db.Put(JniDBFactory.Bytes(key), p.ToByteArray());
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreFinishedApplication(ApplicationId appId)
		{
			string key = FinishedAppsKeyPrefix + appId;
			try
			{
				db.Put(JniDBFactory.Bytes(key), new byte[0]);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveApplication(ApplicationId appId)
		{
			try
			{
				WriteBatch batch = db.CreateWriteBatch();
				try
				{
					string key = ApplicationsKeyPrefix + appId;
					batch.Delete(JniDBFactory.Bytes(key));
					key = FinishedAppsKeyPrefix + appId;
					batch.Delete(JniDBFactory.Bytes(key));
					db.Write(batch);
				}
				finally
				{
					batch.Close();
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredLocalizationState LoadLocalizationState
			()
		{
			NMStateStoreService.RecoveredLocalizationState state = new NMStateStoreService.RecoveredLocalizationState
				();
			LeveldbIterator iter = null;
			try
			{
				iter = new LeveldbIterator(db);
				iter.Seek(JniDBFactory.Bytes(LocalizationPublicKeyPrefix));
				state.publicTrackerState = LoadResourceTrackerState(iter, LocalizationPublicKeyPrefix
					);
				iter.Seek(JniDBFactory.Bytes(LocalizationPrivateKeyPrefix));
				while (iter.HasNext())
				{
					KeyValuePair<byte[], byte[]> entry = iter.PeekNext();
					string key = JniDBFactory.AsString(entry.Key);
					if (!key.StartsWith(LocalizationPrivateKeyPrefix))
					{
						break;
					}
					int userEndPos = key.IndexOf('/', LocalizationPrivateKeyPrefix.Length);
					if (userEndPos < 0)
					{
						throw new IOException("Unable to determine user in resource key: " + key);
					}
					string user = Sharpen.Runtime.Substring(key, LocalizationPrivateKeyPrefix.Length, 
						userEndPos);
					state.userResources[user] = LoadUserLocalizedResources(iter, Sharpen.Runtime.Substring
						(key, 0, userEndPos + 1));
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				if (iter != null)
				{
					iter.Close();
				}
			}
			return state;
		}

		/// <exception cref="System.IO.IOException"/>
		private NMStateStoreService.LocalResourceTrackerState LoadResourceTrackerState(LeveldbIterator
			 iter, string keyPrefix)
		{
			string completedPrefix = keyPrefix + LocalizationCompletedSuffix;
			string startedPrefix = keyPrefix + LocalizationStartedSuffix;
			NMStateStoreService.LocalResourceTrackerState state = new NMStateStoreService.LocalResourceTrackerState
				();
			while (iter.HasNext())
			{
				KeyValuePair<byte[], byte[]> entry = iter.PeekNext();
				string key = JniDBFactory.AsString(entry.Key);
				if (!key.StartsWith(keyPrefix))
				{
					break;
				}
				if (key.StartsWith(completedPrefix))
				{
					state.localizedResources = LoadCompletedResources(iter, completedPrefix);
				}
				else
				{
					if (key.StartsWith(startedPrefix))
					{
						state.inProgressResources = LoadStartedResources(iter, startedPrefix);
					}
					else
					{
						throw new IOException("Unexpected key in resource tracker state: " + key);
					}
				}
			}
			return state;
		}

		/// <exception cref="System.IO.IOException"/>
		private IList<YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto> LoadCompletedResources
			(LeveldbIterator iter, string keyPrefix)
		{
			IList<YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto> rsrcs = new AList
				<YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto>();
			while (iter.HasNext())
			{
				KeyValuePair<byte[], byte[]> entry = iter.PeekNext();
				string key = JniDBFactory.AsString(entry.Key);
				if (!key.StartsWith(keyPrefix))
				{
					break;
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Loading completed resource from " + key);
				}
				rsrcs.AddItem(YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto.ParseFrom
					(entry.Value));
				iter.Next();
			}
			return rsrcs;
		}

		/// <exception cref="System.IO.IOException"/>
		private IDictionary<YarnProtos.LocalResourceProto, Path> LoadStartedResources(LeveldbIterator
			 iter, string keyPrefix)
		{
			IDictionary<YarnProtos.LocalResourceProto, Path> rsrcs = new Dictionary<YarnProtos.LocalResourceProto
				, Path>();
			while (iter.HasNext())
			{
				KeyValuePair<byte[], byte[]> entry = iter.PeekNext();
				string key = JniDBFactory.AsString(entry.Key);
				if (!key.StartsWith(keyPrefix))
				{
					break;
				}
				Path localPath = new Path(Sharpen.Runtime.Substring(key, keyPrefix.Length));
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Loading in-progress resource at " + localPath);
				}
				rsrcs[YarnProtos.LocalResourceProto.ParseFrom(entry.Value)] = localPath;
				iter.Next();
			}
			return rsrcs;
		}

		/// <exception cref="System.IO.IOException"/>
		private NMStateStoreService.RecoveredUserResources LoadUserLocalizedResources(LeveldbIterator
			 iter, string keyPrefix)
		{
			NMStateStoreService.RecoveredUserResources userResources = new NMStateStoreService.RecoveredUserResources
				();
			while (iter.HasNext())
			{
				KeyValuePair<byte[], byte[]> entry = iter.PeekNext();
				string key = JniDBFactory.AsString(entry.Key);
				if (!key.StartsWith(keyPrefix))
				{
					break;
				}
				if (key.StartsWith(LocalizationFilecacheSuffix, keyPrefix.Length))
				{
					userResources.privateTrackerState = LoadResourceTrackerState(iter, keyPrefix + LocalizationFilecacheSuffix
						);
				}
				else
				{
					if (key.StartsWith(LocalizationAppcacheSuffix, keyPrefix.Length))
					{
						int appIdStartPos = keyPrefix.Length + LocalizationAppcacheSuffix.Length;
						int appIdEndPos = key.IndexOf('/', appIdStartPos);
						if (appIdEndPos < 0)
						{
							throw new IOException("Unable to determine appID in resource key: " + key);
						}
						ApplicationId appId = ConverterUtils.ToApplicationId(Sharpen.Runtime.Substring(key
							, appIdStartPos, appIdEndPos));
						userResources.appTrackerStates[appId] = LoadResourceTrackerState(iter, Sharpen.Runtime.Substring
							(key, 0, appIdEndPos + 1));
					}
					else
					{
						throw new IOException("Unexpected user resource key " + key);
					}
				}
			}
			return userResources;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StartResourceLocalization(string user, ApplicationId appId, 
			YarnProtos.LocalResourceProto proto, Path localPath)
		{
			string key = GetResourceStartedKey(user, appId, localPath.ToString());
			try
			{
				db.Put(JniDBFactory.Bytes(key), proto.ToByteArray());
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void FinishResourceLocalization(string user, ApplicationId appId, 
			YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto proto)
		{
			string localPath = proto.GetLocalPath();
			string startedKey = GetResourceStartedKey(user, appId, localPath);
			string completedKey = GetResourceCompletedKey(user, appId, localPath);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Storing localized resource to " + completedKey);
			}
			try
			{
				WriteBatch batch = db.CreateWriteBatch();
				try
				{
					batch.Delete(JniDBFactory.Bytes(startedKey));
					batch.Put(JniDBFactory.Bytes(completedKey), proto.ToByteArray());
					db.Write(batch);
				}
				finally
				{
					batch.Close();
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveLocalizedResource(string user, ApplicationId appId, Path
			 localPath)
		{
			string localPathStr = localPath.ToString();
			string startedKey = GetResourceStartedKey(user, appId, localPathStr);
			string completedKey = GetResourceCompletedKey(user, appId, localPathStr);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Removing local resource at " + localPathStr);
			}
			try
			{
				WriteBatch batch = db.CreateWriteBatch();
				try
				{
					batch.Delete(JniDBFactory.Bytes(startedKey));
					batch.Delete(JniDBFactory.Bytes(completedKey));
					db.Write(batch);
				}
				finally
				{
					batch.Close();
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		private string GetResourceStartedKey(string user, ApplicationId appId, string localPath
			)
		{
			return GetResourceTrackerKeyPrefix(user, appId) + LocalizationStartedSuffix + localPath;
		}

		private string GetResourceCompletedKey(string user, ApplicationId appId, string localPath
			)
		{
			return GetResourceTrackerKeyPrefix(user, appId) + LocalizationCompletedSuffix + localPath;
		}

		private string GetResourceTrackerKeyPrefix(string user, ApplicationId appId)
		{
			if (user == null)
			{
				return LocalizationPublicKeyPrefix;
			}
			if (appId == null)
			{
				return LocalizationPrivateKeyPrefix + user + "/" + LocalizationFilecacheSuffix;
			}
			return LocalizationPrivateKeyPrefix + user + "/" + LocalizationAppcacheSuffix + appId
				 + "/";
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredDeletionServiceState LoadDeletionServiceState
			()
		{
			NMStateStoreService.RecoveredDeletionServiceState state = new NMStateStoreService.RecoveredDeletionServiceState
				();
			state.tasks = new AList<YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
				>();
			LeveldbIterator iter = null;
			try
			{
				iter = new LeveldbIterator(db);
				iter.Seek(JniDBFactory.Bytes(DeletionTaskKeyPrefix));
				while (iter.HasNext())
				{
					KeyValuePair<byte[], byte[]> entry = iter.Next();
					string key = JniDBFactory.AsString(entry.Key);
					if (!key.StartsWith(DeletionTaskKeyPrefix))
					{
						break;
					}
					state.tasks.AddItem(YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
						.ParseFrom(entry.Value));
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				if (iter != null)
				{
					iter.Close();
				}
			}
			return state;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreDeletionTask(int taskId, YarnServerNodemanagerRecoveryProtos.DeletionServiceDeleteTaskProto
			 taskProto)
		{
			string key = DeletionTaskKeyPrefix + taskId;
			try
			{
				db.Put(JniDBFactory.Bytes(key), taskProto.ToByteArray());
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveDeletionTask(int taskId)
		{
			string key = DeletionTaskKeyPrefix + taskId;
			try
			{
				db.Delete(JniDBFactory.Bytes(key));
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredNMTokensState LoadNMTokensState()
		{
			NMStateStoreService.RecoveredNMTokensState state = new NMStateStoreService.RecoveredNMTokensState
				();
			state.applicationMasterKeys = new Dictionary<ApplicationAttemptId, MasterKey>();
			LeveldbIterator iter = null;
			try
			{
				iter = new LeveldbIterator(db);
				iter.Seek(JniDBFactory.Bytes(NmTokensKeyPrefix));
				while (iter.HasNext())
				{
					KeyValuePair<byte[], byte[]> entry = iter.Next();
					string fullKey = JniDBFactory.AsString(entry.Key);
					if (!fullKey.StartsWith(NmTokensKeyPrefix))
					{
						break;
					}
					string key = Sharpen.Runtime.Substring(fullKey, NmTokensKeyPrefix.Length);
					if (key.Equals(CurrentMasterKeySuffix))
					{
						state.currentMasterKey = ParseMasterKey(entry.Value);
					}
					else
					{
						if (key.Equals(PrevMasterKeySuffix))
						{
							state.previousMasterKey = ParseMasterKey(entry.Value);
						}
						else
						{
							if (key.StartsWith(ApplicationAttemptId.appAttemptIdStrPrefix))
							{
								ApplicationAttemptId attempt;
								try
								{
									attempt = ConverterUtils.ToApplicationAttemptId(key);
								}
								catch (ArgumentException e)
								{
									throw new IOException("Bad application master key state for " + fullKey, e);
								}
								state.applicationMasterKeys[attempt] = ParseMasterKey(entry.Value);
							}
						}
					}
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				if (iter != null)
				{
					iter.Close();
				}
			}
			return state;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreNMTokenCurrentMasterKey(MasterKey key)
		{
			StoreMasterKey(NmTokensCurrentMasterKey, key);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreNMTokenPreviousMasterKey(MasterKey key)
		{
			StoreMasterKey(NmTokensPrevMasterKey, key);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreNMTokenApplicationMasterKey(ApplicationAttemptId attempt
			, MasterKey key)
		{
			StoreMasterKey(NmTokensKeyPrefix + attempt, key);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveNMTokenApplicationMasterKey(ApplicationAttemptId attempt
			)
		{
			string key = NmTokensKeyPrefix + attempt;
			try
			{
				db.Delete(JniDBFactory.Bytes(key));
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private MasterKey ParseMasterKey(byte[] keyData)
		{
			return new MasterKeyPBImpl(YarnServerCommonProtos.MasterKeyProto.ParseFrom(keyData
				));
		}

		/// <exception cref="System.IO.IOException"/>
		private void StoreMasterKey(string dbKey, MasterKey key)
		{
			MasterKeyPBImpl pb = (MasterKeyPBImpl)key;
			try
			{
				db.Put(JniDBFactory.Bytes(dbKey), pb.GetProto().ToByteArray());
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredContainerTokensState LoadContainerTokensState
			()
		{
			NMStateStoreService.RecoveredContainerTokensState state = new NMStateStoreService.RecoveredContainerTokensState
				();
			state.activeTokens = new Dictionary<ContainerId, long>();
			LeveldbIterator iter = null;
			try
			{
				iter = new LeveldbIterator(db);
				iter.Seek(JniDBFactory.Bytes(ContainerTokensKeyPrefix));
				int containerTokensKeyPrefixLength = ContainerTokensKeyPrefix.Length;
				while (iter.HasNext())
				{
					KeyValuePair<byte[], byte[]> entry = iter.Next();
					string fullKey = JniDBFactory.AsString(entry.Key);
					if (!fullKey.StartsWith(ContainerTokensKeyPrefix))
					{
						break;
					}
					string key = Sharpen.Runtime.Substring(fullKey, containerTokensKeyPrefixLength);
					if (key.Equals(CurrentMasterKeySuffix))
					{
						state.currentMasterKey = ParseMasterKey(entry.Value);
					}
					else
					{
						if (key.Equals(PrevMasterKeySuffix))
						{
							state.previousMasterKey = ParseMasterKey(entry.Value);
						}
						else
						{
							if (key.StartsWith(ConverterUtils.ContainerPrefix))
							{
								LoadContainerToken(state, fullKey, key, entry.Value);
							}
						}
					}
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				if (iter != null)
				{
					iter.Close();
				}
			}
			return state;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void LoadContainerToken(NMStateStoreService.RecoveredContainerTokensState
			 state, string key, string containerIdStr, byte[] value)
		{
			ContainerId containerId;
			long expTime;
			try
			{
				containerId = ConverterUtils.ToContainerId(containerIdStr);
				expTime = long.Parse(JniDBFactory.AsString(value));
			}
			catch (ArgumentException e)
			{
				throw new IOException("Bad container token state for " + key, e);
			}
			state.activeTokens[containerId] = expTime;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerTokenCurrentMasterKey(MasterKey key)
		{
			StoreMasterKey(ContainerTokensCurrentMasterKey, key);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerTokenPreviousMasterKey(MasterKey key)
		{
			StoreMasterKey(ContainerTokensPrevMasterKey, key);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreContainerToken(ContainerId containerId, long expTime)
		{
			string key = ContainerTokensKeyPrefix + containerId;
			try
			{
				db.Put(JniDBFactory.Bytes(key), JniDBFactory.Bytes(expTime.ToString()));
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveContainerToken(ContainerId containerId)
		{
			string key = ContainerTokensKeyPrefix + containerId;
			try
			{
				db.Delete(JniDBFactory.Bytes(key));
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override NMStateStoreService.RecoveredLogDeleterState LoadLogDeleterState(
			)
		{
			NMStateStoreService.RecoveredLogDeleterState state = new NMStateStoreService.RecoveredLogDeleterState
				();
			state.logDeleterMap = new Dictionary<ApplicationId, YarnServerNodemanagerRecoveryProtos.LogDeleterProto
				>();
			LeveldbIterator iter = null;
			try
			{
				iter = new LeveldbIterator(db);
				iter.Seek(JniDBFactory.Bytes(LogDeleterKeyPrefix));
				int logDeleterKeyPrefixLength = LogDeleterKeyPrefix.Length;
				while (iter.HasNext())
				{
					KeyValuePair<byte[], byte[]> entry = iter.Next();
					string fullKey = JniDBFactory.AsString(entry.Key);
					if (!fullKey.StartsWith(LogDeleterKeyPrefix))
					{
						break;
					}
					string appIdStr = Sharpen.Runtime.Substring(fullKey, logDeleterKeyPrefixLength);
					ApplicationId appId = null;
					try
					{
						appId = ConverterUtils.ToApplicationId(appIdStr);
					}
					catch (ArgumentException)
					{
						Log.Warn("Skipping unknown log deleter key " + fullKey);
						continue;
					}
					YarnServerNodemanagerRecoveryProtos.LogDeleterProto proto = YarnServerNodemanagerRecoveryProtos.LogDeleterProto
						.ParseFrom(entry.Value);
					state.logDeleterMap[appId] = proto;
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				if (iter != null)
				{
					iter.Close();
				}
			}
			return state;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreLogDeleter(ApplicationId appId, YarnServerNodemanagerRecoveryProtos.LogDeleterProto
			 proto)
		{
			string key = GetLogDeleterKey(appId);
			try
			{
				db.Put(JniDBFactory.Bytes(key), proto.ToByteArray());
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveLogDeleter(ApplicationId appId)
		{
			string key = GetLogDeleterKey(appId);
			try
			{
				db.Delete(JniDBFactory.Bytes(key));
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		private string GetLogDeleterKey(ApplicationId appId)
		{
			return LogDeleterKeyPrefix + appId;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void InitStorage(Configuration conf)
		{
			Path storeRoot = CreateStorageDir(conf);
			Options options = new Options();
			options.CreateIfMissing(false);
			options.Logger(new NMLeveldbStateStoreService.LeveldbLogger());
			Log.Info("Using state database at " + storeRoot + " for recovery");
			FilePath dbfile = new FilePath(storeRoot.ToString());
			try
			{
				db = JniDBFactory.factory.Open(dbfile, options);
			}
			catch (NativeDB.DBException e)
			{
				if (e.IsNotFound() || e.Message.Contains(" does not exist "))
				{
					Log.Info("Creating state database at " + dbfile);
					isNewlyCreated = true;
					options.CreateIfMissing(true);
					try
					{
						db = JniDBFactory.factory.Open(dbfile, options);
						// store version
						StoreVersion();
					}
					catch (DBException dbErr)
					{
						throw new IOException(dbErr.Message, dbErr);
					}
				}
				else
				{
					throw;
				}
			}
			CheckVersion();
		}

		/// <exception cref="System.IO.IOException"/>
		private Path CreateStorageDir(Configuration conf)
		{
			string storeUri = conf.Get(YarnConfiguration.NmRecoveryDir);
			if (storeUri == null)
			{
				throw new IOException("No store location directory configured in " + YarnConfiguration
					.NmRecoveryDir);
			}
			Path root = new Path(storeUri, DbName);
			FileSystem fs = FileSystem.GetLocal(conf);
			fs.Mkdirs(root, new FsPermission((short)0x1c0));
			return root;
		}

		private class LeveldbLogger : Logger
		{
			private static readonly Org.Apache.Commons.Logging.Log Log = LogFactory.GetLog(typeof(
				NMLeveldbStateStoreService.LeveldbLogger));

			public virtual void Log(string message)
			{
				Log.Info(message);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual Version LoadVersion()
		{
			byte[] data = db.Get(JniDBFactory.Bytes(DbSchemaVersionKey));
			// if version is not stored previously, treat it as CURRENT_VERSION_INFO.
			if (data == null || data.Length == 0)
			{
				return GetCurrentVersion();
			}
			Version version = new VersionPBImpl(YarnServerCommonProtos.VersionProto.ParseFrom
				(data));
			return version;
		}

		/// <exception cref="System.IO.IOException"/>
		private void StoreVersion()
		{
			DbStoreVersion(CurrentVersionInfo);
		}

		// Only used for test
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void StoreVersion(Version state)
		{
			DbStoreVersion(state);
		}

		/// <exception cref="System.IO.IOException"/>
		private void DbStoreVersion(Version state)
		{
			string key = DbSchemaVersionKey;
			byte[] data = ((VersionPBImpl)state).GetProto().ToByteArray();
			try
			{
				db.Put(JniDBFactory.Bytes(key), data);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		internal virtual Version GetCurrentVersion()
		{
			return CurrentVersionInfo;
		}

		/// <summary>1) Versioning scheme: major.minor.</summary>
		/// <remarks>
		/// 1) Versioning scheme: major.minor. For e.g. 1.0, 1.1, 1.2...1.25, 2.0 etc.
		/// 2) Any incompatible change of state-store is a major upgrade, and any
		/// compatible change of state-store is a minor upgrade.
		/// 3) Within a minor upgrade, say 1.1 to 1.2:
		/// overwrite the version info and proceed as normal.
		/// 4) Within a major upgrade, say 1.2 to 2.0:
		/// throw exception and indicate user to use a separate upgrade tool to
		/// upgrade NM state or remove incompatible old state.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void CheckVersion()
		{
			Version loadedVersion = LoadVersion();
			Log.Info("Loaded NM state version info " + loadedVersion);
			if (loadedVersion.Equals(GetCurrentVersion()))
			{
				return;
			}
			if (loadedVersion.IsCompatibleTo(GetCurrentVersion()))
			{
				Log.Info("Storing NM state version info " + GetCurrentVersion());
				StoreVersion();
			}
			else
			{
				throw new IOException("Incompatible version for NM state: expecting NM state version "
					 + GetCurrentVersion() + ", but loading version " + loadedVersion);
			}
		}
	}
}
