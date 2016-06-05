using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Fusesource.Leveldbjni;
using Org.Fusesource.Leveldbjni.Internal;
using Org.Iq80.Leveldb;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class LeveldbRMStateStore : RMStateStore
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(LeveldbRMStateStore));

		private const string Separator = "/";

		private const string DbName = "yarn-rm-state";

		private const string RmDtMasterKeyKeyPrefix = RmDtSecretManagerRoot + Separator +
			 DelegationKeyPrefix;

		private const string RmDtTokenKeyPrefix = RmDtSecretManagerRoot + Separator + DelegationTokenPrefix;

		private const string RmDtSequenceNumberKey = RmDtSecretManagerRoot + Separator + 
			"RMDTSequentialNumber";

		private const string RmAppKeyPrefix = RmAppRoot + Separator + ApplicationId.appIdStrPrefix;

		private static readonly Version CurrentVersionInfo = Version.NewInstance(1, 0);

		private DB db;

		private string GetApplicationNodeKey(ApplicationId appId)
		{
			return RmAppRoot + Separator + appId;
		}

		private string GetApplicationAttemptNodeKey(ApplicationAttemptId attemptId)
		{
			return GetApplicationAttemptNodeKey(GetApplicationNodeKey(attemptId.GetApplicationId
				()), attemptId);
		}

		private string GetApplicationAttemptNodeKey(string appNodeKey, ApplicationAttemptId
			 attemptId)
		{
			return appNodeKey + Separator + attemptId;
		}

		private string GetRMDTMasterKeyNodeKey(DelegationKey masterKey)
		{
			return RmDtMasterKeyKeyPrefix + masterKey.GetKeyId();
		}

		private string GetRMDTTokenNodeKey(RMDelegationTokenIdentifier tokenId)
		{
			return RmDtTokenKeyPrefix + tokenId.GetSequenceNumber();
		}

		/// <exception cref="System.Exception"/>
		protected internal override void InitInternal(Configuration conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		private Path GetStorageDir()
		{
			Configuration conf = GetConfig();
			string storePath = conf.Get(YarnConfiguration.RmLeveldbStorePath);
			if (storePath == null)
			{
				throw new IOException("No store location directory configured in " + YarnConfiguration
					.RmLeveldbStorePath);
			}
			return new Path(storePath, DbName);
		}

		/// <exception cref="System.IO.IOException"/>
		private Path CreateStorageDir()
		{
			Path root = GetStorageDir();
			FileSystem fs = FileSystem.GetLocal(GetConfig());
			fs.Mkdirs(root, new FsPermission((short)0x1c0));
			return root;
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StartInternal()
		{
			Path storeRoot = CreateStorageDir();
			Options options = new Options();
			options.CreateIfMissing(false);
			options.Logger(new LeveldbRMStateStore.LeveldbLogger());
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
		}

		/// <exception cref="System.Exception"/>
		protected internal override void CloseInternal()
		{
			if (db != null)
			{
				db.Close();
				db = null;
			}
		}

		[VisibleForTesting]
		internal virtual bool IsClosed()
		{
			return db == null;
		}

		/// <exception cref="System.Exception"/>
		protected internal override Version LoadVersion()
		{
			Version version = null;
			try
			{
				byte[] data = db.Get(JniDBFactory.Bytes(VersionNode));
				if (data != null)
				{
					version = new VersionPBImpl(YarnServerCommonProtos.VersionProto.ParseFrom(data));
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			return version;
		}

		/// <exception cref="System.Exception"/>
		protected internal override void StoreVersion()
		{
			DbStoreVersion(CurrentVersionInfo);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void DbStoreVersion(Version state)
		{
			string key = VersionNode;
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

		protected internal override Version GetCurrentVersion()
		{
			return CurrentVersionInfo;
		}

		/// <exception cref="System.Exception"/>
		public override long GetAndIncrementEpoch()
		{
			lock (this)
			{
				long currentEpoch = 0;
				byte[] dbKeyBytes = JniDBFactory.Bytes(EpochNode);
				try
				{
					byte[] data = db.Get(dbKeyBytes);
					if (data != null)
					{
						currentEpoch = YarnServerResourceManagerRecoveryProtos.EpochProto.ParseFrom(data)
							.GetEpoch();
					}
					YarnServerResourceManagerRecoveryProtos.EpochProto proto = Epoch.NewInstance(currentEpoch
						 + 1).GetProto();
					db.Put(dbKeyBytes, proto.ToByteArray());
				}
				catch (DBException e)
				{
					throw new IOException(e);
				}
				return currentEpoch;
			}
		}

		/// <exception cref="System.Exception"/>
		public override RMStateStore.RMState LoadState()
		{
			RMStateStore.RMState rmState = new RMStateStore.RMState();
			LoadRMDTSecretManagerState(rmState);
			LoadRMApps(rmState);
			LoadAMRMTokenSecretManagerState(rmState);
			return rmState;
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadRMDTSecretManagerState(RMStateStore.RMState state)
		{
			int numKeys = LoadRMDTSecretManagerKeys(state);
			Log.Info("Recovered " + numKeys + " RM delegation token master keys");
			int numTokens = LoadRMDTSecretManagerTokens(state);
			Log.Info("Recovered " + numTokens + " RM delegation tokens");
			LoadRMDTSecretManagerTokenSequenceNumber(state);
		}

		/// <exception cref="System.IO.IOException"/>
		private int LoadRMDTSecretManagerKeys(RMStateStore.RMState state)
		{
			int numKeys = 0;
			LeveldbIterator iter = null;
			try
			{
				iter = new LeveldbIterator(db);
				iter.Seek(JniDBFactory.Bytes(RmDtMasterKeyKeyPrefix));
				while (iter.HasNext())
				{
					KeyValuePair<byte[], byte[]> entry = iter.Next();
					string key = JniDBFactory.AsString(entry.Key);
					if (!key.StartsWith(RmDtMasterKeyKeyPrefix))
					{
						break;
					}
					DelegationKey masterKey = LoadDelegationKey(entry.Value);
					state.rmSecretManagerState.masterKeyState.AddItem(masterKey);
					++numKeys;
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Loaded RM delegation key from " + key + ": keyId=" + masterKey.GetKeyId
							() + ", expirationDate=" + masterKey.GetExpiryDate());
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
			return numKeys;
		}

		/// <exception cref="System.IO.IOException"/>
		private DelegationKey LoadDelegationKey(byte[] data)
		{
			DelegationKey key = new DelegationKey();
			DataInputStream @in = new DataInputStream(new ByteArrayInputStream(data));
			try
			{
				key.ReadFields(@in);
			}
			finally
			{
				IOUtils.Cleanup(Log, @in);
			}
			return key;
		}

		/// <exception cref="System.IO.IOException"/>
		private int LoadRMDTSecretManagerTokens(RMStateStore.RMState state)
		{
			int numTokens = 0;
			LeveldbIterator iter = null;
			try
			{
				iter = new LeveldbIterator(db);
				iter.Seek(JniDBFactory.Bytes(RmDtTokenKeyPrefix));
				while (iter.HasNext())
				{
					KeyValuePair<byte[], byte[]> entry = iter.Next();
					string key = JniDBFactory.AsString(entry.Key);
					if (!key.StartsWith(RmDtTokenKeyPrefix))
					{
						break;
					}
					RMDelegationTokenIdentifierData tokenData = LoadDelegationToken(entry.Value);
					RMDelegationTokenIdentifier tokenId = tokenData.GetTokenIdentifier();
					long renewDate = tokenData.GetRenewDate();
					state.rmSecretManagerState.delegationTokenState[tokenId] = renewDate;
					++numTokens;
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Loaded RM delegation token from " + key + ": tokenId=" + tokenId + ", renewDate="
							 + renewDate);
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
			return numTokens;
		}

		/// <exception cref="System.IO.IOException"/>
		private RMDelegationTokenIdentifierData LoadDelegationToken(byte[] data)
		{
			RMDelegationTokenIdentifierData tokenData = new RMDelegationTokenIdentifierData();
			DataInputStream @in = new DataInputStream(new ByteArrayInputStream(data));
			try
			{
				tokenData.ReadFields(@in);
			}
			finally
			{
				IOUtils.Cleanup(Log, @in);
			}
			return tokenData;
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadRMDTSecretManagerTokenSequenceNumber(RMStateStore.RMState state)
		{
			byte[] data = null;
			try
			{
				data = db.Get(JniDBFactory.Bytes(RmDtSequenceNumberKey));
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			if (data != null)
			{
				DataInputStream @in = new DataInputStream(new ByteArrayInputStream(data));
				try
				{
					state.rmSecretManagerState.dtSequenceNumber = @in.ReadInt();
				}
				finally
				{
					IOUtils.Cleanup(Log, @in);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadRMApps(RMStateStore.RMState state)
		{
			int numApps = 0;
			int numAppAttempts = 0;
			LeveldbIterator iter = null;
			try
			{
				iter = new LeveldbIterator(db);
				iter.Seek(JniDBFactory.Bytes(RmAppKeyPrefix));
				while (iter.HasNext())
				{
					KeyValuePair<byte[], byte[]> entry = iter.Next();
					string key = JniDBFactory.AsString(entry.Key);
					if (!key.StartsWith(RmAppKeyPrefix))
					{
						break;
					}
					string appIdStr = Sharpen.Runtime.Substring(key, RmAppRoot.Length + 1);
					if (appIdStr.Contains(Separator))
					{
						Log.Warn("Skipping extraneous data " + key);
						continue;
					}
					numAppAttempts += LoadRMApp(state, iter, appIdStr, entry.Value);
					++numApps;
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
			Log.Info("Recovered " + numApps + " applications and " + numAppAttempts + " application attempts"
				);
		}

		/// <exception cref="System.IO.IOException"/>
		private int LoadRMApp(RMStateStore.RMState rmState, LeveldbIterator iter, string 
			appIdStr, byte[] appData)
		{
			ApplicationStateData appState = CreateApplicationState(appIdStr, appData);
			ApplicationId appId = appState.GetApplicationSubmissionContext().GetApplicationId
				();
			rmState.appState[appId] = appState;
			string attemptNodePrefix = GetApplicationNodeKey(appId) + Separator;
			while (iter.HasNext())
			{
				KeyValuePair<byte[], byte[]> entry = iter.PeekNext();
				string key = JniDBFactory.AsString(entry.Key);
				if (!key.StartsWith(attemptNodePrefix))
				{
					break;
				}
				string attemptId = Sharpen.Runtime.Substring(key, attemptNodePrefix.Length);
				if (attemptId.StartsWith(ApplicationAttemptId.appAttemptIdStrPrefix))
				{
					ApplicationAttemptStateData attemptState = CreateAttemptState(attemptId, entry.Value
						);
					appState.attempts[attemptState.GetAttemptId()] = attemptState;
				}
				else
				{
					Log.Warn("Ignoring unknown application key: " + key);
				}
				iter.Next();
			}
			int numAttempts = appState.attempts.Count;
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Loaded application " + appId + " with " + numAttempts + " attempts");
			}
			return numAttempts;
		}

		/// <exception cref="System.IO.IOException"/>
		private ApplicationStateData CreateApplicationState(string appIdStr, byte[] data)
		{
			ApplicationId appId = ConverterUtils.ToApplicationId(appIdStr);
			ApplicationStateDataPBImpl appState = new ApplicationStateDataPBImpl(YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto
				.ParseFrom(data));
			if (!appId.Equals(appState.GetApplicationSubmissionContext().GetApplicationId()))
			{
				throw new YarnRuntimeException("The database entry for " + appId + " contains data for "
					 + appState.GetApplicationSubmissionContext().GetApplicationId());
			}
			return appState;
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual ApplicationStateData LoadRMAppState(ApplicationId appId)
		{
			string appKey = GetApplicationNodeKey(appId);
			byte[] data = null;
			try
			{
				data = db.Get(JniDBFactory.Bytes(appKey));
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			if (data == null)
			{
				return null;
			}
			return CreateApplicationState(appId.ToString(), data);
		}

		/// <exception cref="System.IO.IOException"/>
		private ApplicationAttemptStateData CreateAttemptState(string itemName, byte[] data
			)
		{
			ApplicationAttemptId attemptId = ConverterUtils.ToApplicationAttemptId(itemName);
			ApplicationAttemptStateDataPBImpl attemptState = new ApplicationAttemptStateDataPBImpl
				(YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto.ParseFrom
				(data));
			if (!attemptId.Equals(attemptState.GetAttemptId()))
			{
				throw new YarnRuntimeException("The database entry for " + attemptId + " contains data for "
					 + attemptState.GetAttemptId());
			}
			return attemptState;
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadAMRMTokenSecretManagerState(RMStateStore.RMState rmState)
		{
			try
			{
				byte[] data = db.Get(JniDBFactory.Bytes(AmrmtokenSecretManagerRoot));
				if (data != null)
				{
					AMRMTokenSecretManagerStatePBImpl stateData = new AMRMTokenSecretManagerStatePBImpl
						(YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto.ParseFrom
						(data));
					rmState.amrmTokenSecretManagerState = AMRMTokenSecretManagerState.NewInstance(stateData
						.GetCurrentMasterKey(), stateData.GetNextMasterKey());
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void StoreApplicationStateInternal(ApplicationId appId
			, ApplicationStateData appStateData)
		{
			string key = GetApplicationNodeKey(appId);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Storing state for app " + appId + " at " + key);
			}
			try
			{
				db.Put(JniDBFactory.Bytes(key), appStateData.GetProto().ToByteArray());
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void UpdateApplicationStateInternal(ApplicationId appId
			, ApplicationStateData appStateData)
		{
			StoreApplicationStateInternal(appId, appStateData);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void StoreApplicationAttemptStateInternal(ApplicationAttemptId
			 attemptId, ApplicationAttemptStateData attemptStateData)
		{
			string key = GetApplicationAttemptNodeKey(attemptId);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Storing state for attempt " + attemptId + " at " + key);
			}
			try
			{
				db.Put(JniDBFactory.Bytes(key), attemptStateData.GetProto().ToByteArray());
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void UpdateApplicationAttemptStateInternal(ApplicationAttemptId
			 attemptId, ApplicationAttemptStateData attemptStateData)
		{
			StoreApplicationAttemptStateInternal(attemptId, attemptStateData);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void RemoveApplicationStateInternal(ApplicationStateData
			 appState)
		{
			ApplicationId appId = appState.GetApplicationSubmissionContext().GetApplicationId
				();
			string appKey = GetApplicationNodeKey(appId);
			try
			{
				WriteBatch batch = db.CreateWriteBatch();
				try
				{
					batch.Delete(JniDBFactory.Bytes(appKey));
					foreach (ApplicationAttemptId attemptId in appState.attempts.Keys)
					{
						string attemptKey = GetApplicationAttemptNodeKey(appKey, attemptId);
						batch.Delete(JniDBFactory.Bytes(attemptKey));
					}
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Removing state for app " + appId + " and " + appState.attempts.Count +
							 " attempts" + " at " + appKey);
					}
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
		private void StoreOrUpdateRMDT(RMDelegationTokenIdentifier tokenId, long renewDate
			, bool isUpdate)
		{
			string tokenKey = GetRMDTTokenNodeKey(tokenId);
			RMDelegationTokenIdentifierData tokenData = new RMDelegationTokenIdentifierData(tokenId
				, renewDate);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Storing token to " + tokenKey);
			}
			try
			{
				WriteBatch batch = db.CreateWriteBatch();
				try
				{
					batch.Put(JniDBFactory.Bytes(tokenKey), tokenData.ToByteArray());
					if (!isUpdate)
					{
						ByteArrayOutputStream bs = new ByteArrayOutputStream();
						using (DataOutputStream ds = new DataOutputStream(bs))
						{
							ds.WriteInt(tokenId.GetSequenceNumber());
						}
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Storing " + tokenId.GetSequenceNumber() + " to " + RmDtSequenceNumberKey
								);
						}
						batch.Put(JniDBFactory.Bytes(RmDtSequenceNumberKey), bs.ToByteArray());
					}
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
		protected internal override void StoreRMDelegationTokenState(RMDelegationTokenIdentifier
			 tokenId, long renewDate)
		{
			StoreOrUpdateRMDT(tokenId, renewDate, false);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void UpdateRMDelegationTokenState(RMDelegationTokenIdentifier
			 tokenId, long renewDate)
		{
			StoreOrUpdateRMDT(tokenId, renewDate, true);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void RemoveRMDelegationTokenState(RMDelegationTokenIdentifier
			 tokenId)
		{
			string tokenKey = GetRMDTTokenNodeKey(tokenId);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Removing token at " + tokenKey);
			}
			try
			{
				db.Delete(JniDBFactory.Bytes(tokenKey));
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void StoreRMDTMasterKeyState(DelegationKey masterKey)
		{
			string dbKey = GetRMDTMasterKeyNodeKey(masterKey);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Storing token master key to " + dbKey);
			}
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			DataOutputStream @out = new DataOutputStream(os);
			try
			{
				masterKey.Write(@out);
			}
			finally
			{
				@out.Close();
			}
			try
			{
				db.Put(JniDBFactory.Bytes(dbKey), os.ToByteArray());
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void RemoveRMDTMasterKeyState(DelegationKey masterKey
			)
		{
			string dbKey = GetRMDTMasterKeyNodeKey(masterKey);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Removing token master key at " + dbKey);
			}
			try
			{
				db.Delete(JniDBFactory.Bytes(dbKey));
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		protected internal override void StoreOrUpdateAMRMTokenSecretManagerState(AMRMTokenSecretManagerState
			 state, bool isUpdate)
		{
			AMRMTokenSecretManagerState data = AMRMTokenSecretManagerState.NewInstance(state);
			byte[] stateData = data.GetProto().ToByteArray();
			db.Put(JniDBFactory.Bytes(AmrmtokenSecretManagerRoot), stateData);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteStore()
		{
			Path root = GetStorageDir();
			Log.Info("Deleting state database at " + root);
			db.Close();
			db = null;
			FileSystem fs = FileSystem.GetLocal(GetConfig());
			fs.Delete(root, true);
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual int GetNumEntriesInDatabase()
		{
			int numEntries = 0;
			LeveldbIterator iter = null;
			try
			{
				iter = new LeveldbIterator(db);
				iter.SeekToFirst();
				while (iter.HasNext())
				{
					KeyValuePair<byte[], byte[]> entry = iter.Next();
					Log.Info("entry: " + JniDBFactory.AsString(entry.Key));
					++numEntries;
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
			return numEntries;
		}

		private class LeveldbLogger : Logger
		{
			private static readonly Org.Apache.Commons.Logging.Log Log = LogFactory.GetLog(typeof(
				LeveldbRMStateStore.LeveldbLogger));

			public virtual void Log(string message)
			{
				Log.Info(message);
			}
		}
	}
}
