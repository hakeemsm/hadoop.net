using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Fusesource.Leveldbjni;
using Org.Fusesource.Leveldbjni.Internal;
using Org.Iq80.Leveldb;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class HistoryServerLeveldbStateStoreService : HistoryServerStateStoreService
	{
		private const string DbName = "mr-jhs-state";

		private const string DbSchemaVersionKey = "jhs-schema-version";

		private const string TokenMasterKeyKeyPrefix = "tokens/key_";

		private const string TokenStateKeyPrefix = "tokens/token_";

		private static readonly Version CurrentVersionInfo = Version.NewInstance(1, 0);

		private DB db;

		public static readonly Log Log = LogFactory.GetLog(typeof(HistoryServerLeveldbStateStoreService
			));

		/// <exception cref="System.IO.IOException"/>
		protected internal override void InitStorage(Configuration conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void StartStorage()
		{
			Path storeRoot = CreateStorageDir(GetConfig());
			Options options = new Options();
			options.CreateIfMissing(false);
			options.Logger(new HistoryServerLeveldbStateStoreService.LeveldbLogger());
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
			CheckVersion();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void CloseStorage()
		{
			if (db != null)
			{
				db.Close();
				db = null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override HistoryServerStateStoreService.HistoryServerState LoadState()
		{
			HistoryServerStateStoreService.HistoryServerState state = new HistoryServerStateStoreService.HistoryServerState
				();
			int numKeys = LoadTokenMasterKeys(state);
			Log.Info("Recovered " + numKeys + " token master keys");
			int numTokens = LoadTokens(state);
			Log.Info("Recovered " + numTokens + " tokens");
			return state;
		}

		/// <exception cref="System.IO.IOException"/>
		private int LoadTokenMasterKeys(HistoryServerStateStoreService.HistoryServerState
			 state)
		{
			int numKeys = 0;
			LeveldbIterator iter = null;
			try
			{
				iter = new LeveldbIterator(db);
				iter.Seek(JniDBFactory.Bytes(TokenMasterKeyKeyPrefix));
				while (iter.HasNext())
				{
					KeyValuePair<byte[], byte[]> entry = iter.Next();
					string key = JniDBFactory.AsString(entry.Key);
					if (!key.StartsWith(TokenMasterKeyKeyPrefix))
					{
						break;
					}
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Loading master key from " + key);
					}
					try
					{
						LoadTokenMasterKey(state, entry.Value);
					}
					catch (IOException e)
					{
						throw new IOException("Error loading token master key from " + key, e);
					}
					++numKeys;
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
		private void LoadTokenMasterKey(HistoryServerStateStoreService.HistoryServerState
			 state, byte[] data)
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
			state.tokenMasterKeyState.AddItem(key);
		}

		/// <exception cref="System.IO.IOException"/>
		private int LoadTokens(HistoryServerStateStoreService.HistoryServerState state)
		{
			int numTokens = 0;
			LeveldbIterator iter = null;
			try
			{
				iter = new LeveldbIterator(db);
				iter.Seek(JniDBFactory.Bytes(TokenStateKeyPrefix));
				while (iter.HasNext())
				{
					KeyValuePair<byte[], byte[]> entry = iter.Next();
					string key = JniDBFactory.AsString(entry.Key);
					if (!key.StartsWith(TokenStateKeyPrefix))
					{
						break;
					}
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Loading token from " + key);
					}
					try
					{
						LoadToken(state, entry.Value);
					}
					catch (IOException e)
					{
						throw new IOException("Error loading token state from " + key, e);
					}
					++numTokens;
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
		private void LoadToken(HistoryServerStateStoreService.HistoryServerState state, byte
			[] data)
		{
			MRDelegationTokenIdentifier tokenId = new MRDelegationTokenIdentifier();
			long renewDate;
			DataInputStream @in = new DataInputStream(new ByteArrayInputStream(data));
			try
			{
				tokenId.ReadFields(@in);
				renewDate = @in.ReadLong();
			}
			finally
			{
				IOUtils.Cleanup(Log, @in);
			}
			state.tokenState[tokenId] = renewDate;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreToken(MRDelegationTokenIdentifier tokenId, long renewDate
			)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Storing token " + tokenId.GetSequenceNumber());
			}
			ByteArrayOutputStream memStream = new ByteArrayOutputStream();
			DataOutputStream dataStream = new DataOutputStream(memStream);
			try
			{
				tokenId.Write(dataStream);
				dataStream.WriteLong(renewDate);
				dataStream.Close();
				dataStream = null;
			}
			finally
			{
				IOUtils.Cleanup(Log, dataStream);
			}
			string dbKey = GetTokenDatabaseKey(tokenId);
			try
			{
				db.Put(JniDBFactory.Bytes(dbKey), memStream.ToByteArray());
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void UpdateToken(MRDelegationTokenIdentifier tokenId, long renewDate
			)
		{
			StoreToken(tokenId, renewDate);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveToken(MRDelegationTokenIdentifier tokenId)
		{
			string dbKey = GetTokenDatabaseKey(tokenId);
			try
			{
				db.Delete(JniDBFactory.Bytes(dbKey));
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		private string GetTokenDatabaseKey(MRDelegationTokenIdentifier tokenId)
		{
			return TokenStateKeyPrefix + tokenId.GetSequenceNumber();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreTokenMasterKey(DelegationKey masterKey)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Storing master key " + masterKey.GetKeyId());
			}
			ByteArrayOutputStream memStream = new ByteArrayOutputStream();
			DataOutputStream dataStream = new DataOutputStream(memStream);
			try
			{
				masterKey.Write(dataStream);
				dataStream.Close();
				dataStream = null;
			}
			finally
			{
				IOUtils.Cleanup(Log, dataStream);
			}
			string dbKey = GetTokenMasterKeyDatabaseKey(masterKey);
			try
			{
				db.Put(JniDBFactory.Bytes(dbKey), memStream.ToByteArray());
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveTokenMasterKey(DelegationKey masterKey)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Removing master key " + masterKey.GetKeyId());
			}
			string dbKey = GetTokenMasterKeyDatabaseKey(masterKey);
			try
			{
				db.Delete(JniDBFactory.Bytes(dbKey));
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		private string GetTokenMasterKeyDatabaseKey(DelegationKey masterKey)
		{
			return TokenMasterKeyKeyPrefix + masterKey.GetKeyId();
		}

		/// <exception cref="System.IO.IOException"/>
		private Path CreateStorageDir(Configuration conf)
		{
			string confPath = conf.Get(JHAdminConfig.MrHsLeveldbStateStorePath);
			if (confPath == null)
			{
				throw new IOException("No store location directory configured in " + JHAdminConfig
					.MrHsLeveldbStateStorePath);
			}
			Path root = new Path(confPath, DbName);
			FileSystem fs = FileSystem.GetLocal(conf);
			fs.Mkdirs(root, new FsPermission((short)0x1c0));
			return root;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual Version LoadVersion()
		{
			byte[] data = db.Get(JniDBFactory.Bytes(DbSchemaVersionKey));
			// if version is not stored previously, treat it as 1.0.
			if (data == null || data.Length == 0)
			{
				return Version.NewInstance(1, 0);
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

		/// <exception cref="System.IO.IOException"/>
		internal virtual void DbStoreVersion(Version state)
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
		/// upgrade state or remove incompatible old state.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void CheckVersion()
		{
			Version loadedVersion = LoadVersion();
			Log.Info("Loaded state version info " + loadedVersion);
			if (loadedVersion.Equals(GetCurrentVersion()))
			{
				return;
			}
			if (loadedVersion.IsCompatibleTo(GetCurrentVersion()))
			{
				Log.Info("Storing state version info " + GetCurrentVersion());
				StoreVersion();
			}
			else
			{
				throw new IOException("Incompatible version for state: expecting state version " 
					+ GetCurrentVersion() + ", but loading version " + loadedVersion);
			}
		}

		private class LeveldbLogger : Logger
		{
			private static readonly Org.Apache.Commons.Logging.Log Log = LogFactory.GetLog(typeof(
				HistoryServerLeveldbStateStoreService.LeveldbLogger));

			public virtual void Log(string message)
			{
				Log.Info(message);
			}
		}
	}
}
