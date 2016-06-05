using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Recovery.Records;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Util;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Fusesource.Leveldbjni;
using Org.Fusesource.Leveldbjni.Internal;
using Org.Iq80.Leveldb;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Recovery
{
	/// <summary>
	/// A timeline service state storage implementation that supports any persistent
	/// storage that adheres to the LevelDB interface.
	/// </summary>
	public class LeveldbTimelineStateStore : TimelineStateStore
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.Recovery.LeveldbTimelineStateStore
			));

		private const string DbName = "timeline-state-store.ldb";

		private static readonly FsPermission LeveldbDirUmask = FsPermission.CreateImmutable
			((short)0x1c0);

		private static readonly byte[] TokenEntryPrefix = JniDBFactory.Bytes("t");

		private static readonly byte[] TokenMasterKeyEntryPrefix = JniDBFactory.Bytes("k"
			);

		private static readonly byte[] LatestSequenceNumberKey = JniDBFactory.Bytes("s");

		private static readonly Version CurrentVersionInfo = Version.NewInstance(1, 0);

		private static readonly byte[] TimelineStateStoreVersionKey = JniDBFactory.Bytes(
			"v");

		private DB db;

		public LeveldbTimelineStateStore()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.Recovery.LeveldbTimelineStateStore
				).FullName)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void InitStorage(Configuration conf)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void StartStorage()
		{
			Options options = new Options();
			Path dbPath = new Path(GetConfig().Get(YarnConfiguration.TimelineServiceLeveldbStateStorePath
				), DbName);
			FileSystem localFS = null;
			try
			{
				localFS = FileSystem.GetLocal(GetConfig());
				if (!localFS.Exists(dbPath))
				{
					if (!localFS.Mkdirs(dbPath))
					{
						throw new IOException("Couldn't create directory for leveldb " + "timeline store "
							 + dbPath);
					}
					localFS.SetPermission(dbPath, LeveldbDirUmask);
				}
			}
			finally
			{
				IOUtils.Cleanup(Log, localFS);
			}
			JniDBFactory factory = new JniDBFactory();
			try
			{
				options.CreateIfMissing(false);
				db = factory.Open(new FilePath(dbPath.ToString()), options);
				Log.Info("Loading the existing database at th path: " + dbPath.ToString());
				CheckVersion();
			}
			catch (NativeDB.DBException e)
			{
				if (e.IsNotFound() || e.Message.Contains(" does not exist "))
				{
					try
					{
						options.CreateIfMissing(true);
						db = factory.Open(new FilePath(dbPath.ToString()), options);
						Log.Info("Creating a new database at th path: " + dbPath.ToString());
						StoreVersion(CurrentVersionInfo);
					}
					catch (DBException ex)
					{
						throw new IOException(ex);
					}
				}
				else
				{
					throw new IOException(e);
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void CloseStorage()
		{
			IOUtils.Cleanup(Log, db);
		}

		/// <exception cref="System.IO.IOException"/>
		public override TimelineStateStore.TimelineServiceState LoadState()
		{
			Log.Info("Loading timeline service state from leveldb");
			TimelineStateStore.TimelineServiceState state = new TimelineStateStore.TimelineServiceState
				();
			int numKeys = LoadTokenMasterKeys(state);
			int numTokens = LoadTokens(state);
			LoadLatestSequenceNumber(state);
			Log.Info("Loaded " + numKeys + " master keys and " + numTokens + " tokens from leveldb, and latest sequence number is "
				 + state.GetLatestSequenceNumber());
			return state;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreToken(TimelineDelegationTokenIdentifier tokenId, long renewDate
			)
		{
			DataOutputStream ds = null;
			WriteBatch batch = null;
			try
			{
				byte[] k = CreateTokenEntryKey(tokenId.GetSequenceNumber());
				if (db.Get(k) != null)
				{
					throw new IOException(tokenId + " already exists");
				}
				byte[] v = BuildTokenData(tokenId, renewDate);
				ByteArrayOutputStream bs = new ByteArrayOutputStream();
				ds = new DataOutputStream(bs);
				ds.WriteInt(tokenId.GetSequenceNumber());
				batch = db.CreateWriteBatch();
				batch.Put(k, v);
				batch.Put(LatestSequenceNumberKey, bs.ToByteArray());
				db.Write(batch);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				IOUtils.Cleanup(Log, ds);
				IOUtils.Cleanup(Log, batch);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void UpdateToken(TimelineDelegationTokenIdentifier tokenId, long 
			renewDate)
		{
			try
			{
				byte[] k = CreateTokenEntryKey(tokenId.GetSequenceNumber());
				if (db.Get(k) == null)
				{
					throw new IOException(tokenId + " doesn't exist");
				}
				byte[] v = BuildTokenData(tokenId, renewDate);
				db.Put(k, v);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveToken(TimelineDelegationTokenIdentifier tokenId)
		{
			try
			{
				byte[] key = CreateTokenEntryKey(tokenId.GetSequenceNumber());
				db.Delete(key);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreTokenMasterKey(DelegationKey key)
		{
			try
			{
				byte[] k = CreateTokenMasterKeyEntryKey(key.GetKeyId());
				if (db.Get(k) != null)
				{
					throw new IOException(key + " already exists");
				}
				byte[] v = BuildTokenMasterKeyData(key);
				db.Put(k, v);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveTokenMasterKey(DelegationKey key)
		{
			try
			{
				byte[] k = CreateTokenMasterKeyEntryKey(key.GetKeyId());
				db.Delete(k);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static byte[] BuildTokenData(TimelineDelegationTokenIdentifier tokenId, long
			 renewDate)
		{
			TimelineDelegationTokenIdentifierData data = new TimelineDelegationTokenIdentifierData
				(tokenId, renewDate);
			return data.ToByteArray();
		}

		/// <exception cref="System.IO.IOException"/>
		private static byte[] BuildTokenMasterKeyData(DelegationKey key)
		{
			ByteArrayOutputStream memStream = new ByteArrayOutputStream();
			DataOutputStream dataStream = new DataOutputStream(memStream);
			try
			{
				key.Write(dataStream);
				dataStream.Close();
			}
			finally
			{
				IOUtils.Cleanup(Log, dataStream);
			}
			return memStream.ToByteArray();
		}

		/// <exception cref="System.IO.IOException"/>
		private static void LoadTokenMasterKeyData(TimelineStateStore.TimelineServiceState
			 state, byte[] keyData)
		{
			DelegationKey key = new DelegationKey();
			DataInputStream @in = new DataInputStream(new ByteArrayInputStream(keyData));
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
		private static void LoadTokenData(TimelineStateStore.TimelineServiceState state, 
			byte[] tokenData)
		{
			TimelineDelegationTokenIdentifierData data = new TimelineDelegationTokenIdentifierData
				();
			DataInputStream @in = new DataInputStream(new ByteArrayInputStream(tokenData));
			try
			{
				data.ReadFields(@in);
			}
			finally
			{
				IOUtils.Cleanup(Log, @in);
			}
			state.tokenState[data.GetTokenIdentifier()] = data.GetRenewDate();
		}

		/// <exception cref="System.IO.IOException"/>
		private int LoadTokenMasterKeys(TimelineStateStore.TimelineServiceState state)
		{
			byte[] @base = LeveldbUtils.KeyBuilder.NewInstance().Add(TokenMasterKeyEntryPrefix
				).GetBytesForLookup();
			int numKeys = 0;
			LeveldbIterator iterator = null;
			try
			{
				for (iterator = new LeveldbIterator(db), iterator.Seek(@base); iterator.HasNext()
					; iterator.Next())
				{
					byte[] k = iterator.PeekNext().Key;
					if (!LeveldbUtils.PrefixMatches(@base, @base.Length, k))
					{
						break;
					}
					byte[] v = iterator.PeekNext().Value;
					LoadTokenMasterKeyData(state, v);
					++numKeys;
				}
			}
			finally
			{
				IOUtils.Cleanup(Log, iterator);
			}
			return numKeys;
		}

		/// <exception cref="System.IO.IOException"/>
		private int LoadTokens(TimelineStateStore.TimelineServiceState state)
		{
			byte[] @base = LeveldbUtils.KeyBuilder.NewInstance().Add(TokenEntryPrefix).GetBytesForLookup
				();
			int numTokens = 0;
			LeveldbIterator iterator = null;
			try
			{
				for (iterator = new LeveldbIterator(db), iterator.Seek(@base); iterator.HasNext()
					; iterator.Next())
				{
					byte[] k = iterator.PeekNext().Key;
					if (!LeveldbUtils.PrefixMatches(@base, @base.Length, k))
					{
						break;
					}
					byte[] v = iterator.PeekNext().Value;
					LoadTokenData(state, v);
					++numTokens;
				}
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				IOUtils.Cleanup(Log, iterator);
			}
			return numTokens;
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadLatestSequenceNumber(TimelineStateStore.TimelineServiceState state
			)
		{
			byte[] data = null;
			try
			{
				data = db.Get(LatestSequenceNumberKey);
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
					state.latestSequenceNumber = @in.ReadInt();
				}
				finally
				{
					IOUtils.Cleanup(Log, @in);
				}
			}
		}

		/// <summary>
		/// Creates a domain entity key with column name suffix, of the form
		/// TOKEN_ENTRY_PREFIX + sequence number.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static byte[] CreateTokenEntryKey(int seqNum)
		{
			return LeveldbUtils.KeyBuilder.NewInstance().Add(TokenEntryPrefix).Add(Sharpen.Extensions.ToString
				(seqNum)).GetBytes();
		}

		/// <summary>
		/// Creates a domain entity key with column name suffix, of the form
		/// TOKEN_MASTER_KEY_ENTRY_PREFIX + sequence number.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private static byte[] CreateTokenMasterKeyEntryKey(int keyId)
		{
			return LeveldbUtils.KeyBuilder.NewInstance().Add(TokenMasterKeyEntryPrefix).Add(Sharpen.Extensions.ToString
				(keyId)).GetBytes();
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual Version LoadVersion()
		{
			try
			{
				byte[] data = db.Get(TimelineStateStoreVersionKey);
				// if version is not stored previously, treat it as CURRENT_VERSION_INFO.
				if (data == null || data.Length == 0)
				{
					return GetCurrentVersion();
				}
				Version version = new VersionPBImpl(YarnServerCommonProtos.VersionProto.ParseFrom
					(data));
				return version;
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void StoreVersion(Version state)
		{
			byte[] data = ((VersionPBImpl)state).GetProto().ToByteArray();
			try
			{
				db.Put(TimelineStateStoreVersionKey, data);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
		}

		[VisibleForTesting]
		internal virtual Version GetCurrentVersion()
		{
			return CurrentVersionInfo;
		}

		/// <summary>
		/// 1) Versioning timeline state store:
		/// major.minor.
		/// </summary>
		/// <remarks>
		/// 1) Versioning timeline state store:
		/// major.minor. For e.g. 1.0, 1.1, 1.2...1.25, 2.0 etc.
		/// 2) Any incompatible change of TS-store is a major upgrade, and any
		/// compatible change of TS-store is a minor upgrade.
		/// 3) Within a minor upgrade, say 1.1 to 1.2:
		/// overwrite the version info and proceed as normal.
		/// 4) Within a major upgrade, say 1.2 to 2.0:
		/// throw exception and indicate user to use a separate upgrade tool to
		/// upgrade timeline store or remove incompatible old state.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void CheckVersion()
		{
			Version loadedVersion = LoadVersion();
			Log.Info("Loaded timeline state store version info " + loadedVersion);
			if (loadedVersion.Equals(GetCurrentVersion()))
			{
				return;
			}
			if (loadedVersion.IsCompatibleTo(GetCurrentVersion()))
			{
				Log.Info("Storing timeline state store version info " + GetCurrentVersion());
				StoreVersion(CurrentVersionInfo);
			}
			else
			{
				string incompatibleMessage = "Incompatible version for timeline state store: expecting version "
					 + GetCurrentVersion() + ", but loading version " + loadedVersion;
				Log.Fatal(incompatibleMessage);
				throw new IOException(incompatibleMessage);
			}
		}
	}
}
