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
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class HistoryServerFileSystemStateStoreService : HistoryServerStateStoreService
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(HistoryServerFileSystemStateStoreService
			));

		private const string RootStateDirName = "HistoryServerState";

		private const string TokenStateDirName = "tokens";

		private const string TokenKeysDirName = "keys";

		private const string TokenBucketDirPrefix = "tb_";

		private const string TokenBucketNameFormat = TokenBucketDirPrefix + "%03d";

		private const string TokenMasterKeyFilePrefix = "key_";

		private const string TokenFilePrefix = "token_";

		private const string TmpFilePrefix = "tmp-";

		private const string UpdateTmpFilePrefix = "update-";

		private static readonly FsPermission DirPermissions = new FsPermission((short)0x1c0
			);

		private static readonly FsPermission FilePermissions = Shell.Windows ? new FsPermission
			((short)0x1c0) : new FsPermission((short)0x100);

		private const int NumTokenBuckets = 1000;

		private FileSystem fs;

		private Path rootStatePath;

		private Path tokenStatePath;

		private Path tokenKeysStatePath;

		/// <exception cref="System.IO.IOException"/>
		protected internal override void InitStorage(Configuration conf)
		{
			string storeUri = conf.Get(JHAdminConfig.MrHsFsStateStoreUri);
			if (storeUri == null)
			{
				throw new IOException("No store location URI configured in " + JHAdminConfig.MrHsFsStateStoreUri
					);
			}
			Log.Info("Using " + storeUri + " for history server state storage");
			rootStatePath = new Path(storeUri, RootStateDirName);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void StartStorage()
		{
			fs = CreateFileSystem();
			CreateDir(rootStatePath);
			tokenStatePath = new Path(rootStatePath, TokenStateDirName);
			CreateDir(tokenStatePath);
			tokenKeysStatePath = new Path(tokenStatePath, TokenKeysDirName);
			CreateDir(tokenKeysStatePath);
			for (int i = 0; i < NumTokenBuckets; ++i)
			{
				CreateDir(GetTokenBucketPath(i));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual FileSystem CreateFileSystem()
		{
			return rootStatePath.GetFileSystem(GetConfig());
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void CloseStorage()
		{
		}

		// don't close the filesystem as it's part of the filesystem cache
		// and other clients may still be using it
		/// <exception cref="System.IO.IOException"/>
		public override HistoryServerStateStoreService.HistoryServerState LoadState()
		{
			Log.Info("Loading history server state from " + rootStatePath);
			HistoryServerStateStoreService.HistoryServerState state = new HistoryServerStateStoreService.HistoryServerState
				();
			LoadTokenState(state);
			return state;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreToken(MRDelegationTokenIdentifier tokenId, long renewDate
			)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Storing token " + tokenId.GetSequenceNumber());
			}
			Path tokenPath = GetTokenPath(tokenId);
			if (fs.Exists(tokenPath))
			{
				throw new IOException(tokenPath + " already exists");
			}
			CreateNewFile(tokenPath, BuildTokenData(tokenId, renewDate));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void UpdateToken(MRDelegationTokenIdentifier tokenId, long renewDate
			)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Updating token " + tokenId.GetSequenceNumber());
			}
			// Files cannot be atomically replaced, therefore we write a temporary
			// update file, remove the original token file, then rename the update
			// file to the token file. During recovery either the token file will be
			// used or if that is missing and an update file is present then the
			// update file is used.
			Path tokenPath = GetTokenPath(tokenId);
			Path tmp = new Path(tokenPath.GetParent(), UpdateTmpFilePrefix + tokenPath.GetName
				());
			WriteFile(tmp, BuildTokenData(tokenId, renewDate));
			try
			{
				DeleteFile(tokenPath);
			}
			catch (IOException e)
			{
				fs.Delete(tmp, false);
				throw;
			}
			if (!fs.Rename(tmp, tokenPath))
			{
				throw new IOException("Could not rename " + tmp + " to " + tokenPath);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveToken(MRDelegationTokenIdentifier tokenId)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Removing token " + tokenId.GetSequenceNumber());
			}
			DeleteFile(GetTokenPath(tokenId));
		}

		/// <exception cref="System.IO.IOException"/>
		public override void StoreTokenMasterKey(DelegationKey key)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Storing master key " + key.GetKeyId());
			}
			Path keyPath = new Path(tokenKeysStatePath, TokenMasterKeyFilePrefix + key.GetKeyId
				());
			if (fs.Exists(keyPath))
			{
				throw new IOException(keyPath + " already exists");
			}
			ByteArrayOutputStream memStream = new ByteArrayOutputStream();
			DataOutputStream dataStream = new DataOutputStream(memStream);
			try
			{
				key.Write(dataStream);
				dataStream.Close();
				dataStream = null;
			}
			finally
			{
				IOUtils.Cleanup(Log, dataStream);
			}
			CreateNewFile(keyPath, memStream.ToByteArray());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RemoveTokenMasterKey(DelegationKey key)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Removing master key " + key.GetKeyId());
			}
			Path keyPath = new Path(tokenKeysStatePath, TokenMasterKeyFilePrefix + key.GetKeyId
				());
			DeleteFile(keyPath);
		}

		private static int GetBucketId(MRDelegationTokenIdentifier tokenId)
		{
			return tokenId.GetSequenceNumber() % NumTokenBuckets;
		}

		private Path GetTokenBucketPath(int bucketId)
		{
			return new Path(tokenStatePath, string.Format(TokenBucketNameFormat, bucketId));
		}

		private Path GetTokenPath(MRDelegationTokenIdentifier tokenId)
		{
			Path bucketPath = GetTokenBucketPath(GetBucketId(tokenId));
			return new Path(bucketPath, TokenFilePrefix + tokenId.GetSequenceNumber());
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateDir(Path dir)
		{
			try
			{
				FileStatus status = fs.GetFileStatus(dir);
				if (!status.IsDirectory())
				{
					throw new FileAlreadyExistsException("Unexpected file in store: " + dir);
				}
				if (!status.GetPermission().Equals(DirPermissions))
				{
					fs.SetPermission(dir, DirPermissions);
				}
			}
			catch (FileNotFoundException)
			{
				fs.Mkdirs(dir, DirPermissions);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateNewFile(Path file, byte[] data)
		{
			Path tmp = new Path(file.GetParent(), TmpFilePrefix + file.GetName());
			WriteFile(tmp, data);
			try
			{
				if (!fs.Rename(tmp, file))
				{
					throw new IOException("Could not rename " + tmp + " to " + file);
				}
			}
			catch (IOException e)
			{
				fs.Delete(tmp, false);
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(Path file, byte[] data)
		{
			int WriteBufferSize = 4096;
			FSDataOutputStream @out = fs.Create(file, FilePermissions, true, WriteBufferSize, 
				fs.GetDefaultReplication(file), fs.GetDefaultBlockSize(file), null);
			try
			{
				try
				{
					@out.Write(data);
					@out.Close();
					@out = null;
				}
				finally
				{
					IOUtils.Cleanup(Log, @out);
				}
			}
			catch (IOException e)
			{
				fs.Delete(file, false);
				throw;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private byte[] ReadFile(Path file, long numBytes)
		{
			byte[] data = new byte[(int)numBytes];
			FSDataInputStream @in = fs.Open(file);
			try
			{
				@in.ReadFully(data);
			}
			finally
			{
				IOUtils.Cleanup(Log, @in);
			}
			return data;
		}

		/// <exception cref="System.IO.IOException"/>
		private void DeleteFile(Path file)
		{
			bool deleted;
			try
			{
				deleted = fs.Delete(file, false);
			}
			catch (FileNotFoundException)
			{
				deleted = true;
			}
			if (!deleted)
			{
				throw new IOException("Unable to delete " + file);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private byte[] BuildTokenData(MRDelegationTokenIdentifier tokenId, long renewDate
			)
		{
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
			return memStream.ToByteArray();
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadTokenMasterKey(HistoryServerStateStoreService.HistoryServerState
			 state, Path keyFile, long numKeyFileBytes)
		{
			DelegationKey key = new DelegationKey();
			byte[] keyData = ReadFile(keyFile, numKeyFileBytes);
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
		private void LoadTokenFromBucket(int bucketId, HistoryServerStateStoreService.HistoryServerState
			 state, Path tokenFile, long numTokenFileBytes)
		{
			MRDelegationTokenIdentifier token = LoadToken(state, tokenFile, numTokenFileBytes
				);
			int tokenBucketId = GetBucketId(token);
			if (tokenBucketId != bucketId)
			{
				throw new IOException("Token " + tokenFile + " should be in bucket " + tokenBucketId
					 + ", found in bucket " + bucketId);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private MRDelegationTokenIdentifier LoadToken(HistoryServerStateStoreService.HistoryServerState
			 state, Path tokenFile, long numTokenFileBytes)
		{
			MRDelegationTokenIdentifier tokenId = new MRDelegationTokenIdentifier();
			long renewDate;
			byte[] tokenData = ReadFile(tokenFile, numTokenFileBytes);
			DataInputStream @in = new DataInputStream(new ByteArrayInputStream(tokenData));
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
			return tokenId;
		}

		/// <exception cref="System.IO.IOException"/>
		private int LoadTokensFromBucket(HistoryServerStateStoreService.HistoryServerState
			 state, Path bucket)
		{
			string numStr = Sharpen.Runtime.Substring(bucket.GetName(), TokenBucketDirPrefix.
				Length);
			int bucketId = System.Convert.ToInt32(numStr);
			int numTokens = 0;
			FileStatus[] tokenStats = fs.ListStatus(bucket);
			ICollection<string> loadedTokens = new HashSet<string>(tokenStats.Length);
			foreach (FileStatus stat in tokenStats)
			{
				string name = stat.GetPath().GetName();
				if (name.StartsWith(TokenFilePrefix))
				{
					LoadTokenFromBucket(bucketId, state, stat.GetPath(), stat.GetLen());
					loadedTokens.AddItem(name);
					++numTokens;
				}
				else
				{
					if (name.StartsWith(UpdateTmpFilePrefix))
					{
						string tokenName = Sharpen.Runtime.Substring(name, UpdateTmpFilePrefix.Length);
						if (loadedTokens.Contains(tokenName))
						{
							// already have the token, update may be partial so ignore it
							fs.Delete(stat.GetPath(), false);
						}
						else
						{
							// token is missing, so try to parse the update temp file
							LoadTokenFromBucket(bucketId, state, stat.GetPath(), stat.GetLen());
							fs.Rename(stat.GetPath(), new Path(stat.GetPath().GetParent(), tokenName));
							loadedTokens.AddItem(tokenName);
							++numTokens;
						}
					}
					else
					{
						if (name.StartsWith(TmpFilePrefix))
						{
							// cleanup incomplete temp files
							fs.Delete(stat.GetPath(), false);
						}
						else
						{
							Log.Warn("Skipping unexpected file in history server token bucket: " + stat.GetPath
								());
						}
					}
				}
			}
			return numTokens;
		}

		/// <exception cref="System.IO.IOException"/>
		private int LoadKeys(HistoryServerStateStoreService.HistoryServerState state)
		{
			FileStatus[] stats = fs.ListStatus(tokenKeysStatePath);
			int numKeys = 0;
			foreach (FileStatus stat in stats)
			{
				string name = stat.GetPath().GetName();
				if (name.StartsWith(TokenMasterKeyFilePrefix))
				{
					LoadTokenMasterKey(state, stat.GetPath(), stat.GetLen());
					++numKeys;
				}
				else
				{
					Log.Warn("Skipping unexpected file in history server token state: " + stat.GetPath
						());
				}
			}
			return numKeys;
		}

		/// <exception cref="System.IO.IOException"/>
		private int LoadTokens(HistoryServerStateStoreService.HistoryServerState state)
		{
			FileStatus[] stats = fs.ListStatus(tokenStatePath);
			int numTokens = 0;
			foreach (FileStatus stat in stats)
			{
				string name = stat.GetPath().GetName();
				if (name.StartsWith(TokenBucketDirPrefix))
				{
					numTokens += LoadTokensFromBucket(state, stat.GetPath());
				}
				else
				{
					if (name.Equals(TokenKeysDirName))
					{
						// key loading is done elsewhere
						continue;
					}
					else
					{
						Log.Warn("Skipping unexpected file in history server token state: " + stat.GetPath
							());
					}
				}
			}
			return numTokens;
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadTokenState(HistoryServerStateStoreService.HistoryServerState state
			)
		{
			int numKeys = LoadKeys(state);
			int numTokens = LoadTokens(state);
			Log.Info("Loaded " + numKeys + " master keys and " + numTokens + " tokens from " 
				+ tokenStatePath);
		}
	}
}
