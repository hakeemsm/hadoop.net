using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security.Token.Block
{
	/// <summary>
	/// BlockTokenSecretManager can be instantiated in 2 modes, master mode and slave
	/// mode.
	/// </summary>
	/// <remarks>
	/// BlockTokenSecretManager can be instantiated in 2 modes, master mode and slave
	/// mode. Master can generate new block keys and export block keys to slaves,
	/// while slaves can only import and use block keys received from master. Both
	/// master and slave can generate and verify block tokens. Typically, master mode
	/// is used by NN and slave mode is used by DN.
	/// </remarks>
	public class BlockTokenSecretManager : SecretManager<BlockTokenIdentifier>
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Security.Token.Block.BlockTokenSecretManager
			));

		private const int LowMask = ~(1 << 31);

		public static readonly Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier
			> DummyToken = new Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>(
			);

		private readonly bool isMaster;

		private int nnIndex;

		/// <summary>keyUpdateInterval is the interval that NN updates its block keys.</summary>
		/// <remarks>
		/// keyUpdateInterval is the interval that NN updates its block keys. It should
		/// be set long enough so that all live DN's and Balancer should have sync'ed
		/// their block keys with NN at least once during each interval.
		/// </remarks>
		private long keyUpdateInterval;

		private volatile long tokenLifetime;

		private int serialNo;

		private BlockKey currentKey;

		private BlockKey nextKey;

		private readonly IDictionary<int, BlockKey> allKeys;

		private string blockPoolId;

		private readonly string encryptionAlgorithm;

		private readonly SecureRandom nonceGenerator = new SecureRandom();

		public enum AccessMode
		{
			Read,
			Write,
			Copy,
			Replace
		}

		/// <summary>Constructor for slaves.</summary>
		/// <param name="keyUpdateInterval">how often a new key will be generated</param>
		/// <param name="tokenLifetime">how long an individual token is valid</param>
		public BlockTokenSecretManager(long keyUpdateInterval, long tokenLifetime, string
			 blockPoolId, string encryptionAlgorithm)
			: this(false, keyUpdateInterval, tokenLifetime, blockPoolId, encryptionAlgorithm)
		{
		}

		/// <summary>Constructor for masters.</summary>
		/// <param name="keyUpdateInterval">how often a new key will be generated</param>
		/// <param name="tokenLifetime">how long an individual token is valid</param>
		/// <param name="nnIndex">namenode index</param>
		/// <param name="blockPoolId">block pool ID</param>
		/// <param name="encryptionAlgorithm">encryption algorithm to use</param>
		public BlockTokenSecretManager(long keyUpdateInterval, long tokenLifetime, int nnIndex
			, string blockPoolId, string encryptionAlgorithm)
			: this(true, keyUpdateInterval, tokenLifetime, blockPoolId, encryptionAlgorithm)
		{
			// We use these in an HA setup to ensure that the pair of NNs produce block
			// token serial numbers that are in different ranges.
			Preconditions.CheckArgument(nnIndex == 0 || nnIndex == 1);
			this.nnIndex = nnIndex;
			SetSerialNo(new SecureRandom().Next());
			GenerateKeys();
		}

		private BlockTokenSecretManager(bool isMaster, long keyUpdateInterval, long tokenLifetime
			, string blockPoolId, string encryptionAlgorithm)
		{
			this.isMaster = isMaster;
			this.keyUpdateInterval = keyUpdateInterval;
			this.tokenLifetime = tokenLifetime;
			this.allKeys = new Dictionary<int, BlockKey>();
			this.blockPoolId = blockPoolId;
			this.encryptionAlgorithm = encryptionAlgorithm;
			GenerateKeys();
		}

		[VisibleForTesting]
		public virtual void SetSerialNo(int serialNo)
		{
			lock (this)
			{
				this.serialNo = (serialNo & LowMask) | (nnIndex << 31);
			}
		}

		public virtual void SetBlockPoolId(string blockPoolId)
		{
			this.blockPoolId = blockPoolId;
		}

		/// <summary>Initialize block keys</summary>
		private void GenerateKeys()
		{
			lock (this)
			{
				if (!isMaster)
				{
					return;
				}
				/*
				* Need to set estimated expiry dates for currentKey and nextKey so that if
				* NN crashes, DN can still expire those keys. NN will stop using the newly
				* generated currentKey after the first keyUpdateInterval, however it may
				* still be used by DN and Balancer to generate new tokens before they get a
				* chance to sync their keys with NN. Since we require keyUpdInterval to be
				* long enough so that all live DN's and Balancer will sync their keys with
				* NN at least once during the period, the estimated expiry date for
				* currentKey is set to now() + 2 * keyUpdateInterval + tokenLifetime.
				* Similarly, the estimated expiry date for nextKey is one keyUpdateInterval
				* more.
				*/
				SetSerialNo(serialNo + 1);
				currentKey = new BlockKey(serialNo, Time.Now() + 2 * keyUpdateInterval + tokenLifetime
					, GenerateSecret());
				SetSerialNo(serialNo + 1);
				nextKey = new BlockKey(serialNo, Time.Now() + 3 * keyUpdateInterval + tokenLifetime
					, GenerateSecret());
				allKeys[currentKey.GetKeyId()] = currentKey;
				allKeys[nextKey.GetKeyId()] = nextKey;
			}
		}

		/// <summary>Export block keys, only to be used in master mode</summary>
		public virtual ExportedBlockKeys ExportKeys()
		{
			lock (this)
			{
				if (!isMaster)
				{
					return null;
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Exporting access keys");
				}
				return new ExportedBlockKeys(true, keyUpdateInterval, tokenLifetime, currentKey, 
					Sharpen.Collections.ToArray(allKeys.Values, new BlockKey[0]));
			}
		}

		private void RemoveExpiredKeys()
		{
			lock (this)
			{
				long now = Time.Now();
				for (IEnumerator<KeyValuePair<int, BlockKey>> it = allKeys.GetEnumerator(); it.HasNext
					(); )
				{
					KeyValuePair<int, BlockKey> e = it.Next();
					if (e.Value.GetExpiryDate() < now)
					{
						it.Remove();
					}
				}
			}
		}

		/// <summary>Set block keys, only to be used in slave mode</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AddKeys(ExportedBlockKeys exportedKeys)
		{
			lock (this)
			{
				if (isMaster || exportedKeys == null)
				{
					return;
				}
				Log.Info("Setting block keys");
				RemoveExpiredKeys();
				this.currentKey = exportedKeys.GetCurrentKey();
				BlockKey[] receivedKeys = exportedKeys.GetAllKeys();
				for (int i = 0; i < receivedKeys.Length; i++)
				{
					if (receivedKeys[i] == null)
					{
						continue;
					}
					this.allKeys[receivedKeys[i].GetKeyId()] = receivedKeys[i];
				}
			}
		}

		/// <summary>Update block keys if update time &gt; update interval.</summary>
		/// <returns>true if the keys are updated.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool UpdateKeys(long updateTime)
		{
			lock (this)
			{
				if (updateTime > keyUpdateInterval)
				{
					return UpdateKeys();
				}
				return false;
			}
		}

		/// <summary>Update block keys, only to be used in master mode</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual bool UpdateKeys()
		{
			lock (this)
			{
				if (!isMaster)
				{
					return false;
				}
				Log.Info("Updating block keys");
				RemoveExpiredKeys();
				// set final expiry date of retiring currentKey
				allKeys[currentKey.GetKeyId()] = new BlockKey(currentKey.GetKeyId(), Time.Now() +
					 keyUpdateInterval + tokenLifetime, currentKey.GetKey());
				// update the estimated expiry date of new currentKey
				currentKey = new BlockKey(nextKey.GetKeyId(), Time.Now() + 2 * keyUpdateInterval 
					+ tokenLifetime, nextKey.GetKey());
				allKeys[currentKey.GetKeyId()] = currentKey;
				// generate a new nextKey
				SetSerialNo(serialNo + 1);
				nextKey = new BlockKey(serialNo, Time.Now() + 3 * keyUpdateInterval + tokenLifetime
					, GenerateSecret());
				allKeys[nextKey.GetKeyId()] = nextKey;
				return true;
			}
		}

		/// <summary>Generate an block token for current user</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> GenerateToken
			(ExtendedBlock block, EnumSet<BlockTokenSecretManager.AccessMode> modes)
		{
			UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
			string userID = (ugi == null ? null : ugi.GetShortUserName());
			return GenerateToken(userID, block, modes);
		}

		/// <summary>Generate a block token for a specified user</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> GenerateToken
			(string userId, ExtendedBlock block, EnumSet<BlockTokenSecretManager.AccessMode>
			 modes)
		{
			BlockTokenIdentifier id = new BlockTokenIdentifier(userId, block.GetBlockPoolId()
				, block.GetBlockId(), modes);
			return new Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>(id, this);
		}

		/// <summary>Check if access should be allowed.</summary>
		/// <remarks>
		/// Check if access should be allowed. userID is not checked if null. This
		/// method doesn't check if token password is correct. It should be used only
		/// when token password has already been verified (e.g., in the RPC layer).
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public virtual void CheckAccess(BlockTokenIdentifier id, string userId, ExtendedBlock
			 block, BlockTokenSecretManager.AccessMode mode)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Checking access for user=" + userId + ", block=" + block + ", access mode="
					 + mode + " using " + id.ToString());
			}
			if (userId != null && !userId.Equals(id.GetUserId()))
			{
				throw new SecretManager.InvalidToken("Block token with " + id.ToString() + " doesn't belong to user "
					 + userId);
			}
			if (!id.GetBlockPoolId().Equals(block.GetBlockPoolId()))
			{
				throw new SecretManager.InvalidToken("Block token with " + id.ToString() + " doesn't apply to block "
					 + block);
			}
			if (id.GetBlockId() != block.GetBlockId())
			{
				throw new SecretManager.InvalidToken("Block token with " + id.ToString() + " doesn't apply to block "
					 + block);
			}
			if (IsExpired(id.GetExpiryDate()))
			{
				throw new SecretManager.InvalidToken("Block token with " + id.ToString() + " is expired."
					);
			}
			if (!id.GetAccessModes().Contains(mode))
			{
				throw new SecretManager.InvalidToken("Block token with " + id.ToString() + " doesn't have "
					 + mode + " permission");
			}
		}

		/// <summary>Check if access should be allowed.</summary>
		/// <remarks>Check if access should be allowed. userID is not checked if null</remarks>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public virtual void CheckAccess(Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier
			> token, string userId, ExtendedBlock block, BlockTokenSecretManager.AccessMode 
			mode)
		{
			BlockTokenIdentifier id = new BlockTokenIdentifier();
			try
			{
				id.ReadFields(new DataInputStream(new ByteArrayInputStream(token.GetIdentifier())
					));
			}
			catch (IOException)
			{
				throw new SecretManager.InvalidToken("Unable to de-serialize block token identifier for user="
					 + userId + ", block=" + block + ", access mode=" + mode);
			}
			CheckAccess(id, userId, block, mode);
			if (!Arrays.Equals(RetrievePassword(id), token.GetPassword()))
			{
				throw new SecretManager.InvalidToken("Block token with " + id.ToString() + " doesn't have the correct token password"
					);
			}
		}

		private static bool IsExpired(long expiryDate)
		{
			return Time.Now() > expiryDate;
		}

		/// <summary>check if a token is expired.</summary>
		/// <remarks>
		/// check if a token is expired. for unit test only. return true when token is
		/// expired, false otherwise
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal static bool IsTokenExpired(Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier
			> token)
		{
			ByteArrayInputStream buf = new ByteArrayInputStream(token.GetIdentifier());
			DataInputStream @in = new DataInputStream(buf);
			long expiryDate = WritableUtils.ReadVLong(@in);
			return IsExpired(expiryDate);
		}

		/// <summary>set token lifetime.</summary>
		public virtual void SetTokenLifetime(long tokenLifetime)
		{
			this.tokenLifetime = tokenLifetime;
		}

		/// <summary>Create an empty block token identifier</summary>
		/// <returns>a newly created empty block token identifier</returns>
		public override BlockTokenIdentifier CreateIdentifier()
		{
			return new BlockTokenIdentifier();
		}

		/// <summary>Create a new password/secret for the given block token identifier.</summary>
		/// <param name="identifier">the block token identifier</param>
		/// <returns>token password/secret</returns>
		protected override byte[] CreatePassword(BlockTokenIdentifier identifier)
		{
			BlockKey key = null;
			lock (this)
			{
				key = currentKey;
			}
			if (key == null)
			{
				throw new InvalidOperationException("currentKey hasn't been initialized.");
			}
			identifier.SetExpiryDate(Time.Now() + tokenLifetime);
			identifier.SetKeyId(key.GetKeyId());
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Generating block token for " + identifier.ToString());
			}
			return CreatePassword(identifier.GetBytes(), key.GetKey());
		}

		/// <summary>Look up the token password/secret for the given block token identifier.</summary>
		/// <param name="identifier">the block token identifier to look up</param>
		/// <returns>token password/secret as byte[]</returns>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public override byte[] RetrievePassword(BlockTokenIdentifier identifier)
		{
			if (IsExpired(identifier.GetExpiryDate()))
			{
				throw new SecretManager.InvalidToken("Block token with " + identifier.ToString() 
					+ " is expired.");
			}
			BlockKey key = null;
			lock (this)
			{
				key = allKeys[identifier.GetKeyId()];
			}
			if (key == null)
			{
				throw new SecretManager.InvalidToken("Can't re-compute password for " + identifier
					.ToString() + ", since the required block key (keyID=" + identifier.GetKeyId() +
					 ") doesn't exist.");
			}
			return CreatePassword(identifier.GetBytes(), key.GetKey());
		}

		/// <summary>
		/// Generate a data encryption key for this block pool, using the current
		/// BlockKey.
		/// </summary>
		/// <returns>
		/// a data encryption key which may be used to encrypt traffic
		/// over the DataTransferProtocol
		/// </returns>
		public virtual DataEncryptionKey GenerateDataEncryptionKey()
		{
			byte[] nonce = new byte[8];
			nonceGenerator.NextBytes(nonce);
			BlockKey key = null;
			lock (this)
			{
				key = currentKey;
			}
			byte[] encryptionKey = CreatePassword(nonce, key.GetKey());
			return new DataEncryptionKey(key.GetKeyId(), blockPoolId, nonce, encryptionKey, Time
				.Now() + tokenLifetime, encryptionAlgorithm);
		}

		/// <summary>Recreate an encryption key based on the given key id and nonce.</summary>
		/// <param name="keyId">identifier of the secret key used to generate the encryption key.
		/// 	</param>
		/// <param name="nonce">random value used to create the encryption key</param>
		/// <returns>the encryption key which corresponds to this (keyId, blockPoolId, nonce)
		/// 	</returns>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.InvalidEncryptionKeyException
		/// 	"/>
		public virtual byte[] RetrieveDataEncryptionKey(int keyId, byte[] nonce)
		{
			BlockKey key = null;
			lock (this)
			{
				key = allKeys[keyId];
				if (key == null)
				{
					throw new InvalidEncryptionKeyException("Can't re-compute encryption key" + " for nonce, since the required block key (keyID="
						 + keyId + ") doesn't exist. Current key: " + currentKey.GetKeyId());
				}
			}
			return CreatePassword(nonce, key.GetKey());
		}

		[VisibleForTesting]
		public virtual void SetKeyUpdateIntervalForTesting(long millis)
		{
			lock (this)
			{
				this.keyUpdateInterval = millis;
			}
		}

		[VisibleForTesting]
		public virtual void ClearAllKeysForTesting()
		{
			allKeys.Clear();
		}

		[VisibleForTesting]
		public virtual int GetSerialNoForTesting()
		{
			lock (this)
			{
				return serialNo;
			}
		}
	}
}
