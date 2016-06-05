using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security.Token.Block
{
	/// <summary>
	/// Manages a
	/// <see cref="BlockTokenSecretManager"/>
	/// per block pool. Routes the requests
	/// given a block pool Id to corresponding
	/// <see cref="BlockTokenSecretManager"/>
	/// </summary>
	public class BlockPoolTokenSecretManager : SecretManager<BlockTokenIdentifier>
	{
		private readonly IDictionary<string, BlockTokenSecretManager> map = new Dictionary
			<string, BlockTokenSecretManager>();

		/// <summary>
		/// Add a block pool Id and corresponding
		/// <see cref="BlockTokenSecretManager"/>
		/// to map
		/// </summary>
		/// <param name="bpid">block pool Id</param>
		/// <param name="secretMgr">
		/// 
		/// <see cref="BlockTokenSecretManager"/>
		/// </param>
		public virtual void AddBlockPool(string bpid, BlockTokenSecretManager secretMgr)
		{
			lock (this)
			{
				map[bpid] = secretMgr;
			}
		}

		internal virtual BlockTokenSecretManager Get(string bpid)
		{
			lock (this)
			{
				BlockTokenSecretManager secretMgr = map[bpid];
				if (secretMgr == null)
				{
					throw new ArgumentException("Block pool " + bpid + " is not found");
				}
				return secretMgr;
			}
		}

		public virtual bool IsBlockPoolRegistered(string bpid)
		{
			lock (this)
			{
				return map.Contains(bpid);
			}
		}

		/// <summary>Return an empty BlockTokenIdentifer</summary>
		public override BlockTokenIdentifier CreateIdentifier()
		{
			return new BlockTokenIdentifier();
		}

		protected override byte[] CreatePassword(BlockTokenIdentifier identifier)
		{
			return Get(identifier.GetBlockPoolId()).CreatePassword(identifier);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public override byte[] RetrievePassword(BlockTokenIdentifier identifier)
		{
			return Get(identifier.GetBlockPoolId()).RetrievePassword(identifier);
		}

		/// <summary>
		/// See
		/// <see cref="BlockTokenSecretManager.CheckAccess(BlockTokenIdentifier, string, Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, AccessMode)
		/// 	"/>
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public virtual void CheckAccess(BlockTokenIdentifier id, string userId, ExtendedBlock
			 block, BlockTokenSecretManager.AccessMode mode)
		{
			Get(block.GetBlockPoolId()).CheckAccess(id, userId, block, mode);
		}

		/// <summary>
		/// See
		/// <see cref="BlockTokenSecretManager.CheckAccess(Org.Apache.Hadoop.Security.Token.Token{T}, string, Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, AccessMode)
		/// 	"/>
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		public virtual void CheckAccess(Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier
			> token, string userId, ExtendedBlock block, BlockTokenSecretManager.AccessMode 
			mode)
		{
			Get(block.GetBlockPoolId()).CheckAccess(token, userId, block, mode);
		}

		/// <summary>
		/// See
		/// <see cref="BlockTokenSecretManager.AddKeys(ExportedBlockKeys)"/>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void AddKeys(string bpid, ExportedBlockKeys exportedKeys)
		{
			Get(bpid).AddKeys(exportedKeys);
		}

		/// <summary>
		/// See
		/// <see cref="BlockTokenSecretManager.GenerateToken(Org.Apache.Hadoop.Hdfs.Protocol.ExtendedBlock, Sharpen.EnumSet{E})
		/// 	"/>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> GenerateToken
			(ExtendedBlock b, EnumSet<BlockTokenSecretManager.AccessMode> of)
		{
			return Get(b.GetBlockPoolId()).GenerateToken(b, of);
		}

		[VisibleForTesting]
		public virtual void ClearAllKeysForTesting()
		{
			foreach (BlockTokenSecretManager btsm in map.Values)
			{
				btsm.ClearAllKeysForTesting();
			}
		}

		public virtual DataEncryptionKey GenerateDataEncryptionKey(string blockPoolId)
		{
			return Get(blockPoolId).GenerateDataEncryptionKey();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual byte[] RetrieveDataEncryptionKey(int keyId, string blockPoolId, byte
			[] nonce)
		{
			return Get(blockPoolId).RetrieveDataEncryptionKey(keyId, nonce);
		}
	}
}
