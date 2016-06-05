using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security.Token.Block
{
	public class BlockTokenIdentifier : TokenIdentifier
	{
		internal static readonly Text KindName = new Text("HDFS_BLOCK_TOKEN");

		private long expiryDate;

		private int keyId;

		private string userId;

		private string blockPoolId;

		private long blockId;

		private readonly EnumSet<BlockTokenSecretManager.AccessMode> modes;

		private byte[] cache;

		public BlockTokenIdentifier()
			: this(null, null, 0, EnumSet.NoneOf<BlockTokenSecretManager.AccessMode>())
		{
		}

		public BlockTokenIdentifier(string userId, string bpid, long blockId, EnumSet<BlockTokenSecretManager.AccessMode
			> modes)
		{
			this.cache = null;
			this.userId = userId;
			this.blockPoolId = bpid;
			this.blockId = blockId;
			this.modes = modes == null ? EnumSet.NoneOf<BlockTokenSecretManager.AccessMode>()
				 : modes;
		}

		public override Text GetKind()
		{
			return KindName;
		}

		public override UserGroupInformation GetUser()
		{
			if (userId == null || string.Empty.Equals(userId))
			{
				string user = blockPoolId + ":" + System.Convert.ToString(blockId);
				return UserGroupInformation.CreateRemoteUser(user);
			}
			return UserGroupInformation.CreateRemoteUser(userId);
		}

		public virtual long GetExpiryDate()
		{
			return expiryDate;
		}

		public virtual void SetExpiryDate(long expiryDate)
		{
			this.cache = null;
			this.expiryDate = expiryDate;
		}

		public virtual int GetKeyId()
		{
			return this.keyId;
		}

		public virtual void SetKeyId(int keyId)
		{
			this.cache = null;
			this.keyId = keyId;
		}

		public virtual string GetUserId()
		{
			return userId;
		}

		public virtual string GetBlockPoolId()
		{
			return blockPoolId;
		}

		public virtual long GetBlockId()
		{
			return blockId;
		}

		public virtual EnumSet<BlockTokenSecretManager.AccessMode> GetAccessModes()
		{
			return modes;
		}

		public override string ToString()
		{
			return "block_token_identifier (expiryDate=" + this.GetExpiryDate() + ", keyId=" 
				+ this.GetKeyId() + ", userId=" + this.GetUserId() + ", blockPoolId=" + this.GetBlockPoolId
				() + ", blockId=" + this.GetBlockId() + ", access modes=" + this.GetAccessModes(
				) + ")";
		}

		internal static bool IsEqual(object a, object b)
		{
			return a == null ? b == null : a.Equals(b);
		}

		public override bool Equals(object obj)
		{
			if (obj == this)
			{
				return true;
			}
			if (obj is Org.Apache.Hadoop.Hdfs.Security.Token.Block.BlockTokenIdentifier)
			{
				Org.Apache.Hadoop.Hdfs.Security.Token.Block.BlockTokenIdentifier that = (Org.Apache.Hadoop.Hdfs.Security.Token.Block.BlockTokenIdentifier
					)obj;
				return this.expiryDate == that.expiryDate && this.keyId == that.keyId && IsEqual(
					this.userId, that.userId) && IsEqual(this.blockPoolId, that.blockPoolId) && this
					.blockId == that.blockId && IsEqual(this.modes, that.modes);
			}
			return false;
		}

		public override int GetHashCode()
		{
			return (int)expiryDate ^ keyId ^ (int)blockId ^ modes.GetHashCode() ^ (userId == 
				null ? 0 : userId.GetHashCode()) ^ (blockPoolId == null ? 0 : blockPoolId.GetHashCode
				());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void ReadFields(DataInput @in)
		{
			this.cache = null;
			expiryDate = WritableUtils.ReadVLong(@in);
			keyId = WritableUtils.ReadVInt(@in);
			userId = WritableUtils.ReadString(@in);
			blockPoolId = WritableUtils.ReadString(@in);
			blockId = WritableUtils.ReadVLong(@in);
			int length = WritableUtils.ReadVIntInRange(@in, 0, typeof(BlockTokenSecretManager.AccessMode
				).GetEnumConstants().Length);
			for (int i = 0; i < length; i++)
			{
				modes.AddItem(WritableUtils.ReadEnum<BlockTokenSecretManager.AccessMode>(@in));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(DataOutput @out)
		{
			WritableUtils.WriteVLong(@out, expiryDate);
			WritableUtils.WriteVInt(@out, keyId);
			WritableUtils.WriteString(@out, userId);
			WritableUtils.WriteString(@out, blockPoolId);
			WritableUtils.WriteVLong(@out, blockId);
			WritableUtils.WriteVInt(@out, modes.Count);
			foreach (BlockTokenSecretManager.AccessMode aMode in modes)
			{
				WritableUtils.WriteEnum(@out, aMode);
			}
		}

		public override byte[] GetBytes()
		{
			if (cache == null)
			{
				cache = base.GetBytes();
			}
			return cache;
		}

		public class Renewer : Token.TrivialRenewer
		{
			protected override Text GetKind()
			{
				return KindName;
			}
		}
	}
}
