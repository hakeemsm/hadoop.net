using System.IO;
using Org.Apache.Avro.Reflect;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token.Delegation
{
	/// <summary>Key used for generating and verifying delegation tokens</summary>
	public class DelegationKey : Writable
	{
		private int keyId;

		private long expiryDate;

		[Nullable]
		private byte[] keyBytes = null;

		private const int MaxKeyLen = 1024 * 1024;

		/// <summary>Default constructore required for Writable</summary>
		public DelegationKey()
			: this(0, 0L, (SecretKey)null)
		{
		}

		public DelegationKey(int keyId, long expiryDate, SecretKey key)
			: this(keyId, expiryDate, key != null ? key.GetEncoded() : null)
		{
		}

		public DelegationKey(int keyId, long expiryDate, byte[] encodedKey)
		{
			this.keyId = keyId;
			this.expiryDate = expiryDate;
			if (encodedKey != null)
			{
				if (encodedKey.Length > MaxKeyLen)
				{
					throw new RuntimeException("can't create " + encodedKey.Length + " byte long DelegationKey."
						);
				}
				this.keyBytes = encodedKey;
			}
		}

		public virtual int GetKeyId()
		{
			return keyId;
		}

		public virtual long GetExpiryDate()
		{
			return expiryDate;
		}

		public virtual SecretKey GetKey()
		{
			if (keyBytes == null || keyBytes.Length == 0)
			{
				return null;
			}
			else
			{
				SecretKey key = AbstractDelegationTokenSecretManager.CreateSecretKey(keyBytes);
				return key;
			}
		}

		public virtual byte[] GetEncodedKey()
		{
			return keyBytes;
		}

		public virtual void SetExpiryDate(long expiryDate)
		{
			this.expiryDate = expiryDate;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			WritableUtils.WriteVInt(@out, keyId);
			WritableUtils.WriteVLong(@out, expiryDate);
			if (keyBytes == null)
			{
				WritableUtils.WriteVInt(@out, -1);
			}
			else
			{
				WritableUtils.WriteVInt(@out, keyBytes.Length);
				@out.Write(keyBytes);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			keyId = WritableUtils.ReadVInt(@in);
			expiryDate = WritableUtils.ReadVLong(@in);
			int len = WritableUtils.ReadVIntInRange(@in, -1, MaxKeyLen);
			if (len == -1)
			{
				keyBytes = null;
			}
			else
			{
				keyBytes = new byte[len];
				@in.ReadFully(keyBytes);
			}
		}

		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + (int)(expiryDate ^ ((long)(((ulong)expiryDate) >> 32)));
			result = prime * result + Arrays.HashCode(keyBytes);
			result = prime * result + keyId;
			return result;
		}

		public override bool Equals(object right)
		{
			if (this == right)
			{
				return true;
			}
			else
			{
				if (right == null || GetType() != right.GetType())
				{
					return false;
				}
				else
				{
					Org.Apache.Hadoop.Security.Token.Delegation.DelegationKey r = (Org.Apache.Hadoop.Security.Token.Delegation.DelegationKey
						)right;
					return keyId == r.keyId && expiryDate == r.expiryDate && Arrays.Equals(keyBytes, 
						r.keyBytes);
				}
			}
		}
	}
}
