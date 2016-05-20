using Sharpen;

namespace org.apache.hadoop.security.token.delegation
{
	/// <summary>Key used for generating and verifying delegation tokens</summary>
	public class DelegationKey : org.apache.hadoop.io.Writable
	{
		private int keyId;

		private long expiryDate;

		[org.apache.avro.reflect.Nullable]
		private byte[] keyBytes = null;

		private const int MAX_KEY_LEN = 1024 * 1024;

		/// <summary>Default constructore required for Writable</summary>
		public DelegationKey()
			: this(0, 0L, (javax.crypto.SecretKey)null)
		{
		}

		public DelegationKey(int keyId, long expiryDate, javax.crypto.SecretKey key)
			: this(keyId, expiryDate, key != null ? key.getEncoded() : null)
		{
		}

		public DelegationKey(int keyId, long expiryDate, byte[] encodedKey)
		{
			this.keyId = keyId;
			this.expiryDate = expiryDate;
			if (encodedKey != null)
			{
				if (encodedKey.Length > MAX_KEY_LEN)
				{
					throw new System.Exception("can't create " + encodedKey.Length + " byte long DelegationKey."
						);
				}
				this.keyBytes = encodedKey;
			}
		}

		public virtual int getKeyId()
		{
			return keyId;
		}

		public virtual long getExpiryDate()
		{
			return expiryDate;
		}

		public virtual javax.crypto.SecretKey getKey()
		{
			if (keyBytes == null || keyBytes.Length == 0)
			{
				return null;
			}
			else
			{
				javax.crypto.SecretKey key = org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager
					.createSecretKey(keyBytes);
				return key;
			}
		}

		public virtual byte[] getEncodedKey()
		{
			return keyBytes;
		}

		public virtual void setExpiryDate(long expiryDate)
		{
			this.expiryDate = expiryDate;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			org.apache.hadoop.io.WritableUtils.writeVInt(@out, keyId);
			org.apache.hadoop.io.WritableUtils.writeVLong(@out, expiryDate);
			if (keyBytes == null)
			{
				org.apache.hadoop.io.WritableUtils.writeVInt(@out, -1);
			}
			else
			{
				org.apache.hadoop.io.WritableUtils.writeVInt(@out, keyBytes.Length);
				@out.write(keyBytes);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			keyId = org.apache.hadoop.io.WritableUtils.readVInt(@in);
			expiryDate = org.apache.hadoop.io.WritableUtils.readVLong(@in);
			int len = org.apache.hadoop.io.WritableUtils.readVIntInRange(@in, -1, MAX_KEY_LEN
				);
			if (len == -1)
			{
				keyBytes = null;
			}
			else
			{
				keyBytes = new byte[len];
				@in.readFully(keyBytes);
			}
		}

		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + (int)(expiryDate ^ ((long)(((ulong)expiryDate) >> 32)));
			result = prime * result + java.util.Arrays.hashCode(keyBytes);
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
				if (right == null || Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject
					(right))
				{
					return false;
				}
				else
				{
					org.apache.hadoop.security.token.delegation.DelegationKey r = (org.apache.hadoop.security.token.delegation.DelegationKey
						)right;
					return keyId == r.keyId && expiryDate == r.expiryDate && java.util.Arrays.equals(
						keyBytes, r.keyBytes);
				}
			}
		}
	}
}
