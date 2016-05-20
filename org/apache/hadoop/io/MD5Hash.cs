using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A Writable for MD5 hash values.</summary>
	public class MD5Hash : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.MD5Hash
		>
	{
		public const int MD5_LEN = 16;

		private sealed class _ThreadLocal_38 : java.lang.ThreadLocal<java.security.MessageDigest
			>
		{
			public _ThreadLocal_38()
			{
			}

			protected override java.security.MessageDigest initialValue()
			{
				try
				{
					return java.security.MessageDigest.getInstance("MD5");
				}
				catch (java.security.NoSuchAlgorithmException e)
				{
					throw new System.Exception(e);
				}
			}
		}

		private static java.lang.ThreadLocal<java.security.MessageDigest> DIGESTER_FACTORY
			 = new _ThreadLocal_38();

		private byte[] digest;

		/// <summary>Constructs an MD5Hash.</summary>
		public MD5Hash()
		{
			this.digest = new byte[MD5_LEN];
		}

		/// <summary>Constructs an MD5Hash from a hex string.</summary>
		public MD5Hash(string hex)
		{
			setDigest(hex);
		}

		/// <summary>Constructs an MD5Hash with a specified value.</summary>
		public MD5Hash(byte[] digest)
		{
			if (digest.Length != MD5_LEN)
			{
				throw new System.ArgumentException("Wrong length: " + digest.Length);
			}
			this.digest = digest;
		}

		// javadoc from Writable
		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			@in.readFully(digest);
		}

		/// <summary>Constructs, reads and returns an instance.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.io.MD5Hash read(java.io.DataInput @in)
		{
			org.apache.hadoop.io.MD5Hash result = new org.apache.hadoop.io.MD5Hash();
			result.readFields(@in);
			return result;
		}

		// javadoc from Writable
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.write(digest);
		}

		/// <summary>Copy the contents of another instance into this instance.</summary>
		public virtual void set(org.apache.hadoop.io.MD5Hash that)
		{
			System.Array.Copy(that.digest, 0, this.digest, 0, MD5_LEN);
		}

		/// <summary>Returns the digest bytes.</summary>
		public virtual byte[] getDigest()
		{
			return digest;
		}

		/// <summary>Construct a hash value for a byte array.</summary>
		public static org.apache.hadoop.io.MD5Hash digest(byte[] data)
		{
			return digest(data, 0, data.Length);
		}

		/// <summary>Create a thread local MD5 digester</summary>
		public static java.security.MessageDigest getDigester()
		{
			java.security.MessageDigest digester = DIGESTER_FACTORY.get();
			digester.reset();
			return digester;
		}

		/// <summary>Construct a hash value for the content from the InputStream.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.io.MD5Hash digest(java.io.InputStream @in)
		{
			byte[] buffer = new byte[4 * 1024];
			java.security.MessageDigest digester = getDigester();
			for (int n; (n = @in.read(buffer)) != -1; )
			{
				digester.update(buffer, 0, n);
			}
			return new org.apache.hadoop.io.MD5Hash(digester.digest());
		}

		/// <summary>Construct a hash value for a byte array.</summary>
		public static org.apache.hadoop.io.MD5Hash digest(byte[] data, int start, int len
			)
		{
			byte[] digest;
			java.security.MessageDigest digester = getDigester();
			digester.update(data, start, len);
			digest = digester.digest();
			return new org.apache.hadoop.io.MD5Hash(digest);
		}

		/// <summary>Construct a hash value for a String.</summary>
		public static org.apache.hadoop.io.MD5Hash digest(string @string)
		{
			return digest(org.apache.hadoop.io.UTF8.getBytes(@string));
		}

		/// <summary>Construct a hash value for a String.</summary>
		public static org.apache.hadoop.io.MD5Hash digest(org.apache.hadoop.io.UTF8 utf8)
		{
			return digest(utf8.getBytes(), 0, utf8.getLength());
		}

		/// <summary>Construct a half-sized version of this MD5.</summary>
		/// <remarks>Construct a half-sized version of this MD5.  Fits in a long</remarks>
		public virtual long halfDigest()
		{
			long value = 0;
			for (int i = 0; i < 8; i++)
			{
				value |= ((digest[i] & unchecked((long)(0xffL))) << (8 * (7 - i)));
			}
			return value;
		}

		/// <summary>Return a 32-bit digest of the MD5.</summary>
		/// <returns>the first 4 bytes of the md5</returns>
		public virtual int quarterDigest()
		{
			int value = 0;
			for (int i = 0; i < 4; i++)
			{
				value |= ((digest[i] & unchecked((int)(0xff))) << (8 * (3 - i)));
			}
			return value;
		}

		/// <summary>
		/// Returns true iff <code>o</code> is an MD5Hash whose digest contains the
		/// same values.
		/// </summary>
		public override bool Equals(object o)
		{
			if (!(o is org.apache.hadoop.io.MD5Hash))
			{
				return false;
			}
			org.apache.hadoop.io.MD5Hash other = (org.apache.hadoop.io.MD5Hash)o;
			return java.util.Arrays.equals(this.digest, other.digest);
		}

		/// <summary>Returns a hash code value for this object.</summary>
		/// <remarks>
		/// Returns a hash code value for this object.
		/// Only uses the first 4 bytes, since md5s are evenly distributed.
		/// </remarks>
		public override int GetHashCode()
		{
			return quarterDigest();
		}

		/// <summary>Compares this object with the specified object for order.</summary>
		public virtual int compareTo(org.apache.hadoop.io.MD5Hash that)
		{
			return org.apache.hadoop.io.WritableComparator.compareBytes(this.digest, 0, MD5_LEN
				, that.digest, 0, MD5_LEN);
		}

		/// <summary>A WritableComparator optimized for MD5Hash keys.</summary>
		public class Comparator : org.apache.hadoop.io.WritableComparator
		{
			public Comparator()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.MD5Hash)))
			{
			}

			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				return compareBytes(b1, s1, MD5_LEN, b2, s2, MD5_LEN);
			}
		}

		static MD5Hash()
		{
			// register this comparator
			org.apache.hadoop.io.WritableComparator.define(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.MD5Hash)), new org.apache.hadoop.io.MD5Hash.Comparator());
		}

		private static readonly char[] HEX_DIGITS = new char[] { '0', '1', '2', '3', '4', 
			'5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

		/// <summary>Returns a string representation of this object.</summary>
		public override string ToString()
		{
			java.lang.StringBuilder buf = new java.lang.StringBuilder(MD5_LEN * 2);
			for (int i = 0; i < MD5_LEN; i++)
			{
				int b = digest[i];
				buf.Append(HEX_DIGITS[(b >> 4) & unchecked((int)(0xf))]);
				buf.Append(HEX_DIGITS[b & unchecked((int)(0xf))]);
			}
			return buf.ToString();
		}

		/// <summary>Sets the digest value from a hex string.</summary>
		public virtual void setDigest(string hex)
		{
			if (hex.Length != MD5_LEN * 2)
			{
				throw new System.ArgumentException("Wrong length: " + hex.Length);
			}
			byte[] digest = new byte[MD5_LEN];
			for (int i = 0; i < MD5_LEN; i++)
			{
				int j = i << 1;
				digest[i] = unchecked((byte)(charToNibble(hex[j]) << 4 | charToNibble(hex[j + 1])
					));
			}
			this.digest = digest;
		}

		private static int charToNibble(char c)
		{
			if (c >= '0' && c <= '9')
			{
				return c - '0';
			}
			else
			{
				if (c >= 'a' && c <= 'f')
				{
					return unchecked((int)(0xa)) + (c - 'a');
				}
				else
				{
					if (c >= 'A' && c <= 'F')
					{
						return unchecked((int)(0xA)) + (c - 'A');
					}
					else
					{
						throw new System.Exception("Not a hex character: " + c);
					}
				}
			}
		}
	}
}
