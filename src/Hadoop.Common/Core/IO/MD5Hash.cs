using System;
using System.IO;
using System.Text;
using Hadoop.Common.Core.IO;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>A Writable for MD5 hash values.</summary>
	public class MD5Hash : IWritableComparable<Org.Apache.Hadoop.IO.MD5Hash>
	{
		public const int Md5Len = 16;

		private sealed class _ThreadLocal_38 : ThreadLocal<MessageDigest>
		{
			public _ThreadLocal_38()
			{
			}

			protected override MessageDigest InitialValue()
			{
				try
				{
					return MessageDigest.GetInstance("MD5");
				}
				catch (NoSuchAlgorithmException e)
				{
					throw new RuntimeException(e);
				}
			}
		}

		private static ThreadLocal<MessageDigest> DigesterFactory = new _ThreadLocal_38();

		private byte[] digest;

		/// <summary>Constructs an MD5Hash.</summary>
		public MD5Hash()
		{
			this.digest = new byte[Md5Len];
		}

		/// <summary>Constructs an MD5Hash from a hex string.</summary>
		public MD5Hash(string hex)
		{
			SetDigest(hex);
		}

		/// <summary>Constructs an MD5Hash with a specified value.</summary>
		public MD5Hash(byte[] digest)
		{
			if (digest.Length != Md5Len)
			{
				throw new ArgumentException("Wrong length: " + digest.Length);
			}
			this.digest = digest;
		}

		// javadoc from Writable
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader reader)
		{
			@in.ReadFully(digest);
		}

		/// <summary>Constructs, reads and returns an instance.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.IO.MD5Hash Read(BinaryReader reader)
		{
			Org.Apache.Hadoop.IO.MD5Hash result = new Org.Apache.Hadoop.IO.MD5Hash();
			result.ReadFields(@in);
			return result;
		}

		// javadoc from Writable
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter writer)
		{
			@out.Write(digest);
		}

		/// <summary>Copy the contents of another instance into this instance.</summary>
		public virtual void Set(Org.Apache.Hadoop.IO.MD5Hash that)
		{
			System.Array.Copy(that.digest, 0, this.digest, 0, Md5Len);
		}

		/// <summary>Returns the digest bytes.</summary>
		public virtual byte[] GetDigest()
		{
			return digest;
		}

		/// <summary>Construct a hash value for a byte array.</summary>
		public static Org.Apache.Hadoop.IO.MD5Hash Digest(byte[] data)
		{
			return Digest(data, 0, data.Length);
		}

		/// <summary>Create a thread local MD5 digester</summary>
		public static MessageDigest GetDigester()
		{
			MessageDigest digester = DigesterFactory.Get();
			digester.Reset();
			return digester;
		}

		/// <summary>Construct a hash value for the content from the InputStream.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.IO.MD5Hash Digest(InputStream @in)
		{
			byte[] buffer = new byte[4 * 1024];
			MessageDigest digester = GetDigester();
			for (int n; (n = @in.Read(buffer)) != -1; )
			{
				digester.Update(buffer, 0, n);
			}
			return new Org.Apache.Hadoop.IO.MD5Hash(digester.Digest());
		}

		/// <summary>Construct a hash value for a byte array.</summary>
		public static Org.Apache.Hadoop.IO.MD5Hash Digest(byte[] data, int start, int len
			)
		{
			byte[] digest;
			MessageDigest digester = GetDigester();
			digester.Update(data, start, len);
			digest = digester.Digest();
			return new Org.Apache.Hadoop.IO.MD5Hash(digest);
		}

		/// <summary>Construct a hash value for a String.</summary>
		public static Org.Apache.Hadoop.IO.MD5Hash Digest(string @string)
		{
			return Digest(UTF8.GetBytes(@string));
		}

		/// <summary>Construct a hash value for a String.</summary>
		public static Org.Apache.Hadoop.IO.MD5Hash Digest(UTF8 utf8)
		{
			return Digest(utf8.GetBytes(), 0, utf8.GetLength());
		}

		/// <summary>Construct a half-sized version of this MD5.</summary>
		/// <remarks>Construct a half-sized version of this MD5.  Fits in a long</remarks>
		public virtual long HalfDigest()
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
		public virtual int QuarterDigest()
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
			if (!(o is Org.Apache.Hadoop.IO.MD5Hash))
			{
				return false;
			}
			Org.Apache.Hadoop.IO.MD5Hash other = (Org.Apache.Hadoop.IO.MD5Hash)o;
			return Arrays.Equals(this.digest, other.digest);
		}

		/// <summary>Returns a hash code value for this object.</summary>
		/// <remarks>
		/// Returns a hash code value for this object.
		/// Only uses the first 4 bytes, since md5s are evenly distributed.
		/// </remarks>
		public override int GetHashCode()
		{
			return QuarterDigest();
		}

		/// <summary>Compares this object with the specified object for order.</summary>
		public virtual int CompareTo(Org.Apache.Hadoop.IO.MD5Hash that)
		{
			return WritableComparator.CompareBytes(this.digest, 0, Md5Len, that.digest, 0, Md5Len
				);
		}

		/// <summary>A WritableComparator optimized for MD5Hash keys.</summary>
		public class Comparator : WritableComparator
		{
			public Comparator()
				: base(typeof(MD5Hash))
			{
			}

			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				return CompareBytes(b1, s1, Md5Len, b2, s2, Md5Len);
			}
		}

		static MD5Hash()
		{
			// register this comparator
			WritableComparator.Define(typeof(MD5Hash), new MD5Hash.Comparator());
		}

		private static readonly char[] HexDigits = new char[] { '0', '1', '2', '3', '4', 
			'5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

		/// <summary>Returns a string representation of this object.</summary>
		public override string ToString()
		{
			StringBuilder buf = new StringBuilder(Md5Len * 2);
			for (int i = 0; i < Md5Len; i++)
			{
				int b = digest[i];
				buf.Append(HexDigits[(b >> 4) & unchecked((int)(0xf))]);
				buf.Append(HexDigits[b & unchecked((int)(0xf))]);
			}
			return buf.ToString();
		}

		/// <summary>Sets the digest value from a hex string.</summary>
		public virtual void SetDigest(string hex)
		{
			if (hex.Length != Md5Len * 2)
			{
				throw new ArgumentException("Wrong length: " + hex.Length);
			}
			byte[] digest = new byte[Md5Len];
			for (int i = 0; i < Md5Len; i++)
			{
				int j = i << 1;
				digest[i] = unchecked((byte)(CharToNibble(hex[j]) << 4 | CharToNibble(hex[j + 1])
					));
			}
			this.digest = digest;
		}

		private static int CharToNibble(char c)
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
						throw new RuntimeException("Not a hex character: " + c);
					}
				}
			}
		}
	}
}
