using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// Interface supported by
	/// <see cref="WritableComparable{T}"/>
	/// types supporting ordering/permutation by a representative set of bytes.
	/// </summary>
	public abstract class BinaryComparable : Comparable<BinaryComparable>
	{
		/// <summary>Return n st bytes 0..n-1 from {#getBytes()} are valid.</summary>
		public abstract int GetLength();

		/// <summary>Return representative byte array for this instance.</summary>
		public abstract byte[] GetBytes();

		/// <summary>Compare bytes from {#getBytes()}.</summary>
		/// <seealso cref="WritableComparator.CompareBytes(byte[], int, int, byte[], int, int)
		/// 	"/>
		public virtual int CompareTo(BinaryComparable other)
		{
			if (this == other)
			{
				return 0;
			}
			return WritableComparator.CompareBytes(GetBytes(), 0, GetLength(), other.GetBytes
				(), 0, other.GetLength());
		}

		/// <summary>Compare bytes from {#getBytes()} to those provided.</summary>
		public virtual int CompareTo(byte[] other, int off, int len)
		{
			return WritableComparator.CompareBytes(GetBytes(), 0, GetLength(), other, off, len
				);
		}

		/// <summary>Return true if bytes from {#getBytes()} match.</summary>
		public override bool Equals(object other)
		{
			if (!(other is BinaryComparable))
			{
				return false;
			}
			BinaryComparable that = (BinaryComparable)other;
			if (this.GetLength() != that.GetLength())
			{
				return false;
			}
			return this.CompareTo(that) == 0;
		}

		/// <summary>Return a hash of the bytes returned from {#getBytes()}.</summary>
		/// <seealso cref="WritableComparator.HashBytes(byte[], int)"/>
		public override int GetHashCode()
		{
			return WritableComparator.HashBytes(GetBytes(), GetLength());
		}
	}
}
