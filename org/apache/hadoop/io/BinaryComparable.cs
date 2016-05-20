using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>
	/// Interface supported by
	/// <see cref="WritableComparable{T}"/>
	/// types supporting ordering/permutation by a representative set of bytes.
	/// </summary>
	public abstract class BinaryComparable : java.lang.Comparable<org.apache.hadoop.io.BinaryComparable
		>
	{
		/// <summary>Return n st bytes 0..n-1 from {#getBytes()} are valid.</summary>
		public abstract int getLength();

		/// <summary>Return representative byte array for this instance.</summary>
		public abstract byte[] getBytes();

		/// <summary>Compare bytes from {#getBytes()}.</summary>
		/// <seealso cref="WritableComparator.compareBytes(byte[], int, int, byte[], int, int)
		/// 	"/>
		public virtual int compareTo(org.apache.hadoop.io.BinaryComparable other)
		{
			if (this == other)
			{
				return 0;
			}
			return org.apache.hadoop.io.WritableComparator.compareBytes(getBytes(), 0, getLength
				(), other.getBytes(), 0, other.getLength());
		}

		/// <summary>Compare bytes from {#getBytes()} to those provided.</summary>
		public virtual int compareTo(byte[] other, int off, int len)
		{
			return org.apache.hadoop.io.WritableComparator.compareBytes(getBytes(), 0, getLength
				(), other, off, len);
		}

		/// <summary>Return true if bytes from {#getBytes()} match.</summary>
		public override bool Equals(object other)
		{
			if (!(other is org.apache.hadoop.io.BinaryComparable))
			{
				return false;
			}
			org.apache.hadoop.io.BinaryComparable that = (org.apache.hadoop.io.BinaryComparable
				)other;
			if (this.getLength() != that.getLength())
			{
				return false;
			}
			return this.compareTo(that) == 0;
		}

		/// <summary>Return a hash of the bytes returned from {#getBytes()}.</summary>
		/// <seealso cref="WritableComparator.hashBytes(byte[], int)"/>
		public override int GetHashCode()
		{
			return org.apache.hadoop.io.WritableComparator.hashBytes(getBytes(), getLength());
		}
	}
}
