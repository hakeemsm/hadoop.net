using System;
using Org.Apache.Hadoop.IO;

namespace Hadoop.Common.Core.IO
{
	/// <summary>
	/// Interface supported by
	/// <see cref="IWritableComparable{T}"/>
	/// types supporting ordering/permutation by a representative set of bytes.
	/// </summary>
	public abstract class BinaryComparable : IComparable<BinaryComparable>
	{
        /// <summary>Return n st bytes 0..n-1 from {#getBytes()} are valid.</summary>
        public abstract int Length { get; }

        /// <summary>Return representative byte array for this instance.</summary>
        public abstract byte[] Bytes { get; }

        /// <summary>Compare bytes from {#getBytes()}.</summary>
        /// <seealso cref="WritableComparator.CompareBytes(byte[], int, int, byte[], int, int)
        /// 	"/>
        public virtual int CompareTo(BinaryComparable other)
		{
			if (this == other)
			{
				return 0;
			}
			return WritableComparator.CompareBytes(Bytes, 0, Length, other.Bytes, 0, other.Length);
		}

		/// <summary>Compare bytes from {#getBytes()} to those provided.</summary>
		public virtual int CompareTo(byte[] other, int off, int len)
		{
			return WritableComparator.CompareBytes(Bytes, 0, Length, other, off, len);
		}

		/// <summary>Return true if bytes from {#getBytes()} match.</summary>
		public override bool Equals(object other)
		{
		    BinaryComparable that = other as BinaryComparable;
			if (this.Length!= that?.Length)
			{
				return false;
			}
			return this.CompareTo(that) == 0;
		}

		/// <summary>Return a hash of the bytes returned from {#getBytes()}.</summary>
		/// <seealso cref="WritableComparator.HashBytes(byte[], int)"/>
		public override int GetHashCode()
		{
			return WritableComparator.HashBytes(Bytes, Length);
		}
	}
}
