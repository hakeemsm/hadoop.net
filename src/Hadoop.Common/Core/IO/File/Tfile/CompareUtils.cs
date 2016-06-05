using System.Collections.Generic;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.IO.File.Tfile
{
	internal class CompareUtils
	{
		/// <summary>Prevent the instantiation of class.</summary>
		private CompareUtils()
		{
		}

		/// <summary>
		/// A comparator to compare anything that implements
		/// <see cref="RawComparable"/>
		/// using a customized comparator.
		/// </summary>
		public sealed class BytesComparator : IComparer<RawComparable>
		{
			private RawComparator<object> cmp;

			public BytesComparator(RawComparator<object> cmp)
			{
				// nothing
				this.cmp = cmp;
			}

			public int Compare(RawComparable o1, RawComparable o2)
			{
				return Compare(o1.Buffer(), o1.Offset(), o1.Size(), o2.Buffer(), o2.Offset(), o2.
					Size());
			}

			public int Compare(byte[] a, int off1, int len1, byte[] b, int off2, int len2)
			{
				return cmp.Compare(a, off1, len1, b, off2, len2);
			}
		}

		/// <summary>Interface for all objects that has a single integer magnitude.</summary>
		internal interface Scalar
		{
			long Magnitude();
		}

		internal sealed class ScalarLong : CompareUtils.Scalar
		{
			private long magnitude;

			public ScalarLong(long m)
			{
				magnitude = m;
			}

			public long Magnitude()
			{
				return magnitude;
			}
		}

		[System.Serializable]
		public sealed class ScalarComparator : IComparer<CompareUtils.Scalar>
		{
			public int Compare(CompareUtils.Scalar o1, CompareUtils.Scalar o2)
			{
				long diff = o1.Magnitude() - o2.Magnitude();
				if (diff < 0)
				{
					return -1;
				}
				if (diff > 0)
				{
					return 1;
				}
				return 0;
			}
		}

		[System.Serializable]
		public sealed class MemcmpRawComparator : RawComparator<object>
		{
			public int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				return WritableComparator.CompareBytes(b1, s1, l1, b2, s2, l2);
			}

			public int Compare(object o1, object o2)
			{
				throw new RuntimeException("Object comparison not supported");
			}
		}
	}
}
