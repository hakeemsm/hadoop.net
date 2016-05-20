using Sharpen;

namespace org.apache.hadoop.io.file.tfile
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
		public sealed class BytesComparator : java.util.Comparator<org.apache.hadoop.io.file.tfile.RawComparable
			>
		{
			private org.apache.hadoop.io.RawComparator<object> cmp;

			public BytesComparator(org.apache.hadoop.io.RawComparator<object> cmp)
			{
				// nothing
				this.cmp = cmp;
			}

			public int compare(org.apache.hadoop.io.file.tfile.RawComparable o1, org.apache.hadoop.io.file.tfile.RawComparable
				 o2)
			{
				return compare(o1.buffer(), o1.offset(), o1.size(), o2.buffer(), o2.offset(), o2.
					size());
			}

			public int compare(byte[] a, int off1, int len1, byte[] b, int off2, int len2)
			{
				return cmp.compare(a, off1, len1, b, off2, len2);
			}
		}

		/// <summary>Interface for all objects that has a single integer magnitude.</summary>
		internal interface Scalar
		{
			long magnitude();
		}

		internal sealed class ScalarLong : org.apache.hadoop.io.file.tfile.CompareUtils.Scalar
		{
			private long magnitude;

			public ScalarLong(long m)
			{
				magnitude = m;
			}

			public long magnitude()
			{
				return magnitude;
			}
		}

		[System.Serializable]
		public sealed class ScalarComparator : java.util.Comparator<org.apache.hadoop.io.file.tfile.CompareUtils.Scalar
			>
		{
			public int compare(org.apache.hadoop.io.file.tfile.CompareUtils.Scalar o1, org.apache.hadoop.io.file.tfile.CompareUtils.Scalar
				 o2)
			{
				long diff = o1.magnitude() - o2.magnitude();
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
		public sealed class MemcmpRawComparator : org.apache.hadoop.io.RawComparator<object
			>
		{
			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				return org.apache.hadoop.io.WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2
					);
			}

			public int compare(object o1, object o2)
			{
				throw new System.Exception("Object comparison not supported");
			}
		}
	}
}
