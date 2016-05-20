using Sharpen;

namespace org.apache.hadoop.io
{
	public class RandomDatum : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.RandomDatum
		>
	{
		private int length;

		private byte[] data;

		public RandomDatum()
		{
		}

		public RandomDatum(java.util.Random random)
		{
			length = 10 + (int)System.Math.pow(10.0, random.nextFloat() * 3.0);
			data = new byte[length];
			random.nextBytes(data);
		}

		public virtual int getLength()
		{
			return length;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeInt(length);
			@out.write(data);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			length = @in.readInt();
			if (data == null || length > data.Length)
			{
				data = new byte[length];
			}
			@in.readFully(data, 0, length);
		}

		public virtual int compareTo(org.apache.hadoop.io.RandomDatum o)
		{
			return org.apache.hadoop.io.WritableComparator.compareBytes(this.data, 0, this.length
				, o.data, 0, o.length);
		}

		public override bool Equals(object o)
		{
			return compareTo((org.apache.hadoop.io.RandomDatum)o) == 0;
		}

		public override int GetHashCode()
		{
			return java.util.Arrays.hashCode(this.data);
		}

		private static readonly char[] HEX_DIGITS = new char[] { '0', '1', '2', '3', '4', 
			'5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

		/// <summary>Returns a string representation of this object.</summary>
		public override string ToString()
		{
			java.lang.StringBuilder buf = new java.lang.StringBuilder(length * 2);
			for (int i = 0; i < length; i++)
			{
				int b = data[i];
				buf.Append(HEX_DIGITS[(b >> 4) & unchecked((int)(0xf))]);
				buf.Append(HEX_DIGITS[b & unchecked((int)(0xf))]);
			}
			return buf.ToString();
		}

		public class Generator
		{
			internal java.util.Random random;

			private org.apache.hadoop.io.RandomDatum key;

			private org.apache.hadoop.io.RandomDatum value;

			public Generator()
			{
				random = new java.util.Random();
			}

			public Generator(int seed)
			{
				random = new java.util.Random(seed);
			}

			public virtual org.apache.hadoop.io.RandomDatum getKey()
			{
				return key;
			}

			public virtual org.apache.hadoop.io.RandomDatum getValue()
			{
				return value;
			}

			public virtual void next()
			{
				key = new org.apache.hadoop.io.RandomDatum(random);
				value = new org.apache.hadoop.io.RandomDatum(random);
			}
		}

		/// <summary>A WritableComparator optimized for RandomDatum.</summary>
		public class Comparator : org.apache.hadoop.io.WritableComparator
		{
			public Comparator()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.RandomDatum)))
			{
			}

			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				int n1 = readInt(b1, s1);
				int n2 = readInt(b2, s2);
				return compareBytes(b1, s1 + 4, n1, b2, s2 + 4, n2);
			}
		}
	}
}
