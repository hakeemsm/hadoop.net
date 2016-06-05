using System;
using System.IO;
using System.Text;
using Hadoop.Common.Core.IO;


namespace Org.Apache.Hadoop.IO
{
	public class RandomDatum : IWritableComparable<Org.Apache.Hadoop.IO.RandomDatum>
	{
		private int length;

		private byte[] data;

		public RandomDatum()
		{
		}

		public RandomDatum(Random random)
		{
			length = 10 + (int)Math.Pow(10.0, random.NextFloat() * 3.0);
			data = new byte[length];
			random.NextBytes(data);
		}

		public virtual int GetLength()
		{
			return length;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter @out)
		{
			@out.WriteInt(length);
			@out.Write(data);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			length = @in.ReadInt();
			if (data == null || length > data.Length)
			{
				data = new byte[length];
			}
			@in.ReadFully(data, 0, length);
		}

		public virtual int CompareTo(Org.Apache.Hadoop.IO.RandomDatum o)
		{
			return WritableComparator.CompareBytes(this.data, 0, this.length, o.data, 0, o.length
				);
		}

		public override bool Equals(object o)
		{
			return CompareTo((Org.Apache.Hadoop.IO.RandomDatum)o) == 0;
		}

		public override int GetHashCode()
		{
			return Arrays.HashCode(this.data);
		}

		private static readonly char[] HexDigits = new char[] { '0', '1', '2', '3', '4', 
			'5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

		/// <summary>Returns a string representation of this object.</summary>
		public override string ToString()
		{
			StringBuilder buf = new StringBuilder(length * 2);
			for (int i = 0; i < length; i++)
			{
				int b = data[i];
				buf.Append(HexDigits[(b >> 4) & unchecked((int)(0xf))]);
				buf.Append(HexDigits[b & unchecked((int)(0xf))]);
			}
			return buf.ToString();
		}

		public class Generator
		{
			internal Random random;

			private RandomDatum key;

			private RandomDatum value;

			public Generator()
			{
				random = new Random();
			}

			public Generator(int seed)
			{
				random = new Random(seed);
			}

			public virtual RandomDatum GetKey()
			{
				return key;
			}

			public virtual RandomDatum GetValue()
			{
				return value;
			}

			public virtual void Next()
			{
				key = new RandomDatum(random);
				value = new RandomDatum(random);
			}
		}

		/// <summary>A WritableComparator optimized for RandomDatum.</summary>
		public class Comparator : WritableComparator
		{
			public Comparator()
				: base(typeof(RandomDatum))
			{
			}

			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				int n1 = ReadInt(b1, s1);
				int n2 = ReadInt(b2, s2);
				return CompareBytes(b1, s1 + 4, n1, b2, s2 + 4, n2);
			}
		}
	}
}
