using System.IO;
using Hadoop.Common.Core.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A WritableComparable for longs.</summary>
	public class LongWritable : IWritableComparable<Org.Apache.Hadoop.IO.LongWritable>
	{
		private long value;

		public LongWritable()
		{
		}

		public LongWritable(long value)
		{
			Set(value);
		}

		/// <summary>Set the value of this LongWritable.</summary>
		public virtual void Set(long value)
		{
			this.value = value;
		}

		/// <summary>Return the value of this LongWritable.</summary>
		public virtual long Get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			value = @in.ReadLong();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteLong(value);
		}

		/// <summary>Returns true iff <code>o</code> is a LongWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.IO.LongWritable))
			{
				return false;
			}
			Org.Apache.Hadoop.IO.LongWritable other = (Org.Apache.Hadoop.IO.LongWritable)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return (int)value;
		}

		/// <summary>Compares two LongWritables.</summary>
		public virtual int CompareTo(Org.Apache.Hadoop.IO.LongWritable o)
		{
			long thisValue = this.value;
			long thatValue = o.value;
			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
		}

		public override string ToString()
		{
			return System.Convert.ToString(value);
		}

		/// <summary>A Comparator optimized for LongWritable.</summary>
		public class Comparator : WritableComparator
		{
			public Comparator()
				: base(typeof(LongWritable))
			{
			}

			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				long thisValue = ReadLong(b1, s1);
				long thatValue = ReadLong(b2, s2);
				return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
			}
		}

		/// <summary>A decreasing Comparator optimized for LongWritable.</summary>
		public class DecreasingComparator : LongWritable.Comparator
		{
			public override int Compare(IWritableComparable<> a, IWritableComparable<> b)
			{
				return base.Compare(b, a);
			}

			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				return base.Compare(b2, s2, l2, b1, s1, l1);
			}
		}

		static LongWritable()
		{
			// register default comparator
			WritableComparator.Define(typeof(LongWritable), new LongWritable.Comparator());
		}
	}
}
