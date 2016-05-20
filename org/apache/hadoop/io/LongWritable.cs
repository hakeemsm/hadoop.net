using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A WritableComparable for longs.</summary>
	public class LongWritable : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.LongWritable
		>
	{
		private long value;

		public LongWritable()
		{
		}

		public LongWritable(long value)
		{
			set(value);
		}

		/// <summary>Set the value of this LongWritable.</summary>
		public virtual void set(long value)
		{
			this.value = value;
		}

		/// <summary>Return the value of this LongWritable.</summary>
		public virtual long get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			value = @in.readLong();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeLong(value);
		}

		/// <summary>Returns true iff <code>o</code> is a LongWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is org.apache.hadoop.io.LongWritable))
			{
				return false;
			}
			org.apache.hadoop.io.LongWritable other = (org.apache.hadoop.io.LongWritable)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return (int)value;
		}

		/// <summary>Compares two LongWritables.</summary>
		public virtual int compareTo(org.apache.hadoop.io.LongWritable o)
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
		public class Comparator : org.apache.hadoop.io.WritableComparator
		{
			public Comparator()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable))
					)
			{
			}

			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				long thisValue = readLong(b1, s1);
				long thatValue = readLong(b2, s2);
				return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
			}
		}

		/// <summary>A decreasing Comparator optimized for LongWritable.</summary>
		public class DecreasingComparator : org.apache.hadoop.io.LongWritable.Comparator
		{
			public override int compare(org.apache.hadoop.io.WritableComparable a, org.apache.hadoop.io.WritableComparable
				 b)
			{
				return base.compare(b, a);
			}

			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				return base.compare(b2, s2, l2, b1, s1, l1);
			}
		}

		static LongWritable()
		{
			// register default comparator
			org.apache.hadoop.io.WritableComparator.define(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.LongWritable)), new org.apache.hadoop.io.LongWritable.Comparator
				());
		}
	}
}
