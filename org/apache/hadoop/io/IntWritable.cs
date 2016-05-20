using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A WritableComparable for ints.</summary>
	public class IntWritable : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.IntWritable
		>
	{
		private int value;

		public IntWritable()
		{
		}

		public IntWritable(int value)
		{
			set(value);
		}

		/// <summary>Set the value of this IntWritable.</summary>
		public virtual void set(int value)
		{
			this.value = value;
		}

		/// <summary>Return the value of this IntWritable.</summary>
		public virtual int get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			value = @in.readInt();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeInt(value);
		}

		/// <summary>Returns true iff <code>o</code> is a IntWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is org.apache.hadoop.io.IntWritable))
			{
				return false;
			}
			org.apache.hadoop.io.IntWritable other = (org.apache.hadoop.io.IntWritable)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return value;
		}

		/// <summary>Compares two IntWritables.</summary>
		public virtual int compareTo(org.apache.hadoop.io.IntWritable o)
		{
			int thisValue = this.value;
			int thatValue = o.value;
			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
		}

		public override string ToString()
		{
			return int.toString(value);
		}

		/// <summary>A Comparator optimized for IntWritable.</summary>
		public class Comparator : org.apache.hadoop.io.WritableComparator
		{
			public Comparator()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable)))
			{
			}

			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				int thisValue = readInt(b1, s1);
				int thatValue = readInt(b2, s2);
				return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
			}
		}

		static IntWritable()
		{
			// register this comparator
			org.apache.hadoop.io.WritableComparator.define(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.IntWritable)), new org.apache.hadoop.io.IntWritable.Comparator
				());
		}
	}
}
