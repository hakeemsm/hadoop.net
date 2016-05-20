using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Writable for Double values.</summary>
	public class DoubleWritable : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.DoubleWritable
		>
	{
		private double value = 0.0;

		public DoubleWritable()
		{
		}

		public DoubleWritable(double value)
		{
			set(value);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			value = @in.readDouble();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeDouble(value);
		}

		public virtual void set(double value)
		{
			this.value = value;
		}

		public virtual double get()
		{
			return value;
		}

		/// <summary>Returns true iff <code>o</code> is a DoubleWritable with the same value.
		/// 	</summary>
		public override bool Equals(object o)
		{
			if (!(o is org.apache.hadoop.io.DoubleWritable))
			{
				return false;
			}
			org.apache.hadoop.io.DoubleWritable other = (org.apache.hadoop.io.DoubleWritable)
				o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return (int)double.doubleToLongBits(value);
		}

		public virtual int compareTo(org.apache.hadoop.io.DoubleWritable o)
		{
			return (value < o.value ? -1 : (value == o.value ? 0 : 1));
		}

		public override string ToString()
		{
			return double.toString(value);
		}

		/// <summary>A Comparator optimized for DoubleWritable.</summary>
		public class Comparator : org.apache.hadoop.io.WritableComparator
		{
			public Comparator()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.DoubleWritable
					)))
			{
			}

			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				double thisValue = readDouble(b1, s1);
				double thatValue = readDouble(b2, s2);
				return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
			}
		}

		static DoubleWritable()
		{
			// register this comparator
			org.apache.hadoop.io.WritableComparator.define(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.DoubleWritable)), new org.apache.hadoop.io.DoubleWritable.Comparator
				());
		}
	}
}
