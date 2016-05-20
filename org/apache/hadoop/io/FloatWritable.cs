using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A WritableComparable for floats.</summary>
	public class FloatWritable : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.FloatWritable
		>
	{
		private float value;

		public FloatWritable()
		{
		}

		public FloatWritable(float value)
		{
			set(value);
		}

		/// <summary>Set the value of this FloatWritable.</summary>
		public virtual void set(float value)
		{
			this.value = value;
		}

		/// <summary>Return the value of this FloatWritable.</summary>
		public virtual float get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			value = @in.readFloat();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeFloat(value);
		}

		/// <summary>Returns true iff <code>o</code> is a FloatWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is org.apache.hadoop.io.FloatWritable))
			{
				return false;
			}
			org.apache.hadoop.io.FloatWritable other = (org.apache.hadoop.io.FloatWritable)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return Sharpen.Runtime.floatToIntBits(value);
		}

		/// <summary>Compares two FloatWritables.</summary>
		public virtual int compareTo(org.apache.hadoop.io.FloatWritable o)
		{
			float thisValue = this.value;
			float thatValue = o.value;
			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
		}

		public override string ToString()
		{
			return float.toString(value);
		}

		/// <summary>A Comparator optimized for FloatWritable.</summary>
		public class Comparator : org.apache.hadoop.io.WritableComparator
		{
			public Comparator()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.FloatWritable)
					))
			{
			}

			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				float thisValue = readFloat(b1, s1);
				float thatValue = readFloat(b2, s2);
				return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
			}
		}

		static FloatWritable()
		{
			// register this comparator
			org.apache.hadoop.io.WritableComparator.define(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.FloatWritable)), new org.apache.hadoop.io.FloatWritable.Comparator
				());
		}
	}
}
