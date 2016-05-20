using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A WritableComparable for longs in a variable-length format.</summary>
	/// <remarks>
	/// A WritableComparable for longs in a variable-length format. Such values take
	/// between one and five bytes.  Smaller values take fewer bytes.
	/// </remarks>
	/// <seealso cref="WritableUtils.readVLong(java.io.DataInput)"/>
	public class VLongWritable : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.VLongWritable
		>
	{
		private long value;

		public VLongWritable()
		{
		}

		public VLongWritable(long value)
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
			value = org.apache.hadoop.io.WritableUtils.readVLong(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			org.apache.hadoop.io.WritableUtils.writeVLong(@out, value);
		}

		/// <summary>Returns true iff <code>o</code> is a VLongWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is org.apache.hadoop.io.VLongWritable))
			{
				return false;
			}
			org.apache.hadoop.io.VLongWritable other = (org.apache.hadoop.io.VLongWritable)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return (int)value;
		}

		/// <summary>Compares two VLongWritables.</summary>
		public virtual int compareTo(org.apache.hadoop.io.VLongWritable o)
		{
			long thisValue = this.value;
			long thatValue = o.value;
			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
		}

		public override string ToString()
		{
			return System.Convert.ToString(value);
		}
	}
}
