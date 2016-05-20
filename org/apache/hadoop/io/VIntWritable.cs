using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A WritableComparable for integer values stored in variable-length format.
	/// 	</summary>
	/// <remarks>
	/// A WritableComparable for integer values stored in variable-length format.
	/// Such values take between one and five bytes.  Smaller values take fewer bytes.
	/// </remarks>
	/// <seealso cref="WritableUtils.readVInt(java.io.DataInput)"/>
	public class VIntWritable : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.VIntWritable
		>
	{
		private int value;

		public VIntWritable()
		{
		}

		public VIntWritable(int value)
		{
			set(value);
		}

		/// <summary>Set the value of this VIntWritable.</summary>
		public virtual void set(int value)
		{
			this.value = value;
		}

		/// <summary>Return the value of this VIntWritable.</summary>
		public virtual int get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			value = org.apache.hadoop.io.WritableUtils.readVInt(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			org.apache.hadoop.io.WritableUtils.writeVInt(@out, value);
		}

		/// <summary>Returns true iff <code>o</code> is a VIntWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is org.apache.hadoop.io.VIntWritable))
			{
				return false;
			}
			org.apache.hadoop.io.VIntWritable other = (org.apache.hadoop.io.VIntWritable)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return value;
		}

		/// <summary>Compares two VIntWritables.</summary>
		public virtual int compareTo(org.apache.hadoop.io.VIntWritable o)
		{
			int thisValue = this.value;
			int thatValue = o.value;
			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
		}

		public override string ToString()
		{
			return int.toString(value);
		}
	}
}
