using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A WritableComparable for a single byte.</summary>
	public class ByteWritable : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.ByteWritable
		>
	{
		private byte value;

		public ByteWritable()
		{
		}

		public ByteWritable(byte value)
		{
			set(value);
		}

		/// <summary>Set the value of this ByteWritable.</summary>
		public virtual void set(byte value)
		{
			this.value = value;
		}

		/// <summary>Return the value of this ByteWritable.</summary>
		public virtual byte get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			value = @in.readByte();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeByte(value);
		}

		/// <summary>Returns true iff <code>o</code> is a ByteWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is org.apache.hadoop.io.ByteWritable))
			{
				return false;
			}
			org.apache.hadoop.io.ByteWritable other = (org.apache.hadoop.io.ByteWritable)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return (int)value;
		}

		/// <summary>Compares two ByteWritables.</summary>
		public virtual int compareTo(org.apache.hadoop.io.ByteWritable o)
		{
			int thisValue = this.value;
			int thatValue = o.value;
			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
		}

		public override string ToString()
		{
			return byte.toString(value);
		}

		/// <summary>A Comparator optimized for ByteWritable.</summary>
		public class Comparator : org.apache.hadoop.io.WritableComparator
		{
			public Comparator()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ByteWritable))
					)
			{
			}

			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				byte thisValue = b1[s1];
				byte thatValue = b2[s2];
				return (((sbyte)thisValue) < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
			}
		}

		static ByteWritable()
		{
			// register this comparator
			org.apache.hadoop.io.WritableComparator.define(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.ByteWritable)), new org.apache.hadoop.io.ByteWritable.Comparator
				());
		}
	}
}
