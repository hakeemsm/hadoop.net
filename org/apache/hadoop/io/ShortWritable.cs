using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A WritableComparable for shorts.</summary>
	public class ShortWritable : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.ShortWritable
		>
	{
		private short value;

		public ShortWritable()
		{
		}

		public ShortWritable(short value)
		{
			set(value);
		}

		/// <summary>Set the value of this ShortWritable.</summary>
		public virtual void set(short value)
		{
			this.value = value;
		}

		/// <summary>Return the value of this ShortWritable.</summary>
		public virtual short get()
		{
			return value;
		}

		/// <summary>read the short value</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			value = @in.readShort();
		}

		/// <summary>write short value</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeShort(value);
		}

		/// <summary>Returns true iff <code>o</code> is a ShortWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is org.apache.hadoop.io.ShortWritable))
			{
				return false;
			}
			org.apache.hadoop.io.ShortWritable other = (org.apache.hadoop.io.ShortWritable)o;
			return this.value == other.value;
		}

		/// <summary>hash code</summary>
		public override int GetHashCode()
		{
			return value;
		}

		/// <summary>Compares two ShortWritable.</summary>
		public virtual int compareTo(org.apache.hadoop.io.ShortWritable o)
		{
			short thisValue = this.value;
			short thatValue = (o).value;
			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
		}

		/// <summary>Short values in string format</summary>
		public override string ToString()
		{
			return short.toString(value);
		}

		/// <summary>A Comparator optimized for ShortWritable.</summary>
		public class Comparator : org.apache.hadoop.io.WritableComparator
		{
			public Comparator()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ShortWritable)
					))
			{
			}

			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				short thisValue = (short)readUnsignedShort(b1, s1);
				short thatValue = (short)readUnsignedShort(b2, s2);
				return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
			}
		}

		static ShortWritable()
		{
			// register this comparator
			org.apache.hadoop.io.WritableComparator.define(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.ShortWritable)), new org.apache.hadoop.io.ShortWritable.Comparator
				());
		}
	}
}
