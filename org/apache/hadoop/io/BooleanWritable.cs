using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A WritableComparable for booleans.</summary>
	public class BooleanWritable : org.apache.hadoop.io.WritableComparable<org.apache.hadoop.io.BooleanWritable
		>
	{
		private bool value;

		public BooleanWritable()
		{
		}

		public BooleanWritable(bool value)
		{
			set(value);
		}

		/// <summary>Set the value of the BooleanWritable</summary>
		public virtual void set(bool value)
		{
			this.value = value;
		}

		/// <summary>Returns the value of the BooleanWritable</summary>
		public virtual bool get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			value = @in.readBoolean();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeBoolean(value);
		}

		public override bool Equals(object o)
		{
			if (!(o is org.apache.hadoop.io.BooleanWritable))
			{
				return false;
			}
			org.apache.hadoop.io.BooleanWritable other = (org.apache.hadoop.io.BooleanWritable
				)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return value ? 0 : 1;
		}

		public virtual int compareTo(org.apache.hadoop.io.BooleanWritable o)
		{
			bool a = this.value;
			bool b = o.value;
			return ((a == b) ? 0 : (a == false) ? -1 : 1);
		}

		public override string ToString()
		{
			return bool.toString(get());
		}

		/// <summary>A Comparator optimized for BooleanWritable.</summary>
		public class Comparator : org.apache.hadoop.io.WritableComparator
		{
			public Comparator()
				: base(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.BooleanWritable
					)))
			{
			}

			public override int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				return compareBytes(b1, s1, l1, b2, s2, l2);
			}
		}

		static BooleanWritable()
		{
			org.apache.hadoop.io.WritableComparator.define(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.io.BooleanWritable)), new org.apache.hadoop.io.BooleanWritable.Comparator
				());
		}
	}
}
