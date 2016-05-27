using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A WritableComparable for a single byte.</summary>
	public class ByteWritable : WritableComparable<Org.Apache.Hadoop.IO.ByteWritable>
	{
		private byte value;

		public ByteWritable()
		{
		}

		public ByteWritable(byte value)
		{
			Set(value);
		}

		/// <summary>Set the value of this ByteWritable.</summary>
		public virtual void Set(byte value)
		{
			this.value = value;
		}

		/// <summary>Return the value of this ByteWritable.</summary>
		public virtual byte Get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			value = @in.ReadByte();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteByte(value);
		}

		/// <summary>Returns true iff <code>o</code> is a ByteWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.IO.ByteWritable))
			{
				return false;
			}
			Org.Apache.Hadoop.IO.ByteWritable other = (Org.Apache.Hadoop.IO.ByteWritable)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return (int)value;
		}

		/// <summary>Compares two ByteWritables.</summary>
		public virtual int CompareTo(Org.Apache.Hadoop.IO.ByteWritable o)
		{
			int thisValue = this.value;
			int thatValue = o.value;
			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
		}

		public override string ToString()
		{
			return byte.ToString(value);
		}

		/// <summary>A Comparator optimized for ByteWritable.</summary>
		public class Comparator : WritableComparator
		{
			public Comparator()
				: base(typeof(ByteWritable))
			{
			}

			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				byte thisValue = b1[s1];
				byte thatValue = b2[s2];
				return (((sbyte)thisValue) < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
			}
		}

		static ByteWritable()
		{
			// register this comparator
			WritableComparator.Define(typeof(ByteWritable), new ByteWritable.Comparator());
		}
	}
}
