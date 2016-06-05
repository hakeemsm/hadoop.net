using System.IO;
using Org.Apache.Hadoop.IO;

namespace Hadoop.Common.Core.IO
{
	/// <summary>A WritableComparable for a single byte.</summary>
	public class ByteWritable : IWritableComparable<ByteWritable>
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
		public virtual void ReadFields(BinaryReader reader)
		{
			value = reader.ReadByte();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter writer)
		{
			writer.Write(value);
		}

		/// <summary>Returns true iff <code>o</code> is a ByteWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is ByteWritable))
			{
				return false;
			}
			ByteWritable other = (ByteWritable)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return (int)value;
		}

		/// <summary>Compares two ByteWritables.</summary>
		public virtual int CompareTo(ByteWritable o)
		{
			int thisValue = this.value;
			int thatValue = o.value;
			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
		}

		public override string ToString()
		{
			return value.ToString();
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
