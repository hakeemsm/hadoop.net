using System.IO;
using Hadoop.Common.Core.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A WritableComparable for shorts.</summary>
	public class ShortWritable : IWritableComparable<Org.Apache.Hadoop.IO.ShortWritable
		>
	{
		private short value;

		public ShortWritable()
		{
		}

		public ShortWritable(short value)
		{
			Set(value);
		}

		/// <summary>Set the value of this ShortWritable.</summary>
		public virtual void Set(short value)
		{
			this.value = value;
		}

		/// <summary>Return the value of this ShortWritable.</summary>
		public virtual short Get()
		{
			return value;
		}

		/// <summary>read the short value</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			value = @in.ReadShort();
		}

		/// <summary>write short value</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter @out)
		{
			@out.WriteShort(value);
		}

		/// <summary>Returns true iff <code>o</code> is a ShortWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.IO.ShortWritable))
			{
				return false;
			}
			Org.Apache.Hadoop.IO.ShortWritable other = (Org.Apache.Hadoop.IO.ShortWritable)o;
			return this.value == other.value;
		}

		/// <summary>hash code</summary>
		public override int GetHashCode()
		{
			return value;
		}

		/// <summary>Compares two ShortWritable.</summary>
		public virtual int CompareTo(Org.Apache.Hadoop.IO.ShortWritable o)
		{
			short thisValue = this.value;
			short thatValue = (o).value;
			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
		}

		/// <summary>Short values in string format</summary>
		public override string ToString()
		{
			return short.ToString(value);
		}

		/// <summary>A Comparator optimized for ShortWritable.</summary>
		public class Comparator : WritableComparator
		{
			public Comparator()
				: base(typeof(ShortWritable))
			{
			}

			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				short thisValue = (short)ReadUnsignedShort(b1, s1);
				short thatValue = (short)ReadUnsignedShort(b2, s2);
				return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
			}
		}

		static ShortWritable()
		{
			// register this comparator
			WritableComparator.Define(typeof(ShortWritable), new ShortWritable.Comparator());
		}
	}
}
