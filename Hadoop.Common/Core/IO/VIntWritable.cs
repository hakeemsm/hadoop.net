using System.IO;
using Hadoop.Common.Core.IO;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>A WritableComparable for integer values stored in variable-length format.
	/// 	</summary>
	/// <remarks>
	/// A WritableComparable for integer values stored in variable-length format.
	/// Such values take between one and five bytes.  Smaller values take fewer bytes.
	/// </remarks>
	/// <seealso cref="WritableUtils.ReadVInt(System.IO.BinaryReader)"/>
	public class VIntWritable : IWritableComparable<Org.Apache.Hadoop.IO.VIntWritable>
	{
		private int value;

		public VIntWritable()
		{
		}

		public VIntWritable(int value)
		{
			Set(value);
		}

		/// <summary>Set the value of this VIntWritable.</summary>
		public virtual void Set(int value)
		{
			this.value = value;
		}

		/// <summary>Return the value of this VIntWritable.</summary>
		public virtual int Get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			value = WritableUtils.ReadVInt(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter @out)
		{
			WritableUtils.WriteVInt(@out, value);
		}

		/// <summary>Returns true iff <code>o</code> is a VIntWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.IO.VIntWritable))
			{
				return false;
			}
			Org.Apache.Hadoop.IO.VIntWritable other = (Org.Apache.Hadoop.IO.VIntWritable)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return value;
		}

		/// <summary>Compares two VIntWritables.</summary>
		public virtual int CompareTo(Org.Apache.Hadoop.IO.VIntWritable o)
		{
			int thisValue = this.value;
			int thatValue = o.value;
			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
		}

		public override string ToString()
		{
			return Extensions.ToString(value);
		}
	}
}
