using System.IO;
using Hadoop.Common.Core.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A WritableComparable for longs in a variable-length format.</summary>
	/// <remarks>
	/// A WritableComparable for longs in a variable-length format. Such values take
	/// between one and five bytes.  Smaller values take fewer bytes.
	/// </remarks>
	/// <seealso cref="WritableUtils.ReadVLong(System.IO.BinaryReader)"/>
	public class VLongWritable : IWritableComparable<Org.Apache.Hadoop.IO.VLongWritable
		>
	{
		private long value;

		public VLongWritable()
		{
		}

		public VLongWritable(long value)
		{
			Set(value);
		}

		/// <summary>Set the value of this LongWritable.</summary>
		public virtual void Set(long value)
		{
			this.value = value;
		}

		/// <summary>Return the value of this LongWritable.</summary>
		public virtual long Get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			value = WritableUtils.ReadVLong(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter @out)
		{
			WritableUtils.WriteVLong(@out, value);
		}

		/// <summary>Returns true iff <code>o</code> is a VLongWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.IO.VLongWritable))
			{
				return false;
			}
			Org.Apache.Hadoop.IO.VLongWritable other = (Org.Apache.Hadoop.IO.VLongWritable)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return (int)value;
		}

		/// <summary>Compares two VLongWritables.</summary>
		public virtual int CompareTo(Org.Apache.Hadoop.IO.VLongWritable o)
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
