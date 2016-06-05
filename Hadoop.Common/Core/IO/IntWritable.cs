using System.IO;
using Hadoop.Common.Core.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A WritableComparable for ints.</summary>
	public class IntWritable : IWritableComparable<Org.Apache.Hadoop.IO.IntWritable>
	{
		private int value;

		public IntWritable()
		{
		}

		public IntWritable(int value)
		{
			Set(value);
		}

		/// <summary>Set the value of this IntWritable.</summary>
		public virtual void Set(int value)
		{
			this.value = value;
		}

		/// <summary>Return the value of this IntWritable.</summary>
		public virtual int Get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			value = @in.ReadInt();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter @out)
		{
			@out.WriteInt(value);
		}

		/// <summary>Returns true iff <code>o</code> is a IntWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.IO.IntWritable))
			{
				return false;
			}
			Org.Apache.Hadoop.IO.IntWritable other = (Org.Apache.Hadoop.IO.IntWritable)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return value;
		}

		/// <summary>Compares two IntWritables.</summary>
		public virtual int CompareTo(Org.Apache.Hadoop.IO.IntWritable o)
		{
			int thisValue = this.value;
			int thatValue = o.value;
			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
		}

		public override string ToString()
		{
			return Sharpen.Extensions.ToString(value);
		}

		/// <summary>A Comparator optimized for IntWritable.</summary>
		public class Comparator : WritableComparator
		{
			public Comparator()
				: base(typeof(IntWritable))
			{
			}

			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				int thisValue = ReadInt(b1, s1);
				int thatValue = ReadInt(b2, s2);
				return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
			}
		}

		static IntWritable()
		{
			// register this comparator
			WritableComparator.Define(typeof(IntWritable), new IntWritable.Comparator());
		}
	}
}
