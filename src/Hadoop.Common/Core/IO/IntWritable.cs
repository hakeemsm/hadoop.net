using System.IO;
using Org.Apache.Hadoop.IO;

namespace Hadoop.Common.Core.IO
{
	/// <summary>A WritableComparable for ints.</summary>
	public class IntWritable : IWritableComparable<IntWritable>
	{
		private int _value;

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
			this._value = value;
		}

		/// <summary>Return the value of this IntWritable.</summary>
		public virtual int Get()
		{
			return _value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader reader)
		{
			_value = reader.ReadInt32();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter writer)
		{
			writer.Write(_value);
		}

		/// <summary>Returns true iff <code>o</code> is a IntWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is IntWritable))
			{
				return false;
			}
			IntWritable other = (IntWritable)o;
			return this._value == other._value;
		}

		public override int GetHashCode()
		{
			return _value;
		}

		/// <summary>Compares two IntWritables.</summary>
		public virtual int CompareTo(IntWritable o)
		{
			int thisValue = this._value;
			int thatValue = o._value;
			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
		}

		public override string ToString()
		{
			return _value.ToString();
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
