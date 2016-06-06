using System.IO;
using Org.Apache.Hadoop.IO;

namespace Hadoop.Common.Core.IO
{
	/// <summary>Writable for Double values.</summary>
	public class DoubleWritable : IWritableComparable<DoubleWritable>
	{
		private double _value;

		public DoubleWritable()
		{
		}

		public DoubleWritable(double value)
		{
			Set(value);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader reader)
		{
			_value = reader.ReadDouble();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter writer)
		{
			writer.Write(_value);
		}

		public virtual void Set(double value)
		{
			this._value = value;
		}

		public virtual double Get()
		{
			return _value;
		}

		/// <summary>Returns true iff <code>o</code> is a DoubleWritable with the same value.
		/// 	</summary>
		public override bool Equals(object o)
		{
			if (!(o is DoubleWritable))
			{
				return false;
			}
			DoubleWritable other = (DoubleWritable)
				o;
			return this._value == other._value;
		}

		public override int GetHashCode()
		{
			return (int)double.DoubleToLongBits(_value);
		}

		public virtual int CompareTo(DoubleWritable o)
		{
			return (_value < o._value ? -1 : (_value == o._value ? 0 : 1));
		}

		public override string ToString()
		{
			return _value.ToString();
		}

		/// <summary>A Comparator optimized for DoubleWritable.</summary>
		public class Comparator : WritableComparator
		{
			public Comparator()
				: base(typeof(DoubleWritable))
			{
			}

			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				double thisValue = ReadDouble(b1, s1);
				double thatValue = ReadDouble(b2, s2);
				return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
			}
		}

		static DoubleWritable()
		{
			// register this comparator
			WritableComparator.Define(typeof(DoubleWritable), new Comparator());
		}
	}
}
