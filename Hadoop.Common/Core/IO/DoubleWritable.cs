using System.IO;
using Hadoop.Common.Core.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>Writable for Double values.</summary>
	public class DoubleWritable : IWritableComparable<Org.Apache.Hadoop.IO.DoubleWritable
		>
	{
		private double value = 0.0;

		public DoubleWritable()
		{
		}

		public DoubleWritable(double value)
		{
			Set(value);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			value = @in.ReadDouble();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteDouble(value);
		}

		public virtual void Set(double value)
		{
			this.value = value;
		}

		public virtual double Get()
		{
			return value;
		}

		/// <summary>Returns true iff <code>o</code> is a DoubleWritable with the same value.
		/// 	</summary>
		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.IO.DoubleWritable))
			{
				return false;
			}
			Org.Apache.Hadoop.IO.DoubleWritable other = (Org.Apache.Hadoop.IO.DoubleWritable)
				o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return (int)double.DoubleToLongBits(value);
		}

		public virtual int CompareTo(Org.Apache.Hadoop.IO.DoubleWritable o)
		{
			return (value < o.value ? -1 : (value == o.value ? 0 : 1));
		}

		public override string ToString()
		{
			return double.ToString(value);
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
			WritableComparator.Define(typeof(DoubleWritable), new DoubleWritable.Comparator()
				);
		}
	}
}
