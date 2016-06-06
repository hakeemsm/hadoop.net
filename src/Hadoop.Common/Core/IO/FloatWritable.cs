using System.IO;
using Org.Apache.Hadoop.IO;

namespace Hadoop.Common.Core.IO
{
	/// <summary>A WritableComparable for floats.</summary>
	public class FloatWritable : IWritableComparable<FloatWritable
		>
	{
		private float value;

		public FloatWritable()
		{
		}

		public FloatWritable(float value)
		{
			Set(value);
		}

		/// <summary>Set the value of this FloatWritable.</summary>
		public virtual void Set(float value)
		{
			this.value = value;
		}

		/// <summary>Return the value of this FloatWritable.</summary>
		public virtual float Get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader reader)
		{
			value = reader.ReadSingle();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter writer)
		{
			writer.Write(value);
		}

		/// <summary>Returns true iff <code>o</code> is a FloatWritable with the same value.</summary>
		public override bool Equals(object o)
		{
			if (!(o is FloatWritable))
			{
				return false;
			}
			FloatWritable other = (FloatWritable)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return Runtime.FloatToIntBits(value);
		}

		/// <summary>Compares two FloatWritables.</summary>
		public virtual int CompareTo(FloatWritable o)
		{
			float thisValue = this.value;
			float thatValue = o.value;
			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
		}

		public override string ToString()
		{
			return value.ToString();
		}

		/// <summary>A Comparator optimized for FloatWritable.</summary>
		public class Comparator : WritableComparator
		{
			public Comparator()
				: base(typeof(FloatWritable))
			{
			}

			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				float thisValue = ReadFloat(b1, s1);
				float thatValue = ReadFloat(b2, s2);
				return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
			}
		}

		static FloatWritable()
		{
			// register this comparator
			WritableComparator.Define(typeof(FloatWritable), new FloatWritable.Comparator());
		}
	}
}
