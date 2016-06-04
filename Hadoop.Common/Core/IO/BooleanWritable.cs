using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.IO
{
	/// <summary>A WritableComparable for booleans.</summary>
	public class BooleanWritable : WritableComparable<Org.Apache.Hadoop.IO.BooleanWritable
		>
	{
		private bool value;

		public BooleanWritable()
		{
		}

		public BooleanWritable(bool value)
		{
			Set(value);
		}

		/// <summary>Set the value of the BooleanWritable</summary>
		public virtual void Set(bool value)
		{
			this.value = value;
		}

		/// <summary>Returns the value of the BooleanWritable</summary>
		public virtual bool Get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			value = @in.ReadBoolean();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			@out.WriteBoolean(value);
		}

		public override bool Equals(object o)
		{
			if (!(o is Org.Apache.Hadoop.IO.BooleanWritable))
			{
				return false;
			}
			Org.Apache.Hadoop.IO.BooleanWritable other = (Org.Apache.Hadoop.IO.BooleanWritable
				)o;
			return this.value == other.value;
		}

		public override int GetHashCode()
		{
			return value ? 0 : 1;
		}

		public virtual int CompareTo(Org.Apache.Hadoop.IO.BooleanWritable o)
		{
			bool a = this.value;
			bool b = o.value;
			return ((a == b) ? 0 : (a == false) ? -1 : 1);
		}

		public override string ToString()
		{
			return bool.ToString(Get());
		}

		/// <summary>A Comparator optimized for BooleanWritable.</summary>
		public class Comparator : WritableComparator
		{
			public Comparator()
				: base(typeof(BooleanWritable))
			{
			}

			public override int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
			{
				return CompareBytes(b1, s1, l1, b2, s2, l2);
			}
		}

		static BooleanWritable()
		{
			WritableComparator.Define(typeof(BooleanWritable), new BooleanWritable.Comparator
				());
		}
	}
}
