using System.IO;
using Org.Apache.Hadoop.IO;

namespace Hadoop.Common.Core.IO
{
	/// <summary>A WritableComparable for booleans.</summary>
	public class BooleanWritable : IWritableComparable<BooleanWritable>
	{
		private bool _value;

		public BooleanWritable()
		{
		}

		public BooleanWritable(bool value)
		{
		    Value = value;
		}
		

        /// <summary>Returns the value of the BooleanWritable</summary>
        public virtual bool Value
        {
            get { return _value; }
            set { _value = value; }
        }

	    public void Write(BinaryWriter writer)
	    {
	        writer.Write(_value);
	    }

	    /// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			_value = @in.ReadBoolean();
		}
		

		public override bool Equals(object o)
		{
		    BooleanWritable other = o as BooleanWritable;
			return _value == other?._value;
		}

		public override int GetHashCode()
		{
			return _value ? 0 : 1;
		}

		public virtual int CompareTo(BooleanWritable o)
		{
			bool a = this._value;
			bool b = o._value;
			return ((a == b) ? 0 : (a == false) ? -1 : 1);
		}

		public override string ToString()
		{
			return _value.ToString();
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
			WritableComparator.Define(typeof(BooleanWritable), new Comparator());
		}
	}
}
