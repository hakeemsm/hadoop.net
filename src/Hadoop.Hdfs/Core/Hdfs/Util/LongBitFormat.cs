using System;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>Bit format in a long.</summary>
	[System.Serializable]
	public class LongBitFormat
	{
		private const long serialVersionUID = 1L;

		private readonly string Name;

		/// <summary>Bit offset</summary>
		private readonly int Offset;

		/// <summary>Bit length</summary>
		private readonly int Length;

		/// <summary>Minimum value</summary>
		private readonly long Min;

		/// <summary>Maximum value</summary>
		private readonly long Max;

		/// <summary>Bit mask</summary>
		private readonly long Mask;

		public LongBitFormat(string name, Org.Apache.Hadoop.Hdfs.Util.LongBitFormat previous
			, int length, long min)
		{
			Name = name;
			Offset = previous == null ? 0 : previous.Offset + previous.Length;
			Length = length;
			Min = min;
			Max = ((long)(((ulong)(-1L)) >> (64 - Length)));
			Mask = Max << Offset;
		}

		/// <summary>Retrieve the value from the record.</summary>
		public virtual long Retrieve(long record)
		{
			return (long)(((ulong)(record & Mask)) >> Offset);
		}

		/// <summary>Combine the value to the record.</summary>
		public virtual long Combine(long value, long record)
		{
			if (value < Min)
			{
				throw new ArgumentException("Illagal value: " + Name + " = " + value + " < MIN = "
					 + Min);
			}
			if (value > Max)
			{
				throw new ArgumentException("Illagal value: " + Name + " = " + value + " > MAX = "
					 + Max);
			}
			return (record & ~Mask) | (value << Offset);
		}

		public virtual long GetMin()
		{
			return Min;
		}
	}
}
