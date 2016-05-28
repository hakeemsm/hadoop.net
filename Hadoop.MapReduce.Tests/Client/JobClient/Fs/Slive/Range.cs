using System;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>Class that represents a numeric minimum and a maximum</summary>
	/// <?/>
	internal class Range<T>
		where T : Number
	{
		private const string Sep = ",";

		private T min;

		private T max;

		internal Range(T min, T max)
		{
			this.min = min;
			this.max = max;
		}

		/// <returns>the minimum value</returns>
		internal virtual T GetLower()
		{
			return min;
		}

		/// <returns>the maximum value</returns>
		internal virtual T GetUpper()
		{
			return max;
		}

		public override string ToString()
		{
			return min + Sep + max;
		}

		/// <summary>Gets a long number between two values</summary>
		/// <param name="rnd"/>
		/// <param name="range"/>
		/// <returns>long</returns>
		internal static long BetweenPositive(Random rnd, Org.Apache.Hadoop.FS.Slive.Range
			<long> range)
		{
			if (range.GetLower().Equals(range.GetUpper()))
			{
				return range.GetLower();
			}
			long nextRnd = rnd.NextLong();
			long normRange = (range.GetUpper() - range.GetLower() + 1);
			return Math.Abs(nextRnd % normRange) + range.GetLower();
		}
	}
}
