using System.Collections.Generic;
using Java.Math;
using Java.Sql;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>Implement DBSplitter over BigDecimal values.</summary>
	public class BigDecimalSplitter : DBSplitter
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(BigDecimalSplitter));

		/// <exception cref="Java.Sql.SQLException"/>
		public virtual IList<InputSplit> Split(Configuration conf, ResultSet results, string
			 colName)
		{
			BigDecimal minVal = results.GetBigDecimal(1);
			BigDecimal maxVal = results.GetBigDecimal(2);
			string lowClausePrefix = colName + " >= ";
			string highClausePrefix = colName + " < ";
			BigDecimal numSplits = new BigDecimal(conf.GetInt(MRJobConfig.NumMaps, 1));
			if (minVal == null && maxVal == null)
			{
				// Range is null to null. Return a null split accordingly.
				IList<InputSplit> splits = new AList<InputSplit>();
				splits.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL"
					, colName + " IS NULL"));
				return splits;
			}
			if (minVal == null || maxVal == null)
			{
				// Don't know what is a reasonable min/max value for interpolation. Fail.
				Log.Error("Cannot find a range for NUMERIC or DECIMAL fields with one end NULL.");
				return null;
			}
			// Get all the split points together.
			IList<BigDecimal> splitPoints = Split(numSplits, minVal, maxVal);
			IList<InputSplit> splits_1 = new AList<InputSplit>();
			// Turn the split points into a set of intervals.
			BigDecimal start = splitPoints[0];
			for (int i = 1; i < splitPoints.Count; i++)
			{
				BigDecimal end = splitPoints[i];
				if (i == splitPoints.Count - 1)
				{
					// This is the last one; use a closed interval.
					splits_1.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix
						 + start.ToString(), colName + " <= " + end.ToString()));
				}
				else
				{
					// Normal open-interval case.
					splits_1.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix
						 + start.ToString(), highClausePrefix + end.ToString()));
				}
				start = end;
			}
			return splits_1;
		}

		private static readonly BigDecimal MinIncrement = new BigDecimal(10000 * double.MinValue
			);

		/// <summary>Divide numerator by denominator.</summary>
		/// <remarks>Divide numerator by denominator. If impossible in exact mode, use rounding.
		/// 	</remarks>
		protected internal virtual BigDecimal TryDivide(BigDecimal numerator, BigDecimal 
			denominator)
		{
			try
			{
				return numerator.Divide(denominator);
			}
			catch (ArithmeticException)
			{
				return numerator.Divide(denominator, BigDecimal.RoundHalfUp);
			}
		}

		/// <summary>Returns a list of BigDecimals one element longer than the list of input splits.
		/// 	</summary>
		/// <remarks>
		/// Returns a list of BigDecimals one element longer than the list of input splits.
		/// This represents the boundaries between input splits.
		/// All splits are open on the top end, except the last one.
		/// So the list [0, 5, 8, 12, 18] would represent splits capturing the intervals:
		/// [0, 5)
		/// [5, 8)
		/// [8, 12)
		/// [12, 18] note the closed interval for the last split.
		/// </remarks>
		/// <exception cref="Java.Sql.SQLException"/>
		internal virtual IList<BigDecimal> Split(BigDecimal numSplits, BigDecimal minVal, 
			BigDecimal maxVal)
		{
			IList<BigDecimal> splits = new AList<BigDecimal>();
			// Use numSplits as a hint. May need an extra task if the size doesn't
			// divide cleanly.
			BigDecimal splitSize = TryDivide(maxVal.Subtract(minVal), (numSplits));
			if (splitSize.CompareTo(MinIncrement) < 0)
			{
				splitSize = MinIncrement;
				Log.Warn("Set BigDecimal splitSize to MIN_INCREMENT");
			}
			BigDecimal curVal = minVal;
			while (curVal.CompareTo(maxVal) <= 0)
			{
				splits.AddItem(curVal);
				curVal = curVal.Add(splitSize);
			}
			if (splits[splits.Count - 1].CompareTo(maxVal) != 0 || splits.Count == 1)
			{
				// We didn't end on the maxVal. Add that to the end of the list.
				splits.AddItem(maxVal);
			}
			return splits;
		}
	}
}
