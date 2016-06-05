using System.Collections.Generic;
using Java.Sql;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>Implement DBSplitter over integer values.</summary>
	public class IntegerSplitter : DBSplitter
	{
		/// <exception cref="Java.Sql.SQLException"/>
		public virtual IList<InputSplit> Split(Configuration conf, ResultSet results, string
			 colName)
		{
			long minVal = results.GetLong(1);
			long maxVal = results.GetLong(2);
			string lowClausePrefix = colName + " >= ";
			string highClausePrefix = colName + " < ";
			int numSplits = conf.GetInt(MRJobConfig.NumMaps, 1);
			if (numSplits < 1)
			{
				numSplits = 1;
			}
			if (results.GetString(1) == null && results.GetString(2) == null)
			{
				// Range is null to null. Return a null split accordingly.
				IList<InputSplit> splits = new AList<InputSplit>();
				splits.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL"
					, colName + " IS NULL"));
				return splits;
			}
			// Get all the split points together.
			IList<long> splitPoints = Split(numSplits, minVal, maxVal);
			IList<InputSplit> splits_1 = new AList<InputSplit>();
			// Turn the split points into a set of intervals.
			long start = splitPoints[0];
			for (int i = 1; i < splitPoints.Count; i++)
			{
				long end = splitPoints[i];
				if (i == splitPoints.Count - 1)
				{
					// This is the last one; use a closed interval.
					splits_1.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix
						 + System.Convert.ToString(start), colName + " <= " + System.Convert.ToString(end
						)));
				}
				else
				{
					// Normal open-interval case.
					splits_1.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix
						 + System.Convert.ToString(start), highClausePrefix + System.Convert.ToString(end
						)));
				}
				start = end;
			}
			if (results.GetString(1) == null || results.GetString(2) == null)
			{
				// At least one extrema is null; add a null split.
				splits_1.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL"
					, colName + " IS NULL"));
			}
			return splits_1;
		}

		/// <summary>Returns a list of longs one element longer than the list of input splits.
		/// 	</summary>
		/// <remarks>
		/// Returns a list of longs one element longer than the list of input splits.
		/// This represents the boundaries between input splits.
		/// All splits are open on the top end, except the last one.
		/// So the list [0, 5, 8, 12, 18] would represent splits capturing the intervals:
		/// [0, 5)
		/// [5, 8)
		/// [8, 12)
		/// [12, 18] note the closed interval for the last split.
		/// </remarks>
		/// <exception cref="Java.Sql.SQLException"/>
		internal virtual IList<long> Split(long numSplits, long minVal, long maxVal)
		{
			IList<long> splits = new AList<long>();
			// Use numSplits as a hint. May need an extra task if the size doesn't
			// divide cleanly.
			long splitSize = (maxVal - minVal) / numSplits;
			if (splitSize < 1)
			{
				splitSize = 1;
			}
			long curVal = minVal;
			while (curVal <= maxVal)
			{
				splits.AddItem(curVal);
				curVal += splitSize;
			}
			if (splits[splits.Count - 1] != maxVal || splits.Count == 1)
			{
				// We didn't end on the maxVal. Add that to the end of the list.
				splits.AddItem(maxVal);
			}
			return splits;
		}
	}
}
