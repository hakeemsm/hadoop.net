using System.Collections.Generic;
using Java.Sql;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>Implement DBSplitter over floating-point values.</summary>
	public class FloatSplitter : DBSplitter
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(FloatSplitter));

		private const double MinIncrement = 10000 * double.MinValue;

		/// <exception cref="Java.Sql.SQLException"/>
		public virtual IList<InputSplit> Split(Configuration conf, ResultSet results, string
			 colName)
		{
			Log.Warn("Generating splits for a floating-point index column. Due to the");
			Log.Warn("imprecise representation of floating-point values in Java, this");
			Log.Warn("may result in an incomplete import.");
			Log.Warn("You are strongly encouraged to choose an integral split column.");
			IList<InputSplit> splits = new AList<InputSplit>();
			if (results.GetString(1) == null && results.GetString(2) == null)
			{
				// Range is null to null. Return a null split accordingly.
				splits.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL"
					, colName + " IS NULL"));
				return splits;
			}
			double minVal = results.GetDouble(1);
			double maxVal = results.GetDouble(2);
			// Use this as a hint. May need an extra task if the size doesn't
			// divide cleanly.
			int numSplits = conf.GetInt(MRJobConfig.NumMaps, 1);
			double splitSize = (maxVal - minVal) / (double)numSplits;
			if (splitSize < MinIncrement)
			{
				splitSize = MinIncrement;
			}
			string lowClausePrefix = colName + " >= ";
			string highClausePrefix = colName + " < ";
			double curLower = minVal;
			double curUpper = curLower + splitSize;
			while (curUpper < maxVal)
			{
				splits.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix
					 + double.ToString(curLower), highClausePrefix + double.ToString(curUpper)));
				curLower = curUpper;
				curUpper += splitSize;
			}
			// Catch any overage and create the closed interval for the last split.
			if (curLower <= maxVal || splits.Count == 1)
			{
				splits.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(lowClausePrefix
					 + double.ToString(curLower), colName + " <= " + double.ToString(maxVal)));
			}
			if (results.GetString(1) == null || results.GetString(2) == null)
			{
				// At least one extrema is null; add a null split.
				splits.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL"
					, colName + " IS NULL"));
			}
			return splits;
		}
	}
}
