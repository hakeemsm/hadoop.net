using System.Collections.Generic;
using Java.Sql;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>Implement DBSplitter over boolean values.</summary>
	public class BooleanSplitter : DBSplitter
	{
		/// <exception cref="Java.Sql.SQLException"/>
		public virtual IList<InputSplit> Split(Configuration conf, ResultSet results, string
			 colName)
		{
			IList<InputSplit> splits = new AList<InputSplit>();
			if (results.GetString(1) == null && results.GetString(2) == null)
			{
				// Range is null to null. Return a null split accordingly.
				splits.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL"
					, colName + " IS NULL"));
				return splits;
			}
			bool minVal = results.GetBoolean(1);
			bool maxVal = results.GetBoolean(2);
			// Use one or two splits.
			if (!minVal)
			{
				splits.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " = FALSE"
					, colName + " = FALSE"));
			}
			if (maxVal)
			{
				splits.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " = TRUE"
					, colName + " = TRUE"));
			}
			if (results.GetString(1) == null || results.GetString(2) == null)
			{
				// Include a null value.
				splits.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit(colName + " IS NULL"
					, colName + " IS NULL"));
			}
			return splits;
		}
	}
}
