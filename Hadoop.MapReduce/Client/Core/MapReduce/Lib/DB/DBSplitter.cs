using System.Collections.Generic;
using Java.Sql;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>DBSplitter will generate DBInputSplits to use with DataDrivenDBInputFormat.
	/// 	</summary>
	/// <remarks>
	/// DBSplitter will generate DBInputSplits to use with DataDrivenDBInputFormat.
	/// DataDrivenDBInputFormat needs to interpolate between two values that
	/// represent the lowest and highest valued records to import. Depending
	/// on the data-type of the column, this requires different behavior.
	/// DBSplitter implementations should perform this for a data type or family
	/// of data types.
	/// </remarks>
	public interface DBSplitter
	{
		/// <summary>
		/// Given a ResultSet containing one record (and already advanced to that record)
		/// with two columns (a low value, and a high value, both of the same type), determine
		/// a set of splits that span the given values.
		/// </summary>
		/// <exception cref="Java.Sql.SQLException"/>
		IList<InputSplit> Split(Configuration conf, ResultSet results, string colName);
	}
}
