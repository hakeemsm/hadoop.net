using System;
using Java.Sql;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>A RecordReader that reads records from a MySQL table via DataDrivenDBRecordReader
	/// 	</summary>
	public class MySQLDataDrivenDBRecordReader<T> : DataDrivenDBRecordReader<T>
		where T : DBWritable
	{
		/// <exception cref="Java.Sql.SQLException"/>
		public MySQLDataDrivenDBRecordReader(DBInputFormat.DBInputSplit split, Type inputClass
			, Configuration conf, Connection conn, DBConfiguration dbConfig, string cond, string
			[] fields, string table)
			: base(split, inputClass, conf, conn, dbConfig, cond, fields, table, "MYSQL")
		{
		}

		// Execute statements for mysql in unbuffered mode.
		/// <exception cref="Java.Sql.SQLException"/>
		protected internal override ResultSet ExecuteQuery(string query)
		{
			statement = GetConnection().PrepareStatement(query, ResultSet.TypeForwardOnly, ResultSet
				.ConcurReadOnly);
			statement.SetFetchSize(int.MinValue);
			// MySQL: read row-at-a-time.
			return statement.ExecuteQuery();
		}
	}
}
