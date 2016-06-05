using System;
using Java.Sql;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>A RecordReader that reads records from a MySQL table.</summary>
	public class MySQLDBRecordReader<T> : DBRecordReader<T>
		where T : DBWritable
	{
		/// <exception cref="Java.Sql.SQLException"/>
		public MySQLDBRecordReader(DBInputFormat.DBInputSplit split, Type inputClass, Configuration
			 conf, Connection conn, DBConfiguration dbConfig, string cond, string[] fields, 
			string table)
			: base(split, inputClass, conf, conn, dbConfig, cond, fields, table)
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
