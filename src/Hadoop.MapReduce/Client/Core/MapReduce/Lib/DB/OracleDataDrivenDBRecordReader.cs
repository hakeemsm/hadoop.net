using System;
using Java.Sql;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>A RecordReader that reads records from a Oracle table via DataDrivenDBRecordReader
	/// 	</summary>
	public class OracleDataDrivenDBRecordReader<T> : DataDrivenDBRecordReader<T>
		where T : DBWritable
	{
		/// <exception cref="Java.Sql.SQLException"/>
		public OracleDataDrivenDBRecordReader(DBInputFormat.DBInputSplit split, Type inputClass
			, Configuration conf, Connection conn, DBConfiguration dbConfig, string cond, string
			[] fields, string table)
			: base(split, inputClass, conf, conn, dbConfig, cond, fields, table, "ORACLE")
		{
			// Must initialize the tz used by the connection for Oracle.
			OracleDBRecordReader.SetSessionTimeZone(conf, conn);
		}
	}
}
