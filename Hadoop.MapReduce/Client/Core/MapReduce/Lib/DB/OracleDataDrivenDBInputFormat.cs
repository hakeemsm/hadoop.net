using System;
using System.IO;
using Java.Sql;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>A InputFormat that reads input data from an SQL table in an Oracle db.</summary>
	public class OracleDataDrivenDBInputFormat<T> : DataDrivenDBInputFormat<T>, Configurable
		where T : DBWritable
	{
		/// <returns>the DBSplitter implementation to use to divide the table/query into InputSplits.
		/// 	</returns>
		protected internal override DBSplitter GetSplitter(int sqlDataType)
		{
			switch (sqlDataType)
			{
				case Types.Date:
				case Types.Time:
				case Types.Timestamp:
				{
					return new OracleDateSplitter();
				}

				default:
				{
					return base.GetSplitter(sqlDataType);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override RecordReader<LongWritable, T> CreateDBRecordReader(DBInputFormat.DBInputSplit
			 split, Configuration conf)
		{
			DBConfiguration dbConf = GetDBConf();
			Type inputClass = (Type)(dbConf.GetInputClass());
			try
			{
				// Use Oracle-specific db reader
				return new OracleDataDrivenDBRecordReader<T>(split, inputClass, conf, CreateConnection
					(), dbConf, dbConf.GetInputConditions(), dbConf.GetInputFieldNames(), dbConf.GetInputTableName
					());
			}
			catch (SQLException ex)
			{
				throw new IOException(ex.Message);
			}
		}
	}
}
