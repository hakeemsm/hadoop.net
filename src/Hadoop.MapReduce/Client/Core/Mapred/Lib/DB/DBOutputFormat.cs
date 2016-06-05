using System;
using System.IO;
using Java.Sql;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.DB;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.DB
{
	public class DBOutputFormat<K, V> : DBOutputFormat<K, V>, OutputFormat<K, V>
		where K : DBWritable
	{
		/// <summary>A RecordWriter that writes the reduce output to a SQL table</summary>
		protected internal class DBRecordWriter : DBOutputFormat.DBRecordWriter, RecordWriter
			<K, V>
		{
			/// <exception cref="Java.Sql.SQLException"/>
			protected internal DBRecordWriter(DBOutputFormat<K, V> _enclosing, Connection connection
				, PreparedStatement statement)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Close(Reporter reporter)
			{
				base.Close(null);
			}

			private readonly DBOutputFormat<K, V> _enclosing;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CheckOutputSpecs(FileSystem filesystem, JobConf job)
		{
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual RecordWriter<K, V> GetRecordWriter(FileSystem filesystem, JobConf 
			job, string name, Progressable progress)
		{
			RecordWriter<K, V> w = base.GetRecordWriter(new TaskAttemptContextImpl(job, TaskAttemptID
				.ForName(job.Get(MRJobConfig.TaskAttemptId))));
			DBOutputFormat.DBRecordWriter writer = (DBOutputFormat.DBRecordWriter)w;
			try
			{
				return new DBOutputFormat.DBRecordWriter(this, writer.GetConnection(), writer.GetStatement
					());
			}
			catch (SQLException se)
			{
				throw new IOException(se);
			}
		}

		/// <summary>Initializes the reduce-part of the job with the appropriate output settings
		/// 	</summary>
		/// <param name="job">The job</param>
		/// <param name="tableName">The table to insert data into</param>
		/// <param name="fieldNames">The field names in the table.</param>
		public static void SetOutput(JobConf job, string tableName, params string[] fieldNames
			)
		{
			if (fieldNames.Length > 0 && fieldNames[0] != null)
			{
				DBConfiguration dbConf = SetOutput(job, tableName);
				dbConf.SetOutputFieldNames(fieldNames);
			}
			else
			{
				if (fieldNames.Length > 0)
				{
					SetOutput(job, tableName, fieldNames.Length);
				}
				else
				{
					throw new ArgumentException("Field names must be greater than 0");
				}
			}
		}

		/// <summary>Initializes the reduce-part of the job with the appropriate output settings
		/// 	</summary>
		/// <param name="job">The job</param>
		/// <param name="tableName">The table to insert data into</param>
		/// <param name="fieldCount">the number of fields in the table.</param>
		public static void SetOutput(JobConf job, string tableName, int fieldCount)
		{
			DBConfiguration dbConf = SetOutput(job, tableName);
			dbConf.SetOutputFieldCount(fieldCount);
		}

		private static DBConfiguration SetOutput(JobConf job, string tableName)
		{
			job.SetOutputFormat(typeof(DBOutputFormat));
			job.SetReduceSpeculativeExecution(false);
			DBConfiguration dbConf = new DBConfiguration(job);
			dbConf.SetOutputTableName(tableName);
			return dbConf;
		}
	}
}
