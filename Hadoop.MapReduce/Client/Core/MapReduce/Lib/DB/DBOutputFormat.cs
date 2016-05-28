using System;
using System.IO;
using System.Text;
using Java.Sql;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>A OutputFormat that sends the reduce output to a SQL table.</summary>
	/// <remarks>
	/// A OutputFormat that sends the reduce output to a SQL table.
	/// <p>
	/// <see cref="DBOutputFormat{K, V}"/>
	/// accepts &lt;key,value&gt; pairs, where
	/// key has a type extending DBWritable. Returned
	/// <see cref="Org.Apache.Hadoop.Mapreduce.RecordWriter{K, V}"/>
	/// 
	/// writes <b>only the key</b> to the database with a batch SQL query.
	/// </remarks>
	public class DBOutputFormat<K, V> : OutputFormat<K, V>
		where K : DBWritable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(DBOutputFormat));

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void CheckOutputSpecs(JobContext context)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override OutputCommitter GetOutputCommitter(TaskAttemptContext context)
		{
			return new FileOutputCommitter(FileOutputFormat.GetOutputPath(context), context);
		}

		/// <summary>A RecordWriter that writes the reduce output to a SQL table</summary>
		public class DBRecordWriter : RecordWriter<K, V>
		{
			private Connection connection;

			private PreparedStatement statement;

			/// <exception cref="Java.Sql.SQLException"/>
			public DBRecordWriter(DBOutputFormat<K, V> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="Java.Sql.SQLException"/>
			public DBRecordWriter(DBOutputFormat<K, V> _enclosing, Connection connection, PreparedStatement
				 statement)
			{
				this._enclosing = _enclosing;
				this.connection = connection;
				this.statement = statement;
				this.connection.SetAutoCommit(false);
			}

			public virtual Connection GetConnection()
			{
				return this.connection;
			}

			public virtual PreparedStatement GetStatement()
			{
				return this.statement;
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public override void Close(TaskAttemptContext context)
			{
				try
				{
					this.statement.ExecuteBatch();
					this.connection.Commit();
				}
				catch (SQLException e)
				{
					try
					{
						this.connection.Rollback();
					}
					catch (SQLException ex)
					{
						DBOutputFormat.Log.Warn(StringUtils.StringifyException(ex));
					}
					throw new IOException(e.Message);
				}
				finally
				{
					try
					{
						this.statement.Close();
						this.connection.Close();
					}
					catch (SQLException ex)
					{
						throw new IOException(ex.Message);
					}
				}
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public override void Write(K key, V value)
			{
				try
				{
					key.Write(this.statement);
					this.statement.AddBatch();
				}
				catch (SQLException e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
			}

			private readonly DBOutputFormat<K, V> _enclosing;
		}

		/// <summary>Constructs the query used as the prepared statement to insert data.</summary>
		/// <param name="table">the table to insert into</param>
		/// <param name="fieldNames">
		/// the fields to insert into. If field names are unknown, supply an
		/// array of nulls.
		/// </param>
		public virtual string ConstructQuery(string table, string[] fieldNames)
		{
			if (fieldNames == null)
			{
				throw new ArgumentException("Field names may not be null");
			}
			StringBuilder query = new StringBuilder();
			query.Append("INSERT INTO ").Append(table);
			if (fieldNames.Length > 0 && fieldNames[0] != null)
			{
				query.Append(" (");
				for (int i = 0; i < fieldNames.Length; i++)
				{
					query.Append(fieldNames[i]);
					if (i != fieldNames.Length - 1)
					{
						query.Append(",");
					}
				}
				query.Append(")");
			}
			query.Append(" VALUES (");
			for (int i_1 = 0; i_1 < fieldNames.Length; i_1++)
			{
				query.Append("?");
				if (i_1 != fieldNames.Length - 1)
				{
					query.Append(",");
				}
			}
			query.Append(");");
			return query.ToString();
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override RecordWriter<K, V> GetRecordWriter(TaskAttemptContext context)
		{
			DBConfiguration dbConf = new DBConfiguration(context.GetConfiguration());
			string tableName = dbConf.GetOutputTableName();
			string[] fieldNames = dbConf.GetOutputFieldNames();
			if (fieldNames == null)
			{
				fieldNames = new string[dbConf.GetOutputFieldCount()];
			}
			try
			{
				Connection connection = dbConf.GetConnection();
				PreparedStatement statement = null;
				statement = connection.PrepareStatement(ConstructQuery(tableName, fieldNames));
				return new DBOutputFormat.DBRecordWriter(this, connection, statement);
			}
			catch (Exception ex)
			{
				throw new IOException(ex.Message);
			}
		}

		/// <summary>
		/// Initializes the reduce-part of the job with
		/// the appropriate output settings
		/// </summary>
		/// <param name="job">The job</param>
		/// <param name="tableName">The table to insert data into</param>
		/// <param name="fieldNames">The field names in the table.</param>
		/// <exception cref="System.IO.IOException"/>
		public static void SetOutput(Job job, string tableName, params string[] fieldNames
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

		/// <summary>
		/// Initializes the reduce-part of the job
		/// with the appropriate output settings
		/// </summary>
		/// <param name="job">The job</param>
		/// <param name="tableName">The table to insert data into</param>
		/// <param name="fieldCount">the number of fields in the table.</param>
		/// <exception cref="System.IO.IOException"/>
		public static void SetOutput(Job job, string tableName, int fieldCount)
		{
			DBConfiguration dbConf = SetOutput(job, tableName);
			dbConf.SetOutputFieldCount(fieldCount);
		}

		/// <exception cref="System.IO.IOException"/>
		private static DBConfiguration SetOutput(Job job, string tableName)
		{
			job.SetOutputFormatClass(typeof(DBOutputFormat));
			job.SetReduceSpeculativeExecution(false);
			DBConfiguration dbConf = new DBConfiguration(job.GetConfiguration());
			dbConf.SetOutputTableName(tableName);
			return dbConf;
		}
	}
}
