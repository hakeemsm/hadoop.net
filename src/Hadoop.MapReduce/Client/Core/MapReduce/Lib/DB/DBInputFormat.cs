using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Java.Sql;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>A InputFormat that reads input data from an SQL table.</summary>
	/// <remarks>
	/// A InputFormat that reads input data from an SQL table.
	/// <p>
	/// DBInputFormat emits LongWritables containing the record number as
	/// key and DBWritables as value.
	/// The SQL query, and input class can be using one of the two
	/// setInput methods.
	/// </remarks>
	public class DBInputFormat<T> : InputFormat<LongWritable, T>, Configurable
		where T : DBWritable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(DBInputFormat));

		protected internal string dbProductName = "DEFAULT";

		/// <summary>A Class that does nothing, implementing DBWritable</summary>
		public class NullDBWritable : DBWritable, Writable
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
			}

			/// <exception cref="Java.Sql.SQLException"/>
			public virtual void ReadFields(ResultSet arg0)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
			}

			/// <exception cref="Java.Sql.SQLException"/>
			public virtual void Write(PreparedStatement arg0)
			{
			}
		}

		/// <summary>A InputSplit that spans a set of rows</summary>
		public class DBInputSplit : InputSplit, Writable
		{
			private long end = 0;

			private long start = 0;

			/// <summary>Default Constructor</summary>
			public DBInputSplit()
			{
			}

			/// <summary>Convenience Constructor</summary>
			/// <param name="start">the index of the first row to select</param>
			/// <param name="end">the index of the last row to select</param>
			public DBInputSplit(long start, long end)
			{
				this.start = start;
				this.end = end;
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public override string[] GetLocations()
			{
				// TODO Add a layer to enable SQL "sharding" and support locality
				return new string[] {  };
			}

			/// <returns>The index of the first row to select</returns>
			public virtual long GetStart()
			{
				return start;
			}

			/// <returns>The index of the last row to select</returns>
			public virtual long GetEnd()
			{
				return end;
			}

			/// <returns>The total row count in this split</returns>
			/// <exception cref="System.IO.IOException"/>
			public override long GetLength()
			{
				return end - start;
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput input)
			{
				start = input.ReadLong();
				end = input.ReadLong();
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput output)
			{
				output.WriteLong(start);
				output.WriteLong(end);
			}
		}

		protected internal string conditions;

		protected internal Connection connection;

		protected internal string tableName;

		protected internal string[] fieldNames;

		protected internal DBConfiguration dbConf;

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public virtual void SetConf(Configuration conf)
		{
			dbConf = new DBConfiguration(conf);
			try
			{
				this.connection = CreateConnection();
				DatabaseMetaData dbMeta = connection.GetMetaData();
				this.dbProductName = StringUtils.ToUpperCase(dbMeta.GetDatabaseProductName());
			}
			catch (Exception ex)
			{
				throw new RuntimeException(ex);
			}
			tableName = dbConf.GetInputTableName();
			fieldNames = dbConf.GetInputFieldNames();
			conditions = dbConf.GetInputConditions();
		}

		public virtual Configuration GetConf()
		{
			return dbConf.GetConf();
		}

		public virtual DBConfiguration GetDBConf()
		{
			return dbConf;
		}

		public virtual Connection GetConnection()
		{
			// TODO Remove this code that handles backward compatibility.
			if (this.connection == null)
			{
				this.connection = CreateConnection();
			}
			return this.connection;
		}

		public virtual Connection CreateConnection()
		{
			try
			{
				Connection newConnection = dbConf.GetConnection();
				newConnection.SetAutoCommit(false);
				newConnection.SetTransactionIsolation(Connection.TransactionSerializable);
				return newConnection;
			}
			catch (Exception e)
			{
				throw new RuntimeException(e);
			}
		}

		public virtual string GetDBProductName()
		{
			return dbProductName;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual RecordReader<LongWritable, T> CreateDBRecordReader(DBInputFormat.DBInputSplit
			 split, Configuration conf)
		{
			Type inputClass = (Type)(dbConf.GetInputClass());
			try
			{
				// use database product name to determine appropriate record reader.
				if (dbProductName.StartsWith("ORACLE"))
				{
					// use Oracle-specific db reader.
					return new OracleDBRecordReader<T>(split, inputClass, conf, CreateConnection(), GetDBConf
						(), conditions, fieldNames, tableName);
				}
				else
				{
					if (dbProductName.StartsWith("MYSQL"))
					{
						// use MySQL-specific db reader.
						return new MySQLDBRecordReader<T>(split, inputClass, conf, CreateConnection(), GetDBConf
							(), conditions, fieldNames, tableName);
					}
					else
					{
						// Generic reader.
						return new DBRecordReader<T>(split, inputClass, conf, CreateConnection(), GetDBConf
							(), conditions, fieldNames, tableName);
					}
				}
			}
			catch (SQLException ex)
			{
				throw new IOException(ex.Message);
			}
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override RecordReader<LongWritable, T> CreateRecordReader(InputSplit split
			, TaskAttemptContext context)
		{
			return CreateDBRecordReader((DBInputFormat.DBInputSplit)split, context.GetConfiguration
				());
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override IList<InputSplit> GetSplits(JobContext job)
		{
			ResultSet results = null;
			Statement statement = null;
			try
			{
				statement = connection.CreateStatement();
				results = statement.ExecuteQuery(GetCountQuery());
				results.Next();
				long count = results.GetLong(1);
				int chunks = job.GetConfiguration().GetInt(MRJobConfig.NumMaps, 1);
				long chunkSize = (count / chunks);
				results.Close();
				statement.Close();
				IList<InputSplit> splits = new AList<InputSplit>();
				// Split the rows into n-number of chunks and adjust the last chunk
				// accordingly
				for (int i = 0; i < chunks; i++)
				{
					DBInputFormat.DBInputSplit split;
					if ((i + 1) == chunks)
					{
						split = new DBInputFormat.DBInputSplit(i * chunkSize, count);
					}
					else
					{
						split = new DBInputFormat.DBInputSplit(i * chunkSize, (i * chunkSize) + chunkSize
							);
					}
					splits.AddItem(split);
				}
				connection.Commit();
				return splits;
			}
			catch (SQLException e)
			{
				throw new IOException("Got SQLException", e);
			}
			finally
			{
				try
				{
					if (results != null)
					{
						results.Close();
					}
				}
				catch (SQLException)
				{
				}
				try
				{
					if (statement != null)
					{
						statement.Close();
					}
				}
				catch (SQLException)
				{
				}
				CloseConnection();
			}
		}

		/// <summary>
		/// Returns the query for getting the total number of rows,
		/// subclasses can override this for custom behaviour.
		/// </summary>
		protected internal virtual string GetCountQuery()
		{
			if (dbConf.GetInputCountQuery() != null)
			{
				return dbConf.GetInputCountQuery();
			}
			StringBuilder query = new StringBuilder();
			query.Append("SELECT COUNT(*) FROM " + tableName);
			if (conditions != null && conditions.Length > 0)
			{
				query.Append(" WHERE " + conditions);
			}
			return query.ToString();
		}

		/// <summary>Initializes the map-part of the job with the appropriate input settings.
		/// 	</summary>
		/// <param name="job">The map-reduce job</param>
		/// <param name="inputClass">
		/// the class object implementing DBWritable, which is the
		/// Java object holding tuple fields.
		/// </param>
		/// <param name="tableName">The table to read data from</param>
		/// <param name="conditions">
		/// The condition which to select data with,
		/// eg. '(updated &gt; 20070101 AND length &gt; 0)'
		/// </param>
		/// <param name="orderBy">the fieldNames in the orderBy clause.</param>
		/// <param name="fieldNames">The field names in the table</param>
		/// <seealso cref="DBInputFormat{T}.SetInput(Org.Apache.Hadoop.Mapreduce.Job, System.Type{T}, string, string)
		/// 	"/>
		public static void SetInput(Job job, Type inputClass, string tableName, string conditions
			, string orderBy, params string[] fieldNames)
		{
			job.SetInputFormatClass(typeof(DBInputFormat));
			DBConfiguration dbConf = new DBConfiguration(job.GetConfiguration());
			dbConf.SetInputClass(inputClass);
			dbConf.SetInputTableName(tableName);
			dbConf.SetInputFieldNames(fieldNames);
			dbConf.SetInputConditions(conditions);
			dbConf.SetInputOrderBy(orderBy);
		}

		/// <summary>Initializes the map-part of the job with the appropriate input settings.
		/// 	</summary>
		/// <param name="job">The map-reduce job</param>
		/// <param name="inputClass">
		/// the class object implementing DBWritable, which is the
		/// Java object holding tuple fields.
		/// </param>
		/// <param name="inputQuery">
		/// the input query to select fields. Example :
		/// "SELECT f1, f2, f3 FROM Mytable ORDER BY f1"
		/// </param>
		/// <param name="inputCountQuery">
		/// the input query that returns
		/// the number of records in the table.
		/// Example : "SELECT COUNT(f1) FROM Mytable"
		/// </param>
		/// <seealso cref="DBInputFormat{T}.SetInput(Org.Apache.Hadoop.Mapreduce.Job, System.Type{T}, string, string, string, string[])
		/// 	"/>
		public static void SetInput(Job job, Type inputClass, string inputQuery, string inputCountQuery
			)
		{
			job.SetInputFormatClass(typeof(DBInputFormat));
			DBConfiguration dbConf = new DBConfiguration(job.GetConfiguration());
			dbConf.SetInputClass(inputClass);
			dbConf.SetInputQuery(inputQuery);
			dbConf.SetInputCountQuery(inputCountQuery);
		}

		protected internal virtual void CloseConnection()
		{
			try
			{
				if (null != this.connection)
				{
					this.connection.Close();
					this.connection = null;
				}
			}
			catch (SQLException sqlE)
			{
				Log.Debug("Exception on close", sqlE);
			}
		}
	}
}
