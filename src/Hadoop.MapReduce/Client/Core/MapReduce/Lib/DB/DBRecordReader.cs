using System;
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
	/// <summary>A RecordReader that reads records from a SQL table.</summary>
	/// <remarks>
	/// A RecordReader that reads records from a SQL table.
	/// Emits LongWritables containing the record number as
	/// key and DBWritables as value.
	/// </remarks>
	public class DBRecordReader<T> : RecordReader<LongWritable, T>
		where T : DBWritable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBRecordReader
			));

		private ResultSet results = null;

		private Type inputClass;

		private Configuration conf;

		private DBInputFormat.DBInputSplit split;

		private long pos = 0;

		private LongWritable key = null;

		private T value = null;

		private Connection connection;

		protected internal PreparedStatement statement;

		private DBConfiguration dbConf;

		private string conditions;

		private string[] fieldNames;

		private string tableName;

		/// <param name="split">The InputSplit to read data for</param>
		/// <exception cref="Java.Sql.SQLException"></exception>
		public DBRecordReader(DBInputFormat.DBInputSplit split, Type inputClass, Configuration
			 conf, Connection conn, DBConfiguration dbConfig, string cond, string[] fields, 
			string table)
		{
			this.inputClass = inputClass;
			this.split = split;
			this.conf = conf;
			this.connection = conn;
			this.dbConf = dbConfig;
			this.conditions = cond;
			this.fieldNames = fields;
			this.tableName = table;
		}

		/// <exception cref="Java.Sql.SQLException"/>
		protected internal virtual ResultSet ExecuteQuery(string query)
		{
			this.statement = connection.PrepareStatement(query, ResultSet.TypeForwardOnly, ResultSet
				.ConcurReadOnly);
			return statement.ExecuteQuery();
		}

		/// <summary>
		/// Returns the query for selecting the records,
		/// subclasses can override this for custom behaviour.
		/// </summary>
		protected internal virtual string GetSelectQuery()
		{
			StringBuilder query = new StringBuilder();
			// Default codepath for MySQL, HSQLDB, etc. Relies on LIMIT/OFFSET for splits.
			if (dbConf.GetInputQuery() == null)
			{
				query.Append("SELECT ");
				for (int i = 0; i < fieldNames.Length; i++)
				{
					query.Append(fieldNames[i]);
					if (i != fieldNames.Length - 1)
					{
						query.Append(", ");
					}
				}
				query.Append(" FROM ").Append(tableName);
				query.Append(" AS ").Append(tableName);
				//in hsqldb this is necessary
				if (conditions != null && conditions.Length > 0)
				{
					query.Append(" WHERE (").Append(conditions).Append(")");
				}
				string orderBy = dbConf.GetInputOrderBy();
				if (orderBy != null && orderBy.Length > 0)
				{
					query.Append(" ORDER BY ").Append(orderBy);
				}
			}
			else
			{
				//PREBUILT QUERY
				query.Append(dbConf.GetInputQuery());
			}
			try
			{
				query.Append(" LIMIT ").Append(split.GetLength());
				query.Append(" OFFSET ").Append(split.GetStart());
			}
			catch (IOException)
			{
			}
			// Ignore, will not throw.
			return query.ToString();
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			try
			{
				if (null != results)
				{
					results.Close();
				}
				if (null != statement)
				{
					statement.Close();
				}
				if (null != connection)
				{
					connection.Commit();
					connection.Close();
				}
			}
			catch (SQLException e)
			{
				throw new IOException(e.Message);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Initialize(InputSplit split, TaskAttemptContext context)
		{
		}

		//do nothing
		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override LongWritable GetCurrentKey()
		{
			return key;
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		public override T GetCurrentValue()
		{
			return value;
		}

		[System.ObsoleteAttribute(@"")]
		public virtual T CreateValue()
		{
			return ReflectionUtils.NewInstance(inputClass, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"")]
		public virtual long GetPos()
		{
			return pos;
		}

		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"Use DBRecordReader{T}.NextKeyValue()")]
		public virtual bool Next(LongWritable key, T value)
		{
			this.key = key;
			this.value = value;
			return NextKeyValue();
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override float GetProgress()
		{
			return pos / (float)split.GetLength();
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool NextKeyValue()
		{
			try
			{
				if (key == null)
				{
					key = new LongWritable();
				}
				if (value == null)
				{
					value = CreateValue();
				}
				if (null == this.results)
				{
					// First time into this method, run the query.
					this.results = ExecuteQuery(GetSelectQuery());
				}
				if (!results.Next())
				{
					return false;
				}
				// Set the key field value as the output key value
				key.Set(pos + split.GetStart());
				value.ReadFields(results);
				pos++;
			}
			catch (SQLException e)
			{
				throw new IOException("SQLException in nextKeyValue", e);
			}
			return true;
		}

		protected internal virtual DBInputFormat.DBInputSplit GetSplit()
		{
			return split;
		}

		protected internal virtual string[] GetFieldNames()
		{
			return fieldNames;
		}

		protected internal virtual string GetTableName()
		{
			return tableName;
		}

		protected internal virtual string GetConditions()
		{
			return conditions;
		}

		protected internal virtual DBConfiguration GetDBConf()
		{
			return dbConf;
		}

		protected internal virtual Connection GetConnection()
		{
			return connection;
		}

		protected internal virtual PreparedStatement GetStatement()
		{
			return statement;
		}

		protected internal virtual void SetStatement(PreparedStatement stmt)
		{
			this.statement = stmt;
		}
	}
}
