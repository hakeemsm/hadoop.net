using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Java.Sql;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>A InputFormat that reads input data from an SQL table.</summary>
	/// <remarks>
	/// A InputFormat that reads input data from an SQL table.
	/// Operates like DBInputFormat, but instead of using LIMIT and OFFSET to demarcate
	/// splits, it tries to generate WHERE clauses which separate the data into roughly
	/// equivalent shards.
	/// </remarks>
	public class DataDrivenDBInputFormat<T> : DBInputFormat<T>, Configurable
		where T : DBWritable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(DataDrivenDBInputFormat
			));

		/// <summary>
		/// If users are providing their own query, the following string is expected to
		/// appear in the WHERE clause, which will be substituted with a pair of conditions
		/// on the input to allow input splits to parallelise the import.
		/// </summary>
		public const string SubstituteToken = "$CONDITIONS";

		/// <summary>A InputSplit that spans a set of rows</summary>
		public class DataDrivenDBInputSplit : DBInputFormat.DBInputSplit
		{
			private string lowerBoundClause;

			private string upperBoundClause;

			/// <summary>Default Constructor</summary>
			public DataDrivenDBInputSplit()
			{
			}

			/// <summary>Convenience Constructor</summary>
			/// <param name="lower">the string to be put in the WHERE clause to guard on the 'lower' end
			/// 	</param>
			/// <param name="upper">the string to be put in the WHERE clause to guard on the 'upper' end
			/// 	</param>
			public DataDrivenDBInputSplit(string lower, string upper)
			{
				this.lowerBoundClause = lower;
				this.upperBoundClause = upper;
			}

			/// <returns>The total row count in this split</returns>
			/// <exception cref="System.IO.IOException"/>
			public override long GetLength()
			{
				return 0;
			}

			// unfortunately, we don't know this.
			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public override void ReadFields(DataInput input)
			{
				this.lowerBoundClause = Text.ReadString(input);
				this.upperBoundClause = Text.ReadString(input);
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public override void Write(DataOutput output)
			{
				Text.WriteString(output, this.lowerBoundClause);
				Text.WriteString(output, this.upperBoundClause);
			}

			public virtual string GetLowerClause()
			{
				return lowerBoundClause;
			}

			public virtual string GetUpperClause()
			{
				return upperBoundClause;
			}
		}

		/// <returns>the DBSplitter implementation to use to divide the table/query into InputSplits.
		/// 	</returns>
		protected internal virtual DBSplitter GetSplitter(int sqlDataType)
		{
			switch (sqlDataType)
			{
				case Types.Numeric:
				case Types.Decimal:
				{
					return new BigDecimalSplitter();
				}

				case Types.Bit:
				case Types.Boolean:
				{
					return new BooleanSplitter();
				}

				case Types.Integer:
				case Types.Tinyint:
				case Types.Smallint:
				case Types.Bigint:
				{
					return new IntegerSplitter();
				}

				case Types.Real:
				case Types.Float:
				case Types.Double:
				{
					return new FloatSplitter();
				}

				case Types.Char:
				case Types.Varchar:
				case Types.Longvarchar:
				{
					return new TextSplitter();
				}

				case Types.Date:
				case Types.Time:
				case Types.Timestamp:
				{
					return new DateSplitter();
				}

				default:
				{
					// TODO: Support BINARY, VARBINARY, LONGVARBINARY, DISTINCT, CLOB, BLOB, ARRAY
					// STRUCT, REF, DATALINK, and JAVA_OBJECT.
					return null;
				}
			}
		}

		/// <summary>
		/// <inheritDoc/>
		/// 
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override IList<InputSplit> GetSplits(JobContext job)
		{
			int targetNumTasks = job.GetConfiguration().GetInt(MRJobConfig.NumMaps, 1);
			if (1 == targetNumTasks)
			{
				// There's no need to run a bounding vals query; just return a split
				// that separates nothing. This can be considerably more optimal for a
				// large table with no index.
				IList<InputSplit> singletonSplit = new AList<InputSplit>();
				singletonSplit.AddItem(new DataDrivenDBInputFormat.DataDrivenDBInputSplit("1=1", 
					"1=1"));
				return singletonSplit;
			}
			ResultSet results = null;
			Statement statement = null;
			try
			{
				statement = connection.CreateStatement();
				results = statement.ExecuteQuery(GetBoundingValsQuery());
				results.Next();
				// Based on the type of the results, use a different mechanism
				// for interpolating split points (i.e., numeric splits, text splits,
				// dates, etc.)
				int sqlDataType = results.GetMetaData().GetColumnType(1);
				DBSplitter splitter = GetSplitter(sqlDataType);
				if (null == splitter)
				{
					throw new IOException("Unknown SQL data type: " + sqlDataType);
				}
				return splitter.Split(job.GetConfiguration(), results, GetDBConf().GetInputOrderBy
					());
			}
			catch (SQLException e)
			{
				throw new IOException(e.Message);
			}
			finally
			{
				// More-or-less ignore SQL exceptions here, but log in case we need it.
				try
				{
					if (null != results)
					{
						results.Close();
					}
				}
				catch (SQLException se)
				{
					Log.Debug("SQLException closing resultset: " + se.ToString());
				}
				try
				{
					if (null != statement)
					{
						statement.Close();
					}
				}
				catch (SQLException se)
				{
					Log.Debug("SQLException closing statement: " + se.ToString());
				}
				try
				{
					connection.Commit();
					CloseConnection();
				}
				catch (SQLException se)
				{
					Log.Debug("SQLException committing split transaction: " + se.ToString());
				}
			}
		}

		/// <returns>
		/// a query which returns the minimum and maximum values for
		/// the order-by column.
		/// The min value should be in the first column, and the
		/// max value should be in the second column of the results.
		/// </returns>
		protected internal virtual string GetBoundingValsQuery()
		{
			// If the user has provided a query, use that instead.
			string userQuery = GetDBConf().GetInputBoundingQuery();
			if (null != userQuery)
			{
				return userQuery;
			}
			// Auto-generate one based on the table name we've been provided with.
			StringBuilder query = new StringBuilder();
			string splitCol = GetDBConf().GetInputOrderBy();
			query.Append("SELECT MIN(").Append(splitCol).Append("), ");
			query.Append("MAX(").Append(splitCol).Append(") FROM ");
			query.Append(GetDBConf().GetInputTableName());
			string conditions = GetDBConf().GetInputConditions();
			if (null != conditions)
			{
				query.Append(" WHERE ( " + conditions + " )");
			}
			return query.ToString();
		}

		/// <summary>Set the user-defined bounding query to use with a user-defined query.</summary>
		/// <remarks>
		/// Set the user-defined bounding query to use with a user-defined query.
		/// This *must* include the substring "$CONDITIONS"
		/// (DataDrivenDBInputFormat.SUBSTITUTE_TOKEN) inside the WHERE clause,
		/// so that DataDrivenDBInputFormat knows where to insert split clauses.
		/// e.g., "SELECT foo FROM mytable WHERE $CONDITIONS"
		/// This will be expanded to something like:
		/// SELECT foo FROM mytable WHERE (id &gt; 100) AND (id &lt; 250)
		/// inside each split.
		/// </remarks>
		public static void SetBoundingQuery(Configuration conf, string query)
		{
			if (null != query)
			{
				// If the user's settng a query, warn if they don't allow conditions.
				if (query.IndexOf(SubstituteToken) == -1)
				{
					Log.Warn("Could not find " + SubstituteToken + " token in query: " + query + "; splits may not partition data."
						);
				}
			}
			conf.Set(DBConfiguration.InputBoundingQuery, query);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override RecordReader<LongWritable, T> CreateDBRecordReader(DBInputFormat.DBInputSplit
			 split, Configuration conf)
		{
			DBConfiguration dbConf = GetDBConf();
			Type inputClass = (Type)(dbConf.GetInputClass());
			string dbProductName = GetDBProductName();
			Log.Debug("Creating db record reader for db product: " + dbProductName);
			try
			{
				// use database product name to determine appropriate record reader.
				if (dbProductName.StartsWith("MYSQL"))
				{
					// use MySQL-specific db reader.
					return new MySQLDataDrivenDBRecordReader<T>(split, inputClass, conf, CreateConnection
						(), dbConf, dbConf.GetInputConditions(), dbConf.GetInputFieldNames(), dbConf.GetInputTableName
						());
				}
				else
				{
					// Generic reader.
					return new DataDrivenDBRecordReader<T>(split, inputClass, conf, CreateConnection(
						), dbConf, dbConf.GetInputConditions(), dbConf.GetInputFieldNames(), dbConf.GetInputTableName
						(), dbProductName);
				}
			}
			catch (SQLException ex)
			{
				throw new IOException(ex.Message);
			}
		}

		// Configuration methods override superclass to ensure that the proper
		// DataDrivenDBInputFormat gets used.
		/// <summary>Note that the "orderBy" column is called the "splitBy" in this version.</summary>
		/// <remarks>
		/// Note that the "orderBy" column is called the "splitBy" in this version.
		/// We reuse the same field, but it's not strictly ordering it -- just partitioning
		/// the results.
		/// </remarks>
		public static void SetInput(Job job, Type inputClass, string tableName, string conditions
			, string splitBy, params string[] fieldNames)
		{
			DBInputFormat.SetInput(job, inputClass, tableName, conditions, splitBy, fieldNames
				);
			job.SetInputFormatClass(typeof(DataDrivenDBInputFormat));
		}

		/// <summary>
		/// setInput() takes a custom query and a separate "bounding query" to use
		/// instead of the custom "count query" used by DBInputFormat.
		/// </summary>
		public static void SetInput(Job job, Type inputClass, string inputQuery, string inputBoundingQuery
			)
		{
			DBInputFormat.SetInput(job, inputClass, inputQuery, string.Empty);
			job.GetConfiguration().Set(DBConfiguration.InputBoundingQuery, inputBoundingQuery
				);
			job.SetInputFormatClass(typeof(DataDrivenDBInputFormat));
		}
	}
}
