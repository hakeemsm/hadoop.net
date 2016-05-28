using System;
using System.Text;
using Java.Sql;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>
	/// A RecordReader that reads records from a SQL table,
	/// using data-driven WHERE clause splits.
	/// </summary>
	/// <remarks>
	/// A RecordReader that reads records from a SQL table,
	/// using data-driven WHERE clause splits.
	/// Emits LongWritables containing the record number as
	/// key and DBWritables as value.
	/// </remarks>
	public class DataDrivenDBRecordReader<T> : DBRecordReader<T>
		where T : DBWritable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.DB.DataDrivenDBRecordReader
			));

		private string dbProductName;

		/// <param name="split">The InputSplit to read data for</param>
		/// <exception cref="Java.Sql.SQLException"></exception>
		public DataDrivenDBRecordReader(DBInputFormat.DBInputSplit split, Type inputClass
			, Configuration conf, Connection conn, DBConfiguration dbConfig, string cond, string
			[] fields, string table, string dbProduct)
			: base(split, inputClass, conf, conn, dbConfig, cond, fields, table)
		{
			// database manufacturer string.
			this.dbProductName = dbProduct;
		}

		/// <summary>
		/// Returns the query for selecting the records,
		/// subclasses can override this for custom behaviour.
		/// </summary>
		protected internal override string GetSelectQuery()
		{
			StringBuilder query = new StringBuilder();
			DataDrivenDBInputFormat.DataDrivenDBInputSplit dataSplit = (DataDrivenDBInputFormat.DataDrivenDBInputSplit
				)GetSplit();
			DBConfiguration dbConf = GetDBConf();
			string[] fieldNames = GetFieldNames();
			string tableName = GetTableName();
			string conditions = GetConditions();
			// Build the WHERE clauses associated with the data split first.
			// We need them in both branches of this function.
			StringBuilder conditionClauses = new StringBuilder();
			conditionClauses.Append("( ").Append(dataSplit.GetLowerClause());
			conditionClauses.Append(" ) AND ( ").Append(dataSplit.GetUpperClause());
			conditionClauses.Append(" )");
			if (dbConf.GetInputQuery() == null)
			{
				// We need to generate the entire query.
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
				if (!dbProductName.StartsWith("ORACLE"))
				{
					// Seems to be necessary for hsqldb? Oracle explicitly does *not*
					// use this clause.
					query.Append(" AS ").Append(tableName);
				}
				query.Append(" WHERE ");
				if (conditions != null && conditions.Length > 0)
				{
					// Put the user's conditions first.
					query.Append("( ").Append(conditions).Append(" ) AND ");
				}
				// Now append the conditions associated with our split.
				query.Append(conditionClauses.ToString());
			}
			else
			{
				// User provided the query. We replace the special token with our WHERE clause.
				string inputQuery = dbConf.GetInputQuery();
				if (inputQuery.IndexOf(DataDrivenDBInputFormat.SubstituteToken) == -1)
				{
					Log.Error("Could not find the clause substitution token " + DataDrivenDBInputFormat
						.SubstituteToken + " in the query: [" + inputQuery + "]. Parallel splits may not work correctly."
						);
				}
				query.Append(inputQuery.Replace(DataDrivenDBInputFormat.SubstituteToken, conditionClauses
					.ToString()));
			}
			Log.Debug("Using query: " + query.ToString());
			return query.ToString();
		}
	}
}
