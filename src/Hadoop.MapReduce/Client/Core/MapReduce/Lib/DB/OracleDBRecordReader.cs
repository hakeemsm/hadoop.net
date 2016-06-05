using System;
using System.IO;
using System.Reflection;
using System.Text;
using Java.Sql;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>A RecordReader that reads records from an Oracle SQL table.</summary>
	public class OracleDBRecordReader<T> : DBRecordReader<T>
		where T : DBWritable
	{
		/// <summary>Configuration key to set to a timezone string.</summary>
		public const string SessionTimezoneKey = "oracle.sessionTimeZone";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.DB.OracleDBRecordReader
			));

		/// <exception cref="Java.Sql.SQLException"/>
		public OracleDBRecordReader(DBInputFormat.DBInputSplit split, Type inputClass, Configuration
			 conf, Connection conn, DBConfiguration dbConfig, string cond, string[] fields, 
			string table)
			: base(split, inputClass, conf, conn, dbConfig, cond, fields, table)
		{
			SetSessionTimeZone(conf, conn);
		}

		/// <summary>Returns the query for selecting the records from an Oracle DB.</summary>
		protected internal override string GetSelectQuery()
		{
			StringBuilder query = new StringBuilder();
			DBConfiguration dbConf = GetDBConf();
			string conditions = GetConditions();
			string tableName = GetTableName();
			string[] fieldNames = GetFieldNames();
			// Oracle-specific codepath to use rownum instead of LIMIT/OFFSET.
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
				if (conditions != null && conditions.Length > 0)
				{
					query.Append(" WHERE ").Append(conditions);
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
				DBInputFormat.DBInputSplit split = GetSplit();
				if (split.GetLength() > 0)
				{
					string querystring = query.ToString();
					query = new StringBuilder();
					query.Append("SELECT * FROM (SELECT a.*,ROWNUM dbif_rno FROM ( ");
					query.Append(querystring);
					query.Append(" ) a WHERE rownum <= ").Append(split.GetEnd());
					query.Append(" ) WHERE dbif_rno > ").Append(split.GetStart());
				}
			}
			catch (IOException)
			{
			}
			// ignore, will not throw.
			return query.ToString();
		}

		/// <summary>Set session time zone</summary>
		/// <param name="conf">
		/// The current configuration.
		/// We read the 'oracle.sessionTimeZone' property from here.
		/// </param>
		/// <param name="conn">The connection to alter the timezone properties of.</param>
		/// <exception cref="Java.Sql.SQLException"/>
		public static void SetSessionTimeZone(Configuration conf, Connection conn)
		{
			// need to use reflection to call the method setSessionTimeZone on
			// the OracleConnection class because oracle specific java libraries are
			// not accessible in this context.
			MethodInfo method;
			try
			{
				method = conn.GetType().GetMethod("setSessionTimeZone", new Type[] { typeof(string
					) });
			}
			catch (Exception ex)
			{
				Log.Error("Could not find method setSessionTimeZone in " + conn.GetType().FullName
					, ex);
				// rethrow SQLException
				throw new SQLException(ex);
			}
			// Need to set the time zone in order for Java
			// to correctly access the column "TIMESTAMP WITH LOCAL TIME ZONE".
			// We can't easily get the correct Oracle-specific timezone string
			// from Java; just let the user set the timezone in a property.
			string clientTimeZone = conf.Get(SessionTimezoneKey, "GMT");
			try
			{
				method.Invoke(conn, clientTimeZone);
				Log.Info("Time zone has been set to " + clientTimeZone);
			}
			catch (Exception ex)
			{
				Log.Warn("Time zone " + clientTimeZone + " could not be set on Oracle database.");
				Log.Warn("Setting default time zone: GMT");
				try
				{
					// "GMT" timezone is guaranteed to exist.
					method.Invoke(conn, "GMT");
				}
				catch (Exception ex2)
				{
					Log.Error("Could not set time zone for oracle connection", ex2);
					// rethrow SQLException
					throw new SQLException(ex);
				}
			}
		}
	}
}
