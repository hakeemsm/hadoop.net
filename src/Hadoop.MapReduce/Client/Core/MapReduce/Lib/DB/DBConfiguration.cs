using System;
using Java.Sql;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>A container for configuration property names for jobs with DB input/output.
	/// 	</summary>
	/// <remarks>
	/// A container for configuration property names for jobs with DB input/output.
	/// The job can be configured using the static methods in this class,
	/// <see cref="DBInputFormat{T}"/>
	/// , and
	/// <see cref="DBOutputFormat{K, V}"/>
	/// .
	/// Alternatively, the properties can be set in the configuration with proper
	/// values.
	/// </remarks>
	/// <seealso cref="ConfigureDB(Org.Apache.Hadoop.Conf.Configuration, string, string, string, string)
	/// 	"/>
	/// <seealso cref="DBInputFormat{T}.SetInput(Org.Apache.Hadoop.Mapreduce.Job, System.Type{T}, string, string)
	/// 	"/>
	/// <seealso cref="DBInputFormat{T}.SetInput(Org.Apache.Hadoop.Mapreduce.Job, System.Type{T}, string, string, string, string[])
	/// 	"/>
	/// <seealso cref="DBOutputFormat{K, V}.SetOutput(Org.Apache.Hadoop.Mapreduce.Job, string, string[])
	/// 	"/>
	public class DBConfiguration
	{
		/// <summary>The JDBC Driver class name</summary>
		public const string DriverClassProperty = "mapreduce.jdbc.driver.class";

		/// <summary>JDBC Database access URL</summary>
		public const string UrlProperty = "mapreduce.jdbc.url";

		/// <summary>User name to access the database</summary>
		public const string UsernameProperty = "mapreduce.jdbc.username";

		/// <summary>Password to access the database</summary>
		public const string PasswordProperty = "mapreduce.jdbc.password";

		/// <summary>Input table name</summary>
		public const string InputTableNameProperty = "mapreduce.jdbc.input.table.name";

		/// <summary>Field names in the Input table</summary>
		public const string InputFieldNamesProperty = "mapreduce.jdbc.input.field.names";

		/// <summary>WHERE clause in the input SELECT statement</summary>
		public const string InputConditionsProperty = "mapreduce.jdbc.input.conditions";

		/// <summary>ORDER BY clause in the input SELECT statement</summary>
		public const string InputOrderByProperty = "mapreduce.jdbc.input.orderby";

		/// <summary>Whole input query, exluding LIMIT...OFFSET</summary>
		public const string InputQuery = "mapreduce.jdbc.input.query";

		/// <summary>Input query to get the count of records</summary>
		public const string InputCountQuery = "mapreduce.jdbc.input.count.query";

		/// <summary>Input query to get the max and min values of the jdbc.input.query</summary>
		public const string InputBoundingQuery = "mapred.jdbc.input.bounding.query";

		/// <summary>Class name implementing DBWritable which will hold input tuples</summary>
		public const string InputClassProperty = "mapreduce.jdbc.input.class";

		/// <summary>Output table name</summary>
		public const string OutputTableNameProperty = "mapreduce.jdbc.output.table.name";

		/// <summary>Field names in the Output table</summary>
		public const string OutputFieldNamesProperty = "mapreduce.jdbc.output.field.names";

		/// <summary>Number of fields in the Output table</summary>
		public const string OutputFieldCountProperty = "mapreduce.jdbc.output.field.count";

		/// <summary>
		/// Sets the DB access related fields in the
		/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
		/// .
		/// </summary>
		/// <param name="conf">the configuration</param>
		/// <param name="driverClass">JDBC Driver class name</param>
		/// <param name="dbUrl">JDBC DB access URL.</param>
		/// <param name="userName">DB access username</param>
		/// <param name="passwd">DB access passwd</param>
		public static void ConfigureDB(Configuration conf, string driverClass, string dbUrl
			, string userName, string passwd)
		{
			conf.Set(DriverClassProperty, driverClass);
			conf.Set(UrlProperty, dbUrl);
			if (userName != null)
			{
				conf.Set(UsernameProperty, userName);
			}
			if (passwd != null)
			{
				conf.Set(PasswordProperty, passwd);
			}
		}

		/// <summary>Sets the DB access related fields in the JobConf.</summary>
		/// <param name="job">the job</param>
		/// <param name="driverClass">JDBC Driver class name</param>
		/// <param name="dbUrl">JDBC DB access URL.</param>
		public static void ConfigureDB(Configuration job, string driverClass, string dbUrl
			)
		{
			ConfigureDB(job, driverClass, dbUrl, null, null);
		}

		private Configuration conf;

		public DBConfiguration(Configuration job)
		{
			this.conf = job;
		}

		/// <summary>Returns a connection object o the DB</summary>
		/// <exception cref="System.TypeLoadException"></exception>
		/// <exception cref="Java.Sql.SQLException"></exception>
		public virtual Connection GetConnection()
		{
			Sharpen.Runtime.GetType(conf.Get(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
				.DriverClassProperty));
			if (conf.Get(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.UsernameProperty)
				 == null)
			{
				return DriverManager.GetConnection(conf.Get(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
					.UrlProperty));
			}
			else
			{
				return DriverManager.GetConnection(conf.Get(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
					.UrlProperty), conf.Get(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.UsernameProperty
					), conf.Get(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.PasswordProperty)
					);
			}
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		public virtual string GetInputTableName()
		{
			return conf.Get(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputTableNameProperty
				);
		}

		public virtual void SetInputTableName(string tableName)
		{
			conf.Set(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputTableNameProperty
				, tableName);
		}

		public virtual string[] GetInputFieldNames()
		{
			return conf.GetStrings(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputFieldNamesProperty
				);
		}

		public virtual void SetInputFieldNames(params string[] fieldNames)
		{
			conf.SetStrings(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputFieldNamesProperty
				, fieldNames);
		}

		public virtual string GetInputConditions()
		{
			return conf.Get(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputConditionsProperty
				);
		}

		public virtual void SetInputConditions(string conditions)
		{
			if (conditions != null && conditions.Length > 0)
			{
				conf.Set(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputConditionsProperty
					, conditions);
			}
		}

		public virtual string GetInputOrderBy()
		{
			return conf.Get(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputOrderByProperty
				);
		}

		public virtual void SetInputOrderBy(string orderby)
		{
			if (orderby != null && orderby.Length > 0)
			{
				conf.Set(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputOrderByProperty, 
					orderby);
			}
		}

		public virtual string GetInputQuery()
		{
			return conf.Get(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputQuery);
		}

		public virtual void SetInputQuery(string query)
		{
			if (query != null && query.Length > 0)
			{
				conf.Set(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputQuery, query);
			}
		}

		public virtual string GetInputCountQuery()
		{
			return conf.Get(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputCountQuery
				);
		}

		public virtual void SetInputCountQuery(string query)
		{
			if (query != null && query.Length > 0)
			{
				conf.Set(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputCountQuery, query
					);
			}
		}

		public virtual void SetInputBoundingQuery(string query)
		{
			if (query != null && query.Length > 0)
			{
				conf.Set(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputBoundingQuery, query
					);
			}
		}

		public virtual string GetInputBoundingQuery()
		{
			return conf.Get(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputBoundingQuery
				);
		}

		public virtual Type GetInputClass()
		{
			return conf.GetClass(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputClassProperty
				, typeof(DBInputFormat.NullDBWritable));
		}

		public virtual void SetInputClass(Type inputClass)
		{
			conf.SetClass(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.InputClassProperty
				, inputClass, typeof(DBWritable));
		}

		public virtual string GetOutputTableName()
		{
			return conf.Get(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.OutputTableNameProperty
				);
		}

		public virtual void SetOutputTableName(string tableName)
		{
			conf.Set(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.OutputTableNameProperty
				, tableName);
		}

		public virtual string[] GetOutputFieldNames()
		{
			return conf.GetStrings(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.OutputFieldNamesProperty
				);
		}

		public virtual void SetOutputFieldNames(params string[] fieldNames)
		{
			conf.SetStrings(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.OutputFieldNamesProperty
				, fieldNames);
		}

		public virtual void SetOutputFieldCount(int fieldCount)
		{
			conf.SetInt(Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration.OutputFieldCountProperty
				, fieldCount);
		}

		public virtual int GetOutputFieldCount()
		{
			return conf.GetInt(OutputFieldCountProperty, 0);
		}
	}
}
