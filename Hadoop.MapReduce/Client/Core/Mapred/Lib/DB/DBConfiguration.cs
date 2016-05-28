using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.DB
{
	public class DBConfiguration : Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
	{
		/// <summary>The JDBC Driver class name</summary>
		public const string DriverClassProperty = Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
			.DriverClassProperty;

		/// <summary>JDBC Database access URL</summary>
		public const string UrlProperty = Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
			.UrlProperty;

		/// <summary>User name to access the database</summary>
		public const string UsernameProperty = Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
			.UsernameProperty;

		/// <summary>Password to access the database</summary>
		public const string PasswordProperty = Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
			.PasswordProperty;

		/// <summary>Input table name</summary>
		public const string InputTableNameProperty = Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
			.InputTableNameProperty;

		/// <summary>Field names in the Input table</summary>
		public const string InputFieldNamesProperty = Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
			.InputFieldNamesProperty;

		/// <summary>WHERE clause in the input SELECT statement</summary>
		public const string InputConditionsProperty = Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
			.InputConditionsProperty;

		/// <summary>ORDER BY clause in the input SELECT statement</summary>
		public const string InputOrderByProperty = Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
			.InputOrderByProperty;

		/// <summary>Whole input query, exluding LIMIT...OFFSET</summary>
		public const string InputQuery = Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
			.InputQuery;

		/// <summary>Input query to get the count of records</summary>
		public const string InputCountQuery = Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
			.InputCountQuery;

		/// <summary>Class name implementing DBWritable which will hold input tuples</summary>
		public const string InputClassProperty = Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
			.InputClassProperty;

		/// <summary>Output table name</summary>
		public const string OutputTableNameProperty = Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
			.OutputTableNameProperty;

		/// <summary>Field names in the Output table</summary>
		public const string OutputFieldNamesProperty = Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
			.OutputFieldNamesProperty;

		/// <summary>Number of fields in the Output table</summary>
		public const string OutputFieldCountProperty = Org.Apache.Hadoop.Mapreduce.Lib.DB.DBConfiguration
			.OutputFieldCountProperty;

		/// <summary>Sets the DB access related fields in the JobConf.</summary>
		/// <param name="job">the job</param>
		/// <param name="driverClass">JDBC Driver class name</param>
		/// <param name="dbUrl">JDBC DB access URL.</param>
		/// <param name="userName">DB access username</param>
		/// <param name="passwd">DB access passwd</param>
		public static void ConfigureDB(JobConf job, string driverClass, string dbUrl, string
			 userName, string passwd)
		{
			job.Set(DriverClassProperty, driverClass);
			job.Set(UrlProperty, dbUrl);
			if (userName != null)
			{
				job.Set(UsernameProperty, userName);
			}
			if (passwd != null)
			{
				job.Set(PasswordProperty, passwd);
			}
		}

		/// <summary>Sets the DB access related fields in the JobConf.</summary>
		/// <param name="job">the job</param>
		/// <param name="driverClass">JDBC Driver class name</param>
		/// <param name="dbUrl">JDBC DB access URL.</param>
		public static void ConfigureDB(JobConf job, string driverClass, string dbUrl)
		{
			ConfigureDB(job, driverClass, dbUrl, null, null);
		}

		internal DBConfiguration(JobConf job)
			: base(job)
		{
		}
	}
}
