using System;
using System.Collections.Generic;
using System.IO;
using Java.Sql;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.DB;
using Org.Apache.Hadoop.Mapreduce.Lib.Reduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	/// <summary>
	/// This is a demonstrative program, which uses DBInputFormat for reading
	/// the input data from a database, and DBOutputFormat for writing the data
	/// to the database.
	/// </summary>
	/// <remarks>
	/// This is a demonstrative program, which uses DBInputFormat for reading
	/// the input data from a database, and DBOutputFormat for writing the data
	/// to the database.
	/// <br />
	/// The Program first creates the necessary tables, populates the input table
	/// and runs the mapred job.
	/// <br />
	/// The input data is a mini access log, with a <code>&lt;url,referrer,time&gt;
	/// </code> schema.The output is the number of pageviews of each url in the log,
	/// having the schema <code>&lt;url,pageview&gt;</code>.
	/// When called with no arguments the program starts a local HSQLDB server, and
	/// uses this database for storing/retrieving the data.
	/// <br />
	/// This program requires some additional configuration relating to HSQLDB.
	/// The the hsqldb jar should be added to the classpath:
	/// <br />
	/// <code>export HADOOP_CLASSPATH=share/hadoop/mapreduce/lib-examples/hsqldb-2.0.0.jar</code>
	/// <br />
	/// And the hsqldb jar should be included with the <code>-libjars</code>
	/// argument when executing it with hadoop:
	/// <br />
	/// <code>-libjars share/hadoop/mapreduce/lib-examples/hsqldb-2.0.0.jar</code>
	/// </remarks>
	public class DBCountPageView : Configured, Tool
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(DBCountPageView));

		private Connection connection;

		private bool initialized = false;

		private static readonly string[] AccessFieldNames = new string[] { "url", "referrer"
			, "time" };

		private static readonly string[] PageviewFieldNames = new string[] { "url", "pageview"
			 };

		private const string DbUrl = "jdbc:hsqldb:hsql://localhost/URLAccess";

		private const string DriverClass = "org.hsqldb.jdbc.JDBCDriver";

		private Org.Hsqldb.Server.Server server;

		private void StartHsqldbServer()
		{
			server = new Org.Hsqldb.Server.Server();
			server.SetDatabasePath(0, Runtime.GetProperty("test.build.data", "/tmp") + "/URLAccess"
				);
			server.SetDatabaseName(0, "URLAccess");
			server.Start();
		}

		/// <exception cref="System.Exception"/>
		private void CreateConnection(string driverClassName, string url)
		{
			Sharpen.Runtime.GetType(driverClassName);
			connection = DriverManager.GetConnection(url);
			connection.SetAutoCommit(false);
		}

		private void Shutdown()
		{
			try
			{
				connection.Commit();
				connection.Close();
			}
			catch (Exception ex)
			{
				Log.Warn("Exception occurred while closing connection :" + StringUtils.StringifyException
					(ex));
			}
			finally
			{
				try
				{
					if (server != null)
					{
						server.Shutdown();
					}
				}
				catch (Exception ex)
				{
					Log.Warn("Exception occurred while shutting down HSQLDB :" + StringUtils.StringifyException
						(ex));
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void Initialize(string driverClassName, string url)
		{
			if (!this.initialized)
			{
				if (driverClassName.Equals(DriverClass))
				{
					StartHsqldbServer();
				}
				CreateConnection(driverClassName, url);
				DropTables();
				CreateTables();
				PopulateAccess();
				this.initialized = true;
			}
		}

		private void DropTables()
		{
			string dropAccess = "DROP TABLE Access";
			string dropPageview = "DROP TABLE Pageview";
			Statement st = null;
			try
			{
				st = connection.CreateStatement();
				st.ExecuteUpdate(dropAccess);
				st.ExecuteUpdate(dropPageview);
				connection.Commit();
				st.Close();
			}
			catch (SQLException)
			{
				try
				{
					if (st != null)
					{
						st.Close();
					}
				}
				catch (Exception)
				{
				}
			}
		}

		/// <exception cref="Java.Sql.SQLException"/>
		private void CreateTables()
		{
			string createAccess = "CREATE TABLE " + "Access(url      VARCHAR(100) NOT NULL," 
				+ " referrer VARCHAR(100)," + " time     BIGINT NOT NULL, " + " PRIMARY KEY (url, time))";
			string createPageview = "CREATE TABLE " + "Pageview(url      VARCHAR(100) NOT NULL,"
				 + " pageview     BIGINT NOT NULL, " + " PRIMARY KEY (url))";
			Statement st = connection.CreateStatement();
			try
			{
				st.ExecuteUpdate(createAccess);
				st.ExecuteUpdate(createPageview);
				connection.Commit();
			}
			finally
			{
				st.Close();
			}
		}

		/// <summary>Populates the Access table with generated records.</summary>
		/// <exception cref="Java.Sql.SQLException"/>
		private void PopulateAccess()
		{
			PreparedStatement statement = null;
			try
			{
				statement = connection.PrepareStatement("INSERT INTO Access(url, referrer, time)"
					 + " VALUES (?, ?, ?)");
				Random random = new Random();
				int time = random.Next(50) + 50;
				int ProbabilityPrecision = 100;
				//  1 / 100 
				int NewPageProbability = 15;
				//  15 / 100
				//Pages in the site :
				string[] pages = new string[] { "/a", "/b", "/c", "/d", "/e", "/f", "/g", "/h", "/i"
					, "/j" };
				//linkMatrix[i] is the array of pages(indexes) that page_i links to.  
				int[][] linkMatrix = new int[][] { new int[] { 1, 5, 7 }, new int[] { 0, 7, 4, 6 }
					, new int[] { 0, 1, 7, 8 }, new int[] { 0, 2, 4, 6, 7, 9 }, new int[] { 0, 1 }, 
					new int[] { 0, 3, 5, 9 }, new int[] { 0 }, new int[] { 0, 1, 3 }, new int[] { 0, 
					2, 6 }, new int[] { 0, 2, 6 } };
				//a mini model of user browsing a la pagerank
				int currentPage = random.Next(pages.Length);
				string referrer = null;
				for (int i = 0; i < time; i++)
				{
					statement.SetString(1, pages[currentPage]);
					statement.SetString(2, referrer);
					statement.SetLong(3, i);
					statement.Execute();
					int action = random.Next(ProbabilityPrecision);
					// go to a new page with probability 
					// NEW_PAGE_PROBABILITY / PROBABILITY_PRECISION
					if (action < NewPageProbability)
					{
						currentPage = random.Next(pages.Length);
						// a random page
						referrer = null;
					}
					else
					{
						referrer = pages[currentPage];
						action = random.Next(linkMatrix[currentPage].Length);
						currentPage = linkMatrix[currentPage][action];
					}
				}
				connection.Commit();
			}
			catch (SQLException ex)
			{
				connection.Rollback();
				throw;
			}
			finally
			{
				if (statement != null)
				{
					statement.Close();
				}
			}
		}

		/// <summary>Verifies the results are correct</summary>
		/// <exception cref="Java.Sql.SQLException"/>
		private bool Verify()
		{
			//check total num pageview
			string countAccessQuery = "SELECT COUNT(*) FROM Access";
			string sumPageviewQuery = "SELECT SUM(pageview) FROM Pageview";
			Statement st = null;
			ResultSet rs = null;
			try
			{
				st = connection.CreateStatement();
				rs = st.ExecuteQuery(countAccessQuery);
				rs.Next();
				long totalPageview = rs.GetLong(1);
				rs = st.ExecuteQuery(sumPageviewQuery);
				rs.Next();
				long sumPageview = rs.GetLong(1);
				Log.Info("totalPageview=" + totalPageview);
				Log.Info("sumPageview=" + sumPageview);
				return totalPageview == sumPageview && totalPageview != 0;
			}
			finally
			{
				if (st != null)
				{
					st.Close();
				}
				if (rs != null)
				{
					rs.Close();
				}
			}
		}

		/// <summary>Holds a &lt;url, referrer, time &gt; tuple</summary>
		internal class AccessRecord : Writable, DBWritable
		{
			internal string url;

			internal string referrer;

			internal long time;

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				this.url = Text.ReadString(@in);
				this.referrer = Text.ReadString(@in);
				this.time = @in.ReadLong();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				Text.WriteString(@out, url);
				Text.WriteString(@out, referrer);
				@out.WriteLong(time);
			}

			/// <exception cref="Java.Sql.SQLException"/>
			public virtual void ReadFields(ResultSet resultSet)
			{
				this.url = resultSet.GetString(1);
				this.referrer = resultSet.GetString(2);
				this.time = resultSet.GetLong(3);
			}

			/// <exception cref="Java.Sql.SQLException"/>
			public virtual void Write(PreparedStatement statement)
			{
				statement.SetString(1, url);
				statement.SetString(2, referrer);
				statement.SetLong(3, time);
			}
		}

		/// <summary>Holds a &lt;url, pageview &gt; tuple</summary>
		internal class PageviewRecord : Writable, DBWritable
		{
			internal string url;

			internal long pageview;

			public PageviewRecord(string url, long pageview)
			{
				this.url = url;
				this.pageview = pageview;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				this.url = Text.ReadString(@in);
				this.pageview = @in.ReadLong();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				Text.WriteString(@out, url);
				@out.WriteLong(pageview);
			}

			/// <exception cref="Java.Sql.SQLException"/>
			public virtual void ReadFields(ResultSet resultSet)
			{
				this.url = resultSet.GetString(1);
				this.pageview = resultSet.GetLong(2);
			}

			/// <exception cref="Java.Sql.SQLException"/>
			public virtual void Write(PreparedStatement statement)
			{
				statement.SetString(1, url);
				statement.SetLong(2, pageview);
			}

			public override string ToString()
			{
				return url + " " + pageview;
			}
		}

		/// <summary>
		/// Mapper extracts URLs from the AccessRecord (tuples from db),
		/// and emits a &lt;url,1&gt; pair for each access record.
		/// </summary>
		internal class PageviewMapper : Mapper<LongWritable, DBCountPageView.AccessRecord
			, Text, LongWritable>
		{
			internal LongWritable One = new LongWritable(1L);

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, DBCountPageView.AccessRecord value, 
				Mapper.Context context)
			{
				Text oKey = new Text(value.url);
				context.Write(oKey, One);
			}
		}

		/// <summary>
		/// Reducer sums up the pageviews and emits a PageviewRecord,
		/// which will correspond to one tuple in the db.
		/// </summary>
		internal class PageviewReducer : Reducer<Text, LongWritable, DBCountPageView.PageviewRecord
			, NullWritable>
		{
			internal NullWritable n = NullWritable.Get();

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(Text key, IEnumerable<LongWritable> values, Reducer.Context
				 context)
			{
				long sum = 0L;
				foreach (LongWritable value in values)
				{
					sum += value.Get();
				}
				context.Write(new DBCountPageView.PageviewRecord(key.ToString(), sum), n);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual int Run(string[] args)
		{
			//Usage DBCountPageView [driverClass dburl]
			string driverClassName = DriverClass;
			string url = DbUrl;
			if (args.Length > 1)
			{
				driverClassName = args[0];
				url = args[1];
			}
			Initialize(driverClassName, url);
			Configuration conf = GetConf();
			DBConfiguration.ConfigureDB(conf, driverClassName, url);
			Job job = new Job(conf);
			job.SetJobName("Count Pageviews of URLs");
			job.SetJarByClass(typeof(DBCountPageView));
			job.SetMapperClass(typeof(DBCountPageView.PageviewMapper));
			job.SetCombinerClass(typeof(LongSumReducer));
			job.SetReducerClass(typeof(DBCountPageView.PageviewReducer));
			DBInputFormat.SetInput(job, typeof(DBCountPageView.AccessRecord), "Access", null, 
				"url", AccessFieldNames);
			DBOutputFormat.SetOutput(job, "Pageview", PageviewFieldNames);
			job.SetMapOutputKeyClass(typeof(Text));
			job.SetMapOutputValueClass(typeof(LongWritable));
			job.SetOutputKeyClass(typeof(DBCountPageView.PageviewRecord));
			job.SetOutputValueClass(typeof(NullWritable));
			int ret;
			try
			{
				ret = job.WaitForCompletion(true) ? 0 : 1;
				bool correct = Verify();
				if (!correct)
				{
					throw new RuntimeException("Evaluation was not correct!");
				}
			}
			finally
			{
				Shutdown();
			}
			return ret;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			int ret = ToolRunner.Run(new DBCountPageView(), args);
			System.Environment.Exit(ret);
		}
	}
}
