using System;
using System.IO;
using Java.Sql;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>Test aspects of DataDrivenDBInputFormat</summary>
	public class TestDataDrivenDBInputFormat : HadoopTestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Lib.DB.TestDataDrivenDBInputFormat
			));

		private const string DbName = "dddbif";

		private const string DbUrl = "jdbc:hsqldb:hsql://localhost/" + DbName;

		private const string DriverClass = "org.hsqldb.jdbc.JDBCDriver";

		private Org.Hsqldb.Server.Server server;

		private Connection connection;

		private static readonly string OutDir;

		/// <exception cref="System.IO.IOException"/>
		public TestDataDrivenDBInputFormat()
			: base(LocalMr, LocalFs, 1, 1)
		{
		}

		static TestDataDrivenDBInputFormat()
		{
			//import org.apache.hadoop.examples.DBCountPageView;
			OutDir = Runtime.GetProperty("test.build.data", "/tmp") + "/dddbifout";
		}

		private void StartHsqldbServer()
		{
			if (null == server)
			{
				server = new Org.Hsqldb.Server.Server();
				server.SetDatabasePath(0, Runtime.GetProperty("test.build.data", "/tmp") + "/" + 
					DbName);
				server.SetDatabaseName(0, DbName);
				server.Start();
			}
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
				connection = null;
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
				server = null;
			}
		}

		/// <exception cref="System.Exception"/>
		private void Initialize(string driverClassName, string url)
		{
			StartHsqldbServer();
			CreateConnection(driverClassName, url);
		}

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			Initialize(DriverClass, DbUrl);
			base.SetUp();
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			base.TearDown();
			Shutdown();
		}

		public class DateCol : DBWritable, WritableComparable
		{
			internal Date d;

			public override string ToString()
			{
				return d.ToString();
			}

			/// <exception cref="Java.Sql.SQLException"/>
			public virtual void ReadFields(ResultSet rs)
			{
				d = rs.GetDate(1);
			}

			public virtual void Write(PreparedStatement ps)
			{
			}

			// not needed.
			/// <exception cref="System.IO.IOException"/>
			public virtual void ReadFields(DataInput @in)
			{
				long v = @in.ReadLong();
				d = Sharpen.Extensions.CreateDate(v);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(DataOutput @out)
			{
				@out.WriteLong(d.GetTime());
			}

			public override int GetHashCode()
			{
				return (int)d.GetTime();
			}

			public virtual int CompareTo(object o)
			{
				if (o is TestDataDrivenDBInputFormat.DateCol)
				{
					long v = Sharpen.Extensions.ValueOf(d.GetTime());
					long other = Sharpen.Extensions.ValueOf(((TestDataDrivenDBInputFormat.DateCol)o).
						d.GetTime());
					return v.CompareTo(other);
				}
				else
				{
					return -1;
				}
			}
		}

		public class ValMapper : Mapper<object, object, object, NullWritable>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(object k, object v, Mapper.Context c)
			{
				c.Write(v, NullWritable.Get());
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDateSplits()
		{
			Statement s = connection.CreateStatement();
			string DateTable = "datetable";
			string Col = "foo";
			try
			{
				// delete the table if it already exists.
				s.ExecuteUpdate("DROP TABLE " + DateTable);
			}
			catch (SQLException)
			{
			}
			// Create the table.
			s.ExecuteUpdate("CREATE TABLE " + DateTable + "(" + Col + " DATE)");
			s.ExecuteUpdate("INSERT INTO " + DateTable + " VALUES('2010-04-01')");
			s.ExecuteUpdate("INSERT INTO " + DateTable + " VALUES('2010-04-02')");
			s.ExecuteUpdate("INSERT INTO " + DateTable + " VALUES('2010-05-01')");
			s.ExecuteUpdate("INSERT INTO " + DateTable + " VALUES('2011-04-01')");
			// commit this tx.
			connection.Commit();
			Configuration conf = new Configuration();
			conf.Set("fs.defaultFS", "file:///");
			FileSystem fs = FileSystem.GetLocal(conf);
			fs.Delete(new Path(OutDir), true);
			// now do a dd import
			Job job = Job.GetInstance(conf);
			job.SetMapperClass(typeof(TestDataDrivenDBInputFormat.ValMapper));
			job.SetReducerClass(typeof(Reducer));
			job.SetMapOutputKeyClass(typeof(TestDataDrivenDBInputFormat.DateCol));
			job.SetMapOutputValueClass(typeof(NullWritable));
			job.SetOutputKeyClass(typeof(TestDataDrivenDBInputFormat.DateCol));
			job.SetOutputValueClass(typeof(NullWritable));
			job.SetNumReduceTasks(1);
			job.GetConfiguration().SetInt("mapreduce.map.tasks", 2);
			FileOutputFormat.SetOutputPath(job, new Path(OutDir));
			DBConfiguration.ConfigureDB(job.GetConfiguration(), DriverClass, DbUrl, null, null
				);
			DataDrivenDBInputFormat.SetInput(job, typeof(TestDataDrivenDBInputFormat.DateCol)
				, DateTable, null, Col, Col);
			bool ret = job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue("job failed", ret);
			// Check to see that we imported as much as we thought we did.
			NUnit.Framework.Assert.AreEqual("Did not get all the records", 4, job.GetCounters
				().FindCounter(TaskCounter.ReduceOutputRecords).GetValue());
		}
	}
}
