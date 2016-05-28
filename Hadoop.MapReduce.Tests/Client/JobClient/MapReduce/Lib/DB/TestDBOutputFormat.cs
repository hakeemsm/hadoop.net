using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	public class TestDBOutputFormat : TestCase
	{
		private string[] fieldNames = new string[] { "id", "name", "value" };

		private string[] nullFieldNames = new string[] { null, null, null };

		private string expected = "INSERT INTO hadoop_output " + "(id,name,value) VALUES (?,?,?);";

		private string nullExpected = "INSERT INTO hadoop_output VALUES (?,?,?);";

		private DBOutputFormat<DBWritable, NullWritable> format = new DBOutputFormat<DBWritable
			, NullWritable>();

		public virtual void TestConstructQuery()
		{
			string actual = format.ConstructQuery("hadoop_output", fieldNames);
			NUnit.Framework.Assert.AreEqual(expected, actual);
			actual = format.ConstructQuery("hadoop_output", nullFieldNames);
			NUnit.Framework.Assert.AreEqual(nullExpected, actual);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSetOutput()
		{
			Job job = Job.GetInstance(new Configuration());
			DBOutputFormat.SetOutput(job, "hadoop_output", fieldNames);
			DBConfiguration dbConf = new DBConfiguration(job.GetConfiguration());
			string actual = format.ConstructQuery(dbConf.GetOutputTableName(), dbConf.GetOutputFieldNames
				());
			NUnit.Framework.Assert.AreEqual(expected, actual);
			job = Job.GetInstance(new Configuration());
			dbConf = new DBConfiguration(job.GetConfiguration());
			DBOutputFormat.SetOutput(job, "hadoop_output", nullFieldNames.Length);
			NUnit.Framework.Assert.IsNull(dbConf.GetOutputFieldNames());
			NUnit.Framework.Assert.AreEqual(nullFieldNames.Length, dbConf.GetOutputFieldCount
				());
			actual = format.ConstructQuery(dbConf.GetOutputTableName(), new string[dbConf.GetOutputFieldCount
				()]);
			NUnit.Framework.Assert.AreEqual(nullExpected, actual);
		}
	}
}
