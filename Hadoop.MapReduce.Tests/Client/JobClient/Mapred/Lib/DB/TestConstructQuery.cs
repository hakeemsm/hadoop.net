using NUnit.Framework;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.DB
{
	public class TestConstructQuery : TestCase
	{
		private string[] fieldNames = new string[] { "id", "name", "value" };

		private string[] nullFieldNames = new string[] { null, null, null };

		private string expected = "INSERT INTO hadoop_output (id,name,value) VALUES (?,?,?);";

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
			JobConf job = new JobConf();
			DBOutputFormat.SetOutput(job, "hadoop_output", fieldNames);
			DBConfiguration dbConf = new DBConfiguration(job);
			string actual = format.ConstructQuery(dbConf.GetOutputTableName(), dbConf.GetOutputFieldNames
				());
			NUnit.Framework.Assert.AreEqual(expected, actual);
			job = new JobConf();
			dbConf = new DBConfiguration(job);
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
