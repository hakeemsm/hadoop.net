using Java.Sql;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.DB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib.DB
{
	public class TestDBInputFormat
	{
		/// <summary>test DBInputFormat class.</summary>
		/// <remarks>test DBInputFormat class. Class should split result for chunks</remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDBInputFormat()
		{
			JobConf configuration = new JobConf();
			SetupDriver(configuration);
			DBInputFormat<DBInputFormat.NullDBWritable> format = new DBInputFormat<DBInputFormat.NullDBWritable
				>();
			format.SetConf(configuration);
			format.SetConf(configuration);
			DBInputFormat.DBInputSplit splitter = new DBInputFormat.DBInputSplit(1, 10);
			Reporter reporter = Org.Mockito.Mockito.Mock<Reporter>();
			RecordReader<LongWritable, DBInputFormat.NullDBWritable> reader = format.GetRecordReader
				(splitter, configuration, reporter);
			configuration.SetInt(MRJobConfig.NumMaps, 3);
			InputSplit[] lSplits = format.GetSplits(configuration, 3);
			NUnit.Framework.Assert.AreEqual(5, lSplits[0].GetLength());
			NUnit.Framework.Assert.AreEqual(3, lSplits.Length);
			// test reader .Some simple tests
			NUnit.Framework.Assert.AreEqual(typeof(LongWritable), reader.CreateKey().GetType(
				));
			NUnit.Framework.Assert.AreEqual(0, reader.GetPos());
			NUnit.Framework.Assert.AreEqual(0, reader.GetProgress(), 0.001);
			reader.Close();
		}

		/// <summary>test configuration for db.</summary>
		/// <remarks>test configuration for db. should works DBConfiguration.* parameters.</remarks>
		public virtual void TestSetInput()
		{
			JobConf configuration = new JobConf();
			string[] fieldNames = new string[] { "field1", "field2" };
			DBInputFormat.SetInput(configuration, typeof(DBInputFormat.NullDBWritable), "table"
				, "conditions", "orderBy", fieldNames);
			NUnit.Framework.Assert.AreEqual("org.apache.hadoop.mapred.lib.db.DBInputFormat$NullDBWritable"
				, configuration.GetClass(DBConfiguration.InputClassProperty, null).FullName);
			NUnit.Framework.Assert.AreEqual("table", configuration.Get(DBConfiguration.InputTableNameProperty
				, null));
			string[] fields = configuration.GetStrings(DBConfiguration.InputFieldNamesProperty
				);
			NUnit.Framework.Assert.AreEqual("field1", fields[0]);
			NUnit.Framework.Assert.AreEqual("field2", fields[1]);
			NUnit.Framework.Assert.AreEqual("conditions", configuration.Get(DBConfiguration.InputConditionsProperty
				, null));
			NUnit.Framework.Assert.AreEqual("orderBy", configuration.Get(DBConfiguration.InputOrderByProperty
				, null));
			configuration = new JobConf();
			DBInputFormat.SetInput(configuration, typeof(DBInputFormat.NullDBWritable), "query"
				, "countQuery");
			NUnit.Framework.Assert.AreEqual("query", configuration.Get(DBConfiguration.InputQuery
				, null));
			NUnit.Framework.Assert.AreEqual("countQuery", configuration.Get(DBConfiguration.InputCountQuery
				, null));
			JobConf jConfiguration = new JobConf();
			DBConfiguration.ConfigureDB(jConfiguration, "driverClass", "dbUrl", "user", "password"
				);
			NUnit.Framework.Assert.AreEqual("driverClass", jConfiguration.Get(DBConfiguration
				.DriverClassProperty));
			NUnit.Framework.Assert.AreEqual("dbUrl", jConfiguration.Get(DBConfiguration.UrlProperty
				));
			NUnit.Framework.Assert.AreEqual("user", jConfiguration.Get(DBConfiguration.UsernameProperty
				));
			NUnit.Framework.Assert.AreEqual("password", jConfiguration.Get(DBConfiguration.PasswordProperty
				));
			jConfiguration = new JobConf();
			DBConfiguration.ConfigureDB(jConfiguration, "driverClass", "dbUrl");
			NUnit.Framework.Assert.AreEqual("driverClass", jConfiguration.Get(DBConfiguration
				.DriverClassProperty));
			NUnit.Framework.Assert.AreEqual("dbUrl", jConfiguration.Get(DBConfiguration.UrlProperty
				));
			NUnit.Framework.Assert.IsNull(jConfiguration.Get(DBConfiguration.UsernameProperty
				));
			NUnit.Framework.Assert.IsNull(jConfiguration.Get(DBConfiguration.PasswordProperty
				));
		}

		/// <summary>test DBRecordReader.</summary>
		/// <remarks>test DBRecordReader. This reader should creates keys, values, know about position..
		/// 	</remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDBRecordReader()
		{
			JobConf job = Org.Mockito.Mockito.Mock<JobConf>();
			DBConfiguration dbConfig = Org.Mockito.Mockito.Mock<DBConfiguration>();
			string[] fields = new string[] { "field1", "filed2" };
			DBInputFormat.DBRecordReader reader = new DBInputFormat.DBRecordReader(this, new 
				DBInputFormat.DBInputSplit(), typeof(DBInputFormat.NullDBWritable), job, DriverForTest
				.GetConnection(), dbConfig, "condition", fields, "table");
			LongWritable key = reader.CreateKey();
			NUnit.Framework.Assert.AreEqual(0, key.Get());
			DBWritable value = ((DBWritable)reader.CreateValue());
			NUnit.Framework.Assert.AreEqual("org.apache.hadoop.mapred.lib.db.DBInputFormat$NullDBWritable"
				, value.GetType().FullName);
			NUnit.Framework.Assert.AreEqual(0, reader.GetPos());
			NUnit.Framework.Assert.IsFalse(reader.Next(key, value));
		}

		/// <exception cref="System.Exception"/>
		private void SetupDriver(JobConf configuration)
		{
			configuration.Set(DBConfiguration.UrlProperty, "testUrl");
			DriverManager.RegisterDriver(new DriverForTest());
			configuration.Set(DBConfiguration.DriverClassProperty, typeof(DriverForTest).GetCanonicalName
				());
		}
	}
}
