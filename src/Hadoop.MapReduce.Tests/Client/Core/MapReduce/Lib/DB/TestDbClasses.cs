using System.Collections.Generic;
using Java.Sql;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	public class TestDbClasses
	{
		/// <summary>test splitters from DataDrivenDBInputFormat.</summary>
		/// <remarks>
		/// test splitters from DataDrivenDBInputFormat. For different data types may
		/// be different splitter
		/// </remarks>
		public virtual void TestDataDrivenDBInputFormatSplitter()
		{
			DataDrivenDBInputFormat<DBInputFormat.NullDBWritable> format = new DataDrivenDBInputFormat
				<DBInputFormat.NullDBWritable>();
			TestCommonSplitterTypes(format);
			NUnit.Framework.Assert.AreEqual(typeof(DateSplitter), format.GetSplitter(Types.Timestamp
				).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(DateSplitter), format.GetSplitter(Types.Date
				).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(DateSplitter), format.GetSplitter(Types.Time
				).GetType());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDataDrivenDBInputFormat()
		{
			JobContext jobContext = Org.Mockito.Mockito.Mock<JobContext>();
			Configuration configuration = new Configuration();
			configuration.SetInt(MRJobConfig.NumMaps, 1);
			Org.Mockito.Mockito.When(jobContext.GetConfiguration()).ThenReturn(configuration);
			DataDrivenDBInputFormat<DBInputFormat.NullDBWritable> format = new DataDrivenDBInputFormat
				<DBInputFormat.NullDBWritable>();
			IList<InputSplit> splits = format.GetSplits(jobContext);
			NUnit.Framework.Assert.AreEqual(1, splits.Count);
			DataDrivenDBInputFormat.DataDrivenDBInputSplit split = (DataDrivenDBInputFormat.DataDrivenDBInputSplit
				)splits[0];
			NUnit.Framework.Assert.AreEqual("1=1", split.GetLowerClause());
			NUnit.Framework.Assert.AreEqual("1=1", split.GetUpperClause());
			// 2
			configuration.SetInt(MRJobConfig.NumMaps, 2);
			DataDrivenDBInputFormat.SetBoundingQuery(configuration, "query");
			NUnit.Framework.Assert.AreEqual("query", configuration.Get(DBConfiguration.InputBoundingQuery
				));
			Job job = Org.Mockito.Mockito.Mock<Job>();
			Org.Mockito.Mockito.When(job.GetConfiguration()).ThenReturn(configuration);
			DataDrivenDBInputFormat.SetInput(job, typeof(DBInputFormat.NullDBWritable), "query"
				, "Bounding Query");
			NUnit.Framework.Assert.AreEqual("Bounding Query", configuration.Get(DBConfiguration
				.InputBoundingQuery));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestOracleDataDrivenDBInputFormat()
		{
			OracleDataDrivenDBInputFormat<DBInputFormat.NullDBWritable> format = new TestDbClasses.OracleDataDrivenDBInputFormatForTest
				(this);
			TestCommonSplitterTypes(format);
			NUnit.Framework.Assert.AreEqual(typeof(OracleDateSplitter), format.GetSplitter(Types
				.Timestamp).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(OracleDateSplitter), format.GetSplitter(Types
				.Date).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(OracleDateSplitter), format.GetSplitter(Types
				.Time).GetType());
		}

		/// <summary>test generate sql script for OracleDBRecordReader.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestOracleDBRecordReader()
		{
			DBInputFormat.DBInputSplit splitter = new DBInputFormat.DBInputSplit(1, 10);
			Configuration configuration = new Configuration();
			Connection connect = DriverForTest.GetConnection();
			DBConfiguration dbConfiguration = new DBConfiguration(configuration);
			dbConfiguration.SetInputOrderBy("Order");
			string[] fields = new string[] { "f1", "f2" };
			OracleDBRecordReader<DBInputFormat.NullDBWritable> recorder = new OracleDBRecordReader
				<DBInputFormat.NullDBWritable>(splitter, typeof(DBInputFormat.NullDBWritable), configuration
				, connect, dbConfiguration, "condition", fields, "table");
			NUnit.Framework.Assert.AreEqual("SELECT * FROM (SELECT a.*,ROWNUM dbif_rno FROM ( SELECT f1, f2 FROM table WHERE condition ORDER BY Order ) a WHERE rownum <= 10 ) WHERE dbif_rno > 1"
				, recorder.GetSelectQuery());
		}

		private void TestCommonSplitterTypes(DataDrivenDBInputFormat<DBInputFormat.NullDBWritable
			> format)
		{
			NUnit.Framework.Assert.AreEqual(typeof(BigDecimalSplitter), format.GetSplitter(Types
				.Decimal).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(BigDecimalSplitter), format.GetSplitter(Types
				.Numeric).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(BooleanSplitter), format.GetSplitter(Types
				.Boolean).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(BooleanSplitter), format.GetSplitter(Types
				.Bit).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(IntegerSplitter), format.GetSplitter(Types
				.Bigint).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(IntegerSplitter), format.GetSplitter(Types
				.Tinyint).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(IntegerSplitter), format.GetSplitter(Types
				.Smallint).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(IntegerSplitter), format.GetSplitter(Types
				.Integer).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(FloatSplitter), format.GetSplitter(Types.Double
				).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(FloatSplitter), format.GetSplitter(Types.Real
				).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(FloatSplitter), format.GetSplitter(Types.Float
				).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(TextSplitter), format.GetSplitter(Types.Longvarchar
				).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(TextSplitter), format.GetSplitter(Types.Char
				).GetType());
			NUnit.Framework.Assert.AreEqual(typeof(TextSplitter), format.GetSplitter(Types.Varchar
				).GetType());
			// if unknown data type splitter is null
			NUnit.Framework.Assert.IsNull(format.GetSplitter(Types.Binary));
		}

		private class OracleDataDrivenDBInputFormatForTest : OracleDataDrivenDBInputFormat
			<DBInputFormat.NullDBWritable>
		{
			public override DBConfiguration GetDBConf()
			{
				string[] names = new string[] { "field1", "field2" };
				DBConfiguration result = Org.Mockito.Mockito.Mock<DBConfiguration>();
				Org.Mockito.Mockito.When(result.GetInputConditions()).ThenReturn("conditions");
				Org.Mockito.Mockito.When(result.GetInputFieldNames()).ThenReturn(names);
				Org.Mockito.Mockito.When(result.GetInputTableName()).ThenReturn("table");
				return result;
			}

			public override Connection GetConnection()
			{
				return DriverForTest.GetConnection();
			}

			internal OracleDataDrivenDBInputFormatForTest(TestDbClasses _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestDbClasses _enclosing;
		}
	}
}
