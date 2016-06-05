using System.Collections.Generic;
using Java.Math;
using Java.Sql;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.DB
{
	/// <summary>Test Splitters.</summary>
	/// <remarks>Test Splitters. Splitters should build parts of sql sentences for split result.
	/// 	</remarks>
	public class TestSplitters
	{
		private Configuration configuration;

		[SetUp]
		public virtual void Setup()
		{
			configuration = new Configuration();
			configuration.SetInt(MRJobConfig.NumMaps, 2);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBooleanSplitter()
		{
			BooleanSplitter splitter = new BooleanSplitter();
			ResultSet result = Org.Mockito.Mockito.Mock<ResultSet>();
			Org.Mockito.Mockito.When(result.GetString(1)).ThenReturn("result1");
			IList<InputSplit> splits = splitter.Split(configuration, result, "column");
			AssertSplits(new string[] { "column = FALSE column = FALSE", "column IS NULL column IS NULL"
				 }, splits);
			Org.Mockito.Mockito.When(result.GetString(1)).ThenReturn("result1");
			Org.Mockito.Mockito.When(result.GetString(2)).ThenReturn("result2");
			Org.Mockito.Mockito.When(result.GetBoolean(1)).ThenReturn(true);
			Org.Mockito.Mockito.When(result.GetBoolean(2)).ThenReturn(false);
			splits = splitter.Split(configuration, result, "column");
			NUnit.Framework.Assert.AreEqual(0, splits.Count);
			Org.Mockito.Mockito.When(result.GetString(1)).ThenReturn("result1");
			Org.Mockito.Mockito.When(result.GetString(2)).ThenReturn("result2");
			Org.Mockito.Mockito.When(result.GetBoolean(1)).ThenReturn(false);
			Org.Mockito.Mockito.When(result.GetBoolean(2)).ThenReturn(true);
			splits = splitter.Split(configuration, result, "column");
			AssertSplits(new string[] { "column = FALSE column = FALSE", ".*column = TRUE" }, 
				splits);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFloatSplitter()
		{
			FloatSplitter splitter = new FloatSplitter();
			ResultSet results = Org.Mockito.Mockito.Mock<ResultSet>();
			IList<InputSplit> splits = splitter.Split(configuration, results, "column");
			AssertSplits(new string[] { ".*column IS NULL" }, splits);
			Org.Mockito.Mockito.When(results.GetString(1)).ThenReturn("result1");
			Org.Mockito.Mockito.When(results.GetString(2)).ThenReturn("result2");
			Org.Mockito.Mockito.When(results.GetDouble(1)).ThenReturn(5.0);
			Org.Mockito.Mockito.When(results.GetDouble(2)).ThenReturn(7.0);
			splits = splitter.Split(configuration, results, "column1");
			AssertSplits(new string[] { "column1 >= 5.0 column1 < 6.0", "column1 >= 6.0 column1 <= 7.0"
				 }, splits);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestBigDecimalSplitter()
		{
			BigDecimalSplitter splitter = new BigDecimalSplitter();
			ResultSet result = Org.Mockito.Mockito.Mock<ResultSet>();
			IList<InputSplit> splits = splitter.Split(configuration, result, "column");
			AssertSplits(new string[] { ".*column IS NULL" }, splits);
			Org.Mockito.Mockito.When(result.GetString(1)).ThenReturn("result1");
			Org.Mockito.Mockito.When(result.GetString(2)).ThenReturn("result2");
			Org.Mockito.Mockito.When(result.GetBigDecimal(1)).ThenReturn(new BigDecimal(10));
			Org.Mockito.Mockito.When(result.GetBigDecimal(2)).ThenReturn(new BigDecimal(12));
			splits = splitter.Split(configuration, result, "column1");
			AssertSplits(new string[] { "column1 >= 10 column1 < 11", "column1 >= 11 column1 <= 12"
				 }, splits);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIntegerSplitter()
		{
			IntegerSplitter splitter = new IntegerSplitter();
			ResultSet result = Org.Mockito.Mockito.Mock<ResultSet>();
			IList<InputSplit> splits = splitter.Split(configuration, result, "column");
			AssertSplits(new string[] { ".*column IS NULL" }, splits);
			Org.Mockito.Mockito.When(result.GetString(1)).ThenReturn("result1");
			Org.Mockito.Mockito.When(result.GetString(2)).ThenReturn("result2");
			Org.Mockito.Mockito.When(result.GetLong(1)).ThenReturn(8L);
			Org.Mockito.Mockito.When(result.GetLong(2)).ThenReturn(19L);
			splits = splitter.Split(configuration, result, "column1");
			AssertSplits(new string[] { "column1 >= 8 column1 < 13", "column1 >= 13 column1 < 18"
				, "column1 >= 18 column1 <= 19" }, splits);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTextSplitter()
		{
			TextSplitter splitter = new TextSplitter();
			ResultSet result = Org.Mockito.Mockito.Mock<ResultSet>();
			IList<InputSplit> splits = splitter.Split(configuration, result, "column");
			AssertSplits(new string[] { "column IS NULL column IS NULL" }, splits);
			Org.Mockito.Mockito.When(result.GetString(1)).ThenReturn("result1");
			Org.Mockito.Mockito.When(result.GetString(2)).ThenReturn("result2");
			splits = splitter.Split(configuration, result, "column1");
			AssertSplits(new string[] { "column1 >= 'result1' column1 < 'result1.'", "column1 >= 'result1' column1 <= 'result2'"
				 }, splits);
		}

		/// <exception cref="System.IO.IOException"/>
		private void AssertSplits(string[] expectedSplitRE, IList<InputSplit> splits)
		{
			NUnit.Framework.Assert.AreEqual(expectedSplitRE.Length, splits.Count);
			for (int i = 0; i < expectedSplitRE.Length; i++)
			{
				DataDrivenDBInputFormat.DataDrivenDBInputSplit split = (DataDrivenDBInputFormat.DataDrivenDBInputSplit
					)splits[i];
				string actualExpr = split.GetLowerClause() + " " + split.GetUpperClause();
				NUnit.Framework.Assert.IsTrue("Split #" + (i + 1) + " expression is wrong." + " Expected "
					 + expectedSplitRE[i] + " Actual " + actualExpr, Sharpen.Pattern.Matches(expectedSplitRE
					[i], actualExpr));
			}
		}
	}
}
