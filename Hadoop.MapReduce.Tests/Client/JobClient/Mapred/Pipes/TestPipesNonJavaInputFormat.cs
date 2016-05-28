using NUnit.Framework;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	public class TestPipesNonJavaInputFormat
	{
		private static FilePath workSpace = new FilePath("target", typeof(TestPipesNonJavaInputFormat
			).FullName + "-workSpace");

		/// <summary>test PipesNonJavaInputFormat</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFormat()
		{
			PipesNonJavaInputFormat inputFormat = new PipesNonJavaInputFormat();
			JobConf conf = new JobConf();
			Reporter reporter = Org.Mockito.Mockito.Mock<Reporter>();
			RecordReader<FloatWritable, NullWritable> reader = inputFormat.GetRecordReader(new 
				TestPipeApplication.FakeSplit(), conf, reporter);
			NUnit.Framework.Assert.AreEqual(0.0f, reader.GetProgress(), 0.001);
			// input and output files
			FilePath input1 = new FilePath(workSpace + FilePath.separator + "input1");
			if (!input1.GetParentFile().Exists())
			{
				NUnit.Framework.Assert.IsTrue(input1.GetParentFile().Mkdirs());
			}
			if (!input1.Exists())
			{
				NUnit.Framework.Assert.IsTrue(input1.CreateNewFile());
			}
			FilePath input2 = new FilePath(workSpace + FilePath.separator + "input2");
			if (!input2.Exists())
			{
				NUnit.Framework.Assert.IsTrue(input2.CreateNewFile());
			}
			// set data for splits
			conf.Set(FileInputFormat.InputDir, StringUtils.EscapeString(input1.GetAbsolutePath
				()) + "," + StringUtils.EscapeString(input2.GetAbsolutePath()));
			InputSplit[] splits = inputFormat.GetSplits(conf, 2);
			NUnit.Framework.Assert.AreEqual(2, splits.Length);
			PipesNonJavaInputFormat.PipesDummyRecordReader dummyRecordReader = new PipesNonJavaInputFormat.PipesDummyRecordReader
				(conf, splits[0]);
			// empty dummyRecordReader
			NUnit.Framework.Assert.IsNull(dummyRecordReader.CreateKey());
			NUnit.Framework.Assert.IsNull(dummyRecordReader.CreateValue());
			NUnit.Framework.Assert.AreEqual(0, dummyRecordReader.GetPos());
			NUnit.Framework.Assert.AreEqual(0.0, dummyRecordReader.GetProgress(), 0.001);
			// test method next
			NUnit.Framework.Assert.IsTrue(dummyRecordReader.Next(new FloatWritable(2.0f), NullWritable
				.Get()));
			NUnit.Framework.Assert.AreEqual(2.0, dummyRecordReader.GetProgress(), 0.001);
			dummyRecordReader.Close();
		}
	}
}
