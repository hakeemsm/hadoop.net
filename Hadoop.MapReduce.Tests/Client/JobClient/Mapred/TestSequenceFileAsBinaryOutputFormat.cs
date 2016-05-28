using System;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestSequenceFileAsBinaryOutputFormat : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestSequenceFileAsBinaryOutputFormat
			).FullName);

		private const int Records = 10000;

		private const string attempt = "attempt_200707121733_0001_m_000000_0";

		// A random task attempt id for testing.
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestBinary()
		{
			JobConf job = new JobConf();
			FileSystem fs = FileSystem.GetLocal(job);
			Path dir = new Path(new Path(new Path(Runtime.GetProperty("test.build.data", ".")
				), FileOutputCommitter.TempDirName), "_" + attempt);
			Path file = new Path(dir, "testbinary.seq");
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			fs.Delete(dir, true);
			if (!fs.Mkdirs(dir))
			{
				Fail("Failed to create output directory");
			}
			job.Set(JobContext.TaskAttemptId, attempt);
			FileOutputFormat.SetOutputPath(job, dir.GetParent().GetParent());
			FileOutputFormat.SetWorkOutputPath(job, dir);
			SequenceFileAsBinaryOutputFormat.SetSequenceFileOutputKeyClass(job, typeof(IntWritable
				));
			SequenceFileAsBinaryOutputFormat.SetSequenceFileOutputValueClass(job, typeof(DoubleWritable
				));
			SequenceFileAsBinaryOutputFormat.SetCompressOutput(job, true);
			SequenceFileAsBinaryOutputFormat.SetOutputCompressionType(job, SequenceFile.CompressionType
				.Block);
			BytesWritable bkey = new BytesWritable();
			BytesWritable bval = new BytesWritable();
			RecordWriter<BytesWritable, BytesWritable> writer = new SequenceFileAsBinaryOutputFormat
				().GetRecordWriter(fs, job, file.ToString(), Reporter.Null);
			IntWritable iwritable = new IntWritable();
			DoubleWritable dwritable = new DoubleWritable();
			DataOutputBuffer outbuf = new DataOutputBuffer();
			Log.Info("Creating data by SequenceFileAsBinaryOutputFormat");
			try
			{
				for (int i = 0; i < Records; ++i)
				{
					iwritable = new IntWritable(r.Next());
					iwritable.Write(outbuf);
					bkey.Set(outbuf.GetData(), 0, outbuf.GetLength());
					outbuf.Reset();
					dwritable = new DoubleWritable(r.NextDouble());
					dwritable.Write(outbuf);
					bval.Set(outbuf.GetData(), 0, outbuf.GetLength());
					outbuf.Reset();
					writer.Write(bkey, bval);
				}
			}
			finally
			{
				writer.Close(Reporter.Null);
			}
			InputFormat<IntWritable, DoubleWritable> iformat = new SequenceFileInputFormat<IntWritable
				, DoubleWritable>();
			int count = 0;
			r.SetSeed(seed);
			DataInputBuffer buf = new DataInputBuffer();
			int NumSplits = 3;
			SequenceFileInputFormat.AddInputPath(job, file);
			Log.Info("Reading data by SequenceFileInputFormat");
			foreach (InputSplit split in iformat.GetSplits(job, NumSplits))
			{
				RecordReader<IntWritable, DoubleWritable> reader = iformat.GetRecordReader(split, 
					job, Reporter.Null);
				try
				{
					int sourceInt;
					double sourceDouble;
					while (reader.Next(iwritable, dwritable))
					{
						sourceInt = r.Next();
						sourceDouble = r.NextDouble();
						NUnit.Framework.Assert.AreEqual("Keys don't match: " + "*" + iwritable.Get() + ":"
							 + sourceInt + "*", sourceInt, iwritable.Get());
						NUnit.Framework.Assert.IsTrue("Vals don't match: " + "*" + dwritable.Get() + ":" 
							+ sourceDouble + "*", double.Compare(dwritable.Get(), sourceDouble) == 0);
						++count;
					}
				}
				finally
				{
					reader.Close();
				}
			}
			NUnit.Framework.Assert.AreEqual("Some records not found", Records, count);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSequenceOutputClassDefaultsToMapRedOutputClass()
		{
			JobConf job = new JobConf();
			FileSystem fs = FileSystem.GetLocal(job);
			// Setting Random class to test getSequenceFileOutput{Key,Value}Class
			job.SetOutputKeyClass(typeof(FloatWritable));
			job.SetOutputValueClass(typeof(BooleanWritable));
			NUnit.Framework.Assert.AreEqual("SequenceFileOutputKeyClass should default to ouputKeyClass"
				, typeof(FloatWritable), SequenceFileAsBinaryOutputFormat.GetSequenceFileOutputKeyClass
				(job));
			NUnit.Framework.Assert.AreEqual("SequenceFileOutputValueClass should default to "
				 + "ouputValueClass", typeof(BooleanWritable), SequenceFileAsBinaryOutputFormat.
				GetSequenceFileOutputValueClass(job));
			SequenceFileAsBinaryOutputFormat.SetSequenceFileOutputKeyClass(job, typeof(IntWritable
				));
			SequenceFileAsBinaryOutputFormat.SetSequenceFileOutputValueClass(job, typeof(DoubleWritable
				));
			NUnit.Framework.Assert.AreEqual("SequenceFileOutputKeyClass not updated", typeof(
				IntWritable), SequenceFileAsBinaryOutputFormat.GetSequenceFileOutputKeyClass(job
				));
			NUnit.Framework.Assert.AreEqual("SequenceFileOutputValueClass not updated", typeof(
				DoubleWritable), SequenceFileAsBinaryOutputFormat.GetSequenceFileOutputValueClass
				(job));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestcheckOutputSpecsForbidRecordCompression()
		{
			JobConf job = new JobConf();
			FileSystem fs = FileSystem.GetLocal(job);
			Path dir = new Path(Runtime.GetProperty("test.build.data", ".") + "/mapred");
			Path outputdir = new Path(Runtime.GetProperty("test.build.data", ".") + "/output"
				);
			fs.Delete(dir, true);
			fs.Delete(outputdir, true);
			if (!fs.Mkdirs(dir))
			{
				Fail("Failed to create output directory");
			}
			FileOutputFormat.SetWorkOutputPath(job, dir);
			// Without outputpath, FileOutputFormat.checkoutputspecs will throw 
			// InvalidJobConfException
			FileOutputFormat.SetOutputPath(job, outputdir);
			// SequenceFileAsBinaryOutputFormat doesn't support record compression
			// It should throw an exception when checked by checkOutputSpecs
			SequenceFileAsBinaryOutputFormat.SetCompressOutput(job, true);
			SequenceFileAsBinaryOutputFormat.SetOutputCompressionType(job, SequenceFile.CompressionType
				.Block);
			try
			{
				new SequenceFileAsBinaryOutputFormat().CheckOutputSpecs(fs, job);
			}
			catch (Exception e)
			{
				Fail("Block compression should be allowed for " + "SequenceFileAsBinaryOutputFormat:"
					 + "Caught " + e.GetType().FullName);
			}
			SequenceFileAsBinaryOutputFormat.SetOutputCompressionType(job, SequenceFile.CompressionType
				.Record);
			try
			{
				new SequenceFileAsBinaryOutputFormat().CheckOutputSpecs(fs, job);
				Fail("Record compression should not be allowed for " + "SequenceFileAsBinaryOutputFormat"
					);
			}
			catch (InvalidJobConfException)
			{
			}
			catch (Exception e)
			{
				// expected
				Fail("Expected " + typeof(InvalidJobConfException).FullName + "but caught " + e.GetType
					().FullName);
			}
		}
	}
}
