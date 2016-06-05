using System;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Task;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	public class TestMRSequenceFileAsBinaryOutputFormat : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMRSequenceFileAsBinaryOutputFormat
			).FullName);

		private const int Records = 10000;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestBinary()
		{
			Configuration conf = new Configuration();
			Job job = Job.GetInstance(conf);
			Path outdir = new Path(Runtime.GetProperty("test.build.data", "/tmp"), "outseq");
			Random r = new Random();
			long seed = r.NextLong();
			r.SetSeed(seed);
			FileOutputFormat.SetOutputPath(job, outdir);
			SequenceFileAsBinaryOutputFormat.SetSequenceFileOutputKeyClass(job, typeof(IntWritable
				));
			SequenceFileAsBinaryOutputFormat.SetSequenceFileOutputValueClass(job, typeof(DoubleWritable
				));
			SequenceFileAsBinaryOutputFormat.SetCompressOutput(job, true);
			SequenceFileAsBinaryOutputFormat.SetOutputCompressionType(job, SequenceFile.CompressionType
				.Block);
			BytesWritable bkey = new BytesWritable();
			BytesWritable bval = new BytesWritable();
			TaskAttemptContext context = MapReduceTestUtil.CreateDummyMapTaskAttemptContext(job
				.GetConfiguration());
			OutputFormat<BytesWritable, BytesWritable> outputFormat = new SequenceFileAsBinaryOutputFormat
				();
			OutputCommitter committer = outputFormat.GetOutputCommitter(context);
			committer.SetupJob(job);
			RecordWriter<BytesWritable, BytesWritable> writer = outputFormat.GetRecordWriter(
				context);
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
				writer.Close(context);
			}
			committer.CommitTask(context);
			committer.CommitJob(job);
			InputFormat<IntWritable, DoubleWritable> iformat = new SequenceFileInputFormat<IntWritable
				, DoubleWritable>();
			int count = 0;
			r.SetSeed(seed);
			SequenceFileInputFormat.SetInputPaths(job, outdir);
			Log.Info("Reading data by SequenceFileInputFormat");
			foreach (InputSplit split in iformat.GetSplits(job))
			{
				RecordReader<IntWritable, DoubleWritable> reader = iformat.CreateRecordReader(split
					, context);
				MapContext<IntWritable, DoubleWritable, BytesWritable, BytesWritable> mcontext = 
					new MapContextImpl<IntWritable, DoubleWritable, BytesWritable, BytesWritable>(job
					.GetConfiguration(), context.GetTaskAttemptID(), reader, null, null, MapReduceTestUtil
					.CreateDummyReporter(), split);
				reader.Initialize(split, mcontext);
				try
				{
					int sourceInt;
					double sourceDouble;
					while (reader.NextKeyValue())
					{
						sourceInt = r.Next();
						sourceDouble = r.NextDouble();
						iwritable = reader.GetCurrentKey();
						dwritable = reader.GetCurrentValue();
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
			Job job = Job.GetInstance();
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
			Job job = Job.GetInstance();
			FileSystem fs = FileSystem.GetLocal(job.GetConfiguration());
			Path outputdir = new Path(Runtime.GetProperty("test.build.data", "/tmp") + "/output"
				);
			fs.Delete(outputdir, true);
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
				new SequenceFileAsBinaryOutputFormat().CheckOutputSpecs(job);
			}
			catch (Exception e)
			{
				Fail("Block compression should be allowed for " + "SequenceFileAsBinaryOutputFormat:Caught "
					 + e.GetType().FullName);
			}
			SequenceFileAsBinaryOutputFormat.SetOutputCompressionType(job, SequenceFile.CompressionType
				.Record);
			try
			{
				new SequenceFileAsBinaryOutputFormat().CheckOutputSpecs(job);
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
