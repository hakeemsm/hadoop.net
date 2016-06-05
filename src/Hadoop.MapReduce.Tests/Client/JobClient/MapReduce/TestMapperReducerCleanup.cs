using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Lib.Reduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public class TestMapperReducerCleanup
	{
		internal static bool mapCleanup = false;

		internal static bool reduceCleanup = false;

		internal static bool recordReaderCleanup = false;

		internal static bool recordWriterCleanup = false;

		internal static void Reset()
		{
			mapCleanup = false;
			reduceCleanup = false;
			recordReaderCleanup = false;
			recordWriterCleanup = false;
		}

		private class FailingMapper : Mapper<LongWritable, Text, LongWritable, Text>
		{
			/// <summary>Map method with different behavior based on the thread id</summary>
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(LongWritable key, Text val, Mapper.Context c)
			{
				throw new IOException("TestMapperReducerCleanup");
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Mapper.Context context)
			{
				mapCleanup = true;
				base.Cleanup(context);
			}
		}

		private class TrackingTokenizerMapper : Mapper<object, Text, Text, IntWritable>
		{
			private static readonly IntWritable one = new IntWritable(1);

			private Text word = new Text();

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(object key, Text value, Mapper.Context context)
			{
				StringTokenizer itr = new StringTokenizer(value.ToString());
				while (itr.HasMoreTokens())
				{
					word.Set(itr.NextToken());
					context.Write(word, one);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Mapper.Context context)
			{
				mapCleanup = true;
				base.Cleanup(context);
			}
		}

		private class FailingReducer : Reducer<LongWritable, Text, LongWritable, LongWritable
			>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Reduce(LongWritable key, IEnumerable<Text> vals, Reducer.Context
				 context)
			{
				throw new IOException("TestMapperReducerCleanup");
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Reducer.Context context)
			{
				reduceCleanup = true;
				base.Cleanup(context);
			}
		}

		private class TrackingIntSumReducer : IntSumReducer
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Cleanup(Reducer.Context context)
			{
				reduceCleanup = true;
				base.Cleanup(context);
			}
		}

		public class TrackingTextInputFormat : TextInputFormat
		{
			public class TrackingRecordReader : LineRecordReader
			{
				/// <exception cref="System.IO.IOException"/>
				public override void Close()
				{
					lock (this)
					{
						recordReaderCleanup = true;
						base.Close();
					}
				}
			}

			public override RecordReader<LongWritable, Text> CreateRecordReader(InputSplit split
				, TaskAttemptContext context)
			{
				return new TestMapperReducerCleanup.TrackingTextInputFormat.TrackingRecordReader(
					);
			}
		}

		public class TrackingTextOutputFormat : TextOutputFormat
		{
			public class TrackingRecordWriter : TextOutputFormat.LineRecordWriter
			{
				public TrackingRecordWriter(DataOutputStream @out)
					: base(@out)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public override void Close(TaskAttemptContext context)
				{
					lock (this)
					{
						recordWriterCleanup = true;
						base.Close(context);
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override RecordWriter GetRecordWriter(TaskAttemptContext job)
			{
				Configuration conf = job.GetConfiguration();
				Path file = GetDefaultWorkFile(job, string.Empty);
				FileSystem fs = file.GetFileSystem(conf);
				FSDataOutputStream fileOut = fs.Create(file, false);
				return new TestMapperReducerCleanup.TrackingTextOutputFormat.TrackingRecordWriter
					(fileOut);
			}
		}

		/// <summary>Create a single input file in the input directory.</summary>
		/// <param name="dirPath">the directory in which the file resides</param>
		/// <param name="id">the file id number</param>
		/// <param name="numRecords">how many records to write to each file.</param>
		/// <exception cref="System.IO.IOException"/>
		private void CreateInputFile(Path dirPath, int id, int numRecords)
		{
			string Message = "This is a line in a file: ";
			Path filePath = new Path(dirPath, string.Empty + id);
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			OutputStream os = fs.Create(filePath);
			BufferedWriter w = new BufferedWriter(new OutputStreamWriter(os));
			for (int i = 0; i < numRecords; i++)
			{
				w.Write(Message + id + " " + i + "\n");
			}
			w.Close();
		}

		private readonly string InputDir = "input";

		private readonly string OutputDir = "output";

		private Path GetInputPath()
		{
			string dataDir = Runtime.GetProperty("test.build.data");
			if (null == dataDir)
			{
				return new Path(InputDir);
			}
			else
			{
				return new Path(new Path(dataDir), InputDir);
			}
		}

		private Path GetOutputPath()
		{
			string dataDir = Runtime.GetProperty("test.build.data");
			if (null == dataDir)
			{
				return new Path(OutputDir);
			}
			else
			{
				return new Path(new Path(dataDir), OutputDir);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private Path CreateInput()
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			Path inputPath = GetInputPath();
			// Clear the input directory if it exists, first.
			if (fs.Exists(inputPath))
			{
				fs.Delete(inputPath, true);
			}
			// Create an input file
			CreateInputFile(inputPath, 0, 10);
			return inputPath;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMapCleanup()
		{
			Reset();
			Job job = Job.GetInstance();
			Path inputPath = CreateInput();
			Path outputPath = GetOutputPath();
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			if (fs.Exists(outputPath))
			{
				fs.Delete(outputPath, true);
			}
			job.SetMapperClass(typeof(TestMapperReducerCleanup.FailingMapper));
			job.SetInputFormatClass(typeof(TestMapperReducerCleanup.TrackingTextInputFormat));
			job.SetOutputFormatClass(typeof(TestMapperReducerCleanup.TrackingTextOutputFormat
				));
			job.SetNumReduceTasks(0);
			FileInputFormat.AddInputPath(job, inputPath);
			FileOutputFormat.SetOutputPath(job, outputPath);
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue(mapCleanup);
			NUnit.Framework.Assert.IsTrue(recordReaderCleanup);
			NUnit.Framework.Assert.IsTrue(recordWriterCleanup);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReduceCleanup()
		{
			Reset();
			Job job = Job.GetInstance();
			Path inputPath = CreateInput();
			Path outputPath = GetOutputPath();
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			if (fs.Exists(outputPath))
			{
				fs.Delete(outputPath, true);
			}
			job.SetMapperClass(typeof(TestMapperReducerCleanup.TrackingTokenizerMapper));
			job.SetReducerClass(typeof(TestMapperReducerCleanup.FailingReducer));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(IntWritable));
			job.SetInputFormatClass(typeof(TestMapperReducerCleanup.TrackingTextInputFormat));
			job.SetOutputFormatClass(typeof(TestMapperReducerCleanup.TrackingTextOutputFormat
				));
			job.SetNumReduceTasks(1);
			FileInputFormat.AddInputPath(job, inputPath);
			FileOutputFormat.SetOutputPath(job, outputPath);
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue(mapCleanup);
			NUnit.Framework.Assert.IsTrue(reduceCleanup);
			NUnit.Framework.Assert.IsTrue(recordReaderCleanup);
			NUnit.Framework.Assert.IsTrue(recordWriterCleanup);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobSuccessCleanup()
		{
			Reset();
			Job job = Job.GetInstance();
			Path inputPath = CreateInput();
			Path outputPath = GetOutputPath();
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.GetLocal(conf);
			if (fs.Exists(outputPath))
			{
				fs.Delete(outputPath, true);
			}
			job.SetMapperClass(typeof(TestMapperReducerCleanup.TrackingTokenizerMapper));
			job.SetReducerClass(typeof(TestMapperReducerCleanup.TrackingIntSumReducer));
			job.SetOutputKeyClass(typeof(Text));
			job.SetOutputValueClass(typeof(IntWritable));
			job.SetInputFormatClass(typeof(TestMapperReducerCleanup.TrackingTextInputFormat));
			job.SetOutputFormatClass(typeof(TestMapperReducerCleanup.TrackingTextOutputFormat
				));
			job.SetNumReduceTasks(1);
			FileInputFormat.AddInputPath(job, inputPath);
			FileOutputFormat.SetOutputPath(job, outputPath);
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue(mapCleanup);
			NUnit.Framework.Assert.IsTrue(reduceCleanup);
			NUnit.Framework.Assert.IsTrue(recordReaderCleanup);
			NUnit.Framework.Assert.IsTrue(recordWriterCleanup);
		}
	}
}
