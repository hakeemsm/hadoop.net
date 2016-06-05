using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public class TestMROutputFormat
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobSubmission()
		{
			JobConf conf = new JobConf();
			Job job = new Job(conf);
			job.SetInputFormatClass(typeof(TestInputFormat));
			job.SetMapperClass(typeof(TestMROutputFormat.TestMapper));
			job.SetOutputFormatClass(typeof(TestOutputFormat));
			job.SetOutputKeyClass(typeof(IntWritable));
			job.SetOutputValueClass(typeof(IntWritable));
			job.WaitForCompletion(true);
			NUnit.Framework.Assert.IsTrue(job.IsSuccessful());
		}

		public class TestMapper : Mapper<IntWritable, IntWritable, IntWritable, IntWritable
			>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected override void Map(IntWritable key, IntWritable value, Mapper.Context context
				)
			{
				context.Write(key, value);
			}
		}
	}

	internal class TestInputFormat : InputFormat<IntWritable, IntWritable>
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override RecordReader<IntWritable, IntWritable> CreateRecordReader(InputSplit
			 split, TaskAttemptContext context)
		{
			return new _RecordReader_66();
		}

		private sealed class _RecordReader_66 : RecordReader<IntWritable, IntWritable>
		{
			public _RecordReader_66()
			{
				this.done = false;
			}

			private bool done;

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override IntWritable GetCurrentKey()
			{
				return new IntWritable(0);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override IntWritable GetCurrentValue()
			{
				return new IntWritable(0);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override float GetProgress()
			{
				return this.done ? 0 : 1;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Initialize(InputSplit split, TaskAttemptContext context)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override bool NextKeyValue()
			{
				if (!this.done)
				{
					this.done = true;
					return true;
				}
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override IList<InputSplit> GetSplits(JobContext context)
		{
			IList<InputSplit> list = new AList<InputSplit>();
			list.AddItem(new TestInputSplit());
			return list;
		}
	}

	internal class TestInputSplit : InputSplit, Writable
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override long GetLength()
		{
			return 1;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override string[] GetLocations()
		{
			string[] hosts = new string[] { "localhost" };
			return hosts;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
		}
	}

	internal class TestOutputFormat : OutputFormat<IntWritable, IntWritable>, Configurable
	{
		public const string TestConfigName = "mapred.test.jobsubmission";

		private Configuration conf;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void CheckOutputSpecs(JobContext context)
		{
			conf.SetBoolean(TestConfigName, true);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override OutputCommitter GetOutputCommitter(TaskAttemptContext context)
		{
			return new _OutputCommitter_153();
		}

		private sealed class _OutputCommitter_153 : OutputCommitter
		{
			public _OutputCommitter_153()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void AbortTask(TaskAttemptContext taskContext)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CommitTask(TaskAttemptContext taskContext)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool NeedsTaskCommit(TaskAttemptContext taskContext)
			{
				return false;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetupJob(JobContext jobContext)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void SetupTask(TaskAttemptContext taskContext)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override RecordWriter<IntWritable, IntWritable> GetRecordWriter(TaskAttemptContext
			 context)
		{
			NUnit.Framework.Assert.IsTrue(context.GetConfiguration().GetBoolean(TestConfigName
				, false));
			return new _RecordWriter_183();
		}

		private sealed class _RecordWriter_183 : RecordWriter<IntWritable, IntWritable>
		{
			public _RecordWriter_183()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Close(TaskAttemptContext context)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Write(IntWritable key, IntWritable value)
			{
			}
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}
	}
}
