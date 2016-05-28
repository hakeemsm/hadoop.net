using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	/// <summary>A Convenience class that creates output lazily.</summary>
	/// <remarks>
	/// A Convenience class that creates output lazily.
	/// Use in conjuction with org.apache.hadoop.mapreduce.lib.output.MultipleOutputs to recreate the
	/// behaviour of org.apache.hadoop.mapred.lib.MultipleTextOutputFormat (etc) of the old Hadoop API.
	/// See
	/// <see cref="MultipleOutputs{KEYOUT, VALUEOUT}"/>
	/// documentation for more information.
	/// </remarks>
	public class LazyOutputFormat<K, V> : FilterOutputFormat<K, V>
	{
		public static string OutputFormat = "mapreduce.output.lazyoutputformat.outputformat";

		/// <summary>Set the underlying output format for LazyOutputFormat.</summary>
		/// <param name="job">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Job"/>
		/// to modify
		/// </param>
		/// <param name="theClass">the underlying class</param>
		public static void SetOutputFormatClass(Job job, Type theClass)
		{
			job.SetOutputFormatClass(typeof(LazyOutputFormat));
			job.GetConfiguration().SetClass(OutputFormat, theClass, typeof(OutputFormat));
		}

		/// <exception cref="System.IO.IOException"/>
		private void GetBaseOutputFormat(Configuration conf)
		{
			baseOut = ((OutputFormat<K, V>)ReflectionUtils.NewInstance(conf.GetClass(OutputFormat
				, null), conf));
			if (baseOut == null)
			{
				throw new IOException("Output Format not set for LazyOutputFormat");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override RecordWriter<K, V> GetRecordWriter(TaskAttemptContext context)
		{
			if (baseOut == null)
			{
				GetBaseOutputFormat(context.GetConfiguration());
			}
			return new LazyOutputFormat.LazyRecordWriter<K, V>(baseOut, context);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void CheckOutputSpecs(JobContext context)
		{
			if (baseOut == null)
			{
				GetBaseOutputFormat(context.GetConfiguration());
			}
			base.CheckOutputSpecs(context);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override OutputCommitter GetOutputCommitter(TaskAttemptContext context)
		{
			if (baseOut == null)
			{
				GetBaseOutputFormat(context.GetConfiguration());
			}
			return base.GetOutputCommitter(context);
		}

		/// <summary>A convenience class to be used with LazyOutputFormat</summary>
		private class LazyRecordWriter<K, V> : FilterOutputFormat.FilterRecordWriter<K, V
			>
		{
			internal readonly OutputFormat<K, V> outputFormat;

			internal readonly TaskAttemptContext taskContext;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public LazyRecordWriter(OutputFormat<K, V> @out, TaskAttemptContext taskContext)
			{
				this.outputFormat = @out;
				this.taskContext = taskContext;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Write(K key, V value)
			{
				if (rawWriter == null)
				{
					rawWriter = outputFormat.GetRecordWriter(taskContext);
				}
				rawWriter.Write(key, value);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Close(TaskAttemptContext context)
			{
				if (rawWriter != null)
				{
					rawWriter.Close(context);
				}
			}
		}
	}
}
