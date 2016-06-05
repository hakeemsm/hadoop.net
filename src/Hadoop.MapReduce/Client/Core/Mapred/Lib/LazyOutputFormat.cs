using System;
using System.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>A Convenience class that creates output lazily.</summary>
	public class LazyOutputFormat<K, V> : FilterOutputFormat<K, V>
	{
		/// <summary>Set the underlying output format for LazyOutputFormat.</summary>
		/// <param name="job">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapred.JobConf"/>
		/// to modify
		/// </param>
		/// <param name="theClass">the underlying class</param>
		public static void SetOutputFormatClass(JobConf job, Type theClass)
		{
			job.SetOutputFormat(typeof(LazyOutputFormat));
			job.SetClass("mapreduce.output.lazyoutputformat.outputformat", theClass, typeof(OutputFormat
				));
		}

		/// <exception cref="System.IO.IOException"/>
		public override RecordWriter<K, V> GetRecordWriter(FileSystem ignored, JobConf job
			, string name, Progressable progress)
		{
			if (baseOut == null)
			{
				GetBaseOutputFormat(job);
			}
			return new LazyOutputFormat.LazyRecordWriter<K, V>(job, baseOut, name, progress);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CheckOutputSpecs(FileSystem ignored, JobConf job)
		{
			if (baseOut == null)
			{
				GetBaseOutputFormat(job);
			}
			base.CheckOutputSpecs(ignored, job);
		}

		/// <exception cref="System.IO.IOException"/>
		private void GetBaseOutputFormat(JobConf job)
		{
			baseOut = ReflectionUtils.NewInstance(job.GetClass<OutputFormat>("mapreduce.output.lazyoutputformat.outputformat"
				, null), job);
			if (baseOut == null)
			{
				throw new IOException("Ouput format not set for LazyOutputFormat");
			}
		}

		/// <summary>
		/// <code>LazyRecordWriter</code> is a convenience
		/// class that works with LazyOutputFormat.
		/// </summary>
		private class LazyRecordWriter<K, V> : FilterOutputFormat.FilterRecordWriter<K, V
			>
		{
			internal readonly OutputFormat of;

			internal readonly string name;

			internal readonly Progressable progress;

			internal readonly JobConf job;

			/// <exception cref="System.IO.IOException"/>
			public LazyRecordWriter(JobConf job, OutputFormat of, string name, Progressable progress
				)
			{
				this.of = of;
				this.job = job;
				this.name = name;
				this.progress = progress;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close(Reporter reporter)
			{
				if (rawWriter != null)
				{
					rawWriter.Close(reporter);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(K key, V value)
			{
				if (rawWriter == null)
				{
					CreateRecordWriter();
				}
				base.Write(key, value);
			}

			/// <exception cref="System.IO.IOException"/>
			private void CreateRecordWriter()
			{
				FileSystem fs = FileSystem.Get(job);
				rawWriter = of.GetRecordWriter(fs, job, name, progress);
			}
		}
	}
}
