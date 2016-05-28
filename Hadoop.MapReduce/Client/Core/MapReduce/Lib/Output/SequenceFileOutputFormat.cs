using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Mapreduce.OutputFormat{K, V}"/>
	/// that writes
	/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
	/// s.
	/// </summary>
	public class SequenceFileOutputFormat<K, V> : FileOutputFormat<K, V>
	{
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual SequenceFile.Writer GetSequenceWriter(TaskAttemptContext
			 context, Type keyClass, Type valueClass)
		{
			Configuration conf = context.GetConfiguration();
			CompressionCodec codec = null;
			SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.None;
			if (GetCompressOutput(context))
			{
				// find the kind of compression to do
				compressionType = GetOutputCompressionType(context);
				// find the right codec
				Type codecClass = GetOutputCompressorClass(context, typeof(DefaultCodec));
				codec = (CompressionCodec)ReflectionUtils.NewInstance(codecClass, conf);
			}
			// get the path of the temporary output file 
			Path file = GetDefaultWorkFile(context, string.Empty);
			FileSystem fs = file.GetFileSystem(conf);
			return SequenceFile.CreateWriter(fs, conf, file, keyClass, valueClass, compressionType
				, codec, context);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override RecordWriter<K, V> GetRecordWriter(TaskAttemptContext context)
		{
			SequenceFile.Writer @out = GetSequenceWriter(context, context.GetOutputKeyClass()
				, context.GetOutputValueClass());
			return new _RecordWriter_78(@out);
		}

		private sealed class _RecordWriter_78 : RecordWriter<K, V>
		{
			public _RecordWriter_78(SequenceFile.Writer @out)
			{
				this.@out = @out;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(K key, V value)
			{
				@out.Append(key, value);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close(TaskAttemptContext context)
			{
				@out.Close();
			}

			private readonly SequenceFile.Writer @out;
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile.CompressionType"/>
		/// for the output
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// .
		/// </summary>
		/// <param name="job">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Job"/>
		/// </param>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile.CompressionType"/>
		/// for the output
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// ,
		/// defaulting to
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile.CompressionType.Record"/>
		/// </returns>
		public static SequenceFile.CompressionType GetOutputCompressionType(JobContext job
			)
		{
			string val = job.GetConfiguration().Get(FileOutputFormat.CompressType, SequenceFile.CompressionType
				.Record.ToString());
			return SequenceFile.CompressionType.ValueOf(val);
		}

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile.CompressionType"/>
		/// for the output
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// .
		/// </summary>
		/// <param name="job">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Job"/>
		/// to modify
		/// </param>
		/// <param name="style">
		/// the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile.CompressionType"/>
		/// for the output
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// 
		/// </param>
		public static void SetOutputCompressionType(Job job, SequenceFile.CompressionType
			 style)
		{
			SetCompressOutput(job, true);
			job.GetConfiguration().Set(FileOutputFormat.CompressType, style.ToString());
		}
	}
}
