using System;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// An
	/// <see cref="OutputFormat{K, V}"/>
	/// that writes keys, values to
	/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
	/// s in binary(raw) format
	/// </summary>
	public class SequenceFileAsBinaryOutputFormat : SequenceFileOutputFormat<BytesWritable
		, BytesWritable>
	{
		/// <summary>Inner class used for appendRaw</summary>
		protected internal class WritableValueBytes : SequenceFileAsBinaryOutputFormat.WritableValueBytes
		{
			public WritableValueBytes()
				: base()
			{
			}

			public WritableValueBytes(BytesWritable value)
				: base(value)
			{
			}
		}

		/// <summary>
		/// Set the key class for the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// <p>This allows the user to specify the key class to be different
		/// from the actual class (
		/// <see cref="Org.Apache.Hadoop.IO.BytesWritable"/>
		/// ) used for writing </p>
		/// </summary>
		/// <param name="conf">
		/// the
		/// <see cref="JobConf"/>
		/// to modify
		/// </param>
		/// <param name="theClass">the SequenceFile output key class.</param>
		public static void SetSequenceFileOutputKeyClass(JobConf conf, Type theClass)
		{
			conf.SetClass(SequenceFileAsBinaryOutputFormat.KeyClass, theClass, typeof(object)
				);
		}

		/// <summary>
		/// Set the value class for the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// <p>This allows the user to specify the value class to be different
		/// from the actual class (
		/// <see cref="Org.Apache.Hadoop.IO.BytesWritable"/>
		/// ) used for writing </p>
		/// </summary>
		/// <param name="conf">
		/// the
		/// <see cref="JobConf"/>
		/// to modify
		/// </param>
		/// <param name="theClass">the SequenceFile output key class.</param>
		public static void SetSequenceFileOutputValueClass(JobConf conf, Type theClass)
		{
			conf.SetClass(SequenceFileAsBinaryOutputFormat.ValueClass, theClass, typeof(object
				));
		}

		/// <summary>
		/// Get the key class for the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// </summary>
		/// <returns>
		/// the key class of the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// </returns>
		public static Type GetSequenceFileOutputKeyClass(JobConf conf)
		{
			return conf.GetClass<WritableComparable>(SequenceFileAsBinaryOutputFormat.KeyClass
				, conf.GetOutputKeyClass().AsSubclass<WritableComparable>());
		}

		/// <summary>
		/// Get the value class for the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// </summary>
		/// <returns>
		/// the value class of the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// </returns>
		public static Type GetSequenceFileOutputValueClass(JobConf conf)
		{
			return conf.GetClass<Writable>(SequenceFileAsBinaryOutputFormat.ValueClass, conf.
				GetOutputValueClass().AsSubclass<Writable>());
		}

		/// <exception cref="System.IO.IOException"/>
		public override RecordWriter<BytesWritable, BytesWritable> GetRecordWriter(FileSystem
			 ignored, JobConf job, string name, Progressable progress)
		{
			// get the path of the temporary output file 
			Path file = FileOutputFormat.GetTaskOutputPath(job, name);
			FileSystem fs = file.GetFileSystem(job);
			CompressionCodec codec = null;
			SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.None;
			if (GetCompressOutput(job))
			{
				// find the kind of compression to do
				compressionType = GetOutputCompressionType(job);
				// find the right codec
				Type codecClass = GetOutputCompressorClass(job, typeof(DefaultCodec));
				codec = ReflectionUtils.NewInstance(codecClass, job);
			}
			SequenceFile.Writer @out = SequenceFile.CreateWriter(fs, job, file, GetSequenceFileOutputKeyClass
				(job), GetSequenceFileOutputValueClass(job), compressionType, codec, progress);
			return new _RecordWriter_138(@out);
		}

		private sealed class _RecordWriter_138 : RecordWriter<BytesWritable, BytesWritable
			>
		{
			public _RecordWriter_138(SequenceFile.Writer @out)
			{
				this.@out = @out;
				this.wvaluebytes = new SequenceFileAsBinaryOutputFormat.WritableValueBytes();
			}

			private SequenceFileAsBinaryOutputFormat.WritableValueBytes wvaluebytes;

			/// <exception cref="System.IO.IOException"/>
			public void Write(BytesWritable bkey, BytesWritable bvalue)
			{
				this.wvaluebytes.Reset(bvalue);
				@out.AppendRaw(bkey.GetBytes(), 0, bkey.GetLength(), this.wvaluebytes);
				this.wvaluebytes.Reset(null);
			}

			/// <exception cref="System.IO.IOException"/>
			public void Close(Reporter reporter)
			{
				@out.Close();
			}

			private readonly SequenceFile.Writer @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CheckOutputSpecs(FileSystem ignored, JobConf job)
		{
			base.CheckOutputSpecs(ignored, job);
			if (GetCompressOutput(job) && GetOutputCompressionType(job) == SequenceFile.CompressionType
				.Record)
			{
				throw new InvalidJobConfException("SequenceFileAsBinaryOutputFormat " + "doesn't support Record Compression"
					);
			}
		}
	}
}
