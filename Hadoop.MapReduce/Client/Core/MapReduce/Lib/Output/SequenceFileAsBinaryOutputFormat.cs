using System;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	/// <summary>
	/// An
	/// <see cref="Org.Apache.Hadoop.Mapreduce.OutputFormat{K, V}"/>
	/// that writes keys,
	/// values to
	/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
	/// s in binary(raw) format
	/// </summary>
	public class SequenceFileAsBinaryOutputFormat : SequenceFileOutputFormat<BytesWritable
		, BytesWritable>
	{
		public static string KeyClass = "mapreduce.output.seqbinaryoutputformat.key.class";

		public static string ValueClass = "mapreduce.output.seqbinaryoutputformat.value.class";

		/// <summary>Inner class used for appendRaw</summary>
		public class WritableValueBytes : SequenceFile.ValueBytes
		{
			private BytesWritable value;

			public WritableValueBytes()
			{
				this.value = null;
			}

			public WritableValueBytes(BytesWritable value)
			{
				this.value = value;
			}

			public virtual void Reset(BytesWritable value)
			{
				this.value = value;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteUncompressedBytes(DataOutputStream outStream)
			{
				outStream.Write(value.GetBytes(), 0, value.GetLength());
			}

			/// <exception cref="System.ArgumentException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteCompressedBytes(DataOutputStream outStream)
			{
				throw new NotSupportedException("WritableValueBytes doesn't support RECORD compression"
					);
			}

			public virtual int GetSize()
			{
				return value.GetLength();
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
		/// <param name="job">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Job"/>
		/// to modify
		/// </param>
		/// <param name="theClass">the SequenceFile output key class.</param>
		public static void SetSequenceFileOutputKeyClass(Job job, Type theClass)
		{
			job.GetConfiguration().SetClass(KeyClass, theClass, typeof(object));
		}

		/// <summary>
		/// Set the value class for the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// <p>This allows the user to specify the value class to be different
		/// from the actual class (
		/// <see cref="Org.Apache.Hadoop.IO.BytesWritable"/>
		/// ) used for writing </p>
		/// </summary>
		/// <param name="job">
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Job"/>
		/// to modify
		/// </param>
		/// <param name="theClass">the SequenceFile output key class.</param>
		public static void SetSequenceFileOutputValueClass(Job job, Type theClass)
		{
			job.GetConfiguration().SetClass(ValueClass, theClass, typeof(object));
		}

		/// <summary>
		/// Get the key class for the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// </summary>
		/// <returns>
		/// the key class of the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// </returns>
		public static Type GetSequenceFileOutputKeyClass(JobContext job)
		{
			return job.GetConfiguration().GetClass<WritableComparable>(KeyClass, job.GetOutputKeyClass
				().AsSubclass<WritableComparable>());
		}

		/// <summary>
		/// Get the value class for the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// </summary>
		/// <returns>
		/// the value class of the
		/// <see cref="Org.Apache.Hadoop.IO.SequenceFile"/>
		/// </returns>
		public static Type GetSequenceFileOutputValueClass(JobContext job)
		{
			return job.GetConfiguration().GetClass<Writable>(ValueClass, job.GetOutputValueClass
				().AsSubclass<Writable>());
		}

		/// <exception cref="System.IO.IOException"/>
		public override RecordWriter<BytesWritable, BytesWritable> GetRecordWriter(TaskAttemptContext
			 context)
		{
			SequenceFile.Writer @out = GetSequenceWriter(context, GetSequenceFileOutputKeyClass
				(context), GetSequenceFileOutputValueClass(context));
			return new _RecordWriter_140(@out);
		}

		private sealed class _RecordWriter_140 : RecordWriter<BytesWritable, BytesWritable
			>
		{
			public _RecordWriter_140(SequenceFile.Writer @out)
			{
				this.@out = @out;
				this.wvaluebytes = new SequenceFileAsBinaryOutputFormat.WritableValueBytes();
			}

			private SequenceFileAsBinaryOutputFormat.WritableValueBytes wvaluebytes;

			/// <exception cref="System.IO.IOException"/>
			public override void Write(BytesWritable bkey, BytesWritable bvalue)
			{
				this.wvaluebytes.Reset(bvalue);
				@out.AppendRaw(bkey.GetBytes(), 0, bkey.GetLength(), this.wvaluebytes);
				this.wvaluebytes.Reset(null);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close(TaskAttemptContext context)
			{
				@out.Close();
			}

			private readonly SequenceFile.Writer @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CheckOutputSpecs(JobContext job)
		{
			base.CheckOutputSpecs(job);
			if (GetCompressOutput(job) && GetOutputCompressionType(job) == SequenceFile.CompressionType
				.Record)
			{
				throw new InvalidJobConfException("SequenceFileAsBinaryOutputFormat " + "doesn't support Record Compression"
					);
			}
		}
	}
}
