using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	/// <summary>
	/// Dummy input format used when non-Java a
	/// <see cref="Org.Apache.Hadoop.Mapred.RecordReader{K, V}"/>
	/// is used by
	/// the Pipes' application.
	/// The only useful thing this does is set up the Map-Reduce job to get the
	/// <see cref="PipesDummyRecordReader"/>
	/// , everything else left for the 'actual'
	/// InputFormat specified by the user which is given by
	/// <i>mapreduce.pipes.inputformat</i>.
	/// </summary>
	internal class PipesNonJavaInputFormat : InputFormat<FloatWritable, NullWritable>
	{
		/// <exception cref="System.IO.IOException"/>
		public virtual RecordReader<FloatWritable, NullWritable> GetRecordReader(InputSplit
			 genericSplit, JobConf job, Reporter reporter)
		{
			return new PipesNonJavaInputFormat.PipesDummyRecordReader(job, genericSplit);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual InputSplit[] GetSplits(JobConf job, int numSplits)
		{
			// Delegate the generation of input splits to the 'original' InputFormat
			return ReflectionUtils.NewInstance(job.GetClass<InputFormat>(Submitter.InputFormat
				, typeof(TextInputFormat)), job).GetSplits(job, numSplits);
		}

		/// <summary>
		/// A dummy
		/// <see cref="Org.Apache.Hadoop.Mapred.RecordReader{K, V}"/>
		/// to help track the
		/// progress of Hadoop Pipes' applications when they are using a non-Java
		/// <code>RecordReader</code>.
		/// The <code>PipesDummyRecordReader</code> is informed of the 'progress' of
		/// the task by the
		/// <see cref="OutputHandler{K, V}.Progress(float)"/>
		/// which calls the
		/// <see cref="Next(Org.Apache.Hadoop.IO.FloatWritable, Org.Apache.Hadoop.IO.NullWritable)
		/// 	"/>
		/// with the progress as the
		/// <code>key</code>.
		/// </summary>
		internal class PipesDummyRecordReader : RecordReader<FloatWritable, NullWritable>
		{
			internal float progress = 0.0f;

			/// <exception cref="System.IO.IOException"/>
			public PipesDummyRecordReader(Configuration job, InputSplit split)
			{
			}

			public virtual FloatWritable CreateKey()
			{
				return null;
			}

			public virtual NullWritable CreateValue()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				lock (this)
				{
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetPos()
			{
				lock (this)
				{
					return 0;
				}
			}

			public virtual float GetProgress()
			{
				return progress;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool Next(FloatWritable key, NullWritable value)
			{
				lock (this)
				{
					progress = key.Get();
					return true;
				}
			}
		}
	}
}
