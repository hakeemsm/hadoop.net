using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task
{
	/// <summary>A context object that allows input and output from the task.</summary>
	/// <remarks>
	/// A context object that allows input and output from the task. It is only
	/// supplied to the
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
	/// 	>
	/// or
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"
	/// 	/>
	/// .
	/// </remarks>
	/// <?/>
	/// <?/>
	/// <?/>
	/// <?/>
	public abstract class TaskInputOutputContextImpl<Keyin, Valuein, Keyout, Valueout
		> : TaskAttemptContextImpl, TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT
		>
	{
		private RecordWriter<KEYOUT, VALUEOUT> output;

		private OutputCommitter committer;

		public TaskInputOutputContextImpl(Configuration conf, TaskAttemptID taskid, RecordWriter
			<KEYOUT, VALUEOUT> output, OutputCommitter committer, StatusReporter reporter)
			: base(conf, taskid, reporter)
		{
			this.output = output;
			this.committer = committer;
		}

		/// <summary>Advance to the next key, value pair, returning null if at end.</summary>
		/// <returns>the key object that was read into, or null if no more</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract bool NextKeyValue();

		/// <summary>Get the current key.</summary>
		/// <returns>the current key object or null if there isn't one</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract KEYIN GetCurrentKey();

		/// <summary>Get the current value.</summary>
		/// <returns>the value object that was read into</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract VALUEIN GetCurrentValue();

		/// <summary>Generate an output key/value pair.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void Write(KEYOUT key, VALUEOUT value)
		{
			output.Write(key, value);
		}

		public virtual OutputCommitter GetOutputCommitter()
		{
			return committer;
		}
	}
}
