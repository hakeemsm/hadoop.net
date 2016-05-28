using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task
{
	/// <summary>
	/// The context that is given to the
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
	/// 	>
	/// .
	/// </summary>
	/// <?/>
	/// <?/>
	/// <?/>
	/// <?/>
	public class MapContextImpl<Keyin, Valuein, Keyout, Valueout> : TaskInputOutputContextImpl
		<KEYIN, VALUEIN, KEYOUT, VALUEOUT>, MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
	{
		private RecordReader<KEYIN, VALUEIN> reader;

		private InputSplit split;

		public MapContextImpl(Configuration conf, TaskAttemptID taskid, RecordReader<KEYIN
			, VALUEIN> reader, RecordWriter<KEYOUT, VALUEOUT> writer, OutputCommitter committer
			, StatusReporter reporter, InputSplit split)
			: base(conf, taskid, writer, committer, reporter)
		{
			this.reader = reader;
			this.split = split;
		}

		/// <summary>Get the input split for this map.</summary>
		public virtual InputSplit GetInputSplit()
		{
			return split;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override KEYIN GetCurrentKey()
		{
			return reader.GetCurrentKey();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override VALUEIN GetCurrentValue()
		{
			return reader.GetCurrentValue();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override bool NextKeyValue()
		{
			return reader.NextKeyValue();
		}
	}
}
