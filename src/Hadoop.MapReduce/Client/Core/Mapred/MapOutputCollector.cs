using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public abstract class MapOutputCollector<K, V>
	{
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.TypeLoadException"/>
		public abstract void Init(MapOutputCollector.Context context);

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract void Collect(K key, V value, int partition);

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract void Close();

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public abstract void Flush();

		public class Context
		{
			private readonly MapTask mapTask;

			private readonly JobConf jobConf;

			private readonly Task.TaskReporter reporter;

			public Context(MapTask mapTask, JobConf jobConf, Task.TaskReporter reporter)
			{
				this.mapTask = mapTask;
				this.jobConf = jobConf;
				this.reporter = reporter;
			}

			public virtual MapTask GetMapTask()
			{
				return mapTask;
			}

			public virtual JobConf GetJobConf()
			{
				return jobConf;
			}

			public virtual Task.TaskReporter GetReporter()
			{
				return reporter;
			}
		}
	}

	public static class MapOutputCollectorConstants
	{
	}
}
