using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Map
{
	/// <summary>Multithreaded implementation for @link org.apache.hadoop.mapreduce.Mapper.
	/// 	</summary>
	/// <remarks>
	/// Multithreaded implementation for @link org.apache.hadoop.mapreduce.Mapper.
	/// <p>
	/// It can be used instead of the default implementation,
	/// <see cref="Org.Apache.Hadoop.Mapred.MapRunner{K1, V1, K2, V2}"/>
	/// , when the Map operation is not CPU
	/// bound in order to improve throughput.
	/// <p>
	/// Mapper implementations using this MapRunnable must be thread-safe.
	/// <p>
	/// The Map-Reduce job has to be configured with the mapper to use via
	/// <see cref="MultithreadedMapper{K1, V1, K2, V2}.SetMapperClass{K1, V1, K2, V2}(Org.Apache.Hadoop.Mapreduce.Job, System.Type{T})
	/// 	"/>
	/// and
	/// the number of thread the thread-pool can use with the
	/// <see cref="MultithreadedMapper{K1, V1, K2, V2}.GetNumberOfThreads(Org.Apache.Hadoop.Mapreduce.JobContext)
	/// 	"/>
	/// method. The default
	/// value is 10 threads.
	/// <p>
	/// </remarks>
	public class MultithreadedMapper<K1, V1, K2, V2> : Mapper<K1, V1, K2, V2>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(MultithreadedMapper));

		public static string NumThreads = "mapreduce.mapper.multithreadedmapper.threads";

		public static string MapClass = "mapreduce.mapper.multithreadedmapper.mapclass";

		private Type mapClass;

		private Mapper.Context outer;

		private IList<MultithreadedMapper.MapRunner> runners;

		/// <summary>The number of threads in the thread pool that will run the map function.
		/// 	</summary>
		/// <param name="job">the job</param>
		/// <returns>the number of threads</returns>
		public static int GetNumberOfThreads(JobContext job)
		{
			return job.GetConfiguration().GetInt(NumThreads, 10);
		}

		/// <summary>Set the number of threads in the pool for running maps.</summary>
		/// <param name="job">the job to modify</param>
		/// <param name="threads">the new number of threads</param>
		public static void SetNumberOfThreads(Job job, int threads)
		{
			job.GetConfiguration().SetInt(NumThreads, threads);
		}

		/// <summary>Get the application's mapper class.</summary>
		/// <?/>
		/// <?/>
		/// <?/>
		/// <?/>
		/// <param name="job">the job</param>
		/// <returns>the mapper class to run</returns>
		public static Type GetMapperClass<K1, V1, K2, V2>(JobContext job)
		{
			return (Type)job.GetConfiguration().GetClass(MapClass, typeof(Mapper));
		}

		/// <summary>Set the application's mapper class.</summary>
		/// <?/>
		/// <?/>
		/// <?/>
		/// <?/>
		/// <param name="job">the job to modify</param>
		/// <param name="cls">the class to use as the mapper</param>
		public static void SetMapperClass<K1, V1, K2, V2>(Job job, Type cls)
		{
			if (typeof(MultithreadedMapper).IsAssignableFrom(cls))
			{
				throw new ArgumentException("Can't have recursive " + "MultithreadedMapper instances."
					);
			}
			job.GetConfiguration().SetClass(MapClass, cls, typeof(Mapper));
		}

		/// <summary>Run the application's maps using a thread pool.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void Run(Mapper.Context context)
		{
			outer = context;
			int numberOfThreads = GetNumberOfThreads(context);
			mapClass = GetMapperClass(context);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Configuring multithread runner to use " + numberOfThreads + " threads"
					);
			}
			runners = new AList<MultithreadedMapper.MapRunner>(numberOfThreads);
			for (int i = 0; i < numberOfThreads; ++i)
			{
				MultithreadedMapper.MapRunner thread = new MultithreadedMapper.MapRunner(this, context
					);
				thread.Start();
				runners.Add(i, thread);
			}
			for (int i_1 = 0; i_1 < numberOfThreads; ++i_1)
			{
				MultithreadedMapper.MapRunner thread = runners[i_1];
				thread.Join();
				Exception th = thread.throwable;
				if (th != null)
				{
					if (th is IOException)
					{
						throw (IOException)th;
					}
					else
					{
						if (th is Exception)
						{
							throw (Exception)th;
						}
						else
						{
							throw new RuntimeException(th);
						}
					}
				}
			}
		}

		private class SubMapRecordReader : RecordReader<K1, V1>
		{
			private K1 key;

			private V1 value;

			private Configuration conf;

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override float GetProgress()
			{
				return 0;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Initialize(InputSplit split, TaskAttemptContext context)
			{
				this.conf = context.GetConfiguration();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override bool NextKeyValue()
			{
				lock (this._enclosing.outer)
				{
					if (!this._enclosing.outer.NextKeyValue())
					{
						return false;
					}
					this.key = ReflectionUtils.Copy(this._enclosing.outer.GetConfiguration(), this._enclosing
						.outer.GetCurrentKey(), this.key);
					this.value = ReflectionUtils.Copy(this.conf, this._enclosing.outer.GetCurrentValue
						(), this.value);
					return true;
				}
			}

			public override K1 GetCurrentKey()
			{
				return this.key;
			}

			public override V1 GetCurrentValue()
			{
				return this.value;
			}

			internal SubMapRecordReader(MultithreadedMapper<K1, V1, K2, V2> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MultithreadedMapper<K1, V1, K2, V2> _enclosing;
		}

		private class SubMapRecordWriter : RecordWriter<K2, V2>
		{
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Close(TaskAttemptContext context)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Write(K2 key, V2 value)
			{
				lock (this._enclosing.outer)
				{
					this._enclosing.outer.Write(key, value);
				}
			}

			internal SubMapRecordWriter(MultithreadedMapper<K1, V1, K2, V2> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MultithreadedMapper<K1, V1, K2, V2> _enclosing;
		}

		private class SubMapStatusReporter : StatusReporter
		{
			public override Counter GetCounter<_T0>(Enum<_T0> name)
			{
				return this._enclosing.outer.GetCounter(name);
			}

			public override Counter GetCounter(string group, string name)
			{
				return this._enclosing.outer.GetCounter(group, name);
			}

			public override void Progress()
			{
				this._enclosing.outer.Progress();
			}

			public override void SetStatus(string status)
			{
				this._enclosing.outer.SetStatus(status);
			}

			public override float GetProgress()
			{
				return this._enclosing.outer.GetProgress();
			}

			internal SubMapStatusReporter(MultithreadedMapper<K1, V1, K2, V2> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MultithreadedMapper<K1, V1, K2, V2> _enclosing;
		}

		private class MapRunner : Sharpen.Thread
		{
			private Mapper<K1, V1, K2, V2> mapper;

			private Mapper.Context subcontext;

			private Exception throwable;

			private RecordReader<K1, V1> reader;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			internal MapRunner(MultithreadedMapper<K1, V1, K2, V2> _enclosing, Mapper.Context
				 context)
			{
				this._enclosing = _enclosing;
				reader = new MultithreadedMapper.SubMapRecordReader(this);
				this.mapper = ReflectionUtils.NewInstance(this._enclosing.mapClass, context.GetConfiguration
					());
				MapContext<K1, V1, K2, V2> mapContext = new MapContextImpl<K1, V1, K2, V2>(this._enclosing
					.outer.GetConfiguration(), this._enclosing.outer.GetTaskAttemptID(), this.reader
					, new MultithreadedMapper.SubMapRecordWriter(this), context.GetOutputCommitter()
					, new MultithreadedMapper.SubMapStatusReporter(this), this._enclosing.outer.GetInputSplit
					());
				this.subcontext = new WrappedMapper<K1, V1, K2, V2>().GetMapContext(mapContext);
				this.reader.Initialize(context.GetInputSplit(), context);
			}

			public override void Run()
			{
				try
				{
					this.mapper.Run(this.subcontext);
					this.reader.Close();
				}
				catch (Exception ie)
				{
					this.throwable = ie;
				}
			}

			private readonly MultithreadedMapper<K1, V1, K2, V2> _enclosing;
		}
	}
}
