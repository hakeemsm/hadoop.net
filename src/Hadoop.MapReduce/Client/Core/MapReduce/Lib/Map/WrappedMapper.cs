using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Map
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
	/// 	>
	/// which wraps a given one to allow custom
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper.Context"/>
	/// implementations.
	/// </summary>
	public class WrappedMapper<Keyin, Valuein, Keyout, Valueout> : Mapper<KEYIN, VALUEIN
		, KEYOUT, VALUEOUT>
	{
		/// <summary>
		/// Get a wrapped
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper.Context"/>
		/// for custom implementations.
		/// </summary>
		/// <param name="mapContext"><code>MapContext</code> to be wrapped</param>
		/// <returns>a wrapped <code>Mapper.Context</code> for custom implementations</returns>
		public virtual Mapper.Context GetMapContext(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT
			> mapContext)
		{
			return new WrappedMapper.Context(this, mapContext);
		}

		public class Context : Mapper.Context
		{
			protected internal MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext;

			public Context(WrappedMapper<Keyin, Valuein, Keyout, Valueout> _enclosing, MapContext
				<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.mapContext = mapContext;
			}

			/// <summary>Get the input split for this map.</summary>
			public override InputSplit GetInputSplit()
			{
				return this.mapContext.GetInputSplit();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override KEYIN GetCurrentKey()
			{
				return this.mapContext.GetCurrentKey();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override VALUEIN GetCurrentValue()
			{
				return this.mapContext.GetCurrentValue();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override bool NextKeyValue()
			{
				return this.mapContext.NextKeyValue();
			}

			public override Counter GetCounter<_T0>(Enum<_T0> counterName)
			{
				return this.mapContext.GetCounter(counterName);
			}

			public override Counter GetCounter(string groupName, string counterName)
			{
				return this.mapContext.GetCounter(groupName, counterName);
			}

			public override OutputCommitter GetOutputCommitter()
			{
				return this.mapContext.GetOutputCommitter();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Write(KEYOUT key, VALUEOUT value)
			{
				this.mapContext.Write(key, value);
			}

			public override string GetStatus()
			{
				return this.mapContext.GetStatus();
			}

			public override TaskAttemptID GetTaskAttemptID()
			{
				return this.mapContext.GetTaskAttemptID();
			}

			public override void SetStatus(string msg)
			{
				this.mapContext.SetStatus(msg);
			}

			public override Path[] GetArchiveClassPaths()
			{
				return this.mapContext.GetArchiveClassPaths();
			}

			public override string[] GetArchiveTimestamps()
			{
				return this.mapContext.GetArchiveTimestamps();
			}

			/// <exception cref="System.IO.IOException"/>
			public override URI[] GetCacheArchives()
			{
				return this.mapContext.GetCacheArchives();
			}

			/// <exception cref="System.IO.IOException"/>
			public override URI[] GetCacheFiles()
			{
				return this.mapContext.GetCacheFiles();
			}

			/// <exception cref="System.TypeLoadException"/>
			public override Type GetCombinerClass()
			{
				return this.mapContext.GetCombinerClass();
			}

			public override Configuration GetConfiguration()
			{
				return this.mapContext.GetConfiguration();
			}

			public override Path[] GetFileClassPaths()
			{
				return this.mapContext.GetFileClassPaths();
			}

			public override string[] GetFileTimestamps()
			{
				return this.mapContext.GetFileTimestamps();
			}

			public override RawComparator<object> GetCombinerKeyGroupingComparator()
			{
				return this.mapContext.GetCombinerKeyGroupingComparator();
			}

			public override RawComparator<object> GetGroupingComparator()
			{
				return this.mapContext.GetGroupingComparator();
			}

			/// <exception cref="System.TypeLoadException"/>
			public override Type GetInputFormatClass()
			{
				return this.mapContext.GetInputFormatClass();
			}

			public override string GetJar()
			{
				return this.mapContext.GetJar();
			}

			public override JobID GetJobID()
			{
				return this.mapContext.GetJobID();
			}

			public override string GetJobName()
			{
				return this.mapContext.GetJobName();
			}

			public override bool GetJobSetupCleanupNeeded()
			{
				return this.mapContext.GetJobSetupCleanupNeeded();
			}

			public override bool GetTaskCleanupNeeded()
			{
				return this.mapContext.GetTaskCleanupNeeded();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path[] GetLocalCacheArchives()
			{
				return this.mapContext.GetLocalCacheArchives();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path[] GetLocalCacheFiles()
			{
				return this.mapContext.GetLocalCacheFiles();
			}

			public override Type GetMapOutputKeyClass()
			{
				return this.mapContext.GetMapOutputKeyClass();
			}

			public override Type GetMapOutputValueClass()
			{
				return this.mapContext.GetMapOutputValueClass();
			}

			/// <exception cref="System.TypeLoadException"/>
			public override Type GetMapperClass()
			{
				return this.mapContext.GetMapperClass();
			}

			public override int GetMaxMapAttempts()
			{
				return this.mapContext.GetMaxMapAttempts();
			}

			public override int GetMaxReduceAttempts()
			{
				return this.mapContext.GetMaxReduceAttempts();
			}

			public override int GetNumReduceTasks()
			{
				return this.mapContext.GetNumReduceTasks();
			}

			/// <exception cref="System.TypeLoadException"/>
			public override Type GetOutputFormatClass()
			{
				return this.mapContext.GetOutputFormatClass();
			}

			public override Type GetOutputKeyClass()
			{
				return this.mapContext.GetOutputKeyClass();
			}

			public override Type GetOutputValueClass()
			{
				return this.mapContext.GetOutputValueClass();
			}

			/// <exception cref="System.TypeLoadException"/>
			public override Type GetPartitionerClass()
			{
				return this.mapContext.GetPartitionerClass();
			}

			/// <exception cref="System.TypeLoadException"/>
			public override Type GetReducerClass()
			{
				return this.mapContext.GetReducerClass();
			}

			public override RawComparator<object> GetSortComparator()
			{
				return this.mapContext.GetSortComparator();
			}

			public override bool GetSymlink()
			{
				return this.mapContext.GetSymlink();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path GetWorkingDirectory()
			{
				return this.mapContext.GetWorkingDirectory();
			}

			public override void Progress()
			{
				this.mapContext.Progress();
			}

			public override bool GetProfileEnabled()
			{
				return this.mapContext.GetProfileEnabled();
			}

			public override string GetProfileParams()
			{
				return this.mapContext.GetProfileParams();
			}

			public override Configuration.IntegerRanges GetProfileTaskRange(bool isMap)
			{
				return this.mapContext.GetProfileTaskRange(isMap);
			}

			public override string GetUser()
			{
				return this.mapContext.GetUser();
			}

			public override Credentials GetCredentials()
			{
				return this.mapContext.GetCredentials();
			}

			public override float GetProgress()
			{
				return this.mapContext.GetProgress();
			}

			private readonly WrappedMapper<Keyin, Valuein, Keyout, Valueout> _enclosing;
		}
	}
}
