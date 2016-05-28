using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Reduce
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"
	/// 	/>
	/// which wraps a given one to allow for custom
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Reducer.Context"/>
	/// implementations.
	/// </summary>
	public class WrappedReducer<Keyin, Valuein, Keyout, Valueout> : Reducer<KEYIN, VALUEIN
		, KEYOUT, VALUEOUT>
	{
		/// <summary>
		/// A a wrapped
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Reducer.Context"/>
		/// for custom implementations.
		/// </summary>
		/// <param name="reduceContext"><code>ReduceContext</code> to be wrapped</param>
		/// <returns>a wrapped <code>Reducer.Context</code> for custom implementations</returns>
		public virtual Reducer.Context GetReducerContext(ReduceContext<KEYIN, VALUEIN, KEYOUT
			, VALUEOUT> reduceContext)
		{
			return new WrappedReducer.Context(this, reduceContext);
		}

		public class Context : Reducer.Context
		{
			protected internal ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext;

			public Context(WrappedReducer<Keyin, Valuein, Keyout, Valueout> _enclosing, ReduceContext
				<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reduceContext)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.reduceContext = reduceContext;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override KEYIN GetCurrentKey()
			{
				return this.reduceContext.GetCurrentKey();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override VALUEIN GetCurrentValue()
			{
				return this.reduceContext.GetCurrentValue();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override bool NextKeyValue()
			{
				return this.reduceContext.NextKeyValue();
			}

			public override Counter GetCounter(Enum counterName)
			{
				return this.reduceContext.GetCounter(counterName);
			}

			public override Counter GetCounter(string groupName, string counterName)
			{
				return this.reduceContext.GetCounter(groupName, counterName);
			}

			public override OutputCommitter GetOutputCommitter()
			{
				return this.reduceContext.GetOutputCommitter();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Write(KEYOUT key, VALUEOUT value)
			{
				this.reduceContext.Write(key, value);
			}

			public override string GetStatus()
			{
				return this.reduceContext.GetStatus();
			}

			public override TaskAttemptID GetTaskAttemptID()
			{
				return this.reduceContext.GetTaskAttemptID();
			}

			public override void SetStatus(string msg)
			{
				this.reduceContext.SetStatus(msg);
			}

			public override Path[] GetArchiveClassPaths()
			{
				return this.reduceContext.GetArchiveClassPaths();
			}

			public override string[] GetArchiveTimestamps()
			{
				return this.reduceContext.GetArchiveTimestamps();
			}

			/// <exception cref="System.IO.IOException"/>
			public override URI[] GetCacheArchives()
			{
				return this.reduceContext.GetCacheArchives();
			}

			/// <exception cref="System.IO.IOException"/>
			public override URI[] GetCacheFiles()
			{
				return this.reduceContext.GetCacheFiles();
			}

			/// <exception cref="System.TypeLoadException"/>
			public override Type GetCombinerClass()
			{
				return this.reduceContext.GetCombinerClass();
			}

			public override Configuration GetConfiguration()
			{
				return this.reduceContext.GetConfiguration();
			}

			public override Path[] GetFileClassPaths()
			{
				return this.reduceContext.GetFileClassPaths();
			}

			public override string[] GetFileTimestamps()
			{
				return this.reduceContext.GetFileTimestamps();
			}

			public override RawComparator<object> GetCombinerKeyGroupingComparator()
			{
				return this.reduceContext.GetCombinerKeyGroupingComparator();
			}

			public override RawComparator<object> GetGroupingComparator()
			{
				return this.reduceContext.GetGroupingComparator();
			}

			/// <exception cref="System.TypeLoadException"/>
			public override Type GetInputFormatClass()
			{
				return this.reduceContext.GetInputFormatClass();
			}

			public override string GetJar()
			{
				return this.reduceContext.GetJar();
			}

			public override JobID GetJobID()
			{
				return this.reduceContext.GetJobID();
			}

			public override string GetJobName()
			{
				return this.reduceContext.GetJobName();
			}

			public override bool GetJobSetupCleanupNeeded()
			{
				return this.reduceContext.GetJobSetupCleanupNeeded();
			}

			public override bool GetTaskCleanupNeeded()
			{
				return this.reduceContext.GetTaskCleanupNeeded();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path[] GetLocalCacheArchives()
			{
				return this.reduceContext.GetLocalCacheArchives();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path[] GetLocalCacheFiles()
			{
				return this.reduceContext.GetLocalCacheFiles();
			}

			public override Type GetMapOutputKeyClass()
			{
				return this.reduceContext.GetMapOutputKeyClass();
			}

			public override Type GetMapOutputValueClass()
			{
				return this.reduceContext.GetMapOutputValueClass();
			}

			/// <exception cref="System.TypeLoadException"/>
			public override Type GetMapperClass()
			{
				return this.reduceContext.GetMapperClass();
			}

			public override int GetMaxMapAttempts()
			{
				return this.reduceContext.GetMaxMapAttempts();
			}

			public override int GetMaxReduceAttempts()
			{
				return this.reduceContext.GetMaxReduceAttempts();
			}

			public override int GetNumReduceTasks()
			{
				return this.reduceContext.GetNumReduceTasks();
			}

			/// <exception cref="System.TypeLoadException"/>
			public override Type GetOutputFormatClass()
			{
				return this.reduceContext.GetOutputFormatClass();
			}

			public override Type GetOutputKeyClass()
			{
				return this.reduceContext.GetOutputKeyClass();
			}

			public override Type GetOutputValueClass()
			{
				return this.reduceContext.GetOutputValueClass();
			}

			/// <exception cref="System.TypeLoadException"/>
			public override Type GetPartitionerClass()
			{
				return this.reduceContext.GetPartitionerClass();
			}

			/// <exception cref="System.TypeLoadException"/>
			public override Type GetReducerClass()
			{
				return this.reduceContext.GetReducerClass();
			}

			public override RawComparator<object> GetSortComparator()
			{
				return this.reduceContext.GetSortComparator();
			}

			public override bool GetSymlink()
			{
				return this.reduceContext.GetSymlink();
			}

			/// <exception cref="System.IO.IOException"/>
			public override Path GetWorkingDirectory()
			{
				return this.reduceContext.GetWorkingDirectory();
			}

			public override void Progress()
			{
				this.reduceContext.Progress();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override IEnumerable<VALUEIN> GetValues()
			{
				return this.reduceContext.GetValues();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override bool NextKey()
			{
				return this.reduceContext.NextKey();
			}

			public override bool GetProfileEnabled()
			{
				return this.reduceContext.GetProfileEnabled();
			}

			public override string GetProfileParams()
			{
				return this.reduceContext.GetProfileParams();
			}

			public override Configuration.IntegerRanges GetProfileTaskRange(bool isMap)
			{
				return this.reduceContext.GetProfileTaskRange(isMap);
			}

			public override string GetUser()
			{
				return this.reduceContext.GetUser();
			}

			public override Credentials GetCredentials()
			{
				return this.reduceContext.GetCredentials();
			}

			public override float GetProgress()
			{
				return this.reduceContext.GetProgress();
			}

			private readonly WrappedReducer<Keyin, Valuein, Keyout, Valueout> _enclosing;
		}
	}
}
