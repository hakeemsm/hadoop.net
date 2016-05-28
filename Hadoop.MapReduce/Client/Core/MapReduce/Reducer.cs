using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// Reduces a set of intermediate values which share a key to a smaller set of
	/// values.
	/// </summary>
	/// <remarks>
	/// Reduces a set of intermediate values which share a key to a smaller set of
	/// values.
	/// <p><code>Reducer</code> implementations
	/// can access the
	/// <see cref="Org.Apache.Hadoop.Conf.Configuration"/>
	/// for the job via the
	/// <see cref="JobContext.GetConfiguration()"/>
	/// method.</p>
	/// <p><code>Reducer</code> has 3 primary phases:</p>
	/// <ol>
	/// <li>
	/// <b id="Shuffle">Shuffle</b>
	/// <p>The <code>Reducer</code> copies the sorted output from each
	/// <see cref="Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
	/// using HTTP across the network.</p>
	/// </li>
	/// <li>
	/// <b id="Sort">Sort</b>
	/// <p>The framework merge sorts <code>Reducer</code> inputs by
	/// <code>key</code>s
	/// (since different <code>Mapper</code>s may have output the same key).</p>
	/// <p>The shuffle and sort phases occur simultaneously i.e. while outputs are
	/// being fetched they are merged.</p>
	/// <b id="SecondarySort">SecondarySort</b>
	/// <p>To achieve a secondary sort on the values returned by the value
	/// iterator, the application should extend the key with the secondary
	/// key and define a grouping comparator. The keys will be sorted using the
	/// entire key, but will be grouped using the grouping comparator to decide
	/// which keys and values are sent in the same call to reduce.The grouping
	/// comparator is specified via
	/// <see cref="Job.SetGroupingComparatorClass(System.Type{T})"/>
	/// . The sort order is
	/// controlled by
	/// <see cref="Job.SetSortComparatorClass(System.Type{T})"/>
	/// .</p>
	/// For example, say that you want to find duplicate web pages and tag them
	/// all with the url of the "best" known example. You would set up the job
	/// like:
	/// <ul>
	/// <li>Map Input Key: url</li>
	/// <li>Map Input Value: document</li>
	/// <li>Map Output Key: document checksum, url pagerank</li>
	/// <li>Map Output Value: url</li>
	/// <li>Partitioner: by checksum</li>
	/// <li>OutputKeyComparator: by checksum and then decreasing pagerank</li>
	/// <li>OutputValueGroupingComparator: by checksum</li>
	/// </ul>
	/// </li>
	/// <li>
	/// <b id="Reduce">Reduce</b>
	/// <p>In this phase the
	/// <see cref="Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}.Reduce(object, System.Collections.IEnumerable{T}, Context)
	/// 	"/>
	/// method is called for each <code>&lt;key, (collection of values)&gt;</code> in
	/// the sorted inputs.</p>
	/// <p>The output of the reduce task is typically written to a
	/// <see cref="RecordWriter{K, V}"/>
	/// via
	/// <see cref="TaskInputOutputContext{KEYIN, VALUEIN, KEYOUT, VALUEOUT}.Write(object, object)
	/// 	"/>
	/// .</p>
	/// </li>
	/// </ol>
	/// <p>The output of the <code>Reducer</code> is <b>not re-sorted</b>.</p>
	/// <p>Example:</p>
	/// <p><blockquote><pre>
	/// public class IntSumReducer&lt;Key&gt; extends Reducer&lt;Key,IntWritable,
	/// Key,IntWritable&gt; {
	/// private IntWritable result = new IntWritable();
	/// public void reduce(Key key, Iterable&lt;IntWritable&gt; values,
	/// Context context) throws IOException, InterruptedException {
	/// int sum = 0;
	/// for (IntWritable val : values) {
	/// sum += val.get();
	/// }
	/// result.set(sum);
	/// context.write(key, result);
	/// }
	/// }
	/// </pre></blockquote>
	/// </remarks>
	/// <seealso cref="Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
	/// <seealso cref="Partitioner{KEY, VALUE}"/>
	public class Reducer<Keyin, Valuein, Keyout, Valueout>
	{
		/// <summary>
		/// The <code>Context</code> passed on to the
		/// <see cref="Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
		/// implementations.
		/// </summary>
		public abstract class Context : ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
		{
			public abstract Path[] GetArchiveClassPaths();

			public abstract string[] GetArchiveTimestamps();

			public abstract URI[] GetCacheArchives();

			public abstract URI[] GetCacheFiles();

			public abstract Type GetCombinerClass();

			public abstract RawComparator<object> GetCombinerKeyGroupingComparator();

			public abstract Configuration GetConfiguration();

			public abstract Credentials GetCredentials();

			public abstract Path[] GetFileClassPaths();

			public abstract string[] GetFileTimestamps();

			public abstract RawComparator<object> GetGroupingComparator();

			public abstract Type GetInputFormatClass();

			public abstract string GetJar();

			public abstract JobID GetJobID();

			public abstract string GetJobName();

			public abstract bool GetJobSetupCleanupNeeded();

			public abstract Path[] GetLocalCacheArchives();

			public abstract Path[] GetLocalCacheFiles();

			public abstract Type GetMapOutputKeyClass();

			public abstract Type GetMapOutputValueClass();

			public abstract Type GetMapperClass();

			public abstract int GetMaxMapAttempts();

			public abstract int GetMaxReduceAttempts();

			public abstract int GetNumReduceTasks();

			public abstract Type GetOutputFormatClass();

			public abstract Type GetOutputKeyClass();

			public abstract Type GetOutputValueClass();

			public abstract Type GetPartitionerClass();

			public abstract bool GetProfileEnabled();

			public abstract string GetProfileParams();

			public abstract Configuration.IntegerRanges GetProfileTaskRange(bool arg1);

			public abstract Type GetReducerClass();

			public abstract RawComparator<object> GetSortComparator();

			public abstract bool GetSymlink();

			public abstract bool GetTaskCleanupNeeded();

			public abstract string GetUser();

			public abstract Path GetWorkingDirectory();

			public abstract void Progress();

			public abstract Counter GetCounter(Enum<object> arg1);

			public abstract Counter GetCounter(string arg1, string arg2);

			public abstract float GetProgress();

			public abstract string GetStatus();

			public abstract TaskAttemptID GetTaskAttemptID();

			public abstract void SetStatus(string arg1);

			public abstract KEYIN GetCurrentKey();

			public abstract VALUEIN GetCurrentValue();

			public abstract OutputCommitter GetOutputCommitter();

			public abstract bool NextKeyValue();

			public abstract void Write(KEYOUT arg1, VALUEOUT arg2);

			public abstract IEnumerable<VALUEIN> GetValues();

			public abstract bool NextKey();

			internal Context(Reducer<Keyin, Valuein, Keyout, Valueout> _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly Reducer<Keyin, Valuein, Keyout, Valueout> _enclosing;
		}

		/// <summary>Called once at the start of the task.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual void Setup(Reducer.Context context)
		{
		}

		// NOTHING
		/// <summary>This method is called once for each key.</summary>
		/// <remarks>
		/// This method is called once for each key. Most applications will define
		/// their reduce class by overriding this method. The default implementation
		/// is an identity function.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual void Reduce(KEYIN key, IEnumerable<VALUEIN> values, Reducer.Context
			 context)
		{
			foreach (VALUEIN value in values)
			{
				context.Write((KEYOUT)key, (VALUEOUT)value);
			}
		}

		/// <summary>Called once at the end of the task.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		protected internal virtual void Cleanup(Reducer.Context context)
		{
		}

		// NOTHING
		/// <summary>
		/// Advanced application writers can use the
		/// <see cref="Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}.Run(Context)"/>
		/// method to
		/// control how the reduce task works.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void Run(Reducer.Context context)
		{
			Setup(context);
			try
			{
				while (context.NextKey())
				{
					Reduce(context.GetCurrentKey(), context.GetValues(), context);
					// If a back up store is used, reset it
					IEnumerator<VALUEIN> iter = context.GetValues().GetEnumerator();
					if (iter is ReduceContext.ValueIterator)
					{
						((ReduceContext.ValueIterator<VALUEIN>)iter).ResetBackupStore();
					}
				}
			}
			finally
			{
				Cleanup(context);
			}
		}
	}
}
