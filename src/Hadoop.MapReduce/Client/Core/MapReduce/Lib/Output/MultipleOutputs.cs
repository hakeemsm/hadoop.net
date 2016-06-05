using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Task;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Output
{
	/// <summary>
	/// The MultipleOutputs class simplifies writing output data
	/// to multiple outputs
	/// <p>
	/// Case one: writing to additional outputs other than the job default output.
	/// </summary>
	/// <remarks>
	/// The MultipleOutputs class simplifies writing output data
	/// to multiple outputs
	/// <p>
	/// Case one: writing to additional outputs other than the job default output.
	/// Each additional output, or named output, may be configured with its own
	/// <code>OutputFormat</code>, with its own key class and with its own value
	/// class.
	/// </p>
	/// <p>
	/// Case two: to write data to different files provided by user
	/// </p>
	/// <p>
	/// MultipleOutputs supports counters, by default they are disabled. The
	/// counters group is the
	/// <see cref="MultipleOutputs{KEYOUT, VALUEOUT}"/>
	/// class name. The names of the
	/// counters are the same as the output name. These count the number records
	/// written to each output name.
	/// </p>
	/// Usage pattern for job submission:
	/// <pre>
	/// Job job = new Job();
	/// FileInputFormat.setInputPath(job, inDir);
	/// FileOutputFormat.setOutputPath(job, outDir);
	/// job.setMapperClass(MOMap.class);
	/// job.setReducerClass(MOReduce.class);
	/// ...
	/// // Defines additional single text based output 'text' for the job
	/// MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class,
	/// LongWritable.class, Text.class);
	/// // Defines additional sequence-file based output 'sequence' for the job
	/// MultipleOutputs.addNamedOutput(job, "seq",
	/// SequenceFileOutputFormat.class,
	/// LongWritable.class, Text.class);
	/// ...
	/// job.waitForCompletion(true);
	/// ...
	/// </pre>
	/// <p>
	/// Usage in Reducer:
	/// <pre>
	/// &lt;K, V&gt; String generateFileName(K k, V v) {
	/// return k.toString() + "_" + v.toString();
	/// }
	/// public class MOReduce extends
	/// Reducer&lt;WritableComparable, Writable,WritableComparable, Writable&gt; {
	/// private MultipleOutputs mos;
	/// public void setup(Context context) {
	/// ...
	/// mos = new MultipleOutputs(context);
	/// }
	/// public void reduce(WritableComparable key, Iterator&lt;Writable&gt; values,
	/// Context context)
	/// throws IOException {
	/// ...
	/// mos.write("text", , key, new Text("Hello"));
	/// mos.write("seq", LongWritable(1), new Text("Bye"), "seq_a");
	/// mos.write("seq", LongWritable(2), key, new Text("Chau"), "seq_b");
	/// mos.write(key, new Text("value"), generateFileName(key, new Text("value")));
	/// ...
	/// }
	/// public void cleanup(Context) throws IOException {
	/// mos.close();
	/// ...
	/// }
	/// }
	/// </pre>
	/// <p>
	/// When used in conjuction with org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat,
	/// MultipleOutputs can mimic the behaviour of MultipleTextOutputFormat and MultipleSequenceFileOutputFormat
	/// from the old Hadoop API - ie, output can be written from the Reducer to more than one location.
	/// </p>
	/// <p>
	/// Use <code>MultipleOutputs.write(KEYOUT key, VALUEOUT value, String baseOutputPath)</code> to write key and
	/// value to a path specified by <code>baseOutputPath</code>, with no need to specify a named output:
	/// </p>
	/// <pre>
	/// private MultipleOutputs&lt;Text, Text&gt; out;
	/// public void setup(Context context) {
	/// out = new MultipleOutputs&lt;Text, Text&gt;(context);
	/// ...
	/// }
	/// public void reduce(Text key, Iterable&lt;Text&gt; values, Context context) throws IOException, InterruptedException {
	/// for (Text t : values) {
	/// out.write(key, t, generateFileName(&lt;<i>parameter list...</i>&gt;));
	/// }
	/// }
	/// protected void cleanup(Context context) throws IOException, InterruptedException {
	/// out.close();
	/// }
	/// </pre>
	/// <p>
	/// Use your own code in <code>generateFileName()</code> to create a custom path to your results.
	/// '/' characters in <code>baseOutputPath</code> will be translated into directory levels in your file system.
	/// Also, append your custom-generated path with "part" or similar, otherwise your output will be -00000, -00001 etc.
	/// No call to <code>context.write()</code> is necessary. See example <code>generateFileName()</code> code below.
	/// </p>
	/// <pre>
	/// private String generateFileName(Text k) {
	/// // expect Text k in format "Surname|Forename"
	/// String[] kStr = k.toString().split("\\|");
	/// String sName = kStr[0];
	/// String fName = kStr[1];
	/// // example for k = Smith|John
	/// // output written to /user/hadoop/path/to/output/Smith/John-r-00000 (etc)
	/// return sName + "/" + fName;
	/// }
	/// </pre>
	/// <p>
	/// Using MultipleOutputs in this way will still create zero-sized default output, eg part-00000.
	/// To prevent this use <code>LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);</code>
	/// instead of <code>job.setOutputFormatClass(TextOutputFormat.class);</code> in your Hadoop job configuration.
	/// </p>
	/// </remarks>
	public class MultipleOutputs<Keyout, Valueout>
	{
		private const string MultipleOutputs = "mapreduce.multipleoutputs";

		private const string MoPrefix = "mapreduce.multipleoutputs.namedOutput.";

		private const string Format = ".format";

		private const string Key = ".key";

		private const string Value = ".value";

		private const string CountersEnabled = "mapreduce.multipleoutputs.counters";

		/// <summary>Counters group used by the counters of MultipleOutputs.</summary>
		private static readonly string CountersGroup = typeof(Org.Apache.Hadoop.Mapreduce.Lib.Output.MultipleOutputs
			).FullName;

		/// <summary>Cache for the taskContexts</summary>
		private IDictionary<string, TaskAttemptContext> taskContexts = new Dictionary<string
			, TaskAttemptContext>();

		/// <summary>Cached TaskAttemptContext which uses the job's configured settings</summary>
		private TaskAttemptContext jobOutputFormatContext;

		/// <summary>Checks if a named output name is valid token.</summary>
		/// <param name="namedOutput">named output Name</param>
		/// <exception cref="System.ArgumentException">if the output name is not valid.</exception>
		private static void CheckTokenName(string namedOutput)
		{
			if (namedOutput == null || namedOutput.Length == 0)
			{
				throw new ArgumentException("Name cannot be NULL or emtpy");
			}
			foreach (char ch in namedOutput.ToCharArray())
			{
				if ((ch >= 'A') && (ch <= 'Z'))
				{
					continue;
				}
				if ((ch >= 'a') && (ch <= 'z'))
				{
					continue;
				}
				if ((ch >= '0') && (ch <= '9'))
				{
					continue;
				}
				throw new ArgumentException("Name cannot be have a '" + ch + "' char");
			}
		}

		/// <summary>Checks if output name is valid.</summary>
		/// <remarks>
		/// Checks if output name is valid.
		/// name cannot be the name used for the default output
		/// </remarks>
		/// <param name="outputPath">base output Name</param>
		/// <exception cref="System.ArgumentException">if the output name is not valid.</exception>
		private static void CheckBaseOutputPath(string outputPath)
		{
			if (outputPath.Equals(FileOutputFormat.Part))
			{
				throw new ArgumentException("output name cannot be 'part'");
			}
		}

		/// <summary>Checks if a named output name is valid.</summary>
		/// <param name="namedOutput">named output Name</param>
		/// <exception cref="System.ArgumentException">if the output name is not valid.</exception>
		private static void CheckNamedOutputName(JobContext job, string namedOutput, bool
			 alreadyDefined)
		{
			CheckTokenName(namedOutput);
			CheckBaseOutputPath(namedOutput);
			IList<string> definedChannels = GetNamedOutputsList(job);
			if (alreadyDefined && definedChannels.Contains(namedOutput))
			{
				throw new ArgumentException("Named output '" + namedOutput + "' already alreadyDefined"
					);
			}
			else
			{
				if (!alreadyDefined && !definedChannels.Contains(namedOutput))
				{
					throw new ArgumentException("Named output '" + namedOutput + "' not defined");
				}
			}
		}

		// Returns list of channel names.
		private static IList<string> GetNamedOutputsList(JobContext job)
		{
			IList<string> names = new AList<string>();
			StringTokenizer st = new StringTokenizer(job.GetConfiguration().Get(MultipleOutputs
				, string.Empty), " ");
			while (st.HasMoreTokens())
			{
				names.AddItem(st.NextToken());
			}
			return names;
		}

		// Returns the named output OutputFormat.
		private static Type GetNamedOutputFormatClass(JobContext job, string namedOutput)
		{
			return (Type)job.GetConfiguration().GetClass<OutputFormat>(MoPrefix + namedOutput
				 + Format, null);
		}

		// Returns the key class for a named output.
		private static Type GetNamedOutputKeyClass(JobContext job, string namedOutput)
		{
			return job.GetConfiguration().GetClass<object>(MoPrefix + namedOutput + Key, null
				);
		}

		// Returns the value class for a named output.
		private static Type GetNamedOutputValueClass(JobContext job, string namedOutput)
		{
			return job.GetConfiguration().GetClass<object>(MoPrefix + namedOutput + Value, null
				);
		}

		/// <summary>Adds a named output for the job.</summary>
		/// <param name="job">job to add the named output</param>
		/// <param name="namedOutput">
		/// named output name, it has to be a word, letters
		/// and numbers only, cannot be the word 'part' as
		/// that is reserved for the default output.
		/// </param>
		/// <param name="outputFormatClass">OutputFormat class.</param>
		/// <param name="keyClass">key class</param>
		/// <param name="valueClass">value class</param>
		public static void AddNamedOutput(Job job, string namedOutput, Type outputFormatClass
			, Type keyClass, Type valueClass)
		{
			CheckNamedOutputName(job, namedOutput, true);
			Configuration conf = job.GetConfiguration();
			conf.Set(MultipleOutputs, conf.Get(MultipleOutputs, string.Empty) + " " + namedOutput
				);
			conf.SetClass(MoPrefix + namedOutput + Format, outputFormatClass, typeof(OutputFormat
				));
			conf.SetClass(MoPrefix + namedOutput + Key, keyClass, typeof(object));
			conf.SetClass(MoPrefix + namedOutput + Value, valueClass, typeof(object));
		}

		/// <summary>Enables or disables counters for the named outputs.</summary>
		/// <remarks>
		/// Enables or disables counters for the named outputs.
		/// The counters group is the
		/// <see cref="MultipleOutputs{KEYOUT, VALUEOUT}"/>
		/// class name.
		/// The names of the counters are the same as the named outputs. These
		/// counters count the number records written to each output name.
		/// By default these counters are disabled.
		/// </remarks>
		/// <param name="job">job  to enable counters</param>
		/// <param name="enabled">indicates if the counters will be enabled or not.</param>
		public static void SetCountersEnabled(Job job, bool enabled)
		{
			job.GetConfiguration().SetBoolean(CountersEnabled, enabled);
		}

		/// <summary>Returns if the counters for the named outputs are enabled or not.</summary>
		/// <remarks>
		/// Returns if the counters for the named outputs are enabled or not.
		/// By default these counters are disabled.
		/// </remarks>
		/// <param name="job">the job</param>
		/// <returns>TRUE if the counters are enabled, FALSE if they are disabled.</returns>
		public static bool GetCountersEnabled(JobContext job)
		{
			return job.GetConfiguration().GetBoolean(CountersEnabled, false);
		}

		/// <summary>Wraps RecordWriter to increment counters.</summary>
		private class RecordWriterWithCounter : RecordWriter
		{
			private RecordWriter writer;

			private string counterName;

			private TaskInputOutputContext context;

			public RecordWriterWithCounter(RecordWriter writer, string counterName, TaskInputOutputContext
				 context)
			{
				this.writer = writer;
				this.counterName = counterName;
				this.context = context;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Write(object key, object value)
			{
				context.GetCounter(CountersGroup, counterName).Increment(1);
				writer.Write(key, value);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Close(TaskAttemptContext context)
			{
				writer.Close(context);
			}
		}

		private TaskInputOutputContext<object, object, KEYOUT, VALUEOUT> context;

		private ICollection<string> namedOutputs;

		private IDictionary<string, RecordWriter<object, object>> recordWriters;

		private bool countersEnabled;

		/// <summary>
		/// Creates and initializes multiple outputs support,
		/// it should be instantiated in the Mapper/Reducer setup method.
		/// </summary>
		/// <param name="context">the TaskInputOutputContext object</param>
		public MultipleOutputs(TaskInputOutputContext<object, object, KEYOUT, VALUEOUT> context
			)
		{
			// instance code, to be used from Mapper/Reducer code
			this.context = context;
			namedOutputs = Sharpen.Collections.UnmodifiableSet(new HashSet<string>(MultipleOutputs
				.GetNamedOutputsList(context)));
			recordWriters = new Dictionary<string, RecordWriter<object, object>>();
			countersEnabled = GetCountersEnabled(context);
		}

		/// <summary>Write key and value to the namedOutput.</summary>
		/// <remarks>
		/// Write key and value to the namedOutput.
		/// Output path is a unique file generated for the namedOutput.
		/// For example, {namedOutput}-(m|r)-{part-number}
		/// </remarks>
		/// <param name="namedOutput">the named output name</param>
		/// <param name="key">the key</param>
		/// <param name="value">the value</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void Write<K, V>(string namedOutput, K key, V value)
		{
			Write(namedOutput, key, value, namedOutput);
		}

		/// <summary>Write key and value to baseOutputPath using the namedOutput.</summary>
		/// <param name="namedOutput">the named output name</param>
		/// <param name="key">the key</param>
		/// <param name="value">the value</param>
		/// <param name="baseOutputPath">
		/// base-output path to write the record to.
		/// Note: Framework will generate unique filename for the baseOutputPath
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void Write<K, V>(string namedOutput, K key, V value, string baseOutputPath
			)
		{
			CheckNamedOutputName(context, namedOutput, false);
			CheckBaseOutputPath(baseOutputPath);
			if (!namedOutputs.Contains(namedOutput))
			{
				throw new ArgumentException("Undefined named output '" + namedOutput + "'");
			}
			TaskAttemptContext taskContext = GetContext(namedOutput);
			GetRecordWriter(taskContext, baseOutputPath).Write(key, value);
		}

		/// <summary>Write key value to an output file name.</summary>
		/// <remarks>
		/// Write key value to an output file name.
		/// Gets the record writer from job's output format.
		/// Job's output format should be a FileOutputFormat.
		/// </remarks>
		/// <param name="key">the key</param>
		/// <param name="value">the value</param>
		/// <param name="baseOutputPath">
		/// base-output path to write the record to.
		/// Note: Framework will generate unique filename for the baseOutputPath
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void Write(KEYOUT key, VALUEOUT value, string baseOutputPath)
		{
			CheckBaseOutputPath(baseOutputPath);
			if (jobOutputFormatContext == null)
			{
				jobOutputFormatContext = new TaskAttemptContextImpl(context.GetConfiguration(), context
					.GetTaskAttemptID(), new MultipleOutputs.WrappedStatusReporter(context));
			}
			GetRecordWriter(jobOutputFormatContext, baseOutputPath).Write(key, value);
		}

		// by being synchronized MultipleOutputTask can be use with a
		// MultithreadedMapper.
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private RecordWriter GetRecordWriter(TaskAttemptContext taskContext, string baseFileName
			)
		{
			lock (this)
			{
				// look for record-writer in the cache
				RecordWriter writer = recordWriters[baseFileName];
				// If not in cache, create a new one
				if (writer == null)
				{
					// get the record writer from context output format
					FileOutputFormat.SetOutputName(taskContext, baseFileName);
					try
					{
						writer = ((OutputFormat)ReflectionUtils.NewInstance(taskContext.GetOutputFormatClass
							(), taskContext.GetConfiguration())).GetRecordWriter(taskContext);
					}
					catch (TypeLoadException e)
					{
						throw new IOException(e);
					}
					// if counters are enabled, wrap the writer with context 
					// to increment counters 
					if (countersEnabled)
					{
						writer = new MultipleOutputs.RecordWriterWithCounter(writer, baseFileName, context
							);
					}
					// add the record-writer to the cache
					recordWriters[baseFileName] = writer;
				}
				return writer;
			}
		}

		// Create a taskAttemptContext for the named output with 
		// output format and output key/value types put in the context
		/// <exception cref="System.IO.IOException"/>
		private TaskAttemptContext GetContext(string nameOutput)
		{
			TaskAttemptContext taskContext = taskContexts[nameOutput];
			if (taskContext != null)
			{
				return taskContext;
			}
			// The following trick leverages the instantiation of a record writer via
			// the job thus supporting arbitrary output formats.
			Job job = Job.GetInstance(context.GetConfiguration());
			job.SetOutputFormatClass(GetNamedOutputFormatClass(context, nameOutput));
			job.SetOutputKeyClass(GetNamedOutputKeyClass(context, nameOutput));
			job.SetOutputValueClass(GetNamedOutputValueClass(context, nameOutput));
			taskContext = new TaskAttemptContextImpl(job.GetConfiguration(), context.GetTaskAttemptID
				(), new MultipleOutputs.WrappedStatusReporter(context));
			taskContexts[nameOutput] = taskContext;
			return taskContext;
		}

		private class WrappedStatusReporter : StatusReporter
		{
			internal TaskAttemptContext context;

			public WrappedStatusReporter(TaskAttemptContext context)
			{
				this.context = context;
			}

			public override Counter GetCounter<_T0>(Enum<_T0> name)
			{
				return context.GetCounter(name);
			}

			public override Counter GetCounter(string group, string name)
			{
				return context.GetCounter(group, name);
			}

			public override void Progress()
			{
				context.Progress();
			}

			public override float GetProgress()
			{
				return context.GetProgress();
			}

			public override void SetStatus(string status)
			{
				context.SetStatus(status);
			}
		}

		/// <summary>Closes all the opened outputs.</summary>
		/// <remarks>
		/// Closes all the opened outputs.
		/// This should be called from cleanup method of map/reduce task.
		/// If overridden subclasses must invoke <code>super.close()</code> at the
		/// end of their <code>close()</code>
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void Close()
		{
			foreach (RecordWriter writer in recordWriters.Values)
			{
				writer.Close(context);
			}
		}
	}
}
