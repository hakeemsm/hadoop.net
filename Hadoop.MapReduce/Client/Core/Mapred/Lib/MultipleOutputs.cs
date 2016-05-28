using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Lib
{
	/// <summary>
	/// The MultipleOutputs class simplifies writting to additional outputs other
	/// than the job default output via the <code>OutputCollector</code> passed to
	/// the <code>map()</code> and <code>reduce()</code> methods of the
	/// <code>Mapper</code> and <code>Reducer</code> implementations.
	/// </summary>
	/// <remarks>
	/// The MultipleOutputs class simplifies writting to additional outputs other
	/// than the job default output via the <code>OutputCollector</code> passed to
	/// the <code>map()</code> and <code>reduce()</code> methods of the
	/// <code>Mapper</code> and <code>Reducer</code> implementations.
	/// <p>
	/// Each additional output, or named output, may be configured with its own
	/// <code>OutputFormat</code>, with its own key class and with its own value
	/// class.
	/// <p>
	/// A named output can be a single file or a multi file. The later is refered as
	/// a multi named output.
	/// <p>
	/// A multi named output is an unbound set of files all sharing the same
	/// <code>OutputFormat</code>, key class and value class configuration.
	/// <p>
	/// When named outputs are used within a <code>Mapper</code> implementation,
	/// key/values written to a name output are not part of the reduce phase, only
	/// key/values written to the job <code>OutputCollector</code> are part of the
	/// reduce phase.
	/// <p>
	/// MultipleOutputs supports counters, by default the are disabled. The counters
	/// group is the
	/// <see cref="MultipleOutputs"/>
	/// class name.
	/// </p>
	/// The names of the counters are the same as the named outputs. For multi
	/// named outputs the name of the counter is the concatenation of the named
	/// output, and underscore '_' and the multiname.
	/// <p>
	/// Job configuration usage pattern is:
	/// <pre>
	/// JobConf conf = new JobConf();
	/// conf.setInputPath(inDir);
	/// FileOutputFormat.setOutputPath(conf, outDir);
	/// conf.setMapperClass(MOMap.class);
	/// conf.setReducerClass(MOReduce.class);
	/// ...
	/// // Defines additional single text based output 'text' for the job
	/// MultipleOutputs.addNamedOutput(conf, "text", TextOutputFormat.class,
	/// LongWritable.class, Text.class);
	/// // Defines additional multi sequencefile based output 'sequence' for the
	/// // job
	/// MultipleOutputs.addMultiNamedOutput(conf, "seq",
	/// SequenceFileOutputFormat.class,
	/// LongWritable.class, Text.class);
	/// ...
	/// JobClient jc = new JobClient();
	/// RunningJob job = jc.submitJob(conf);
	/// ...
	/// </pre>
	/// <p>
	/// Job configuration usage pattern is:
	/// <pre>
	/// public class MOReduce implements
	/// Reducer&lt;WritableComparable, Writable&gt; {
	/// private MultipleOutputs mos;
	/// public void configure(JobConf conf) {
	/// ...
	/// mos = new MultipleOutputs(conf);
	/// }
	/// public void reduce(WritableComparable key, Iterator&lt;Writable&gt; values,
	/// OutputCollector output, Reporter reporter)
	/// throws IOException {
	/// ...
	/// mos.getCollector("text", reporter).collect(key, new Text("Hello"));
	/// mos.getCollector("seq", "A", reporter).collect(key, new Text("Bye"));
	/// mos.getCollector("seq", "B", reporter).collect(key, new Text("Chau"));
	/// ...
	/// }
	/// public void close() throws IOException {
	/// mos.close();
	/// ...
	/// }
	/// }
	/// </pre>
	/// </remarks>
	public class MultipleOutputs
	{
		private const string NamedOutputs = "mo.namedOutputs";

		private const string MoPrefix = "mo.namedOutput.";

		private const string Format = ".format";

		private const string Key = ".key";

		private const string Value = ".value";

		private const string Multi = ".multi";

		private const string CountersEnabled = "mo.counters";

		/// <summary>Counters group used by the counters of MultipleOutputs.</summary>
		private static readonly string CountersGroup = typeof(Org.Apache.Hadoop.Mapred.Lib.MultipleOutputs
			).FullName;

		/// <summary>Checks if a named output is alreadyDefined or not.</summary>
		/// <param name="conf">job conf</param>
		/// <param name="namedOutput">named output names</param>
		/// <param name="alreadyDefined">
		/// whether the existence/non-existence of
		/// the named output is to be checked
		/// </param>
		/// <exception cref="System.ArgumentException">
		/// if the output name is alreadyDefined or
		/// not depending on the value of the
		/// 'alreadyDefined' parameter
		/// </exception>
		private static void CheckNamedOutput(JobConf conf, string namedOutput, bool alreadyDefined
			)
		{
			IList<string> definedChannels = GetNamedOutputsList(conf);
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

		/// <summary>Checks if a named output name is valid.</summary>
		/// <param name="namedOutput">named output Name</param>
		/// <exception cref="System.ArgumentException">if the output name is not valid.</exception>
		private static void CheckNamedOutputName(string namedOutput)
		{
			CheckTokenName(namedOutput);
			// name cannot be the name used for the default output
			if (namedOutput.Equals("part"))
			{
				throw new ArgumentException("Named output name cannot be 'part'");
			}
		}

		/// <summary>Returns list of channel names.</summary>
		/// <param name="conf">job conf</param>
		/// <returns>List of channel Names</returns>
		public static IList<string> GetNamedOutputsList(JobConf conf)
		{
			IList<string> names = new AList<string>();
			StringTokenizer st = new StringTokenizer(conf.Get(NamedOutputs, string.Empty), " "
				);
			while (st.HasMoreTokens())
			{
				names.AddItem(st.NextToken());
			}
			return names;
		}

		/// <summary>Returns if a named output is multiple.</summary>
		/// <param name="conf">job conf</param>
		/// <param name="namedOutput">named output</param>
		/// <returns>
		/// <code>true</code> if the name output is multi, <code>false</code>
		/// if it is single. If the name output is not defined it returns
		/// <code>false</code>
		/// </returns>
		public static bool IsMultiNamedOutput(JobConf conf, string namedOutput)
		{
			CheckNamedOutput(conf, namedOutput, false);
			return conf.GetBoolean(MoPrefix + namedOutput + Multi, false);
		}

		/// <summary>Returns the named output OutputFormat.</summary>
		/// <param name="conf">job conf</param>
		/// <param name="namedOutput">named output</param>
		/// <returns>namedOutput OutputFormat</returns>
		public static Type GetNamedOutputFormatClass(JobConf conf, string namedOutput)
		{
			CheckNamedOutput(conf, namedOutput, false);
			return conf.GetClass<OutputFormat>(MoPrefix + namedOutput + Format, null);
		}

		/// <summary>Returns the key class for a named output.</summary>
		/// <param name="conf">job conf</param>
		/// <param name="namedOutput">named output</param>
		/// <returns>class for the named output key</returns>
		public static Type GetNamedOutputKeyClass(JobConf conf, string namedOutput)
		{
			CheckNamedOutput(conf, namedOutput, false);
			return conf.GetClass<object>(MoPrefix + namedOutput + Key, null);
		}

		/// <summary>Returns the value class for a named output.</summary>
		/// <param name="conf">job conf</param>
		/// <param name="namedOutput">named output</param>
		/// <returns>class of named output value</returns>
		public static Type GetNamedOutputValueClass(JobConf conf, string namedOutput)
		{
			CheckNamedOutput(conf, namedOutput, false);
			return conf.GetClass<object>(MoPrefix + namedOutput + Value, null);
		}

		/// <summary>Adds a named output for the job.</summary>
		/// <param name="conf">job conf to add the named output</param>
		/// <param name="namedOutput">
		/// named output name, it has to be a word, letters
		/// and numbers only, cannot be the word 'part' as
		/// that is reserved for the
		/// default output.
		/// </param>
		/// <param name="outputFormatClass">OutputFormat class.</param>
		/// <param name="keyClass">key class</param>
		/// <param name="valueClass">value class</param>
		public static void AddNamedOutput(JobConf conf, string namedOutput, Type outputFormatClass
			, Type keyClass, Type valueClass)
		{
			AddNamedOutput(conf, namedOutput, false, outputFormatClass, keyClass, valueClass);
		}

		/// <summary>Adds a multi named output for the job.</summary>
		/// <param name="conf">job conf to add the named output</param>
		/// <param name="namedOutput">
		/// named output name, it has to be a word, letters
		/// and numbers only, cannot be the word 'part' as
		/// that is reserved for the
		/// default output.
		/// </param>
		/// <param name="outputFormatClass">OutputFormat class.</param>
		/// <param name="keyClass">key class</param>
		/// <param name="valueClass">value class</param>
		public static void AddMultiNamedOutput(JobConf conf, string namedOutput, Type outputFormatClass
			, Type keyClass, Type valueClass)
		{
			AddNamedOutput(conf, namedOutput, true, outputFormatClass, keyClass, valueClass);
		}

		/// <summary>Adds a named output for the job.</summary>
		/// <param name="conf">job conf to add the named output</param>
		/// <param name="namedOutput">
		/// named output name, it has to be a word, letters
		/// and numbers only, cannot be the word 'part' as
		/// that is reserved for the
		/// default output.
		/// </param>
		/// <param name="multi">indicates if the named output is multi</param>
		/// <param name="outputFormatClass">OutputFormat class.</param>
		/// <param name="keyClass">key class</param>
		/// <param name="valueClass">value class</param>
		private static void AddNamedOutput(JobConf conf, string namedOutput, bool multi, 
			Type outputFormatClass, Type keyClass, Type valueClass)
		{
			CheckNamedOutputName(namedOutput);
			CheckNamedOutput(conf, namedOutput, true);
			conf.Set(NamedOutputs, conf.Get(NamedOutputs, string.Empty) + " " + namedOutput);
			conf.SetClass(MoPrefix + namedOutput + Format, outputFormatClass, typeof(OutputFormat
				));
			conf.SetClass(MoPrefix + namedOutput + Key, keyClass, typeof(object));
			conf.SetClass(MoPrefix + namedOutput + Value, valueClass, typeof(object));
			conf.SetBoolean(MoPrefix + namedOutput + Multi, multi);
		}

		/// <summary>Enables or disables counters for the named outputs.</summary>
		/// <remarks>
		/// Enables or disables counters for the named outputs.
		/// <p>
		/// By default these counters are disabled.
		/// <p>
		/// MultipleOutputs supports counters, by default the are disabled.
		/// The counters group is the
		/// <see cref="MultipleOutputs"/>
		/// class name.
		/// </p>
		/// The names of the counters are the same as the named outputs. For multi
		/// named outputs the name of the counter is the concatenation of the named
		/// output, and underscore '_' and the multiname.
		/// </remarks>
		/// <param name="conf">job conf to enableadd the named output.</param>
		/// <param name="enabled">indicates if the counters will be enabled or not.</param>
		public static void SetCountersEnabled(JobConf conf, bool enabled)
		{
			conf.SetBoolean(CountersEnabled, enabled);
		}

		/// <summary>Returns if the counters for the named outputs are enabled or not.</summary>
		/// <remarks>
		/// Returns if the counters for the named outputs are enabled or not.
		/// <p>
		/// By default these counters are disabled.
		/// <p>
		/// MultipleOutputs supports counters, by default the are disabled.
		/// The counters group is the
		/// <see cref="MultipleOutputs"/>
		/// class name.
		/// </p>
		/// The names of the counters are the same as the named outputs. For multi
		/// named outputs the name of the counter is the concatenation of the named
		/// output, and underscore '_' and the multiname.
		/// </remarks>
		/// <param name="conf">job conf to enableadd the named output.</param>
		/// <returns>TRUE if the counters are enabled, FALSE if they are disabled.</returns>
		public static bool GetCountersEnabled(JobConf conf)
		{
			return conf.GetBoolean(CountersEnabled, false);
		}

		private JobConf conf;

		private OutputFormat outputFormat;

		private ICollection<string> namedOutputs;

		private IDictionary<string, RecordWriter> recordWriters;

		private bool countersEnabled;

		/// <summary>
		/// Creates and initializes multiple named outputs support, it should be
		/// instantiated in the Mapper/Reducer configure method.
		/// </summary>
		/// <param name="job">the job configuration object</param>
		public MultipleOutputs(JobConf job)
		{
			// instance code, to be used from Mapper/Reducer code
			this.conf = job;
			outputFormat = new MultipleOutputs.InternalFileOutputFormat();
			namedOutputs = Sharpen.Collections.UnmodifiableSet(new HashSet<string>(Org.Apache.Hadoop.Mapred.Lib.MultipleOutputs
				.GetNamedOutputsList(job)));
			recordWriters = new Dictionary<string, RecordWriter>();
			countersEnabled = GetCountersEnabled(job);
		}

		/// <summary>Returns iterator with the defined name outputs.</summary>
		/// <returns>iterator with the defined named outputs</returns>
		public virtual IEnumerator<string> GetNamedOutputs()
		{
			return namedOutputs.GetEnumerator();
		}

		// by being synchronized MultipleOutputTask can be use with a
		// MultithreaderMapRunner.
		/// <exception cref="System.IO.IOException"/>
		private RecordWriter GetRecordWriter(string namedOutput, string baseFileName, Reporter
			 reporter)
		{
			lock (this)
			{
				RecordWriter writer = recordWriters[baseFileName];
				if (writer == null)
				{
					if (countersEnabled && reporter == null)
					{
						throw new ArgumentException("Counters are enabled, Reporter cannot be NULL");
					}
					JobConf jobConf = new JobConf(conf);
					jobConf.Set(MultipleOutputs.InternalFileOutputFormat.ConfigNamedOutput, namedOutput
						);
					FileSystem fs = FileSystem.Get(conf);
					writer = outputFormat.GetRecordWriter(fs, jobConf, baseFileName, reporter);
					if (countersEnabled)
					{
						if (reporter == null)
						{
							throw new ArgumentException("Counters are enabled, Reporter cannot be NULL");
						}
						writer = new MultipleOutputs.RecordWriterWithCounter(writer, baseFileName, reporter
							);
					}
					recordWriters[baseFileName] = writer;
				}
				return writer;
			}
		}

		private class RecordWriterWithCounter : RecordWriter
		{
			private RecordWriter writer;

			private string counterName;

			private Reporter reporter;

			public RecordWriterWithCounter(RecordWriter writer, string counterName, Reporter 
				reporter)
			{
				this.writer = writer;
				this.counterName = counterName;
				this.reporter = reporter;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Write(object key, object value)
			{
				reporter.IncrCounter(CountersGroup, counterName, 1);
				writer.Write(key, value);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close(Reporter reporter)
			{
				writer.Close(reporter);
			}
		}

		/// <summary>Gets the output collector for a named output.</summary>
		/// <param name="namedOutput">the named output name</param>
		/// <param name="reporter">the reporter</param>
		/// <returns>the output collector for the given named output</returns>
		/// <exception cref="System.IO.IOException">thrown if output collector could not be created
		/// 	</exception>
		public virtual OutputCollector GetCollector(string namedOutput, Reporter reporter
			)
		{
			return GetCollector(namedOutput, null, reporter);
		}

		/// <summary>Gets the output collector for a multi named output.</summary>
		/// <param name="namedOutput">the named output name</param>
		/// <param name="multiName">the multi name part</param>
		/// <param name="reporter">the reporter</param>
		/// <returns>the output collector for the given named output</returns>
		/// <exception cref="System.IO.IOException">thrown if output collector could not be created
		/// 	</exception>
		public virtual OutputCollector GetCollector(string namedOutput, string multiName, 
			Reporter reporter)
		{
			CheckNamedOutputName(namedOutput);
			if (!namedOutputs.Contains(namedOutput))
			{
				throw new ArgumentException("Undefined named output '" + namedOutput + "'");
			}
			bool multi = IsMultiNamedOutput(conf, namedOutput);
			if (!multi && multiName != null)
			{
				throw new ArgumentException("Name output '" + namedOutput + "' has not been defined as multi"
					);
			}
			if (multi)
			{
				CheckTokenName(multiName);
			}
			string baseFileName = (multi) ? namedOutput + "_" + multiName : namedOutput;
			RecordWriter writer = GetRecordWriter(namedOutput, baseFileName, reporter);
			return new _OutputCollector_511(writer);
		}

		private sealed class _OutputCollector_511 : OutputCollector
		{
			public _OutputCollector_511(RecordWriter writer)
			{
				this.writer = writer;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Collect(object key, object value)
			{
				writer.Write(key, value);
			}

			private readonly RecordWriter writer;
		}

		/// <summary>Closes all the opened named outputs.</summary>
		/// <remarks>
		/// Closes all the opened named outputs.
		/// <p>
		/// If overriden subclasses must invoke <code>super.close()</code> at the
		/// end of their <code>close()</code>
		/// </remarks>
		/// <exception cref="System.IO.IOException">
		/// thrown if any of the MultipleOutput files
		/// could not be closed properly.
		/// </exception>
		public virtual void Close()
		{
			foreach (RecordWriter writer in recordWriters.Values)
			{
				writer.Close(null);
			}
		}

		private class InternalFileOutputFormat : FileOutputFormat<object, object>
		{
			public const string ConfigNamedOutput = "mo.config.namedOutput";

			/// <exception cref="System.IO.IOException"/>
			public override RecordWriter<object, object> GetRecordWriter(FileSystem fs, JobConf
				 job, string baseFileName, Progressable progress)
			{
				string nameOutput = job.Get(ConfigNamedOutput, null);
				string fileName = GetUniqueName(job, baseFileName);
				// The following trick leverages the instantiation of a record writer via
				// the job conf thus supporting arbitrary output formats.
				JobConf outputConf = new JobConf(job);
				outputConf.SetOutputFormat(GetNamedOutputFormatClass(job, nameOutput));
				outputConf.SetOutputKeyClass(GetNamedOutputKeyClass(job, nameOutput));
				outputConf.SetOutputValueClass(GetNamedOutputValueClass(job, nameOutput));
				OutputFormat outputFormat = outputConf.GetOutputFormat();
				return outputFormat.GetRecordWriter(fs, outputConf, fileName, progress);
			}
		}
	}
}
