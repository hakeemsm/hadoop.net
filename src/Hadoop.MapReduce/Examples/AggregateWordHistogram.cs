using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Aggregate;
using Sharpen;

namespace Org.Apache.Hadoop.Examples
{
	/// <summary>This is an example Aggregated Hadoop Map/Reduce application.</summary>
	/// <remarks>
	/// This is an example Aggregated Hadoop Map/Reduce application. Computes the
	/// histogram of the words in the input texts.
	/// To run: bin/hadoop jar hadoop-*-examples.jar aggregatewordhist <i>in-dir</i>
	/// <i>out-dir</i> <i>numOfReducers</i> textinputformat
	/// </remarks>
	public class AggregateWordHistogram
	{
		public class AggregateWordHistogramPlugin : ValueAggregatorBaseDescriptor
		{
			/// <summary>Parse the given value, generate an aggregation-id/value pair per word.</summary>
			/// <remarks>
			/// Parse the given value, generate an aggregation-id/value pair per word.
			/// The ID is of type VALUE_HISTOGRAM, with WORD_HISTOGRAM as the real id.
			/// The value is WORD\t1.
			/// </remarks>
			/// <returns>a list of the generated pairs.</returns>
			public override AList<KeyValuePair<Text, Text>> GenerateKeyValPairs(object key, object
				 val)
			{
				string[] words = val.ToString().Split(" |\t");
				AList<KeyValuePair<Text, Text>> retv = new AList<KeyValuePair<Text, Text>>();
				for (int i = 0; i < words.Length; i++)
				{
					Text valCount = new Text(words[i] + "\t" + "1");
					KeyValuePair<Text, Text> en = GenerateEntry(ValueHistogram, "WORD_HISTOGRAM", valCount
						);
					retv.AddItem(en);
				}
				return retv;
			}
		}

		/// <summary>The main driver for word count map/reduce program.</summary>
		/// <remarks>
		/// The main driver for word count map/reduce program. Invoke this method to
		/// submit the map/reduce job.
		/// </remarks>
		/// <exception cref="System.IO.IOException">When there is communication problems with the job tracker.
		/// 	</exception>
		/// <exception cref="System.Exception"/>
		/// <exception cref="System.TypeLoadException"/>
		public static void Main(string[] args)
		{
			Job job = ValueAggregatorJob.CreateValueAggregatorJob(args, new Type[] { typeof(AggregateWordHistogram.AggregateWordHistogramPlugin
				) });
			job.SetJarByClass(typeof(AggregateWordCount));
			int ret = job.WaitForCompletion(true) ? 0 : 1;
			System.Environment.Exit(ret);
		}
	}
}
