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
	/// This is an example Aggregated Hadoop Map/Reduce application. It reads the
	/// text input files, breaks each line into words and counts them. The output is
	/// a locally sorted list of words and the count of how often they occurred.
	/// To run: bin/hadoop jar hadoop-*-examples.jar aggregatewordcount
	/// <i>in-dir</i> <i>out-dir</i> <i>numOfReducers</i> textinputformat
	/// </remarks>
	public class AggregateWordCount
	{
		public class WordCountPlugInClass : ValueAggregatorBaseDescriptor
		{
			public override AList<KeyValuePair<Text, Text>> GenerateKeyValPairs(object key, object
				 val)
			{
				string countType = LongValueSum;
				AList<KeyValuePair<Text, Text>> retv = new AList<KeyValuePair<Text, Text>>();
				string line = val.ToString();
				StringTokenizer itr = new StringTokenizer(line);
				while (itr.HasMoreTokens())
				{
					KeyValuePair<Text, Text> e = GenerateEntry(countType, itr.NextToken(), One);
					if (e != null)
					{
						retv.AddItem(e);
					}
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
			Job job = ValueAggregatorJob.CreateValueAggregatorJob(args, new Type[] { typeof(AggregateWordCount.WordCountPlugInClass
				) });
			job.SetJarByClass(typeof(AggregateWordCount));
			int ret = job.WaitForCompletion(true) ? 0 : 1;
			System.Environment.Exit(ret);
		}
	}
}
