using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// The slive reducer which iterates over the given input values and merges them
	/// together into a final output value.
	/// </summary>
	public class SliveReducer : MapReduceBase, Reducer<Text, Text, Text, Text>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(SliveReducer));

		private ConfigExtractor config;

		/// <summary>Logs to the given reporter and logs to the internal logger at info level
		/// 	</summary>
		/// <param name="r">the reporter to set status on</param>
		/// <param name="msg">the message to log</param>
		private void LogAndSetStatus(Reporter r, string msg)
		{
			r.SetStatus(msg);
			Log.Info(msg);
		}

		/// <summary>Fetches the config this object uses</summary>
		/// <returns>ConfigExtractor</returns>
		private ConfigExtractor GetConfig()
		{
			return config;
		}

		/*
		* (non-Javadoc)
		*
		* @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		* java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		* org.apache.hadoop.mapred.Reporter)
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void Reduce(Text key, IEnumerator<Text> values, OutputCollector<Text
			, Text> output, Reporter reporter)
		{
			// Reducer
			OperationOutput collector = null;
			int reduceAm = 0;
			int errorAm = 0;
			LogAndSetStatus(reporter, "Iterating over reduction values for key " + key);
			while (values.HasNext())
			{
				Text value = values.Next();
				try
				{
					OperationOutput val = new OperationOutput(key, value);
					if (collector == null)
					{
						collector = val;
					}
					else
					{
						collector = OperationOutput.Merge(collector, val);
					}
					Log.Info("Combined " + val + " into/with " + collector);
					++reduceAm;
				}
				catch (Exception e)
				{
					++errorAm;
					LogAndSetStatus(reporter, "Error iterating over reduction input " + value + " due to : "
						 + StringUtils.StringifyException(e));
					if (GetConfig().ShouldExitOnFirstError())
					{
						break;
					}
				}
			}
			LogAndSetStatus(reporter, "Reduced " + reduceAm + " values with " + errorAm + " errors"
				);
			if (collector != null)
			{
				LogAndSetStatus(reporter, "Writing output " + collector.GetKey() + " : " + collector
					.GetOutputValue());
				output.Collect(collector.GetKey(), collector.GetOutputValue());
			}
		}

		/*
		* (non-Javadoc)
		*
		* @see
		* org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop.mapred
		* .JobConf)
		*/
		public override void Configure(JobConf conf)
		{
			// MapReduceBase
			config = new ConfigExtractor(conf);
			ConfigExtractor.DumpOptions(config);
		}
	}
}
