using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// The slive class which sets up the mapper to be used which itself will receive
	/// a single dummy key and value and then in a loop run the various operations
	/// that have been selected and upon operation completion output the collected
	/// output from that operation (and repeat until finished).
	/// </summary>
	public class SliveMapper : MapReduceBase, Mapper<object, object, Text, Text>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(SliveMapper));

		private static readonly string OpType = typeof(SliveMapper).Name;

		private FileSystem filesystem;

		private ConfigExtractor config;

		private int taskId;

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
			try
			{
				config = new ConfigExtractor(conf);
				ConfigExtractor.DumpOptions(config);
				filesystem = config.GetBaseDirectory().GetFileSystem(conf);
			}
			catch (Exception e)
			{
				Log.Error("Unable to setup slive " + StringUtils.StringifyException(e));
				throw new RuntimeException("Unable to setup slive configuration", e);
			}
			if (conf.Get(MRJobConfig.TaskAttemptId) != null)
			{
				this.taskId = TaskAttemptID.ForName(conf.Get(MRJobConfig.TaskAttemptId)).GetTaskID
					().GetId();
			}
			else
			{
				// So that branch-1/0.20 can run this same code as well
				this.taskId = TaskAttemptID.ForName(conf.Get("mapred.task.id")).GetTaskID().GetId
					();
			}
		}

		/// <summary>Fetches the config this object uses</summary>
		/// <returns>ConfigExtractor</returns>
		private ConfigExtractor GetConfig()
		{
			return config;
		}

		/// <summary>Logs to the given reporter and logs to the internal logger at info level
		/// 	</summary>
		/// <param name="r">the reporter to set status on</param>
		/// <param name="msg">the message to log</param>
		private void LogAndSetStatus(Reporter r, string msg)
		{
			r.SetStatus(msg);
			Log.Info(msg);
		}

		/// <summary>Runs the given operation and reports on its results</summary>
		/// <param name="op">the operation to run</param>
		/// <param name="reporter">the status reporter to notify</param>
		/// <param name="output">the output to write to</param>
		/// <exception cref="System.IO.IOException"/>
		private void RunOperation(Operation op, Reporter reporter, OutputCollector<Text, 
			Text> output, long opNum)
		{
			if (op == null)
			{
				return;
			}
			LogAndSetStatus(reporter, "Running operation #" + opNum + " (" + op + ")");
			IList<OperationOutput> opOut = op.Run(filesystem);
			LogAndSetStatus(reporter, "Finished operation #" + opNum + " (" + op + ")");
			if (opOut != null && !opOut.IsEmpty())
			{
				foreach (OperationOutput outData in opOut)
				{
					output.Collect(outData.GetKey(), outData.GetOutputValue());
				}
			}
		}

		/*
		* (non-Javadoc)
		*
		* @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		* java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		* org.apache.hadoop.mapred.Reporter)
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void Map(object key, object value, OutputCollector<Text, Text> output
			, Reporter reporter)
		{
			// Mapper
			LogAndSetStatus(reporter, "Running slive mapper for dummy key " + key + " and dummy value "
				 + value);
			//Add taskID to randomSeed to deterministically seed rnd.
			Random rnd = config.GetRandomSeed() != null ? new Random(this.taskId + config.GetRandomSeed
				()) : new Random();
			WeightSelector selector = new WeightSelector(config, rnd);
			long startTime = Timer.Now();
			long opAm = 0;
			long sleepOps = 0;
			int duration = GetConfig().GetDurationMilliseconds();
			Range<long> sleepRange = GetConfig().GetSleepRange();
			Operation sleeper = null;
			if (sleepRange != null)
			{
				sleeper = new SleepOp(GetConfig(), rnd);
			}
			while (Timer.Elapsed(startTime) < duration)
			{
				try
				{
					LogAndSetStatus(reporter, "Attempting to select operation #" + (opAm + 1));
					int currElapsed = (int)(Timer.Elapsed(startTime));
					Operation op = selector.Select(currElapsed, duration);
					if (op == null)
					{
						// no ops left
						break;
					}
					else
					{
						// got a good op
						++opAm;
						RunOperation(op, reporter, output, opAm);
					}
					// do a sleep??
					if (sleeper != null)
					{
						// these don't count against the number of operations
						++sleepOps;
						RunOperation(sleeper, reporter, output, sleepOps);
					}
				}
				catch (Exception e)
				{
					LogAndSetStatus(reporter, "Failed at running due to " + StringUtils.StringifyException
						(e));
					if (GetConfig().ShouldExitOnFirstError())
					{
						break;
					}
				}
			}
			{
				// write out any accumulated mapper stats
				long timeTaken = Timer.Elapsed(startTime);
				OperationOutput opCount = new OperationOutput(OperationOutput.OutputType.Long, OpType
					, ReportWriter.OpCount, opAm);
				output.Collect(opCount.GetKey(), opCount.GetOutputValue());
				OperationOutput overallTime = new OperationOutput(OperationOutput.OutputType.Long
					, OpType, ReportWriter.OkTimeTaken, timeTaken);
				output.Collect(overallTime.GetKey(), overallTime.GetOutputValue());
				LogAndSetStatus(reporter, "Finished " + opAm + " operations in " + timeTaken + " milliseconds"
					);
			}
		}
	}
}
