using System;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	/// <summary>An adaptor to run a C++ mapper.</summary>
	internal class PipesMapRunner<K1, V1, K2, V2> : MapRunner<K1, V1, K2, V2>
		where K1 : WritableComparable
		where V1 : Writable
		where K2 : WritableComparable
		where V2 : Writable
	{
		private JobConf job;

		/// <summary>Get the new configuration.</summary>
		/// <param name="job">the job's configuration</param>
		public override void Configure(JobConf job)
		{
			this.job = job;
			//disable the auto increment of the counter. For pipes, no of processed 
			//records could be different(equal or less) than the no of records input.
			SkipBadRecords.SetAutoIncrMapperProcCount(job, false);
		}

		/// <summary>Run the map task.</summary>
		/// <param name="input">the set of inputs</param>
		/// <param name="output">the object to collect the outputs of the map</param>
		/// <param name="reporter">the object to update with status</param>
		/// <exception cref="System.IO.IOException"/>
		public override void Run(RecordReader<K1, V1> input, OutputCollector<K2, V2> output
			, Reporter reporter)
		{
			Application<K1, V1, K2, V2> application = null;
			try
			{
				RecordReader<FloatWritable, NullWritable> fakeInput = (!Submitter.GetIsJavaRecordReader
					(job) && !Submitter.GetIsJavaMapper(job)) ? (RecordReader<FloatWritable, NullWritable
					>)input : null;
				application = new Application<K1, V1, K2, V2>(job, fakeInput, output, reporter, (
					Type)job.GetOutputKeyClass(), (Type)job.GetOutputValueClass());
			}
			catch (Exception ie)
			{
				throw new RuntimeException("interrupted", ie);
			}
			DownwardProtocol<K1, V1> downlink = application.GetDownlink();
			bool isJavaInput = Submitter.GetIsJavaRecordReader(job);
			downlink.RunMap(reporter.GetInputSplit(), job.GetNumReduceTasks(), isJavaInput);
			bool skipping = job.GetBoolean(MRJobConfig.SkipRecords, false);
			try
			{
				if (isJavaInput)
				{
					// allocate key & value instances that are re-used for all entries
					K1 key = input.CreateKey();
					V1 value = input.CreateValue();
					downlink.SetInputTypes(key.GetType().FullName, value.GetType().FullName);
					while (input.Next(key, value))
					{
						// map pair to output
						downlink.MapItem(key, value);
						if (skipping)
						{
							//flush the streams on every record input if running in skip mode
							//so that we don't buffer other records surrounding a bad record.
							downlink.Flush();
						}
					}
					downlink.EndOfInput();
				}
				application.WaitForFinish();
			}
			catch (Exception t)
			{
				application.Abort(t);
			}
			finally
			{
				application.Cleanup();
			}
		}
	}
}
