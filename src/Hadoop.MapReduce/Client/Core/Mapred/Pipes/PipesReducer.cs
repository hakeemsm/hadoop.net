using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	/// <summary>This class is used to talk to a C++ reduce task.</summary>
	internal class PipesReducer<K2, V2, K3, V3> : Reducer<K2, V2, K3, V3>
		where K2 : WritableComparable
		where V2 : Writable
		where K3 : WritableComparable
		where V3 : Writable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(PipesReducer).FullName
			);

		private JobConf job;

		private Application<K2, V2, K3, V3> application = null;

		private DownwardProtocol<K2, V2> downlink = null;

		private bool isOk = true;

		private bool skipping = false;

		public virtual void Configure(JobConf job)
		{
			this.job = job;
			//disable the auto increment of the counter. For pipes, no of processed 
			//records could be different(equal or less) than the no of records input.
			SkipBadRecords.SetAutoIncrReducerProcCount(job, false);
			skipping = job.GetBoolean(MRJobConfig.SkipRecords, false);
		}

		/// <summary>Process all of the keys and values.</summary>
		/// <remarks>
		/// Process all of the keys and values. Start up the application if we haven't
		/// started it yet.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Reduce(K2 key, IEnumerator<V2> values, OutputCollector<K3, V3
			> output, Reporter reporter)
		{
			isOk = false;
			StartApplication(output, reporter);
			downlink.ReduceKey(key);
			while (values.HasNext())
			{
				downlink.ReduceValue(values.Next());
			}
			if (skipping)
			{
				//flush the streams on every record input if running in skip mode
				//so that we don't buffer other records surrounding a bad record.
				downlink.Flush();
			}
			isOk = true;
		}

		/// <exception cref="System.IO.IOException"/>
		private void StartApplication(OutputCollector<K3, V3> output, Reporter reporter)
		{
			if (application == null)
			{
				try
				{
					Log.Info("starting application");
					application = new Application<K2, V2, K3, V3>(job, null, output, reporter, (Type)
						job.GetOutputKeyClass(), (Type)job.GetOutputValueClass());
					downlink = application.GetDownlink();
				}
				catch (Exception ie)
				{
					throw new RuntimeException("interrupted", ie);
				}
				int reduce = 0;
				downlink.RunReduce(reduce, Submitter.GetIsJavaRecordWriter(job));
			}
		}

		/// <summary>Handle the end of the input by closing down the application.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			// if we haven't started the application, we have nothing to do
			if (isOk)
			{
				OutputCollector<K3, V3> nullCollector = new _OutputCollector_102();
				// NULL
				StartApplication(nullCollector, Reporter.Null);
			}
			try
			{
				if (isOk)
				{
					application.GetDownlink().EndOfInput();
				}
				else
				{
					// send the abort to the application and let it clean up
					application.GetDownlink().Abort();
				}
				Log.Info("waiting for finish");
				application.WaitForFinish();
				Log.Info("got done");
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

		private sealed class _OutputCollector_102 : OutputCollector<K3, V3>
		{
			public _OutputCollector_102()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public void Collect(K3 key, V3 value)
			{
			}
		}
	}
}
