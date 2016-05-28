using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>
	/// Reads in history events from the JobHistoryFile and sends them out again
	/// to be recorded.
	/// </summary>
	public class JobHistoryCopyService : CompositeService, HistoryEventHandler
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.Jobhistory.JobHistoryCopyService
			));

		private readonly ApplicationAttemptId applicationAttemptId;

		private readonly EventHandler handler;

		private readonly JobId jobId;

		public JobHistoryCopyService(ApplicationAttemptId applicationAttemptId, EventHandler
			 handler)
			: base("JobHistoryCopyService")
		{
			this.applicationAttemptId = applicationAttemptId;
			this.jobId = TypeConverter.ToYarn(TypeConverter.FromYarn(applicationAttemptId.GetApplicationId
				()));
			this.handler = handler;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void HandleEvent(HistoryEvent @event)
		{
			//Skip over the AM Events this is handled elsewhere
			if (!(@event is AMStartedEvent))
			{
				handler.Handle(new JobHistoryEvent(jobId, @event));
			}
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			try
			{
				//TODO should we parse on a background thread???
				Parse();
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
			base.ServiceStart();
		}

		/// <exception cref="System.IO.IOException"/>
		private void Parse()
		{
			FSDataInputStream @in = null;
			try
			{
				@in = GetPreviousJobHistoryFileStream(GetConfig(), applicationAttemptId);
			}
			catch (IOException e)
			{
				Log.Warn("error trying to open previous history file. No history data " + "will be copied over."
					, e);
				return;
			}
			JobHistoryParser parser = new JobHistoryParser(@in);
			parser.Parse(this);
			Exception parseException = parser.GetParseException();
			if (parseException != null)
			{
				Log.Info("Got an error parsing job-history file" + ", ignoring incomplete events."
					, parseException);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static FSDataInputStream GetPreviousJobHistoryFileStream(Configuration conf
			, ApplicationAttemptId applicationAttemptId)
		{
			FSDataInputStream @in = null;
			Path historyFile = null;
			string jobId = TypeConverter.FromYarn(applicationAttemptId.GetApplicationId()).ToString
				();
			string jobhistoryDir = JobHistoryUtils.GetConfiguredHistoryStagingDirPrefix(conf, 
				jobId);
			Path histDirPath = FileContext.GetFileContext(conf).MakeQualified(new Path(jobhistoryDir
				));
			FileContext fc = FileContext.GetFileContext(histDirPath.ToUri(), conf);
			// read the previous history file
			historyFile = fc.MakeQualified(JobHistoryUtils.GetStagingJobHistoryFile(histDirPath
				, jobId, (applicationAttemptId.GetAttemptId() - 1)));
			Log.Info("History file is at " + historyFile);
			@in = fc.Open(historyFile);
			return @in;
		}
	}
}
