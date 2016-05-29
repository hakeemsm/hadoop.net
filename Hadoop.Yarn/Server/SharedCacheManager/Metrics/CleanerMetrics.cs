using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics
{
	/// <summary>
	/// This class is for maintaining the various Cleaner activity statistics and
	/// publishing them through the metrics interfaces.
	/// </summary>
	public class CleanerMetrics
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.CleanerMetrics
			));

		private readonly MetricsRegistry registry = new MetricsRegistry("cleaner");

		private static readonly Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.CleanerMetrics
			 Instance = Create();

		public static Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.CleanerMetrics
			 GetInstance()
		{
			return Instance;
		}

		private MutableCounterLong totalDeletedFiles;

		public virtual long GetTotalDeletedFiles()
		{
			return totalDeletedFiles.Value();
		}

		private MutableGaugeLong deletedFiles;

		public virtual long GetDeletedFiles()
		{
			return deletedFiles.Value();
		}

		private MutableCounterLong totalProcessedFiles;

		public virtual long GetTotalProcessedFiles()
		{
			return totalProcessedFiles.Value();
		}

		private MutableGaugeLong processedFiles;

		public virtual long GetProcessedFiles()
		{
			return processedFiles.Value();
		}

		private MutableCounterLong totalFileErrors;

		public virtual long GetTotalFileErrors()
		{
			return totalFileErrors.Value();
		}

		private MutableGaugeLong fileErrors;

		public virtual long GetFileErrors()
		{
			return fileErrors.Value();
		}

		private CleanerMetrics()
		{
		}

		/// <summary>The metric source obtained after parsing the annotations</summary>
		internal MetricsSource metricSource;

		internal static Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.CleanerMetrics
			 Create()
		{
			MetricsSystem ms = DefaultMetricsSystem.Instance();
			Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.CleanerMetrics metricObject
				 = new Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics.CleanerMetrics();
			MetricsSourceBuilder sb = MetricsAnnotations.NewSourceBuilder(metricObject);
			MetricsSource s = sb.Build();
			ms.Register("cleaner", "The cleaner service of truly shared cache", s);
			metricObject.metricSource = s;
			return metricObject;
		}

		/// <summary>Report a delete operation at the current system time</summary>
		public virtual void ReportAFileDelete()
		{
			totalProcessedFiles.Incr();
			processedFiles.Incr();
			totalDeletedFiles.Incr();
			deletedFiles.Incr();
		}

		/// <summary>Report a process operation at the current system time</summary>
		public virtual void ReportAFileProcess()
		{
			totalProcessedFiles.Incr();
			processedFiles.Incr();
		}

		/// <summary>Report a process operation error at the current system time</summary>
		public virtual void ReportAFileError()
		{
			totalProcessedFiles.Incr();
			processedFiles.Incr();
			totalFileErrors.Incr();
			fileErrors.Incr();
		}

		/// <summary>Report the start a new run of the cleaner.</summary>
		public virtual void ReportCleaningStart()
		{
			processedFiles.Set(0);
			deletedFiles.Set(0);
			fileErrors.Set(0);
		}
	}
}
