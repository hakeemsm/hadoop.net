using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Webapp
{
	/// <summary>
	/// This class is used to summarize useful shared cache manager metrics for the
	/// webUI display.
	/// </summary>
	public class SCMMetricsInfo
	{
		protected internal long totalDeletedFiles;

		protected internal long totalProcessedFiles;

		protected internal long cacheHits;

		protected internal long cacheMisses;

		protected internal long cacheReleases;

		protected internal long acceptedUploads;

		protected internal long rejectedUploads;

		public SCMMetricsInfo()
		{
		}

		public SCMMetricsInfo(CleanerMetrics cleanerMetrics, ClientSCMMetrics clientSCMMetrics
			, SharedCacheUploaderMetrics scmUploaderMetrics)
		{
			totalDeletedFiles = cleanerMetrics.GetTotalDeletedFiles();
			totalProcessedFiles = cleanerMetrics.GetTotalProcessedFiles();
			cacheHits = clientSCMMetrics.GetCacheHits();
			cacheMisses = clientSCMMetrics.GetCacheMisses();
			cacheReleases = clientSCMMetrics.GetCacheReleases();
			acceptedUploads = scmUploaderMetrics.GetAcceptedUploads();
			rejectedUploads = scmUploaderMetrics.GetRejectUploads();
		}

		public virtual long GetTotalDeletedFiles()
		{
			return totalDeletedFiles;
		}

		public virtual long GetTotalProcessedFiles()
		{
			return totalProcessedFiles;
		}

		public virtual long GetCacheHits()
		{
			return cacheHits;
		}

		public virtual long GetCacheMisses()
		{
			return cacheMisses;
		}

		public virtual long GetCacheReleases()
		{
			return cacheReleases;
		}

		public virtual long GetAcceptedUploads()
		{
			return acceptedUploads;
		}

		public virtual long GetRejectUploads()
		{
			return rejectedUploads;
		}
	}
}
