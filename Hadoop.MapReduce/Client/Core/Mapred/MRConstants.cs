using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Some handy constants</summary>
	public abstract class MRConstants
	{
		public const long CounterUpdateInterval = 60 * 1000;

		public const int Success = 0;

		public const int FileNotFound = -1;

		/// <summary>The custom http header used for the map output length.</summary>
		public const string MapOutputLength = "Map-Output-Length";

		/// <summary>The custom http header used for the "raw" map output length.</summary>
		public const string RawMapOutputLength = "Raw-Map-Output-Length";

		/// <summary>The map task from which the map output data is being transferred</summary>
		public const string FromMapTask = "from-map-task";

		/// <summary>The reduce task number for which this map output is being transferred</summary>
		public const string ForReduceTask = "for-reduce-task";

		/// <summary>Used in MRv1, mostly in TaskTracker code</summary>
		public const string Workdir = "work";

		/// <summary>Used on by MRv2</summary>
		public const string ApplicationAttemptId = "mapreduce.job.application.attempt.id";
		//
		// Timeouts, constants
		//
		//
		// Result codes
		//
	}

	public static class MRConstantsConstants
	{
	}
}
