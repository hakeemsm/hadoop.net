using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>Constants used in various places in slive</summary>
	internal class Constants
	{
		/// <summary>This class should be static members only - no construction allowed</summary>
		private Constants()
		{
		}

		/// <summary>The distributions supported (or that maybe supported)</summary>
		[System.Serializable]
		internal sealed class Distribution
		{
			public static readonly Constants.Distribution Beg = new Constants.Distribution();

			public static readonly Constants.Distribution End = new Constants.Distribution();

			public static readonly Constants.Distribution Uniform = new Constants.Distribution
				();

			public static readonly Constants.Distribution Mid = new Constants.Distribution();

			internal string LowerName()
			{
				return StringUtils.ToLowerCase(this.ToString());
			}
		}

		/// <summary>Allowed operation types</summary>
		[System.Serializable]
		internal sealed class OperationType
		{
			public static readonly Constants.OperationType Read = new Constants.OperationType
				();

			public static readonly Constants.OperationType Append = new Constants.OperationType
				();

			public static readonly Constants.OperationType Rename = new Constants.OperationType
				();

			public static readonly Constants.OperationType Ls = new Constants.OperationType();

			public static readonly Constants.OperationType Mkdir = new Constants.OperationType
				();

			public static readonly Constants.OperationType Delete = new Constants.OperationType
				();

			public static readonly Constants.OperationType Create = new Constants.OperationType
				();

			public static readonly Constants.OperationType Truncate = new Constants.OperationType
				();

			internal string LowerName()
			{
				return StringUtils.ToLowerCase(this.ToString());
			}
		}

		internal static readonly string ProgName = typeof(SliveTest).Name;

		internal const string ProgVersion = "0.1.0";

		internal const int Megabytes = 1048576;

		internal const int Buffersize = 64 * 1024;

		internal const int BytesPerLong = 8;

		internal const string ReducerFile = "part-%s";

		internal const string BytesPerChecksum = "io.bytes.per.checksum";

		internal const string MinReplication = "dfs.namenode.replication.min";

		internal const string OpDescr = "pct,distribution where distribution is one of %s";

		internal const string OpPercent = "slive.op.%s.pct";

		internal const string Op = "slive.op.%s";

		internal const string OpDistr = "slive.op.%s.dist";

		internal const string BaseDir = "slive";

		internal const string DataDir = "data";

		internal const string OutputDir = "output";

		internal const bool FlushWrites = false;
		// program info
		// useful constants
		// must be a multiple of
		// BYTES_PER_LONG - used for reading and writing buffer sizes
		// 8 bytes per long
		// used for finding the reducer file for a given number
		// this is used to ensure the blocksize is a multiple of this config setting
		// min replication setting for verification
		// used for getting an option description given a set of distributions
		// to substitute
		// keys for looking up a specific operation in the hadoop config
		// path constants
		// whether whenever data is written a flush should occur
	}
}
