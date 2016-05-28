using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Common
{
	/// <summary>A GenerationStamp is a Hadoop FS primitive, identified by a long.</summary>
	public class GenerationStamp : SequentialNumber
	{
		/// <summary>The last reserved generation stamp.</summary>
		public const long LastReservedStamp = 1000L;

		/// <summary>
		/// Generation stamp of blocks that pre-date the introduction
		/// of a generation stamp.
		/// </summary>
		public const long GrandfatherGenerationStamp = 0;

		/// <summary>
		/// Create a new instance, initialized to
		/// <see cref="LastReservedStamp"/>
		/// .
		/// </summary>
		public GenerationStamp()
			: base(LastReservedStamp)
		{
		}
	}
}
