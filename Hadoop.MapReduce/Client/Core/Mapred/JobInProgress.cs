using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class JobInProgress
	{
		[System.ObsoleteAttribute(@"Provided for compatibility. Use JobCounter instead.")]
		public enum Counter
		{
			NumFailedMaps,
			NumFailedReduces,
			TotalLaunchedMaps,
			TotalLaunchedReduces,
			OtherLocalMaps,
			DataLocalMaps,
			RackLocalMaps,
			SlotsMillisMaps,
			SlotsMillisReduces,
			FallowSlotsMillisMaps,
			FallowSlotsMillisReduces
		}
	}
}
