using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public enum JobCounter
	{
		NumFailedMaps,
		NumFailedReduces,
		NumKilledMaps,
		NumKilledReduces,
		TotalLaunchedMaps,
		TotalLaunchedReduces,
		OtherLocalMaps,
		DataLocalMaps,
		RackLocalMaps,
		SlotsMillisMaps,
		SlotsMillisReduces,
		FallowSlotsMillisMaps,
		FallowSlotsMillisReduces,
		TotalLaunchedUbertasks,
		NumUberSubmaps,
		NumUberSubreduces,
		NumFailedUbertasks,
		MillisMaps,
		MillisReduces,
		VcoresMillisMaps,
		VcoresMillisReduces,
		MbMillisMaps,
		MbMillisReduces
	}
}
