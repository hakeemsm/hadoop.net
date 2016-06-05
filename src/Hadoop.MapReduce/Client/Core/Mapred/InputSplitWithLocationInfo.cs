using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public interface InputSplitWithLocationInfo : InputSplit
	{
		/// <summary>
		/// Gets info about which nodes the input split is stored on and how it is
		/// stored at each location.
		/// </summary>
		/// <returns>
		/// list of <code>SplitLocationInfo</code>s describing how the split
		/// data is stored at each location. A null value indicates that all the
		/// locations have the data stored on disk.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		SplitLocationInfo[] GetLocationInfo();
	}
}
