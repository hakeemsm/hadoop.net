using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>
	/// Class which is used to create the data to write for a given path and offset
	/// into that file for writing and later verification that the expected value is
	/// read at that file bytes offset
	/// </summary>
	internal class DataHasher
	{
		private Random rnd;

		internal DataHasher(long mixIn)
		{
			this.rnd = new Random(mixIn);
		}

		/// <param name="offSet">the byte offset into the file</param>
		/// <returns>the data to be expected at that offset</returns>
		internal virtual long Generate(long offSet)
		{
			return ((offSet * 47) ^ (rnd.NextLong() * 97)) * 37;
		}
	}
}
