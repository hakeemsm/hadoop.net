using Sharpen;

namespace org.apache.hadoop.util
{
	public interface IPList
	{
		/// <summary>returns true if the ipAddress is in the IPList.</summary>
		/// <param name="ipAddress"/>
		/// <returns>boolean value indicating whether the ipAddress is in the IPList</returns>
		bool isIn(string ipAddress);
	}
}
