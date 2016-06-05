

namespace Org.Apache.Hadoop.Util
{
	public interface IPList
	{
		/// <summary>returns true if the ipAddress is in the IPList.</summary>
		/// <param name="ipAddress"/>
		/// <returns>boolean value indicating whether the ipAddress is in the IPList</returns>
		bool IsIn(string ipAddress);
	}
}
