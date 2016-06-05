using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public interface VersionInfoMXBean
	{
		/// <returns>the compilation information which contains date, user and branch</returns>
		string GetCompileInfo();

		/// <returns>the software version</returns>
		string GetSoftwareVersion();
	}
}
