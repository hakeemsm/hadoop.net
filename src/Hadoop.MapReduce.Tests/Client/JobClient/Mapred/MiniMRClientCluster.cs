using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public interface MiniMRClientCluster
	{
		/*
		* A simple interface for a client MR cluster used for testing. This interface
		* provides basic methods which are independent of the underlying Mini Cluster (
		* either through MR1 or MR2).
		*/
		/// <exception cref="System.IO.IOException"/>
		void Start();

		/// <summary>Stop and start back the cluster using the same configuration.</summary>
		/// <exception cref="System.IO.IOException"/>
		void Restart();

		/// <exception cref="System.IO.IOException"/>
		void Stop();

		/// <exception cref="System.IO.IOException"/>
		Configuration GetConfig();
	}
}
