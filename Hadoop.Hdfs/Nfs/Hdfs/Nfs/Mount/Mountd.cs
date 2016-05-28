using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Mount;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Mount
{
	/// <summary>Main class for starting mountd daemon.</summary>
	/// <remarks>
	/// Main class for starting mountd daemon. This daemon implements the NFS
	/// mount protocol. When receiving a MOUNT request from an NFS client, it checks
	/// the request against the list of currently exported file systems. If the
	/// client is permitted to mount the file system, rpc.mountd obtains a file
	/// handle for requested directory and returns it to the client.
	/// </remarks>
	public class Mountd : MountdBase
	{
		/// <exception cref="System.IO.IOException"/>
		public Mountd(NfsConfiguration config, DatagramSocket registrationSocket, bool allowInsecurePorts
			)
			: base(new RpcProgramMountd(config, registrationSocket, allowInsecurePorts))
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			NfsConfiguration config = new NfsConfiguration();
			Org.Apache.Hadoop.Hdfs.Nfs.Mount.Mountd mountd = new Org.Apache.Hadoop.Hdfs.Nfs.Mount.Mountd
				(config, null, true);
			mountd.Start(true);
		}
	}
}
