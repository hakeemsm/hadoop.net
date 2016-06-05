using System.Net;
using Org.Apache.Commons.Daemon;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	/// <summary>
	/// This class is used to allow the initial registration of the NFS gateway with
	/// the system portmap daemon to come from a privileged (&lt; 1024) port.
	/// </summary>
	/// <remarks>
	/// This class is used to allow the initial registration of the NFS gateway with
	/// the system portmap daemon to come from a privileged (&lt; 1024) port. This is
	/// necessary on certain operating systems to work around this bug in rpcbind:
	/// Red Hat: https://bugzilla.redhat.com/show_bug.cgi?id=731542
	/// SLES: https://bugzilla.novell.com/show_bug.cgi?id=823364
	/// Debian: https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=594880
	/// </remarks>
	public class PrivilegedNfsGatewayStarter : Org.Apache.Commons.Daemon.Daemon
	{
		private string[] args = null;

		private DatagramSocket registrationSocket = null;

		/// <exception cref="System.Exception"/>
		public virtual void Init(DaemonContext context)
		{
			System.Console.Error.WriteLine("Initializing privileged NFS client socket...");
			NfsConfiguration conf = new NfsConfiguration();
			int clientPort = conf.GetInt(NfsConfigKeys.DfsNfsRegistrationPortKey, NfsConfigKeys
				.DfsNfsRegistrationPortDefault);
			if (clientPort < 1 || clientPort > 1023)
			{
				throw new RuntimeException("Must start privileged NFS server with '" + NfsConfigKeys
					.DfsNfsRegistrationPortKey + "' configured to a " + "privileged port.");
			}
			registrationSocket = new DatagramSocket(new IPEndPoint("localhost", clientPort));
			registrationSocket.SetReuseAddress(true);
			args = context.GetArguments();
		}

		/// <exception cref="System.Exception"/>
		public virtual void Start()
		{
			Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3.StartService(args, registrationSocket);
		}

		/// <exception cref="System.Exception"/>
		public virtual void Stop()
		{
		}

		// Nothing to do.
		public virtual void Destroy()
		{
			if (registrationSocket != null && !registrationSocket.IsClosed())
			{
				registrationSocket.Close();
			}
		}
	}
}
