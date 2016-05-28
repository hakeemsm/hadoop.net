using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Hdfs.Nfs.Mount;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	/// <summary>Nfs server.</summary>
	/// <remarks>
	/// Nfs server. Supports NFS v3 using
	/// <see cref="RpcProgramNfs3"/>
	/// .
	/// Currently Mountd program is also started inside this class.
	/// Only TCP server is supported and UDP is not supported.
	/// </remarks>
	public class Nfs3 : Nfs3Base
	{
		private Mountd mountd;

		/// <exception cref="System.IO.IOException"/>
		public Nfs3(NfsConfiguration conf)
			: this(conf, null, true)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public Nfs3(NfsConfiguration conf, DatagramSocket registrationSocket, bool allowInsecurePorts
			)
			: base(RpcProgramNfs3.CreateRpcProgramNfs3(conf, registrationSocket, allowInsecurePorts
				), conf)
		{
			mountd = new Mountd(conf, registrationSocket, allowInsecurePorts);
		}

		public virtual Mountd GetMountd()
		{
			return mountd;
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public virtual void StartServiceInternal(bool register)
		{
			mountd.Start(register);
			// Start mountd
			Start(register);
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void StartService(string[] args, DatagramSocket registrationSocket
			)
		{
			StringUtils.StartupShutdownMessage(typeof(Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3), 
				args, Log);
			NfsConfiguration conf = new NfsConfiguration();
			bool allowInsecurePorts = conf.GetBoolean(NfsConfigKeys.DfsNfsPortMonitoringDisabledKey
				, NfsConfigKeys.DfsNfsPortMonitoringDisabledDefault);
			Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3 nfsServer = new Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3
				(conf, registrationSocket, allowInsecurePorts);
			nfsServer.StartServiceInternal(true);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			StartService(args, null);
		}
	}
}
