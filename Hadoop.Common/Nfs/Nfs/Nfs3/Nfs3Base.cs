using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Portmap;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3
{
	/// <summary>Nfs server.</summary>
	/// <remarks>
	/// Nfs server. Supports NFS v3 using
	/// <see cref="Org.Apache.Hadoop.Oncrpc.RpcProgram"/>
	/// .
	/// Only TCP server is supported and UDP is not supported.
	/// </remarks>
	public abstract class Nfs3Base
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Nfs.Nfs3.Nfs3Base
			));

		private readonly RpcProgram rpcProgram;

		private int nfsBoundPort;

		// Will set after server starts
		public virtual RpcProgram GetRpcProgram()
		{
			return rpcProgram;
		}

		protected internal Nfs3Base(RpcProgram rpcProgram, Configuration conf)
		{
			this.rpcProgram = rpcProgram;
			Log.Info("NFS server port set to: " + rpcProgram.GetPort());
		}

		public virtual void Start(bool register)
		{
			StartTCPServer();
			// Start TCP server
			if (register)
			{
				ShutdownHookManager.Get().AddShutdownHook(new Nfs3Base.NfsShutdownHook(this), ShutdownHookPriority
					);
				try
				{
					rpcProgram.Register(PortmapMapping.TransportTcp, nfsBoundPort);
				}
				catch (Exception e)
				{
					Log.Fatal("Failed to register the NFSv3 service.", e);
					ExitUtil.Terminate(1, e);
				}
			}
		}

		private void StartTCPServer()
		{
			SimpleTcpServer tcpServer = new SimpleTcpServer(rpcProgram.GetPort(), rpcProgram, 
				0);
			rpcProgram.StartDaemons();
			try
			{
				tcpServer.Run();
			}
			catch (Exception e)
			{
				Log.Fatal("Failed to start the TCP server.", e);
				if (tcpServer.GetBoundPort() > 0)
				{
					rpcProgram.Unregister(PortmapMapping.TransportTcp, tcpServer.GetBoundPort());
				}
				tcpServer.Shutdown();
				ExitUtil.Terminate(1, e);
			}
			nfsBoundPort = tcpServer.GetBoundPort();
		}

		/// <summary>Priority of the nfsd shutdown hook.</summary>
		public const int ShutdownHookPriority = 10;

		private class NfsShutdownHook : Runnable
		{
			public virtual void Run()
			{
				lock (this)
				{
					this._enclosing.rpcProgram.Unregister(PortmapMapping.TransportTcp, this._enclosing
						.nfsBoundPort);
					this._enclosing.rpcProgram.StopDaemons();
				}
			}

			internal NfsShutdownHook(Nfs3Base _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly Nfs3Base _enclosing;
		}
	}
}
