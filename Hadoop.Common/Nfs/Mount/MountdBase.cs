using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Portmap;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mount
{
	/// <summary>Main class for starting mountd daemon.</summary>
	/// <remarks>
	/// Main class for starting mountd daemon. This daemon implements the NFS
	/// mount protocol. When receiving a MOUNT request from an NFS client, it checks
	/// the request against the list of currently exported file systems. If the
	/// client is permitted to mount the file system, rpc.mountd obtains a file
	/// handle for requested directory and returns it to the client.
	/// </remarks>
	public abstract class MountdBase
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mount.MountdBase
			));

		private readonly RpcProgram rpcProgram;

		private int udpBoundPort;

		private int tcpBoundPort;

		// Will set after server starts
		// Will set after server starts
		public virtual RpcProgram GetRpcProgram()
		{
			return rpcProgram;
		}

		/// <summary>Constructor</summary>
		/// <param name="program"/>
		/// <exception cref="System.IO.IOException"/>
		public MountdBase(RpcProgram program)
		{
			rpcProgram = program;
		}

		/* Start UDP server */
		private void StartUDPServer()
		{
			SimpleUdpServer udpServer = new SimpleUdpServer(rpcProgram.GetPort(), rpcProgram, 
				1);
			rpcProgram.StartDaemons();
			try
			{
				udpServer.Run();
			}
			catch (Exception e)
			{
				Log.Fatal("Failed to start the UDP server.", e);
				if (udpServer.GetBoundPort() > 0)
				{
					rpcProgram.Unregister(PortmapMapping.TransportUdp, udpServer.GetBoundPort());
				}
				udpServer.Shutdown();
				ExitUtil.Terminate(1, e);
			}
			udpBoundPort = udpServer.GetBoundPort();
		}

		/* Start TCP server */
		private void StartTCPServer()
		{
			SimpleTcpServer tcpServer = new SimpleTcpServer(rpcProgram.GetPort(), rpcProgram, 
				1);
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
			tcpBoundPort = tcpServer.GetBoundPort();
		}

		public virtual void Start(bool register)
		{
			StartUDPServer();
			StartTCPServer();
			if (register)
			{
				ShutdownHookManager.Get().AddShutdownHook(new MountdBase.Unregister(this), ShutdownHookPriority
					);
				try
				{
					rpcProgram.Register(PortmapMapping.TransportUdp, udpBoundPort);
					rpcProgram.Register(PortmapMapping.TransportTcp, tcpBoundPort);
				}
				catch (Exception e)
				{
					Log.Fatal("Failed to register the MOUNT service.", e);
					ExitUtil.Terminate(1, e);
				}
			}
		}

		/// <summary>Priority of the mountd shutdown hook.</summary>
		public const int ShutdownHookPriority = 10;

		private class Unregister : Runnable
		{
			public virtual void Run()
			{
				lock (this)
				{
					this._enclosing.rpcProgram.Unregister(PortmapMapping.TransportUdp, this._enclosing
						.udpBoundPort);
					this._enclosing.rpcProgram.Unregister(PortmapMapping.TransportTcp, this._enclosing
						.tcpBoundPort);
				}
			}

			internal Unregister(MountdBase _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MountdBase _enclosing;
		}
	}
}
