using System.Net;
using System.Net.Sockets;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Daemon;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Utility class to start a datanode in a secure cluster, first obtaining
	/// privileged resources before main startup and handing them to the datanode.
	/// </summary>
	public class SecureDataNodeStarter : Org.Apache.Commons.Daemon.Daemon
	{
		/// <summary>Stash necessary resources needed for datanode operation in a secure env.
		/// 	</summary>
		public class SecureResources
		{
			private readonly Socket streamingSocket;

			private readonly ServerSocketChannel httpServerSocket;

			public SecureResources(Socket streamingSocket, ServerSocketChannel httpServerSocket
				)
			{
				this.streamingSocket = streamingSocket;
				this.httpServerSocket = httpServerSocket;
			}

			public virtual Socket GetStreamingSocket()
			{
				return streamingSocket;
			}

			public virtual ServerSocketChannel GetHttpServerChannel()
			{
				return httpServerSocket;
			}
		}

		private string[] args;

		private SecureDataNodeStarter.SecureResources resources;

		/// <exception cref="System.Exception"/>
		public virtual void Init(DaemonContext context)
		{
			System.Console.Error.WriteLine("Initializing secure datanode resources");
			// Create a new HdfsConfiguration object to ensure that the configuration in
			// hdfs-site.xml is picked up.
			Configuration conf = new HdfsConfiguration();
			// Stash command-line arguments for regular datanode
			args = context.GetArguments();
			resources = GetSecureResources(conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void Start()
		{
			System.Console.Error.WriteLine("Starting regular datanode initialization");
			DataNode.SecureMain(args, resources);
		}

		public virtual void Destroy()
		{
		}

		/// <exception cref="System.Exception"/>
		public virtual void Stop()
		{
		}

		/* Nothing to do */
		/// <summary>
		/// Acquire privileged resources (i.e., the privileged ports) for the data
		/// node.
		/// </summary>
		/// <remarks>
		/// Acquire privileged resources (i.e., the privileged ports) for the data
		/// node. The privileged resources consist of the port of the RPC server and
		/// the port of HTTP (not HTTPS) server.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		public static SecureDataNodeStarter.SecureResources GetSecureResources(Configuration
			 conf)
		{
			HttpConfig.Policy policy = DFSUtil.GetHttpPolicy(conf);
			bool isSecure = UserGroupInformation.IsSecurityEnabled();
			// Obtain secure port for data streaming to datanode
			IPEndPoint streamingAddr = DataNode.GetStreamingAddr(conf);
			int socketWriteTimeout = conf.GetInt(DFSConfigKeys.DfsDatanodeSocketWriteTimeoutKey
				, HdfsServerConstants.WriteTimeout);
			Socket ss = (socketWriteTimeout > 0) ? ServerSocketChannel.Open().Socket() : new 
				Socket();
			ss.Bind(streamingAddr, 0);
			// Check that we got the port we need
			if (ss.GetLocalPort() != streamingAddr.Port)
			{
				throw new RuntimeException("Unable to bind on specified streaming port in secure "
					 + "context. Needed " + streamingAddr.Port + ", got " + ss.GetLocalPort());
			}
			if (!SecurityUtil.IsPrivilegedPort(ss.GetLocalPort()) && isSecure)
			{
				throw new RuntimeException("Cannot start secure datanode with unprivileged RPC ports"
					);
			}
			System.Console.Error.WriteLine("Opened streaming server at " + streamingAddr);
			// Bind a port for the web server. The code intends to bind HTTP server to
			// privileged port only, as the client can authenticate the server using
			// certificates if they are communicating through SSL.
			ServerSocketChannel httpChannel;
			if (policy.IsHttpEnabled())
			{
				httpChannel = ServerSocketChannel.Open();
				IPEndPoint infoSocAddr = DataNode.GetInfoAddr(conf);
				httpChannel.Socket().Bind(infoSocAddr);
				IPEndPoint localAddr = (IPEndPoint)httpChannel.Socket().LocalEndPoint;
				if (localAddr.Port != infoSocAddr.Port)
				{
					throw new RuntimeException("Unable to bind on specified info port in secure " + "context. Needed "
						 + streamingAddr.Port + ", got " + ss.GetLocalPort());
				}
				System.Console.Error.WriteLine("Successfully obtained privileged resources (streaming port = "
					 + ss + " ) (http listener port = " + localAddr.Port + ")");
				if (localAddr.Port > 1023 && isSecure)
				{
					throw new RuntimeException("Cannot start secure datanode with unprivileged HTTP ports"
						);
				}
				System.Console.Error.WriteLine("Opened info server at " + infoSocAddr);
			}
			else
			{
				httpChannel = null;
			}
			return new SecureDataNodeStarter.SecureResources(ss, httpChannel);
		}
	}
}
