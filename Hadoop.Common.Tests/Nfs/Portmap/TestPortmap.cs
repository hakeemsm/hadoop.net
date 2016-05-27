using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using NUnit.Framework;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Portmap
{
	public class TestPortmap
	{
		private static Org.Apache.Hadoop.Portmap.Portmap pm = new Org.Apache.Hadoop.Portmap.Portmap
			();

		private const int ShortTimeoutMilliseconds = 10;

		private const int RetryTimes = 5;

		private int xid;

		[BeforeClass]
		public static void Setup()
		{
			pm.Start(ShortTimeoutMilliseconds, new IPEndPoint("localhost", 0), new IPEndPoint
				("localhost", 0));
		}

		[AfterClass]
		public static void TearDown()
		{
			pm.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestIdle()
		{
			Socket s = new Socket();
			try
			{
				s.Connect(pm.GetTcpServerLocalAddress());
				int i = 0;
				while (!s.Connected && i < RetryTimes)
				{
					++i;
					Sharpen.Thread.Sleep(ShortTimeoutMilliseconds);
				}
				NUnit.Framework.Assert.IsTrue("Failed to connect to the server", s.Connected && i
					 < RetryTimes);
				int b = s.GetInputStream().Read();
				NUnit.Framework.Assert.IsTrue("The server failed to disconnect", b == -1);
			}
			finally
			{
				s.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestRegistration()
		{
			XDR req = new XDR();
			RpcCall.GetInstance(++xid, RpcProgramPortmap.Program, RpcProgramPortmap.Version, 
				RpcProgramPortmap.PmapprocSet, new CredentialsNone(), new VerifierNone()).Write(
				req);
			PortmapMapping sent = new PortmapMapping(90000, 1, PortmapMapping.TransportTcp, 1234
				);
			sent.Serialize(req);
			byte[] reqBuf = req.GetBytes();
			DatagramSocket s = new DatagramSocket();
			DatagramPacket p = new DatagramPacket(reqBuf, reqBuf.Length, pm.GetUdpServerLoAddress
				());
			try
			{
				s.Send(p);
			}
			finally
			{
				s.Close();
			}
			// Give the server a chance to process the request
			Sharpen.Thread.Sleep(100);
			bool found = false;
			IDictionary<string, PortmapMapping> map = (IDictionary<string, PortmapMapping>)Whitebox
				.GetInternalState(pm.GetHandler(), "map");
			foreach (PortmapMapping m in map.Values)
			{
				if (m.GetPort() == sent.GetPort() && PortmapMapping.Key(m).Equals(PortmapMapping.
					Key(sent)))
				{
					found = true;
					break;
				}
			}
			NUnit.Framework.Assert.IsTrue("Registration failed", found);
		}
	}
}
