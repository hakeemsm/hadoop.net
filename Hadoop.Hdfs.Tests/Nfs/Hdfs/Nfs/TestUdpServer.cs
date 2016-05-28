using System.IO;
using System.Net;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs
{
	public class TestUdpServer
	{
		// TODO: convert this to Junit
		internal static void TestRequest(XDR request, XDR request2)
		{
			try
			{
				DatagramSocket clientSocket = new DatagramSocket();
				IPAddress IPAddress = Sharpen.Extensions.GetAddressByName("localhost");
				byte[] sendData = request.GetBytes();
				byte[] receiveData = new byte[65535];
				DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.Length, IPAddress
					, Nfs3Constant.SunRpcbind);
				clientSocket.Send(sendPacket);
				DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.Length
					);
				clientSocket.Receive(receivePacket);
				clientSocket.Close();
			}
			catch (UnknownHostException)
			{
				System.Console.Error.WriteLine("Don't know about host: localhost.");
				System.Environment.Exit(1);
			}
			catch (IOException)
			{
				System.Console.Error.WriteLine("Couldn't get I/O for " + "the connection to: localhost."
					);
				System.Environment.Exit(1);
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			Sharpen.Thread t1 = new TestUdpServer.Runtest1();
			// TODO: cleanup
			//Thread t2 = new Runtest2();
			t1.Start();
			//t2.start();
			t1.Join();
		}

		internal class Runtest1 : Sharpen.Thread
		{
			//t2.join();
			//testDump();
			public override void Run()
			{
				TestGetportMount();
			}
		}

		internal class Runtest2 : Sharpen.Thread
		{
			public override void Run()
			{
				TestDump();
			}
		}

		internal static void CreatePortmapXDRheader(XDR xdr_out, int procedure)
		{
			// Make this a method
			RpcCall.GetInstance(0, 100000, 2, procedure, new CredentialsNone(), new VerifierNone
				()).Write(xdr_out);
		}

		internal static void TestGetportMount()
		{
			XDR xdr_out = new XDR();
			CreatePortmapXDRheader(xdr_out, 3);
			xdr_out.WriteInt(100005);
			xdr_out.WriteInt(1);
			xdr_out.WriteInt(6);
			xdr_out.WriteInt(0);
			XDR request2 = new XDR();
			CreatePortmapXDRheader(xdr_out, 3);
			request2.WriteInt(100005);
			request2.WriteInt(1);
			request2.WriteInt(6);
			request2.WriteInt(0);
			TestRequest(xdr_out, request2);
		}

		internal static void TestGetport()
		{
			XDR xdr_out = new XDR();
			CreatePortmapXDRheader(xdr_out, 3);
			xdr_out.WriteInt(100003);
			xdr_out.WriteInt(3);
			xdr_out.WriteInt(6);
			xdr_out.WriteInt(0);
			XDR request2 = new XDR();
			CreatePortmapXDRheader(xdr_out, 3);
			request2.WriteInt(100003);
			request2.WriteInt(3);
			request2.WriteInt(6);
			request2.WriteInt(0);
			TestRequest(xdr_out, request2);
		}

		internal static void TestDump()
		{
			XDR xdr_out = new XDR();
			CreatePortmapXDRheader(xdr_out, 4);
			TestRequest(xdr_out, xdr_out);
		}
	}
}
