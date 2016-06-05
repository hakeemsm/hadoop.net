using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Oncrpc.Security;
using Org.Apache.Log4j;
using Org.Jboss.Netty.Buffer;
using Org.Jboss.Netty.Channel;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	public class TestFrameDecoder
	{
		static TestFrameDecoder()
		{
			((Log4JLogger)RpcProgram.Log).GetLogger().SetLevel(Level.All);
		}

		private static int resultSize;

		internal static void TestRequest(XDR request, int serverPort)
		{
			// Reset resultSize so as to avoid interference from other tests in this class.
			resultSize = 0;
			SimpleTcpClient tcpClient = new SimpleTcpClient("localhost", serverPort, request, 
				true);
			tcpClient.Run();
		}

		internal class TestRpcProgram : RpcProgram
		{
			protected internal TestRpcProgram(string program, string host, int port, int progNumber
				, int lowProgVersion, int highProgVersion, bool allowInsecurePorts)
				: base(program, host, port, progNumber, lowProgVersion, highProgVersion, null, allowInsecurePorts
					)
			{
			}

			protected internal override void HandleInternal(ChannelHandlerContext ctx, RpcInfo
				 info)
			{
				// This is just like what's done in RpcProgramMountd#handleInternal and
				// RpcProgramNfs3#handleInternal.
				RpcCall rpcCall = (RpcCall)info.Header();
				int procedure = rpcCall.GetProcedure();
				if (procedure != 0)
				{
					bool portMonitorSuccess = DoPortMonitoring(info.RemoteAddress());
					if (!portMonitorSuccess)
					{
						SendRejectedReply(rpcCall, info.RemoteAddress(), ctx);
						return;
					}
				}
				resultSize = info.Data().ReadableBytes();
				RpcAcceptedReply reply = RpcAcceptedReply.GetAcceptInstance(1234, new VerifierNone
					());
				XDR @out = new XDR();
				reply.Write(@out);
				ChannelBuffer b = ChannelBuffers.WrappedBuffer(@out.AsReadOnlyWrap().Buffer());
				RpcResponse rsp = new RpcResponse(b, info.RemoteAddress());
				RpcUtil.SendRpcResponse(ctx, rsp);
			}

			protected internal override bool IsIdempotent(RpcCall call)
			{
				return false;
			}
		}

		[Fact]
		public virtual void TestSingleFrame()
		{
			RpcUtil.RpcFrameDecoder decoder = new RpcUtil.RpcFrameDecoder();
			// Test "Length field is not received yet"
			ByteBuffer buffer = ByteBuffer.Allocate(1);
			ChannelBuffer buf = new ByteBufferBackedChannelBuffer(buffer);
			ChannelBuffer channelBuffer = (ChannelBuffer)decoder.Decode(Org.Mockito.Mockito.Mock
				<ChannelHandlerContext>(), Org.Mockito.Mockito.Mock<Org.Jboss.Netty.Channel.Channel
				>(), buf);
			Assert.True(channelBuffer == null);
			// Test all bytes are not received yet
			byte[] fragment = new byte[4 + 9];
			fragment[0] = unchecked((byte)(1 << 7));
			// final fragment
			fragment[1] = 0;
			fragment[2] = 0;
			fragment[3] = unchecked((byte)10);
			// fragment size = 10 bytes
			Assert.True(XDR.IsLastFragment(fragment));
			Assert.True(XDR.FragmentSize(fragment) == 10);
			buffer = ByteBuffer.Allocate(4 + 9);
			buffer.Put(fragment);
			buffer.Flip();
			buf = new ByteBufferBackedChannelBuffer(buffer);
			channelBuffer = (ChannelBuffer)decoder.Decode(Org.Mockito.Mockito.Mock<ChannelHandlerContext
				>(), Org.Mockito.Mockito.Mock<Org.Jboss.Netty.Channel.Channel>(), buf);
			Assert.True(channelBuffer == null);
		}

		[Fact]
		public virtual void TestMultipleFrames()
		{
			RpcUtil.RpcFrameDecoder decoder = new RpcUtil.RpcFrameDecoder();
			// Test multiple frames
			byte[] fragment1 = new byte[4 + 10];
			fragment1[0] = 0;
			// not final fragment
			fragment1[1] = 0;
			fragment1[2] = 0;
			fragment1[3] = unchecked((byte)10);
			// fragment size = 10 bytes
			NUnit.Framework.Assert.IsFalse(XDR.IsLastFragment(fragment1));
			Assert.True(XDR.FragmentSize(fragment1) == 10);
			// decoder should wait for the final fragment
			ByteBuffer buffer = ByteBuffer.Allocate(4 + 10);
			buffer.Put(fragment1);
			buffer.Flip();
			ChannelBuffer buf = new ByteBufferBackedChannelBuffer(buffer);
			ChannelBuffer channelBuffer = (ChannelBuffer)decoder.Decode(Org.Mockito.Mockito.Mock
				<ChannelHandlerContext>(), Org.Mockito.Mockito.Mock<Org.Jboss.Netty.Channel.Channel
				>(), buf);
			Assert.True(channelBuffer == null);
			byte[] fragment2 = new byte[4 + 10];
			fragment2[0] = unchecked((byte)(1 << 7));
			// final fragment
			fragment2[1] = 0;
			fragment2[2] = 0;
			fragment2[3] = unchecked((byte)10);
			// fragment size = 10 bytes
			Assert.True(XDR.IsLastFragment(fragment2));
			Assert.True(XDR.FragmentSize(fragment2) == 10);
			buffer = ByteBuffer.Allocate(4 + 10);
			buffer.Put(fragment2);
			buffer.Flip();
			buf = new ByteBufferBackedChannelBuffer(buffer);
			channelBuffer = (ChannelBuffer)decoder.Decode(Org.Mockito.Mockito.Mock<ChannelHandlerContext
				>(), Org.Mockito.Mockito.Mock<Org.Jboss.Netty.Channel.Channel>(), buf);
			Assert.True(channelBuffer != null);
			// Complete frame should have to total size 10+10=20
			Assert.Equal(20, channelBuffer.ReadableBytes());
		}

		[Fact]
		public virtual void TestFrames()
		{
			int serverPort = StartRpcServer(true);
			XDR xdrOut = CreateGetportMount();
			int headerSize = xdrOut.Size();
			int bufsize = 2 * 1024 * 1024;
			byte[] buffer = new byte[bufsize];
			xdrOut.WriteFixedOpaque(buffer);
			int requestSize = xdrOut.Size() - headerSize;
			// Send the request to the server
			TestRequest(xdrOut, serverPort);
			// Verify the server got the request with right size
			Assert.Equal(requestSize, resultSize);
		}

		[Fact]
		public virtual void TestUnprivilegedPort()
		{
			// Don't allow connections from unprivileged ports. Given that this test is
			// presumably not being run by root, this will be the case.
			int serverPort = StartRpcServer(false);
			XDR xdrOut = CreateGetportMount();
			int bufsize = 2 * 1024 * 1024;
			byte[] buffer = new byte[bufsize];
			xdrOut.WriteFixedOpaque(buffer);
			// Send the request to the server
			TestRequest(xdrOut, serverPort);
			// Verify the server rejected the request.
			Assert.Equal(0, resultSize);
			// Ensure that the NULL procedure does in fact succeed.
			xdrOut = new XDR();
			CreatePortmapXDRheader(xdrOut, 0);
			int headerSize = xdrOut.Size();
			buffer = new byte[bufsize];
			xdrOut.WriteFixedOpaque(buffer);
			int requestSize = xdrOut.Size() - headerSize;
			// Send the request to the server
			TestRequest(xdrOut, serverPort);
			// Verify the server did not reject the request.
			Assert.Equal(requestSize, resultSize);
		}

		private static int StartRpcServer(bool allowInsecurePorts)
		{
			Random rand = new Random();
			int serverPort = 30000 + rand.Next(10000);
			int retries = 10;
			// A few retries in case initial choice is in use.
			while (true)
			{
				try
				{
					RpcProgram program = new TestFrameDecoder.TestRpcProgram("TestRpcProgram", "localhost"
						, serverPort, 100000, 1, 2, allowInsecurePorts);
					SimpleTcpServer tcpServer = new SimpleTcpServer(serverPort, program, 1);
					tcpServer.Run();
					break;
				}
				catch (ChannelException ce)
				{
					// Successfully bound a port, break out.
					if (retries-- > 0)
					{
						serverPort += rand.Next(20);
					}
					else
					{
						// Port in use? Try another.
						throw;
					}
				}
			}
			// Out of retries.
			return serverPort;
		}

		internal static void CreatePortmapXDRheader(XDR xdr_out, int procedure)
		{
			// Make this a method
			RpcCall.GetInstance(0, 100000, 2, procedure, new CredentialsNone(), new VerifierNone
				()).Write(xdr_out);
		}

		internal static XDR CreateGetportMount()
		{
			XDR xdr_out = new XDR();
			CreatePortmapXDRheader(xdr_out, 3);
			return xdr_out;
		}
		/*
		* static void testGetport() { XDR xdr_out = new XDR();
		*
		* createPortmapXDRheader(xdr_out, 3);
		*
		* xdr_out.writeInt(100003); xdr_out.writeInt(3); xdr_out.writeInt(6);
		* xdr_out.writeInt(0);
		*
		* XDR request2 = new XDR();
		*
		* createPortmapXDRheader(xdr_out, 3); request2.writeInt(100003);
		* request2.writeInt(3); request2.writeInt(6); request2.writeInt(0);
		*
		* testRequest(xdr_out); }
		*
		* static void testDump() { XDR xdr_out = new XDR();
		* createPortmapXDRheader(xdr_out, 4); testRequest(xdr_out); }
		*/
	}
}
