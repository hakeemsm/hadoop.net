using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Hdfs.Nfs.Nfs3;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Nfs.Nfs3.Request;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Org.Jboss.Netty.Buffer;
using Org.Jboss.Netty.Channel;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs
{
	public class TestOutOfOrderWrite
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestOutOfOrderWrite));

		internal static FileHandle handle = null;

		internal static Org.Jboss.Netty.Channel.Channel channel;

		internal static byte[] data1 = new byte[1000];

		internal static byte[] data2 = new byte[1000];

		internal static byte[] data3 = new byte[1000];

		internal static XDR Create()
		{
			XDR request = new XDR();
			RpcCall.GetInstance(unchecked((int)(0x8000004c)), Nfs3Constant.Program, Nfs3Constant
				.Version, Nfs3Constant.NFSPROC3.Create.GetValue(), new CredentialsNone(), new VerifierNone
				()).Write(request);
			SetAttr3 objAttr = new SetAttr3();
			CREATE3Request createReq = new CREATE3Request(new FileHandle("/"), "out-of-order-write"
				 + Runtime.CurrentTimeMillis(), 0, objAttr, 0);
			createReq.Serialize(request);
			return request;
		}

		internal static XDR Write(FileHandle handle, int xid, long offset, int count, byte
			[] data)
		{
			XDR request = new XDR();
			RpcCall.GetInstance(xid, Nfs3Constant.Program, Nfs3Constant.Version, Nfs3Constant.NFSPROC3
				.Create.GetValue(), new CredentialsNone(), new VerifierNone()).Write(request);
			WRITE3Request write1 = new WRITE3Request(handle, offset, count, Nfs3Constant.WriteStableHow
				.Unstable, ByteBuffer.Wrap(data));
			write1.Serialize(request);
			return request;
		}

		internal static void TestRequest(XDR request)
		{
			RegistrationClient registrationClient = new RegistrationClient("localhost", Nfs3Constant
				.SunRpcbind, request);
			registrationClient.Run();
		}

		internal class WriteHandler : SimpleTcpClientHandler
		{
			public WriteHandler(XDR request)
				: base(request)
			{
			}

			public override void MessageReceived(ChannelHandlerContext ctx, MessageEvent e)
			{
				// Get handle from create response
				ChannelBuffer buf = (ChannelBuffer)e.GetMessage();
				XDR rsp = new XDR(buf.Array());
				if (rsp.GetBytes().Length == 0)
				{
					Log.Info("rsp length is zero, why?");
					return;
				}
				Log.Info("rsp length=" + rsp.GetBytes().Length);
				RpcReply reply = RpcReply.Read(rsp);
				int xid = reply.GetXid();
				// Only process the create response
				if (xid != unchecked((int)(0x8000004c)))
				{
					return;
				}
				int status = rsp.ReadInt();
				if (status != Nfs3Status.Nfs3Ok)
				{
					Log.Error("Create failed, status =" + status);
					return;
				}
				Log.Info("Create succeeded");
				rsp.ReadBoolean();
				// value follow
				handle = new FileHandle();
				handle.Deserialize(rsp);
				channel = e.GetChannel();
			}
		}

		internal class WriteClient : SimpleTcpClient
		{
			public WriteClient(string host, int port, XDR request, bool oneShot)
				: base(host, port, request, oneShot)
			{
			}

			protected override ChannelPipelineFactory SetPipelineFactory()
			{
				this.pipelineFactory = new _ChannelPipelineFactory_139(this);
				return this.pipelineFactory;
			}

			private sealed class _ChannelPipelineFactory_139 : ChannelPipelineFactory
			{
				public _ChannelPipelineFactory_139(WriteClient _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public ChannelPipeline GetPipeline()
				{
					return Channels.Pipeline(RpcUtil.ConstructRpcFrameDecoder(), new TestOutOfOrderWrite.WriteHandler
						(this._enclosing.request));
				}

				private readonly WriteClient _enclosing;
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			Arrays.Fill(data1, unchecked((byte)7));
			Arrays.Fill(data2, unchecked((byte)8));
			Arrays.Fill(data3, unchecked((byte)9));
			// NFS3 Create request
			NfsConfiguration conf = new NfsConfiguration();
			TestOutOfOrderWrite.WriteClient client = new TestOutOfOrderWrite.WriteClient("localhost"
				, conf.GetInt(NfsConfigKeys.DfsNfsServerPortKey, NfsConfigKeys.DfsNfsServerPortDefault
				), Create(), false);
			client.Run();
			while (handle == null)
			{
				Sharpen.Thread.Sleep(1000);
				System.Console.Out.WriteLine("handle is still null...");
			}
			Log.Info("Send write1 request");
			XDR writeReq;
			writeReq = Write(handle, unchecked((int)(0x8000005c)), 2000, 1000, data3);
			Nfs3Utils.WriteChannel(channel, writeReq, 1);
			writeReq = Write(handle, unchecked((int)(0x8000005d)), 1000, 1000, data2);
			Nfs3Utils.WriteChannel(channel, writeReq, 2);
			writeReq = Write(handle, unchecked((int)(0x8000005e)), 0, 1000, data1);
			Nfs3Utils.WriteChannel(channel, writeReq, 3);
		}
		// TODO: convert to Junit test, and validate result automatically
	}
}
