using NUnit.Framework;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>
	/// Test for
	/// <see cref="RpcDeniedReply"/>
	/// </summary>
	public class TestRpcDeniedReply
	{
		[Fact]
		public virtual void TestRejectStateFromValue()
		{
			Assert.Equal(RpcDeniedReply.RejectState.RpcMismatch, RpcDeniedReply.RejectState
				.FromValue(0));
			Assert.Equal(RpcDeniedReply.RejectState.AuthError, RpcDeniedReply.RejectState
				.FromValue(1));
		}

		public virtual void TestRejectStateFromInvalidValue1()
		{
			RpcDeniedReply.RejectState.FromValue(2);
		}

		[Fact]
		public virtual void TestConstructor()
		{
			RpcDeniedReply reply = new RpcDeniedReply(0, RpcReply.ReplyState.MsgAccepted, RpcDeniedReply.RejectState
				.AuthError, new VerifierNone());
			Assert.Equal(0, reply.GetXid());
			Assert.Equal(RpcMessage.Type.RpcReply, reply.GetMessageType());
			Assert.Equal(RpcReply.ReplyState.MsgAccepted, reply.GetState()
				);
			Assert.Equal(RpcDeniedReply.RejectState.AuthError, reply.GetRejectState
				());
		}
	}
}
