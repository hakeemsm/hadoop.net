using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>
	/// Test for
	/// <see cref="RpcAcceptedReply"/>
	/// </summary>
	public class TestRpcAcceptedReply
	{
		[Fact]
		public virtual void TestAcceptState()
		{
			Assert.Equal(RpcAcceptedReply.AcceptState.Success, RpcAcceptedReply.AcceptState
				.FromValue(0));
			Assert.Equal(RpcAcceptedReply.AcceptState.ProgUnavail, RpcAcceptedReply.AcceptState
				.FromValue(1));
			Assert.Equal(RpcAcceptedReply.AcceptState.ProgMismatch, RpcAcceptedReply.AcceptState
				.FromValue(2));
			Assert.Equal(RpcAcceptedReply.AcceptState.ProcUnavail, RpcAcceptedReply.AcceptState
				.FromValue(3));
			Assert.Equal(RpcAcceptedReply.AcceptState.GarbageArgs, RpcAcceptedReply.AcceptState
				.FromValue(4));
			Assert.Equal(RpcAcceptedReply.AcceptState.SystemErr, RpcAcceptedReply.AcceptState
				.FromValue(5));
		}

		public virtual void TestAcceptStateFromInvalidValue()
		{
			RpcAcceptedReply.AcceptState.FromValue(6);
		}

		[Fact]
		public virtual void TestConstructor()
		{
			Verifier verifier = new VerifierNone();
			RpcAcceptedReply reply = new RpcAcceptedReply(0, RpcReply.ReplyState.MsgAccepted, 
				verifier, RpcAcceptedReply.AcceptState.Success);
			Assert.Equal(0, reply.GetXid());
			Assert.Equal(RpcMessage.Type.RpcReply, reply.GetMessageType());
			Assert.Equal(RpcReply.ReplyState.MsgAccepted, reply.GetState()
				);
			Assert.Equal(verifier, reply.GetVerifier());
			Assert.Equal(RpcAcceptedReply.AcceptState.Success, reply.GetAcceptState
				());
		}
	}
}
