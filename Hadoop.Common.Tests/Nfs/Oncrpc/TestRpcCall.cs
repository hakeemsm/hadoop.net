using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc
{
	/// <summary>
	/// Tests for
	/// <see cref="RpcCall"/>
	/// </summary>
	public class TestRpcCall
	{
		[NUnit.Framework.Test]
		public virtual void TestConstructor()
		{
			Credentials credential = new CredentialsNone();
			Verifier verifier = new VerifierNone();
			int rpcVersion = RpcCall.RpcVersion;
			int program = 2;
			int version = 3;
			int procedure = 4;
			RpcCall call = new RpcCall(0, RpcMessage.Type.RpcCall, rpcVersion, program, version
				, procedure, credential, verifier);
			NUnit.Framework.Assert.AreEqual(0, call.GetXid());
			NUnit.Framework.Assert.AreEqual(RpcMessage.Type.RpcCall, call.GetMessageType());
			NUnit.Framework.Assert.AreEqual(rpcVersion, call.GetRpcVersion());
			NUnit.Framework.Assert.AreEqual(program, call.GetProgram());
			NUnit.Framework.Assert.AreEqual(version, call.GetVersion());
			NUnit.Framework.Assert.AreEqual(procedure, call.GetProcedure());
			NUnit.Framework.Assert.AreEqual(credential, call.GetCredential());
			NUnit.Framework.Assert.AreEqual(verifier, call.GetVerifier());
		}

		public virtual void TestInvalidRpcVersion()
		{
			int invalidRpcVersion = 3;
			new RpcCall(0, RpcMessage.Type.RpcCall, invalidRpcVersion, 2, 3, 4, null, null);
		}

		public virtual void TestInvalidRpcMessageType()
		{
			RpcMessage.Type invalidMessageType = RpcMessage.Type.RpcReply;
			// Message typ is not RpcMessage.RPC_CALL
			new RpcCall(0, invalidMessageType, RpcCall.RpcVersion, 2, 3, 4, null, null);
		}
	}
}
