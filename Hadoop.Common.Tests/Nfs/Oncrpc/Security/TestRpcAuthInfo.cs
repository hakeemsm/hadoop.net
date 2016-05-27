using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc.Security
{
	/// <summary>
	/// Tests for
	/// <see cref="RpcAuthInfo"/>
	/// </summary>
	public class TestRpcAuthInfo
	{
		[NUnit.Framework.Test]
		public virtual void TestAuthFlavor()
		{
			NUnit.Framework.Assert.AreEqual(RpcAuthInfo.AuthFlavor.AuthNone, RpcAuthInfo.AuthFlavor
				.FromValue(0));
			NUnit.Framework.Assert.AreEqual(RpcAuthInfo.AuthFlavor.AuthSys, RpcAuthInfo.AuthFlavor
				.FromValue(1));
			NUnit.Framework.Assert.AreEqual(RpcAuthInfo.AuthFlavor.AuthShort, RpcAuthInfo.AuthFlavor
				.FromValue(2));
			NUnit.Framework.Assert.AreEqual(RpcAuthInfo.AuthFlavor.AuthDh, RpcAuthInfo.AuthFlavor
				.FromValue(3));
			NUnit.Framework.Assert.AreEqual(RpcAuthInfo.AuthFlavor.RpcsecGss, RpcAuthInfo.AuthFlavor
				.FromValue(6));
		}

		public virtual void TestInvalidAuthFlavor()
		{
			NUnit.Framework.Assert.AreEqual(RpcAuthInfo.AuthFlavor.AuthNone, RpcAuthInfo.AuthFlavor
				.FromValue(4));
		}
	}
}
