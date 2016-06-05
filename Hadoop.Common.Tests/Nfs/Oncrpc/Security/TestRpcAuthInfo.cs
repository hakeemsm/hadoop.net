

namespace Org.Apache.Hadoop.Oncrpc.Security
{
	/// <summary>
	/// Tests for
	/// <see cref="RpcAuthInfo"/>
	/// </summary>
	public class TestRpcAuthInfo
	{
		[Fact]
		public virtual void TestAuthFlavor()
		{
			Assert.Equal(RpcAuthInfo.AuthFlavor.AuthNone, RpcAuthInfo.AuthFlavor
				.FromValue(0));
			Assert.Equal(RpcAuthInfo.AuthFlavor.AuthSys, RpcAuthInfo.AuthFlavor
				.FromValue(1));
			Assert.Equal(RpcAuthInfo.AuthFlavor.AuthShort, RpcAuthInfo.AuthFlavor
				.FromValue(2));
			Assert.Equal(RpcAuthInfo.AuthFlavor.AuthDh, RpcAuthInfo.AuthFlavor
				.FromValue(3));
			Assert.Equal(RpcAuthInfo.AuthFlavor.RpcsecGss, RpcAuthInfo.AuthFlavor
				.FromValue(6));
		}

		public virtual void TestInvalidAuthFlavor()
		{
			Assert.Equal(RpcAuthInfo.AuthFlavor.AuthNone, RpcAuthInfo.AuthFlavor
				.FromValue(4));
		}
	}
}
