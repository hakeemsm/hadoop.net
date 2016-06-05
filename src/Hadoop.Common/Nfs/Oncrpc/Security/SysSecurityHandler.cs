using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Security;


namespace Org.Apache.Hadoop.Oncrpc.Security
{
	public class SysSecurityHandler : SecurityHandler
	{
		private readonly IdMappingServiceProvider iug;

		private readonly CredentialsSys mCredentialsSys;

		public SysSecurityHandler(CredentialsSys credentialsSys, IdMappingServiceProvider
			 iug)
		{
			this.mCredentialsSys = credentialsSys;
			this.iug = iug;
		}

		public override string GetUser()
		{
			return iug.GetUserName(mCredentialsSys.GetUID(), IdMappingConstant.UnknownUser);
		}

		public override bool ShouldSilentlyDrop(RpcCall request)
		{
			return false;
		}

		public override Verifier GetVerifer(RpcCall request)
		{
			return new VerifierNone();
		}

		public override int GetUid()
		{
			return mCredentialsSys.GetUID();
		}

		public override int GetGid()
		{
			return mCredentialsSys.GetGID();
		}

		public override int[] GetAuxGids()
		{
			return mCredentialsSys.GetAuxGIDs();
		}
	}
}
