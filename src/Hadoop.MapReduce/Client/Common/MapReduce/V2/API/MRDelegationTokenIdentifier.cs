using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api
{
	/// <summary>
	/// <see cref="Org.Apache.Hadoop.Security.Token.TokenIdentifier"/>
	/// that identifies delegation tokens
	/// issued by JobHistoryServer to delegate
	/// MR tasks talking to the JobHistoryServer.
	/// </summary>
	public class MRDelegationTokenIdentifier : AbstractDelegationTokenIdentifier
	{
		public static readonly Text KindName = new Text("MR_DELEGATION_TOKEN");

		public MRDelegationTokenIdentifier()
		{
		}

		/// <summary>Create a new delegation token identifier</summary>
		/// <param name="owner">the effective username of the token owner</param>
		/// <param name="renewer">the username of the renewer</param>
		/// <param name="realUser">the real username of the token owner</param>
		public MRDelegationTokenIdentifier(Text owner, Text renewer, Text realUser)
			: base(owner, renewer, realUser)
		{
		}

		// TODO Move to a different package.
		public override Text GetKind()
		{
			return KindName;
		}

		public class Renewer : Token.TrivialRenewer
		{
			protected override Text GetKind()
			{
				return KindName;
			}
		}
	}
}
