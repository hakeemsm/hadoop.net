using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security.Token.Delegation
{
	/// <summary>A delegation token identifier that is specific to MapReduce.</summary>
	public class DelegationTokenIdentifier : AbstractDelegationTokenIdentifier
	{
		public static readonly Text MapreduceDelegationKind = new Text("MAPREDUCE_DELEGATION_TOKEN"
			);

		/// <summary>Create an empty delegation token identifier for reading into.</summary>
		public DelegationTokenIdentifier()
		{
		}

		/// <summary>Create a new delegation token identifier</summary>
		/// <param name="owner">the effective username of the token owner</param>
		/// <param name="renewer">the username of the renewer</param>
		/// <param name="realUser">the real username of the token owner</param>
		public DelegationTokenIdentifier(Text owner, Text renewer, Text realUser)
			: base(owner, renewer, realUser)
		{
		}

		public override Text GetKind()
		{
			return MapreduceDelegationKind;
		}
	}
}
