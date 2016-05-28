using System.IO;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security.Token.Delegation
{
	/// <summary>A delegation token identifier that is specific to HDFS.</summary>
	public class DelegationTokenIdentifier : AbstractDelegationTokenIdentifier
	{
		public static readonly Text HdfsDelegationKind = new Text("HDFS_DELEGATION_TOKEN"
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
			return HdfsDelegationKind;
		}

		public override string ToString()
		{
			return GetKind() + " token " + GetSequenceNumber() + " for " + GetUser().GetShortUserName
				();
		}

		/// <returns>a string representation of the token</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string StringifyToken<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0
			> token)
			where _T0 : TokenIdentifier
		{
			Org.Apache.Hadoop.Hdfs.Security.Token.Delegation.DelegationTokenIdentifier ident = 
				new Org.Apache.Hadoop.Hdfs.Security.Token.Delegation.DelegationTokenIdentifier();
			ByteArrayInputStream buf = new ByteArrayInputStream(token.GetIdentifier());
			DataInputStream @in = new DataInputStream(buf);
			ident.ReadFields(@in);
			if (token.GetService().GetLength() > 0)
			{
				return ident + " on " + token.GetService();
			}
			else
			{
				return ident.ToString();
			}
		}

		public class WebHdfsDelegationTokenIdentifier : DelegationTokenIdentifier
		{
			public WebHdfsDelegationTokenIdentifier()
				: base()
			{
			}

			public override Text GetKind()
			{
				return WebHdfsFileSystem.TokenKind;
			}
		}

		public class SWebHdfsDelegationTokenIdentifier : DelegationTokenIdentifier.WebHdfsDelegationTokenIdentifier
		{
			public SWebHdfsDelegationTokenIdentifier()
				: base()
			{
			}

			public override Text GetKind()
			{
				return SWebHdfsFileSystem.TokenKind;
			}
		}
	}
}
