using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security.Client
{
	public class TimelineDelegationTokenIdentifier : YARNDelegationTokenIdentifier
	{
		public static readonly Text KindName = new Text("TIMELINE_DELEGATION_TOKEN");

		public TimelineDelegationTokenIdentifier()
		{
		}

		/// <summary>Create a new timeline delegation token identifier</summary>
		/// <param name="owner">the effective username of the token owner</param>
		/// <param name="renewer">the username of the renewer</param>
		/// <param name="realUser">the real username of the token owner</param>
		public TimelineDelegationTokenIdentifier(Text owner, Text renewer, Text realUser)
			: base(owner, renewer, realUser)
		{
		}

		public override Text GetKind()
		{
			return KindName;
		}

		public class Renewer : TokenRenewer
		{
			public override bool HandleKind(Text kind)
			{
				return KindName.Equals(kind);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool IsManaged<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				)
			{
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override long Renew<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				, Configuration conf)
			{
				TimelineClient client = TimelineClient.CreateTimelineClient();
				try
				{
					client.Init(conf);
					client.Start();
					return client.RenewDelegationToken((Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier
						>)token);
				}
				catch (YarnException e)
				{
					throw new IOException(e);
				}
				finally
				{
					client.Stop();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public override void Cancel<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
				, Configuration conf)
			{
				TimelineClient client = TimelineClient.CreateTimelineClient();
				try
				{
					client.Init(conf);
					client.Start();
					client.CancelDelegationToken((Org.Apache.Hadoop.Security.Token.Token<TimelineDelegationTokenIdentifier
						>)token);
				}
				catch (YarnException e)
				{
					throw new IOException(e);
				}
				finally
				{
					client.Stop();
				}
			}
		}
	}
}
