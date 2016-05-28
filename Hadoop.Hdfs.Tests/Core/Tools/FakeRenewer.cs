using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Tools
{
	public class FakeRenewer : TokenRenewer
	{
		internal static Org.Apache.Hadoop.Security.Token.Token<object> lastRenewed = null;

		internal static Org.Apache.Hadoop.Security.Token.Token<object> lastCanceled = null;

		internal static readonly Text Kind = new Text("TESTING-TOKEN-KIND");

		public override bool HandleKind(Text kind)
		{
			return FakeRenewer.Kind.Equals(kind);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool IsManaged<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
			)
		{
			return true;
		}

		public override long Renew<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
			, Configuration conf)
		{
			lastRenewed = token;
			return 0;
		}

		public override void Cancel<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
			, Configuration conf)
		{
			lastCanceled = token;
		}

		public static void Reset()
		{
			lastRenewed = null;
			lastCanceled = null;
		}
	}
}
