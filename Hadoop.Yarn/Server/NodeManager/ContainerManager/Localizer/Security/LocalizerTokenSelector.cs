using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Security
{
	public class LocalizerTokenSelector : TokenSelector<LocalizerTokenIdentifier>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(LocalizerTokenSelector
			));

		public virtual Org.Apache.Hadoop.Security.Token.Token<LocalizerTokenIdentifier> SelectToken
			(Text service, ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier
			>> tokens)
		{
			Log.Debug("Using localizerTokenSelector.");
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in tokens)
			{
				Log.Debug("Token of kind " + token.GetKind() + " is found");
				if (LocalizerTokenIdentifier.Kind.Equals(token.GetKind()))
				{
					return (Org.Apache.Hadoop.Security.Token.Token<LocalizerTokenIdentifier>)token;
				}
			}
			Log.Debug("Returning null.");
			return null;
		}
	}
}
