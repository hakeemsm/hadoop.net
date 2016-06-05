using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao
{
	public class CredentialsInfo
	{
		internal Dictionary<string, string> tokens;

		internal Dictionary<string, string> secrets;

		public CredentialsInfo()
		{
			tokens = new Dictionary<string, string>();
			secrets = new Dictionary<string, string>();
		}

		public virtual Dictionary<string, string> GetTokens()
		{
			return tokens;
		}

		public virtual Dictionary<string, string> GetSecrets()
		{
			return secrets;
		}

		public virtual void SetTokens(Dictionary<string, string> tokens)
		{
			this.tokens = tokens;
		}

		public virtual void SetSecrets(Dictionary<string, string> secrets)
		{
			this.secrets = secrets;
		}
	}
}
