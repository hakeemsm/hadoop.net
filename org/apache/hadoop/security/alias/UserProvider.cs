using Sharpen;

namespace org.apache.hadoop.security.alias
{
	/// <summary>A CredentialProvider for UGIs.</summary>
	/// <remarks>
	/// A CredentialProvider for UGIs. It uses the credentials object associated
	/// with the current user to find credentials. This provider is created using a
	/// URI of "user:///".
	/// </remarks>
	public class UserProvider : org.apache.hadoop.security.alias.CredentialProvider
	{
		public const string SCHEME_NAME = "user";

		private readonly org.apache.hadoop.security.UserGroupInformation user;

		private readonly org.apache.hadoop.security.Credentials credentials;

		/// <exception cref="System.IO.IOException"/>
		private UserProvider()
		{
			user = org.apache.hadoop.security.UserGroupInformation.getCurrentUser();
			credentials = user.getCredentials();
		}

		public override bool isTransient()
		{
			return true;
		}

		public override org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry
			 getCredentialEntry(string alias)
		{
			byte[] bytes = credentials.getSecretKey(new org.apache.hadoop.io.Text(alias));
			if (bytes == null)
			{
				return null;
			}
			return new org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry(alias
				, new string(bytes, org.apache.commons.io.Charsets.UTF_8).ToCharArray());
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry
			 createCredentialEntry(string name, char[] credential)
		{
			org.apache.hadoop.io.Text nameT = new org.apache.hadoop.io.Text(name);
			if (credentials.getSecretKey(nameT) != null)
			{
				throw new System.IO.IOException("Credential " + name + " already exists in " + this
					);
			}
			credentials.addSecretKey(new org.apache.hadoop.io.Text(name), Sharpen.Runtime.getBytesForString
				(new string(credential), "UTF-8"));
			return new org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry(name
				, credential);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void deleteCredentialEntry(string name)
		{
			byte[] cred = credentials.getSecretKey(new org.apache.hadoop.io.Text(name));
			if (cred != null)
			{
				credentials.removeSecretKey(new org.apache.hadoop.io.Text(name));
			}
			else
			{
				throw new System.IO.IOException("Credential " + name + " does not exist in " + this
					);
			}
		}

		public override string ToString()
		{
			return SCHEME_NAME + ":///";
		}

		public override void flush()
		{
			user.addCredentials(credentials);
		}

		public class Factory : org.apache.hadoop.security.alias.CredentialProviderFactory
		{
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.security.alias.CredentialProvider createProvider
				(java.net.URI providerName, org.apache.hadoop.conf.Configuration conf)
			{
				if (SCHEME_NAME.Equals(providerName.getScheme()))
				{
					return new org.apache.hadoop.security.alias.UserProvider();
				}
				return null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> getAliases()
		{
			System.Collections.Generic.IList<string> list = new System.Collections.Generic.List
				<string>();
			System.Collections.Generic.IList<org.apache.hadoop.io.Text> aliases = credentials
				.getAllSecretKeys();
			foreach (org.apache.hadoop.io.Text key in aliases)
			{
				list.add(key.ToString());
			}
			return list;
		}
	}
}
