using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;


namespace Org.Apache.Hadoop.Security.Alias
{
	/// <summary>A CredentialProvider for UGIs.</summary>
	/// <remarks>
	/// A CredentialProvider for UGIs. It uses the credentials object associated
	/// with the current user to find credentials. This provider is created using a
	/// URI of "user:///".
	/// </remarks>
	public class UserProvider : CredentialProvider
	{
		public const string SchemeName = "user";

		private readonly UserGroupInformation user;

		private readonly Credentials credentials;

		/// <exception cref="System.IO.IOException"/>
		private UserProvider()
		{
			user = UserGroupInformation.GetCurrentUser();
			credentials = user.GetCredentials();
		}

		public override bool IsTransient()
		{
			return true;
		}

		public override CredentialProvider.CredentialEntry GetCredentialEntry(string alias
			)
		{
			byte[] bytes = credentials.GetSecretKey(new Text(alias));
			if (bytes == null)
			{
				return null;
			}
			return new CredentialProvider.CredentialEntry(alias, new string(bytes, Charsets.Utf8
				).ToCharArray());
		}

		/// <exception cref="System.IO.IOException"/>
		public override CredentialProvider.CredentialEntry CreateCredentialEntry(string name
			, char[] credential)
		{
			Text nameT = new Text(name);
			if (credentials.GetSecretKey(nameT) != null)
			{
				throw new IOException("Credential " + name + " already exists in " + this);
			}
			credentials.AddSecretKey(new Text(name), Runtime.GetBytesForString(new string
				(credential), "UTF-8"));
			return new CredentialProvider.CredentialEntry(name, credential);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteCredentialEntry(string name)
		{
			byte[] cred = credentials.GetSecretKey(new Text(name));
			if (cred != null)
			{
				credentials.RemoveSecretKey(new Text(name));
			}
			else
			{
				throw new IOException("Credential " + name + " does not exist in " + this);
			}
		}

		public override string ToString()
		{
			return SchemeName + ":///";
		}

		public override void Flush()
		{
			user.AddCredentials(credentials);
		}

		public class Factory : CredentialProviderFactory
		{
			/// <exception cref="System.IO.IOException"/>
			public override CredentialProvider CreateProvider(URI providerName, Configuration
				 conf)
			{
				if (SchemeName.Equals(providerName.GetScheme()))
				{
					return new UserProvider();
				}
				return null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> GetAliases()
		{
			IList<string> list = new AList<string>();
			IList<Text> aliases = credentials.GetAllSecretKeys();
			foreach (Text key in aliases)
			{
				list.AddItem(key.ToString());
			}
			return list;
		}
	}
}
