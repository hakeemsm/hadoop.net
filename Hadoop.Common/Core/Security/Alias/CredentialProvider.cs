using System.Collections.Generic;
using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Alias
{
	/// <summary>A provider of credentials or password for Hadoop applications.</summary>
	/// <remarks>
	/// A provider of credentials or password for Hadoop applications. Provides an
	/// abstraction to separate credential storage from users of them. It
	/// is intended to support getting or storing passwords in a variety of ways,
	/// including third party bindings.
	/// </remarks>
	public abstract class CredentialProvider
	{
		public const string ClearTextFallback = "hadoop.security.credential.clear-text-fallback";

		/// <summary>The combination of both the alias and the actual credential value.</summary>
		public class CredentialEntry
		{
			private readonly string alias;

			private readonly char[] credential;

			protected internal CredentialEntry(string alias, char[] credential)
			{
				this.alias = alias;
				this.credential = credential;
			}

			public virtual string GetAlias()
			{
				return alias;
			}

			public virtual char[] GetCredential()
			{
				return credential;
			}

			public override string ToString()
			{
				StringBuilder buf = new StringBuilder();
				buf.Append("alias(");
				buf.Append(alias);
				buf.Append(")=");
				if (credential == null)
				{
					buf.Append("null");
				}
				else
				{
					foreach (char c in credential)
					{
						buf.Append(c);
					}
				}
				return buf.ToString();
			}
		}

		/// <summary>
		/// Indicates whether this provider represents a store
		/// that is intended for transient use - such as the UserProvider
		/// is.
		/// </summary>
		/// <remarks>
		/// Indicates whether this provider represents a store
		/// that is intended for transient use - such as the UserProvider
		/// is. These providers are generally used to provide job access to
		/// passwords rather than for long term storage.
		/// </remarks>
		/// <returns>true if transient, false otherwise</returns>
		public virtual bool IsTransient()
		{
			return false;
		}

		/// <summary>Ensures that any changes to the credentials are written to persistent store.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Flush();

		/// <summary>Get the credential entry for a specific alias.</summary>
		/// <param name="alias">the name of a specific credential</param>
		/// <returns>the credentialEntry</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract CredentialProvider.CredentialEntry GetCredentialEntry(string alias
			);

		/// <summary>Get the aliases for all credentials.</summary>
		/// <returns>the list of alias names</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<string> GetAliases();

		/// <summary>Create a new credential.</summary>
		/// <remarks>Create a new credential. The given alias must not already exist.</remarks>
		/// <param name="name">the alias of the credential</param>
		/// <param name="credential">the credential value for the alias.</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract CredentialProvider.CredentialEntry CreateCredentialEntry(string name
			, char[] credential);

		/// <summary>Delete the given credential.</summary>
		/// <param name="name">the alias of the credential to delete</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void DeleteCredentialEntry(string name);
	}
}
