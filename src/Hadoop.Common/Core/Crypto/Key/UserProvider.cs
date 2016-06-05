using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;


namespace Org.Apache.Hadoop.Crypto.Key
{
	/// <summary>A KeyProvider factory for UGIs.</summary>
	/// <remarks>
	/// A KeyProvider factory for UGIs. It uses the credentials object associated
	/// with the current user to find keys. This provider is created using a
	/// URI of "user:///".
	/// </remarks>
	public class UserProvider : KeyProvider
	{
		public const string SchemeName = "user";

		private readonly UserGroupInformation user;

		private readonly Credentials credentials;

		private readonly IDictionary<string, KeyProvider.Metadata> cache = new Dictionary
			<string, KeyProvider.Metadata>();

		/// <exception cref="System.IO.IOException"/>
		private UserProvider(Configuration conf)
			: base(conf)
		{
			user = UserGroupInformation.GetCurrentUser();
			credentials = user.GetCredentials();
		}

		public override bool IsTransient()
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion GetKeyVersion(string versionName)
		{
			lock (this)
			{
				byte[] bytes = credentials.GetSecretKey(new Text(versionName));
				if (bytes == null)
				{
					return null;
				}
				return new KeyProvider.KeyVersion(GetBaseName(versionName), versionName, bytes);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.Metadata GetMetadata(string name)
		{
			lock (this)
			{
				if (cache.Contains(name))
				{
					return cache[name];
				}
				byte[] serialized = credentials.GetSecretKey(new Text(name));
				if (serialized == null)
				{
					return null;
				}
				KeyProvider.Metadata result = new KeyProvider.Metadata(serialized);
				cache[name] = result;
				return result;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion CreateKey(string name, byte[] material, KeyProvider.Options
			 options)
		{
			lock (this)
			{
				Text nameT = new Text(name);
				if (credentials.GetSecretKey(nameT) != null)
				{
					throw new IOException("Key " + name + " already exists in " + this);
				}
				if (options.GetBitLength() != 8 * material.Length)
				{
					throw new IOException("Wrong key length. Required " + options.GetBitLength() + ", but got "
						 + (8 * material.Length));
				}
				KeyProvider.Metadata meta = new KeyProvider.Metadata(options.GetCipher(), options
					.GetBitLength(), options.GetDescription(), options.GetAttributes(), new DateTime
					(), 1);
				cache[name] = meta;
				string versionName = BuildVersionName(name, 0);
				credentials.AddSecretKey(nameT, meta.Serialize());
				credentials.AddSecretKey(new Text(versionName), material);
				return new KeyProvider.KeyVersion(name, versionName, material);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteKey(string name)
		{
			lock (this)
			{
				KeyProvider.Metadata meta = GetMetadata(name);
				if (meta == null)
				{
					throw new IOException("Key " + name + " does not exist in " + this);
				}
				for (int v = 0; v < meta.GetVersions(); ++v)
				{
					credentials.RemoveSecretKey(new Text(BuildVersionName(name, v)));
				}
				credentials.RemoveSecretKey(new Text(name));
				Collections.Remove(cache, name);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion RollNewVersion(string name, byte[] material
			)
		{
			lock (this)
			{
				KeyProvider.Metadata meta = GetMetadata(name);
				if (meta == null)
				{
					throw new IOException("Key " + name + " not found");
				}
				if (meta.GetBitLength() != 8 * material.Length)
				{
					throw new IOException("Wrong key length. Required " + meta.GetBitLength() + ", but got "
						 + (8 * material.Length));
				}
				int nextVersion = meta.AddVersion();
				credentials.AddSecretKey(new Text(name), meta.Serialize());
				string versionName = BuildVersionName(name, nextVersion);
				credentials.AddSecretKey(new Text(versionName), material);
				return new KeyProvider.KeyVersion(name, versionName, material);
			}
		}

		public override string ToString()
		{
			return SchemeName + ":///";
		}

		public override void Flush()
		{
			lock (this)
			{
				user.AddCredentials(credentials);
			}
		}

		public class Factory : KeyProviderFactory
		{
			/// <exception cref="System.IO.IOException"/>
			public override KeyProvider CreateProvider(URI providerName, Configuration conf)
			{
				if (SchemeName.Equals(providerName.GetScheme()))
				{
					return new UserProvider(conf);
				}
				return null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> GetKeys()
		{
			lock (this)
			{
				IList<string> list = new AList<string>();
				IList<Text> keys = credentials.GetAllSecretKeys();
				foreach (Text key in keys)
				{
					if (key.Find("@") == -1)
					{
						list.AddItem(key.ToString());
					}
				}
				return list;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<KeyProvider.KeyVersion> GetKeyVersions(string name)
		{
			lock (this)
			{
				IList<KeyProvider.KeyVersion> list = new AList<KeyProvider.KeyVersion>();
				KeyProvider.Metadata km = GetMetadata(name);
				if (km != null)
				{
					int latestVersion = km.GetVersions();
					for (int i = 0; i < latestVersion; i++)
					{
						KeyProvider.KeyVersion v = GetKeyVersion(BuildVersionName(name, i));
						if (v != null)
						{
							list.AddItem(v);
						}
					}
				}
				return list;
			}
		}
	}
}
