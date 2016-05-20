using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	/// <summary>A KeyProvider factory for UGIs.</summary>
	/// <remarks>
	/// A KeyProvider factory for UGIs. It uses the credentials object associated
	/// with the current user to find keys. This provider is created using a
	/// URI of "user:///".
	/// </remarks>
	public class UserProvider : org.apache.hadoop.crypto.key.KeyProvider
	{
		public const string SCHEME_NAME = "user";

		private readonly org.apache.hadoop.security.UserGroupInformation user;

		private readonly org.apache.hadoop.security.Credentials credentials;

		private readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.crypto.key.KeyProvider.Metadata
			> cache = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.crypto.key.KeyProvider.Metadata
			>();

		/// <exception cref="System.IO.IOException"/>
		private UserProvider(org.apache.hadoop.conf.Configuration conf)
			: base(conf)
		{
			user = org.apache.hadoop.security.UserGroupInformation.getCurrentUser();
			credentials = user.getCredentials();
		}

		public override bool isTransient()
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion getKeyVersion
			(string versionName)
		{
			lock (this)
			{
				byte[] bytes = credentials.getSecretKey(new org.apache.hadoop.io.Text(versionName
					));
				if (bytes == null)
				{
					return null;
				}
				return new org.apache.hadoop.crypto.key.KeyProvider.KeyVersion(getBaseName(versionName
					), versionName, bytes);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.Metadata getMetadata(string
			 name)
		{
			lock (this)
			{
				if (cache.Contains(name))
				{
					return cache[name];
				}
				byte[] serialized = credentials.getSecretKey(new org.apache.hadoop.io.Text(name));
				if (serialized == null)
				{
					return null;
				}
				org.apache.hadoop.crypto.key.KeyProvider.Metadata result = new org.apache.hadoop.crypto.key.KeyProvider.Metadata
					(serialized);
				cache[name] = result;
				return result;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion createKey(string
			 name, byte[] material, org.apache.hadoop.crypto.key.KeyProvider.Options options
			)
		{
			lock (this)
			{
				org.apache.hadoop.io.Text nameT = new org.apache.hadoop.io.Text(name);
				if (credentials.getSecretKey(nameT) != null)
				{
					throw new System.IO.IOException("Key " + name + " already exists in " + this);
				}
				if (options.getBitLength() != 8 * material.Length)
				{
					throw new System.IO.IOException("Wrong key length. Required " + options.getBitLength
						() + ", but got " + (8 * material.Length));
				}
				org.apache.hadoop.crypto.key.KeyProvider.Metadata meta = new org.apache.hadoop.crypto.key.KeyProvider.Metadata
					(options.getCipher(), options.getBitLength(), options.getDescription(), options.
					getAttributes(), new System.DateTime(), 1);
				cache[name] = meta;
				string versionName = buildVersionName(name, 0);
				credentials.addSecretKey(nameT, meta.serialize());
				credentials.addSecretKey(new org.apache.hadoop.io.Text(versionName), material);
				return new org.apache.hadoop.crypto.key.KeyProvider.KeyVersion(name, versionName, 
					material);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void deleteKey(string name)
		{
			lock (this)
			{
				org.apache.hadoop.crypto.key.KeyProvider.Metadata meta = getMetadata(name);
				if (meta == null)
				{
					throw new System.IO.IOException("Key " + name + " does not exist in " + this);
				}
				for (int v = 0; v < meta.getVersions(); ++v)
				{
					credentials.removeSecretKey(new org.apache.hadoop.io.Text(buildVersionName(name, 
						v)));
				}
				credentials.removeSecretKey(new org.apache.hadoop.io.Text(name));
				Sharpen.Collections.Remove(cache, name);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion rollNewVersion
			(string name, byte[] material)
		{
			lock (this)
			{
				org.apache.hadoop.crypto.key.KeyProvider.Metadata meta = getMetadata(name);
				if (meta == null)
				{
					throw new System.IO.IOException("Key " + name + " not found");
				}
				if (meta.getBitLength() != 8 * material.Length)
				{
					throw new System.IO.IOException("Wrong key length. Required " + meta.getBitLength
						() + ", but got " + (8 * material.Length));
				}
				int nextVersion = meta.addVersion();
				credentials.addSecretKey(new org.apache.hadoop.io.Text(name), meta.serialize());
				string versionName = buildVersionName(name, nextVersion);
				credentials.addSecretKey(new org.apache.hadoop.io.Text(versionName), material);
				return new org.apache.hadoop.crypto.key.KeyProvider.KeyVersion(name, versionName, 
					material);
			}
		}

		public override string ToString()
		{
			return SCHEME_NAME + ":///";
		}

		public override void flush()
		{
			lock (this)
			{
				user.addCredentials(credentials);
			}
		}

		public class Factory : org.apache.hadoop.crypto.key.KeyProviderFactory
		{
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.crypto.key.KeyProvider createProvider(java.net.URI
				 providerName, org.apache.hadoop.conf.Configuration conf)
			{
				if (SCHEME_NAME.Equals(providerName.getScheme()))
				{
					return new org.apache.hadoop.crypto.key.UserProvider(conf);
				}
				return null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> getKeys()
		{
			lock (this)
			{
				System.Collections.Generic.IList<string> list = new System.Collections.Generic.List
					<string>();
				System.Collections.Generic.IList<org.apache.hadoop.io.Text> keys = credentials.getAllSecretKeys
					();
				foreach (org.apache.hadoop.io.Text key in keys)
				{
					if (key.find("@") == -1)
					{
						list.add(key.ToString());
					}
				}
				return list;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
			> getKeyVersions(string name)
		{
			lock (this)
			{
				System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
					> list = new System.Collections.Generic.List<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
					>();
				org.apache.hadoop.crypto.key.KeyProvider.Metadata km = getMetadata(name);
				if (km != null)
				{
					int latestVersion = km.getVersions();
					for (int i = 0; i < latestVersion; i++)
					{
						org.apache.hadoop.crypto.key.KeyProvider.KeyVersion v = getKeyVersion(buildVersionName
							(name, i));
						if (v != null)
						{
							list.add(v);
						}
					}
				}
				return list;
			}
		}
	}
}
