using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	/// <summary>
	/// This is a utility class used to extend the functionality of KeyProvider, that
	/// takes a KeyProvider and an Extension.
	/// </summary>
	/// <remarks>
	/// This is a utility class used to extend the functionality of KeyProvider, that
	/// takes a KeyProvider and an Extension. It implements all the required methods
	/// of the KeyProvider by delegating it to the provided KeyProvider.
	/// </remarks>
	public abstract class KeyProviderExtension<E> : org.apache.hadoop.crypto.key.KeyProvider
		where E : org.apache.hadoop.crypto.key.KeyProviderExtension.Extension
	{
		/// <summary>A marker interface for the KeyProviderExtension subclass implement.</summary>
		public interface Extension
		{
		}

		private org.apache.hadoop.crypto.key.KeyProvider keyProvider;

		private E extension;

		public KeyProviderExtension(org.apache.hadoop.crypto.key.KeyProvider keyProvider, 
			E extensions)
			: base(keyProvider.getConf())
		{
			this.keyProvider = keyProvider;
			this.extension = extensions;
		}

		protected internal virtual E getExtension()
		{
			return extension;
		}

		protected internal virtual org.apache.hadoop.crypto.key.KeyProvider getKeyProvider
			()
		{
			return keyProvider;
		}

		public override bool isTransient()
		{
			return keyProvider.isTransient();
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.Metadata[] getKeysMetadata
			(params string[] names)
		{
			return keyProvider.getKeysMetadata(names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion getCurrentKey
			(string name)
		{
			return keyProvider.getCurrentKey(name);
		}

		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion createKey(string
			 name, org.apache.hadoop.crypto.key.KeyProvider.Options options)
		{
			return keyProvider.createKey(name, options);
		}

		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion rollNewVersion
			(string name)
		{
			return keyProvider.rollNewVersion(name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion getKeyVersion
			(string versionName)
		{
			return keyProvider.getKeyVersion(versionName);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> getKeys()
		{
			return keyProvider.getKeys();
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
			> getKeyVersions(string name)
		{
			return keyProvider.getKeyVersions(name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.Metadata getMetadata(string
			 name)
		{
			return keyProvider.getMetadata(name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion createKey(string
			 name, byte[] material, org.apache.hadoop.crypto.key.KeyProvider.Options options
			)
		{
			return keyProvider.createKey(name, material, options);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void deleteKey(string name)
		{
			keyProvider.deleteKey(name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion rollNewVersion
			(string name, byte[] material)
		{
			return keyProvider.rollNewVersion(name, material);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void flush()
		{
			keyProvider.flush();
		}

		public override string ToString()
		{
			return Sharpen.Runtime.getClassForObject(this).getSimpleName() + ": " + keyProvider
				.ToString();
		}
	}
}
