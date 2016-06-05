using System.Collections.Generic;


namespace Org.Apache.Hadoop.Crypto.Key
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
	public abstract class KeyProviderExtension<E> : KeyProvider
		where E : KeyProviderExtension.Extension
	{
		/// <summary>A marker interface for the KeyProviderExtension subclass implement.</summary>
		public interface Extension
		{
		}

		private KeyProvider keyProvider;

		private E extension;

		public KeyProviderExtension(KeyProvider keyProvider, E extensions)
			: base(keyProvider.GetConf())
		{
			this.keyProvider = keyProvider;
			this.extension = extensions;
		}

		protected internal virtual E GetExtension()
		{
			return extension;
		}

		protected internal virtual KeyProvider GetKeyProvider()
		{
			return keyProvider;
		}

		public override bool IsTransient()
		{
			return keyProvider.IsTransient();
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.Metadata[] GetKeysMetadata(params string[] names)
		{
			return keyProvider.GetKeysMetadata(names);
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion GetCurrentKey(string name)
		{
			return keyProvider.GetCurrentKey(name);
		}

		/// <exception cref="NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion CreateKey(string name, KeyProvider.Options
			 options)
		{
			return keyProvider.CreateKey(name, options);
		}

		/// <exception cref="NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion RollNewVersion(string name)
		{
			return keyProvider.RollNewVersion(name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion GetKeyVersion(string versionName)
		{
			return keyProvider.GetKeyVersion(versionName);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> GetKeys()
		{
			return keyProvider.GetKeys();
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<KeyProvider.KeyVersion> GetKeyVersions(string name)
		{
			return keyProvider.GetKeyVersions(name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.Metadata GetMetadata(string name)
		{
			return keyProvider.GetMetadata(name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion CreateKey(string name, byte[] material, KeyProvider.Options
			 options)
		{
			return keyProvider.CreateKey(name, material, options);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteKey(string name)
		{
			keyProvider.DeleteKey(name);
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion RollNewVersion(string name, byte[] material
			)
		{
			return keyProvider.RollNewVersion(name, material);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Flush()
		{
			keyProvider.Flush();
		}

		public override string ToString()
		{
			return GetType().Name + ": " + keyProvider.ToString();
		}
	}
}
