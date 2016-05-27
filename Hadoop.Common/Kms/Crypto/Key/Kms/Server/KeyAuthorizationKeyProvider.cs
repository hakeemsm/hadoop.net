using System;
using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.Crypto.Key.KeyProvider"/>
	/// proxy that checks whether the current user derived via
	/// <see cref="Org.Apache.Hadoop.Security.UserGroupInformation"/>
	/// , is authorized to perform the following
	/// type of operations on a Key :
	/// <ol>
	/// <li>MANAGEMENT operations : createKey, rollNewVersion, deleteKey</li>
	/// <li>GENERATE_EEK operations : generateEncryptedKey, warmUpEncryptedKeys</li>
	/// <li>DECRYPT_EEK operation : decryptEncryptedKey</li>
	/// <li>READ operations : getKeyVersion, getKeyVersions, getMetadata,
	/// getKeysMetadata, getCurrentKey</li>
	/// </ol>
	/// The read operations (getCurrentKeyVersion / getMetadata) etc are not checked.
	/// </summary>
	public class KeyAuthorizationKeyProvider : KeyProviderCryptoExtension
	{
		public const string KeyAcl = "key.acl.";

		private const string KeyAclName = KeyAcl + "name";

		public enum KeyOpType
		{
			All,
			Read,
			Management,
			GenerateEek,
			DecryptEek
		}

		/// <summary>
		/// Interface that needs to be implemented by a client of the
		/// <code>KeyAuthorizationKeyProvider</code>.
		/// </summary>
		public interface KeyACLs
		{
			/// <summary>
			/// This is called by the KeyProvider to check if the given user is
			/// authorized to perform the specified operation on the given acl name.
			/// </summary>
			/// <param name="aclName">name of the key ACL</param>
			/// <param name="ugi">User's UserGroupInformation</param>
			/// <param name="opType">Operation Type</param>
			/// <returns>true if user has access to the aclName and opType else false</returns>
			bool HasAccessToKey(string aclName, UserGroupInformation ugi, KeyAuthorizationKeyProvider.KeyOpType
				 opType);

			/// <param name="aclName">ACL name</param>
			/// <param name="opType">Operation Type</param>
			/// <returns>true if AclName exists else false</returns>
			bool IsACLPresent(string aclName, KeyAuthorizationKeyProvider.KeyOpType opType);
		}

		private readonly KeyProviderCryptoExtension provider;

		private readonly KeyAuthorizationKeyProvider.KeyACLs acls;

		private Lock readLock;

		private Lock writeLock;

		/// <summary>
		/// The constructor takes a
		/// <see cref="Org.Apache.Hadoop.Crypto.Key.KeyProviderCryptoExtension"/>
		/// and an
		/// implementation of <code>KeyACLs</code>. All calls are delegated to the
		/// provider keyProvider after authorization check (if required)
		/// </summary>
		/// <param name="keyProvider"></param>
		/// <param name="acls"/>
		public KeyAuthorizationKeyProvider(KeyProviderCryptoExtension keyProvider, KeyAuthorizationKeyProvider.KeyACLs
			 acls)
			: base(keyProvider, null)
		{
			this.provider = keyProvider;
			this.acls = acls;
			ReadWriteLock Lock = new ReentrantReadWriteLock(true);
			readLock = Lock.ReadLock();
			writeLock = Lock.WriteLock();
		}

		// This method first checks if "key.acl.name" attribute is present as an
		// attribute in the provider Options. If yes, use the aclName for any
		// subsequent access checks, else use the keyName as the aclName and set it
		// as the value of the "key.acl.name" in the key's metadata.
		/// <exception cref="System.IO.IOException"/>
		private void AuthorizeCreateKey(string keyName, KeyProvider.Options options, UserGroupInformation
			 ugi)
		{
			Preconditions.CheckNotNull(ugi, "UserGroupInformation cannot be null");
			IDictionary<string, string> attributes = options.GetAttributes();
			string aclName = attributes[KeyAclName];
			bool success = false;
			if (Strings.IsNullOrEmpty(aclName))
			{
				if (acls.IsACLPresent(keyName, KeyAuthorizationKeyProvider.KeyOpType.Management))
				{
					options.SetAttributes(ImmutableMap.Builder<string, string>().PutAll(attributes).Put
						(KeyAclName, keyName).Build());
					success = acls.HasAccessToKey(keyName, ugi, KeyAuthorizationKeyProvider.KeyOpType
						.Management) || acls.HasAccessToKey(keyName, ugi, KeyAuthorizationKeyProvider.KeyOpType
						.All);
				}
				else
				{
					success = false;
				}
			}
			else
			{
				success = acls.IsACLPresent(aclName, KeyAuthorizationKeyProvider.KeyOpType.Management
					) && (acls.HasAccessToKey(aclName, ugi, KeyAuthorizationKeyProvider.KeyOpType.Management
					) || acls.HasAccessToKey(aclName, ugi, KeyAuthorizationKeyProvider.KeyOpType.All
					));
			}
			if (!success)
			{
				throw new AuthorizationException(string.Format("User [%s] is not" + " authorized to create key !!"
					, ugi.GetShortUserName()));
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Authorize.AuthorizationException"/>
		private void CheckAccess(string aclName, UserGroupInformation ugi, KeyAuthorizationKeyProvider.KeyOpType
			 opType)
		{
			Preconditions.CheckNotNull(aclName, "Key ACL name cannot be null");
			Preconditions.CheckNotNull(ugi, "UserGroupInformation cannot be null");
			if (acls.IsACLPresent(aclName, opType) && (acls.HasAccessToKey(aclName, ugi, opType
				) || acls.HasAccessToKey(aclName, ugi, KeyAuthorizationKeyProvider.KeyOpType.All
				)))
			{
				return;
			}
			else
			{
				throw new AuthorizationException(string.Format("User [%s] is not" + " authorized to perform [%s] on key with ACL name [%s]!!"
					, ugi.GetShortUserName(), opType, aclName));
			}
		}

		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion CreateKey(string name, KeyProvider.Options
			 options)
		{
			writeLock.Lock();
			try
			{
				AuthorizeCreateKey(name, options, GetUser());
				return provider.CreateKey(name, options);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion CreateKey(string name, byte[] material, KeyProvider.Options
			 options)
		{
			writeLock.Lock();
			try
			{
				AuthorizeCreateKey(name, options, GetUser());
				return provider.CreateKey(name, material, options);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion RollNewVersion(string name)
		{
			writeLock.Lock();
			try
			{
				DoAccessCheck(name, KeyAuthorizationKeyProvider.KeyOpType.Management);
				return provider.RollNewVersion(name);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteKey(string name)
		{
			writeLock.Lock();
			try
			{
				DoAccessCheck(name, KeyAuthorizationKeyProvider.KeyOpType.Management);
				provider.DeleteKey(name);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion RollNewVersion(string name, byte[] material
			)
		{
			writeLock.Lock();
			try
			{
				DoAccessCheck(name, KeyAuthorizationKeyProvider.KeyOpType.Management);
				return provider.RollNewVersion(name, material);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void WarmUpEncryptedKeys(params string[] names)
		{
			readLock.Lock();
			try
			{
				foreach (string name in names)
				{
					DoAccessCheck(name, KeyAuthorizationKeyProvider.KeyOpType.GenerateEek);
				}
				provider.WarmUpEncryptedKeys(names);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.GeneralSecurityException"/>
		public override KeyProviderCryptoExtension.EncryptedKeyVersion GenerateEncryptedKey
			(string encryptionKeyName)
		{
			readLock.Lock();
			try
			{
				DoAccessCheck(encryptionKeyName, KeyAuthorizationKeyProvider.KeyOpType.GenerateEek
					);
				return provider.GenerateEncryptedKey(encryptionKeyName);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyKeyVersionBelongsToKey(KeyProviderCryptoExtension.EncryptedKeyVersion
			 ekv)
		{
			string kn = ekv.GetEncryptionKeyName();
			string kvn = ekv.GetEncryptionKeyVersionName();
			KeyProvider.KeyVersion kv = provider.GetKeyVersion(kvn);
			if (kv == null)
			{
				throw new ArgumentException(string.Format("'%s' not found", kvn));
			}
			if (!kv.GetName().Equals(kn))
			{
				throw new ArgumentException(string.Format("KeyVersion '%s' does not belong to the key '%s'"
					, kvn, kn));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.GeneralSecurityException"/>
		public override KeyProvider.KeyVersion DecryptEncryptedKey(KeyProviderCryptoExtension.EncryptedKeyVersion
			 encryptedKeyVersion)
		{
			readLock.Lock();
			try
			{
				VerifyKeyVersionBelongsToKey(encryptedKeyVersion);
				DoAccessCheck(encryptedKeyVersion.GetEncryptionKeyName(), KeyAuthorizationKeyProvider.KeyOpType
					.DecryptEek);
				return provider.DecryptEncryptedKey(encryptedKeyVersion);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion GetKeyVersion(string versionName)
		{
			readLock.Lock();
			try
			{
				KeyProvider.KeyVersion keyVersion = provider.GetKeyVersion(versionName);
				if (keyVersion != null)
				{
					DoAccessCheck(keyVersion.GetName(), KeyAuthorizationKeyProvider.KeyOpType.Read);
				}
				return keyVersion;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> GetKeys()
		{
			return provider.GetKeys();
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<KeyProvider.KeyVersion> GetKeyVersions(string name)
		{
			readLock.Lock();
			try
			{
				DoAccessCheck(name, KeyAuthorizationKeyProvider.KeyOpType.Read);
				return provider.GetKeyVersions(name);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.Metadata GetMetadata(string name)
		{
			readLock.Lock();
			try
			{
				DoAccessCheck(name, KeyAuthorizationKeyProvider.KeyOpType.Read);
				return provider.GetMetadata(name);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.Metadata[] GetKeysMetadata(params string[] names)
		{
			readLock.Lock();
			try
			{
				foreach (string name in names)
				{
					DoAccessCheck(name, KeyAuthorizationKeyProvider.KeyOpType.Read);
				}
				return provider.GetKeysMetadata(names);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion GetCurrentKey(string name)
		{
			readLock.Lock();
			try
			{
				DoAccessCheck(name, KeyAuthorizationKeyProvider.KeyOpType.Read);
				return provider.GetCurrentKey(name);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Flush()
		{
			provider.Flush();
		}

		public override bool IsTransient()
		{
			return provider.IsTransient();
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoAccessCheck(string keyName, KeyAuthorizationKeyProvider.KeyOpType 
			opType)
		{
			KeyProvider.Metadata metadata = provider.GetMetadata(keyName);
			if (metadata != null)
			{
				string aclName = metadata.GetAttributes()[KeyAclName];
				CheckAccess((aclName == null) ? keyName : aclName, GetUser(), opType);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private UserGroupInformation GetUser()
		{
			return UserGroupInformation.GetCurrentUser();
		}

		protected override KeyProvider GetKeyProvider()
		{
			return this;
		}

		public override string ToString()
		{
			return provider.ToString();
		}
	}
}
