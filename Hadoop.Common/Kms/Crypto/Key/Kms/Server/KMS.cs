using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Javax.WS.RS;
using Javax.WS.RS.Core;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.Crypto.Key.Kms;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	/// <summary>Class providing the REST bindings, via Jersey, for the KMS.</summary>
	public class KMS
	{
		public enum KMSOp
		{
			CreateKey,
			DeleteKey,
			RollNewVersion,
			GetKeys,
			GetKeysMetadata,
			GetKeyVersions,
			GetMetadata,
			GetKeyVersion,
			GetCurrentKey,
			GenerateEek,
			DecryptEek
		}

		private KeyProviderCryptoExtension provider;

		private KMSAudit kmsAudit;

		/// <exception cref="System.Exception"/>
		public KMS()
		{
			provider = KMSWebApp.GetKeyProvider();
			kmsAudit = KMSWebApp.GetKMSAudit();
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		private void AssertAccess(KMSACLs.Type aclType, UserGroupInformation ugi, KMS.KMSOp
			 operation)
		{
			KMSWebApp.GetACLs().AssertAccess(aclType, ugi, operation, null);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		private void AssertAccess(KMSACLs.Type aclType, UserGroupInformation ugi, KMS.KMSOp
			 operation, string key)
		{
			KMSWebApp.GetACLs().AssertAccess(aclType, ugi, operation, key);
		}

		private static KeyProvider.KeyVersion RemoveKeyMaterial(KeyProvider.KeyVersion keyVersion
			)
		{
			return new KMSClientProvider.KMSKeyVersion(keyVersion.GetName(), keyVersion.GetVersionName
				(), null);
		}

		/// <exception cref="Sharpen.URISyntaxException"/>
		private static URI GetKeyURI(string name)
		{
			return new URI(KMSRESTConstants.ServiceVersion + "/" + KMSRESTConstants.KeyResource
				 + "/" + name);
		}

		/// <exception cref="System.Exception"/>
		[POST]
		public virtual Response CreateKey(IDictionary jsonKey)
		{
			KMSWebApp.GetAdminCallsMeter().Mark();
			UserGroupInformation user = HttpUserGroupInformation.Get();
			string name = (string)jsonKey[KMSRESTConstants.NameField];
			KMSClientProvider.CheckNotEmpty(name, KMSRESTConstants.NameField);
			AssertAccess(KMSACLs.Type.Create, user, KMS.KMSOp.CreateKey, name);
			string cipher = (string)jsonKey[KMSRESTConstants.CipherField];
			string material = (string)jsonKey[KMSRESTConstants.MaterialField];
			int length = (jsonKey.Contains(KMSRESTConstants.LengthField)) ? (int)jsonKey[KMSRESTConstants
				.LengthField] : 0;
			string description = (string)jsonKey[KMSRESTConstants.DescriptionField];
			IDictionary<string, string> attributes = (IDictionary<string, string>)jsonKey[KMSRESTConstants
				.AttributesField];
			if (material != null)
			{
				AssertAccess(KMSACLs.Type.SetKeyMaterial, user, KMS.KMSOp.CreateKey, name);
			}
			KeyProvider.Options options = new KeyProvider.Options(KMSWebApp.GetConfiguration(
				));
			if (cipher != null)
			{
				options.SetCipher(cipher);
			}
			if (length != 0)
			{
				options.SetBitLength(length);
			}
			options.SetDescription(description);
			options.SetAttributes(attributes);
			KeyProvider.KeyVersion keyVersion = user.DoAs(new _PrivilegedExceptionAction_132(
				this, material, name, options));
			kmsAudit.Ok(user, KMS.KMSOp.CreateKey, name, "UserProvidedMaterial:" + (material 
				!= null) + " Description:" + description);
			if (!KMSWebApp.GetACLs().HasAccess(KMSACLs.Type.Get, user))
			{
				keyVersion = RemoveKeyMaterial(keyVersion);
			}
			IDictionary json = KMSServerJSONUtils.ToJSON(keyVersion);
			string requestURL = KMSMDCFilter.GetURL();
			int idx = requestURL.LastIndexOf(KMSRESTConstants.KeysResource);
			requestURL = Sharpen.Runtime.Substring(requestURL, 0, idx);
			string keyURL = requestURL + KMSRESTConstants.KeyResource + "/" + name;
			return Response.Created(GetKeyURI(name)).Type(MediaType.ApplicationJson).Header("Location"
				, keyURL).Entity(json).Build();
		}

		private sealed class _PrivilegedExceptionAction_132 : PrivilegedExceptionAction<KeyProvider.KeyVersion
			>
		{
			public _PrivilegedExceptionAction_132(KMS _enclosing, string material, string name
				, KeyProvider.Options options)
			{
				this._enclosing = _enclosing;
				this.material = material;
				this.name = name;
				this.options = options;
			}

			/// <exception cref="System.Exception"/>
			public KeyProvider.KeyVersion Run()
			{
				KeyProvider.KeyVersion keyVersion = (material != null) ? this._enclosing.provider
					.CreateKey(name, Base64.DecodeBase64(material), options) : this._enclosing.provider
					.CreateKey(name, options);
				this._enclosing.provider.Flush();
				return keyVersion;
			}

			private readonly KMS _enclosing;

			private readonly string material;

			private readonly string name;

			private readonly KeyProvider.Options options;
		}

		/// <exception cref="System.Exception"/>
		[DELETE]
		public virtual Response DeleteKey(string name)
		{
			KMSWebApp.GetAdminCallsMeter().Mark();
			UserGroupInformation user = HttpUserGroupInformation.Get();
			AssertAccess(KMSACLs.Type.Delete, user, KMS.KMSOp.DeleteKey, name);
			KMSClientProvider.CheckNotEmpty(name, "name");
			user.DoAs(new _PrivilegedExceptionAction_168(this, name));
			kmsAudit.Ok(user, KMS.KMSOp.DeleteKey, name, string.Empty);
			return Response.Ok().Build();
		}

		private sealed class _PrivilegedExceptionAction_168 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_168(KMS _enclosing, string name)
			{
				this._enclosing = _enclosing;
				this.name = name;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.provider.DeleteKey(name);
				this._enclosing.provider.Flush();
				return null;
			}

			private readonly KMS _enclosing;

			private readonly string name;
		}

		/// <exception cref="System.Exception"/>
		[POST]
		public virtual Response RolloverKey(string name, IDictionary jsonMaterial)
		{
			KMSWebApp.GetAdminCallsMeter().Mark();
			UserGroupInformation user = HttpUserGroupInformation.Get();
			AssertAccess(KMSACLs.Type.Rollover, user, KMS.KMSOp.RollNewVersion, name);
			KMSClientProvider.CheckNotEmpty(name, "name");
			string material = (string)jsonMaterial[KMSRESTConstants.MaterialField];
			if (material != null)
			{
				AssertAccess(KMSACLs.Type.SetKeyMaterial, user, KMS.KMSOp.RollNewVersion, name);
			}
			KeyProvider.KeyVersion keyVersion = user.DoAs(new _PrivilegedExceptionAction_200(
				this, material, name));
			kmsAudit.Ok(user, KMS.KMSOp.RollNewVersion, name, "UserProvidedMaterial:" + (material
				 != null) + " NewVersion:" + keyVersion.GetVersionName());
			if (!KMSWebApp.GetACLs().HasAccess(KMSACLs.Type.Get, user))
			{
				keyVersion = RemoveKeyMaterial(keyVersion);
			}
			IDictionary json = KMSServerJSONUtils.ToJSON(keyVersion);
			return Response.Ok().Type(MediaType.ApplicationJson).Entity(json).Build();
		}

		private sealed class _PrivilegedExceptionAction_200 : PrivilegedExceptionAction<KeyProvider.KeyVersion
			>
		{
			public _PrivilegedExceptionAction_200(KMS _enclosing, string material, string name
				)
			{
				this._enclosing = _enclosing;
				this.material = material;
				this.name = name;
			}

			/// <exception cref="System.Exception"/>
			public KeyProvider.KeyVersion Run()
			{
				KeyProvider.KeyVersion keyVersion = (material != null) ? this._enclosing.provider
					.RollNewVersion(name, Base64.DecodeBase64(material)) : this._enclosing.provider.
					RollNewVersion(name);
				this._enclosing.provider.Flush();
				return keyVersion;
			}

			private readonly KMS _enclosing;

			private readonly string material;

			private readonly string name;
		}

		/// <exception cref="System.Exception"/>
		[GET]
		public virtual Response GetKeysMetadata(IList<string> keyNamesList)
		{
			KMSWebApp.GetAdminCallsMeter().Mark();
			UserGroupInformation user = HttpUserGroupInformation.Get();
			string[] keyNames = Sharpen.Collections.ToArray(keyNamesList, new string[keyNamesList
				.Count]);
			AssertAccess(KMSACLs.Type.GetMetadata, user, KMS.KMSOp.GetKeysMetadata);
			KeyProvider.Metadata[] keysMeta = user.DoAs(new _PrivilegedExceptionAction_234(this
				, keyNames));
			object json = KMSServerJSONUtils.ToJSON(keyNames, keysMeta);
			kmsAudit.Ok(user, KMS.KMSOp.GetKeysMetadata, string.Empty);
			return Response.Ok().Type(MediaType.ApplicationJson).Entity(json).Build();
		}

		private sealed class _PrivilegedExceptionAction_234 : PrivilegedExceptionAction<KeyProvider.Metadata
			[]>
		{
			public _PrivilegedExceptionAction_234(KMS _enclosing, string[] keyNames)
			{
				this._enclosing = _enclosing;
				this.keyNames = keyNames;
			}

			/// <exception cref="System.Exception"/>
			public KeyProvider.Metadata[] Run()
			{
				return this._enclosing.provider.GetKeysMetadata(keyNames);
			}

			private readonly KMS _enclosing;

			private readonly string[] keyNames;
		}

		/// <exception cref="System.Exception"/>
		[GET]
		public virtual Response GetKeyNames()
		{
			KMSWebApp.GetAdminCallsMeter().Mark();
			UserGroupInformation user = HttpUserGroupInformation.Get();
			AssertAccess(KMSACLs.Type.GetKeys, user, KMS.KMSOp.GetKeys);
			IList<string> json = user.DoAs(new _PrivilegedExceptionAction_256(this));
			kmsAudit.Ok(user, KMS.KMSOp.GetKeys, string.Empty);
			return Response.Ok().Type(MediaType.ApplicationJson).Entity(json).Build();
		}

		private sealed class _PrivilegedExceptionAction_256 : PrivilegedExceptionAction<IList
			<string>>
		{
			public _PrivilegedExceptionAction_256(KMS _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public IList<string> Run()
			{
				return this._enclosing.provider.GetKeys();
			}

			private readonly KMS _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[GET]
		public virtual Response GetKey(string name)
		{
			return GetMetadata(name);
		}

		/// <exception cref="System.Exception"/>
		[GET]
		public virtual Response GetMetadata(string name)
		{
			UserGroupInformation user = HttpUserGroupInformation.Get();
			KMSClientProvider.CheckNotEmpty(name, "name");
			KMSWebApp.GetAdminCallsMeter().Mark();
			AssertAccess(KMSACLs.Type.GetMetadata, user, KMS.KMSOp.GetMetadata, name);
			KeyProvider.Metadata metadata = user.DoAs(new _PrivilegedExceptionAction_287(this
				, name));
			object json = KMSServerJSONUtils.ToJSON(name, metadata);
			kmsAudit.Ok(user, KMS.KMSOp.GetMetadata, name, string.Empty);
			return Response.Ok().Type(MediaType.ApplicationJson).Entity(json).Build();
		}

		private sealed class _PrivilegedExceptionAction_287 : PrivilegedExceptionAction<KeyProvider.Metadata
			>
		{
			public _PrivilegedExceptionAction_287(KMS _enclosing, string name)
			{
				this._enclosing = _enclosing;
				this.name = name;
			}

			/// <exception cref="System.Exception"/>
			public KeyProvider.Metadata Run()
			{
				return this._enclosing.provider.GetMetadata(name);
			}

			private readonly KMS _enclosing;

			private readonly string name;
		}

		/// <exception cref="System.Exception"/>
		[GET]
		public virtual Response GetCurrentVersion(string name)
		{
			UserGroupInformation user = HttpUserGroupInformation.Get();
			KMSClientProvider.CheckNotEmpty(name, "name");
			KMSWebApp.GetKeyCallsMeter().Mark();
			AssertAccess(KMSACLs.Type.Get, user, KMS.KMSOp.GetCurrentKey, name);
			KeyProvider.KeyVersion keyVersion = user.DoAs(new _PrivilegedExceptionAction_312(
				this, name));
			object json = KMSServerJSONUtils.ToJSON(keyVersion);
			kmsAudit.Ok(user, KMS.KMSOp.GetCurrentKey, name, string.Empty);
			return Response.Ok().Type(MediaType.ApplicationJson).Entity(json).Build();
		}

		private sealed class _PrivilegedExceptionAction_312 : PrivilegedExceptionAction<KeyProvider.KeyVersion
			>
		{
			public _PrivilegedExceptionAction_312(KMS _enclosing, string name)
			{
				this._enclosing = _enclosing;
				this.name = name;
			}

			/// <exception cref="System.Exception"/>
			public KeyProvider.KeyVersion Run()
			{
				return this._enclosing.provider.GetCurrentKey(name);
			}

			private readonly KMS _enclosing;

			private readonly string name;
		}

		/// <exception cref="System.Exception"/>
		[GET]
		public virtual Response GetKeyVersion(string versionName)
		{
			UserGroupInformation user = HttpUserGroupInformation.Get();
			KMSClientProvider.CheckNotEmpty(versionName, "versionName");
			KMSWebApp.GetKeyCallsMeter().Mark();
			AssertAccess(KMSACLs.Type.Get, user, KMS.KMSOp.GetKeyVersion);
			KeyProvider.KeyVersion keyVersion = user.DoAs(new _PrivilegedExceptionAction_336(
				this, versionName));
			if (keyVersion != null)
			{
				kmsAudit.Ok(user, KMS.KMSOp.GetKeyVersion, keyVersion.GetName(), string.Empty);
			}
			object json = KMSServerJSONUtils.ToJSON(keyVersion);
			return Response.Ok().Type(MediaType.ApplicationJson).Entity(json).Build();
		}

		private sealed class _PrivilegedExceptionAction_336 : PrivilegedExceptionAction<KeyProvider.KeyVersion
			>
		{
			public _PrivilegedExceptionAction_336(KMS _enclosing, string versionName)
			{
				this._enclosing = _enclosing;
				this.versionName = versionName;
			}

			/// <exception cref="System.Exception"/>
			public KeyProvider.KeyVersion Run()
			{
				return this._enclosing.provider.GetKeyVersion(versionName);
			}

			private readonly KMS _enclosing;

			private readonly string versionName;
		}

		/// <exception cref="System.Exception"/>
		[GET]
		public virtual Response GenerateEncryptedKeys(string name, string edekOp, int numKeys
			)
		{
			UserGroupInformation user = HttpUserGroupInformation.Get();
			KMSClientProvider.CheckNotEmpty(name, "name");
			KMSClientProvider.CheckNotNull(edekOp, "eekOp");
			object retJSON;
			if (edekOp.Equals(KMSRESTConstants.EekGenerate))
			{
				AssertAccess(KMSACLs.Type.GenerateEek, user, KMS.KMSOp.GenerateEek, name);
				IList<KeyProviderCryptoExtension.EncryptedKeyVersion> retEdeks = new List<KeyProviderCryptoExtension.EncryptedKeyVersion
					>();
				try
				{
					user.DoAs(new _PrivilegedExceptionAction_375(this, numKeys, retEdeks, name));
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
				kmsAudit.Ok(user, KMS.KMSOp.GenerateEek, name, string.Empty);
				retJSON = new ArrayList();
				foreach (KeyProviderCryptoExtension.EncryptedKeyVersion edek in retEdeks)
				{
					((ArrayList)retJSON).AddItem(KMSServerJSONUtils.ToJSON(edek));
				}
			}
			else
			{
				throw new ArgumentException("Wrong " + KMSRESTConstants.EekOp + " value, it must be "
					 + KMSRESTConstants.EekGenerate + " or " + KMSRESTConstants.EekDecrypt);
			}
			KMSWebApp.GetGenerateEEKCallsMeter().Mark();
			return Response.Ok().Type(MediaType.ApplicationJson).Entity(retJSON).Build();
		}

		private sealed class _PrivilegedExceptionAction_375 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_375(KMS _enclosing, int numKeys, IList<KeyProviderCryptoExtension.EncryptedKeyVersion
				> retEdeks, string name)
			{
				this._enclosing = _enclosing;
				this.numKeys = numKeys;
				this.retEdeks = retEdeks;
				this.name = name;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				for (int i = 0; i < numKeys; i++)
				{
					retEdeks.AddItem(this._enclosing.provider.GenerateEncryptedKey(name));
				}
				return null;
			}

			private readonly KMS _enclosing;

			private readonly int numKeys;

			private readonly IList<KeyProviderCryptoExtension.EncryptedKeyVersion> retEdeks;

			private readonly string name;
		}

		/// <exception cref="System.Exception"/>
		[POST]
		public virtual Response DecryptEncryptedKey(string versionName, string eekOp, IDictionary
			 jsonPayload)
		{
			UserGroupInformation user = HttpUserGroupInformation.Get();
			KMSClientProvider.CheckNotEmpty(versionName, "versionName");
			KMSClientProvider.CheckNotNull(eekOp, "eekOp");
			string keyName = (string)jsonPayload[KMSRESTConstants.NameField];
			string ivStr = (string)jsonPayload[KMSRESTConstants.IvField];
			string encMaterialStr = (string)jsonPayload[KMSRESTConstants.MaterialField];
			object retJSON;
			if (eekOp.Equals(KMSRESTConstants.EekDecrypt))
			{
				AssertAccess(KMSACLs.Type.DecryptEek, user, KMS.KMSOp.DecryptEek, keyName);
				KMSClientProvider.CheckNotNull(ivStr, KMSRESTConstants.IvField);
				byte[] iv = Base64.DecodeBase64(ivStr);
				KMSClientProvider.CheckNotNull(encMaterialStr, KMSRESTConstants.MaterialField);
				byte[] encMaterial = Base64.DecodeBase64(encMaterialStr);
				KeyProvider.KeyVersion retKeyVersion = user.DoAs(new _PrivilegedExceptionAction_433
					(this, keyName, versionName, iv, encMaterial));
				retJSON = KMSServerJSONUtils.ToJSON(retKeyVersion);
				kmsAudit.Ok(user, KMS.KMSOp.DecryptEek, keyName, string.Empty);
			}
			else
			{
				throw new ArgumentException("Wrong " + KMSRESTConstants.EekOp + " value, it must be "
					 + KMSRESTConstants.EekGenerate + " or " + KMSRESTConstants.EekDecrypt);
			}
			KMSWebApp.GetDecryptEEKCallsMeter().Mark();
			return Response.Ok().Type(MediaType.ApplicationJson).Entity(retJSON).Build();
		}

		private sealed class _PrivilegedExceptionAction_433 : PrivilegedExceptionAction<KeyProvider.KeyVersion
			>
		{
			public _PrivilegedExceptionAction_433(KMS _enclosing, string keyName, string versionName
				, byte[] iv, byte[] encMaterial)
			{
				this._enclosing = _enclosing;
				this.keyName = keyName;
				this.versionName = versionName;
				this.iv = iv;
				this.encMaterial = encMaterial;
			}

			/// <exception cref="System.Exception"/>
			public KeyProvider.KeyVersion Run()
			{
				return this._enclosing.provider.DecryptEncryptedKey(new KMSClientProvider.KMSEncryptedKeyVersion
					(keyName, versionName, iv, KeyProviderCryptoExtension.Eek, encMaterial));
			}

			private readonly KMS _enclosing;

			private readonly string keyName;

			private readonly string versionName;

			private readonly byte[] iv;

			private readonly byte[] encMaterial;
		}

		/// <exception cref="System.Exception"/>
		[GET]
		public virtual Response GetKeyVersions(string name)
		{
			UserGroupInformation user = HttpUserGroupInformation.Get();
			KMSClientProvider.CheckNotEmpty(name, "name");
			KMSWebApp.GetKeyCallsMeter().Mark();
			AssertAccess(KMSACLs.Type.Get, user, KMS.KMSOp.GetKeyVersions, name);
			IList<KeyProvider.KeyVersion> ret = user.DoAs(new _PrivilegedExceptionAction_469(
				this, name));
			object json = KMSServerJSONUtils.ToJSON(ret);
			kmsAudit.Ok(user, KMS.KMSOp.GetKeyVersions, name, string.Empty);
			return Response.Ok().Type(MediaType.ApplicationJson).Entity(json).Build();
		}

		private sealed class _PrivilegedExceptionAction_469 : PrivilegedExceptionAction<IList
			<KeyProvider.KeyVersion>>
		{
			public _PrivilegedExceptionAction_469(KMS _enclosing, string name)
			{
				this._enclosing = _enclosing;
				this.name = name;
			}

			/// <exception cref="System.Exception"/>
			public IList<KeyProvider.KeyVersion> Run()
			{
				return this._enclosing.provider.GetKeyVersions(name);
			}

			private readonly KMS _enclosing;

			private readonly string name;
		}
	}
}
