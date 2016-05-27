using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Org.Apache.Hadoop.Security.Ssl;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Org.Apache.Hadoop.Util;
using Org.Apache.Http.Client.Utils;
using Org.Codehaus.Jackson.Map;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Crypto.Key.Kms
{
	/// <summary>KMS client <code>KeyProvider</code> implementation.</summary>
	public class KMSClientProvider : KeyProvider, KeyProviderCryptoExtension.CryptoExtension
		, KeyProviderDelegationTokenExtension.DelegationTokenExtension
	{
		private const string InvalidSignature = "Invalid signature";

		private const string AnonymousRequestsDisallowed = "Anonymous requests are disallowed";

		public const string TokenKind = "kms-dt";

		public const string SchemeName = "kms";

		private const string Utf8 = "UTF-8";

		private const string ContentType = "Content-Type";

		private const string ApplicationJsonMime = "application/json";

		private const string HttpGet = "GET";

		private const string HttpPost = "POST";

		private const string HttpPut = "PUT";

		private const string HttpDelete = "DELETE";

		private const string ConfigPrefix = "hadoop.security.kms.client.";

		public const string TimeoutAttr = ConfigPrefix + "timeout";

		public const int DefaultTimeout = 60;

		public const string AuthRetry = ConfigPrefix + "authentication.retry-count";

		public const int DefaultAuthRetry = 1;

		private readonly ValueQueue<KeyProviderCryptoExtension.EncryptedKeyVersion> encKeyVersionQueue;

		private class EncryptedQueueRefiller : ValueQueue.QueueRefiller<KeyProviderCryptoExtension.EncryptedKeyVersion
			>
		{
			/* It's possible to specify a timeout, in seconds, in the config file */
			/* Number of times to retry authentication in the event of auth failure
			* (normally happens due to stale authToken)
			*/
			/// <exception cref="System.IO.IOException"/>
			public virtual void FillQueueForKey(string keyName, Queue<KeyProviderCryptoExtension.EncryptedKeyVersion
				> keyQueue, int numEKVs)
			{
				KMSClientProvider.CheckNotNull(keyName, "keyName");
				IDictionary<string, string> @params = new Dictionary<string, string>();
				@params[KMSRESTConstants.EekOp] = KMSRESTConstants.EekGenerate;
				@params[KMSRESTConstants.EekNumKeys] = string.Empty + numEKVs;
				Uri url = this._enclosing.CreateURL(KMSRESTConstants.KeyResource, keyName, KMSRESTConstants
					.EekSubResource, @params);
				HttpURLConnection conn = this._enclosing.CreateConnection(url, KMSClientProvider.
					HttpGet);
				conn.SetRequestProperty(KMSClientProvider.ContentType, KMSClientProvider.ApplicationJsonMime
					);
				IList response = this._enclosing.Call<IList>(conn, null, HttpURLConnection.HttpOk
					);
				IList<KeyProviderCryptoExtension.EncryptedKeyVersion> ekvs = KMSClientProvider.ParseJSONEncKeyVersion
					(keyName, response);
				Sharpen.Collections.AddAll(keyQueue, ekvs);
			}

			internal EncryptedQueueRefiller(KMSClientProvider _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly KMSClientProvider _enclosing;
		}

		public class KMSEncryptedKeyVersion : KeyProviderCryptoExtension.EncryptedKeyVersion
		{
			public KMSEncryptedKeyVersion(string keyName, string keyVersionName, byte[] iv, string
				 encryptedVersionName, byte[] keyMaterial)
				: base(keyName, keyVersionName, iv, new KMSClientProvider.KMSKeyVersion(null, encryptedVersionName
					, keyMaterial))
			{
			}
		}

		private static IList<KeyProviderCryptoExtension.EncryptedKeyVersion> ParseJSONEncKeyVersion
			(string keyName, IList valueList)
		{
			IList<KeyProviderCryptoExtension.EncryptedKeyVersion> ekvs = new List<KeyProviderCryptoExtension.EncryptedKeyVersion
				>();
			if (!valueList.IsEmpty())
			{
				foreach (object values in valueList)
				{
					IDictionary valueMap = (IDictionary)values;
					string versionName = CheckNotNull((string)valueMap[KMSRESTConstants.VersionNameField
						], KMSRESTConstants.VersionNameField);
					byte[] iv = Base64.DecodeBase64(CheckNotNull((string)valueMap[KMSRESTConstants.IvField
						], KMSRESTConstants.IvField));
					IDictionary encValueMap = CheckNotNull((IDictionary)valueMap[KMSRESTConstants.EncryptedKeyVersionField
						], KMSRESTConstants.EncryptedKeyVersionField);
					string encVersionName = CheckNotNull((string)encValueMap[KMSRESTConstants.VersionNameField
						], KMSRESTConstants.VersionNameField);
					byte[] encKeyMaterial = Base64.DecodeBase64(CheckNotNull((string)encValueMap[KMSRESTConstants
						.MaterialField], KMSRESTConstants.MaterialField));
					ekvs.AddItem(new KMSClientProvider.KMSEncryptedKeyVersion(keyName, versionName, iv
						, encVersionName, encKeyMaterial));
				}
			}
			return ekvs;
		}

		private static KeyProvider.KeyVersion ParseJSONKeyVersion(IDictionary valueMap)
		{
			KeyProvider.KeyVersion keyVersion = null;
			if (!valueMap.IsEmpty())
			{
				byte[] material = (valueMap.Contains(KMSRESTConstants.MaterialField)) ? Base64.DecodeBase64
					((string)valueMap[KMSRESTConstants.MaterialField]) : null;
				string versionName = (string)valueMap[KMSRESTConstants.VersionNameField];
				string keyName = (string)valueMap[KMSRESTConstants.NameField];
				keyVersion = new KMSClientProvider.KMSKeyVersion(keyName, versionName, material);
			}
			return keyVersion;
		}

		private static KeyProvider.Metadata ParseJSONMetadata(IDictionary valueMap)
		{
			KeyProvider.Metadata metadata = null;
			if (!valueMap.IsEmpty())
			{
				metadata = new KMSClientProvider.KMSMetadata((string)valueMap[KMSRESTConstants.CipherField
					], (int)valueMap[KMSRESTConstants.LengthField], (string)valueMap[KMSRESTConstants
					.DescriptionField], (IDictionary<string, string>)valueMap[KMSRESTConstants.AttributesField
					], Sharpen.Extensions.CreateDate((long)valueMap[KMSRESTConstants.CreatedField]), 
					(int)valueMap[KMSRESTConstants.VersionsField]);
			}
			return metadata;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteJson(IDictionary map, OutputStream os)
		{
			TextWriter writer = new OutputStreamWriter(os, Charsets.Utf8);
			ObjectMapper jsonMapper = new ObjectMapper();
			jsonMapper.WriterWithDefaultPrettyPrinter().WriteValue(writer, map);
		}

		/// <summary>
		/// The factory to create KMSClientProvider, which is used by the
		/// ServiceLoader.
		/// </summary>
		public class Factory : KeyProviderFactory
		{
			/// <summary>
			/// This provider expects URIs in the following form :
			/// kms://<PROTO>@<AUTHORITY>/<PATH>
			/// where :
			/// - PROTO = http or https
			/// - AUTHORITY = <HOSTS>[:<PORT>]
			/// - HOSTS = <HOSTNAME>[;<HOSTS>]
			/// - HOSTNAME = string
			/// - PORT = integer
			/// If multiple hosts are provider, the Factory will create a
			/// <see cref="LoadBalancingKMSClientProvider"/>
			/// that round-robins requests
			/// across the provided list of hosts.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public override KeyProvider CreateProvider(URI providerUri, Configuration conf)
			{
				if (SchemeName.Equals(providerUri.GetScheme()))
				{
					Uri origUrl = new Uri(ExtractKMSPath(providerUri).ToString());
					string authority = origUrl.GetAuthority();
					// check for ';' which delimits the backup hosts
					if (Strings.IsNullOrEmpty(authority))
					{
						throw new IOException("No valid authority in kms uri [" + origUrl + "]");
					}
					// Check if port is present in authority
					// In the current scheme, all hosts have to run on the same port
					int port = -1;
					string hostsPart = authority;
					if (authority.Contains(":"))
					{
						string[] t = authority.Split(":");
						try
						{
							port = System.Convert.ToInt32(t[1]);
						}
						catch (Exception)
						{
							throw new IOException("Could not parse port in kms uri [" + origUrl + "]");
						}
						hostsPart = t[0];
					}
					return CreateProvider(providerUri, conf, origUrl, port, hostsPart);
				}
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			private KeyProvider CreateProvider(URI providerUri, Configuration conf, Uri origUrl
				, int port, string hostsPart)
			{
				string[] hosts = hostsPart.Split(";");
				if (hosts.Length == 1)
				{
					return new KMSClientProvider(providerUri, conf);
				}
				else
				{
					KMSClientProvider[] providers = new KMSClientProvider[hosts.Length];
					for (int i = 0; i < hosts.Length; i++)
					{
						try
						{
							providers[i] = new KMSClientProvider(new URI("kms", origUrl.Scheme, hosts[i], port
								, origUrl.AbsolutePath, null, null), conf);
						}
						catch (URISyntaxException e)
						{
							throw new IOException("Could not instantiate KMSProvider..", e);
						}
					}
					return new LoadBalancingKMSClientProvider(providers, conf);
				}
			}
		}

		/// <exception cref="System.ArgumentException"/>
		public static T CheckNotNull<T>(T o, string name)
		{
			if (o == null)
			{
				throw new ArgumentException("Parameter '" + name + "' cannot be null");
			}
			return o;
		}

		/// <exception cref="System.ArgumentException"/>
		public static string CheckNotEmpty(string s, string name)
		{
			CheckNotNull(s, name);
			if (s.IsEmpty())
			{
				throw new ArgumentException("Parameter '" + name + "' cannot be empty");
			}
			return s;
		}

		private string kmsUrl;

		private SSLFactory sslFactory;

		private ConnectionConfigurator configurator;

		private DelegationTokenAuthenticatedURL.Token authToken;

		private readonly int authRetry;

		private readonly UserGroupInformation actualUgi;

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder("KMSClientProvider[");
			sb.Append(kmsUrl).Append("]");
			return sb.ToString();
		}

		/// <summary>This small class exists to set the timeout values for a connection</summary>
		private class TimeoutConnConfigurator : ConnectionConfigurator
		{
			private ConnectionConfigurator cc;

			private int timeout;

			/// <summary>Sets the timeout and wraps another connection configurator</summary>
			/// <param name="timeout">- will set both connect and read timeouts - in seconds</param>
			/// <param name="cc">- another configurator to wrap - may be null</param>
			public TimeoutConnConfigurator(int timeout, ConnectionConfigurator cc)
			{
				this.timeout = timeout;
				this.cc = cc;
			}

			/// <summary>Calls the wrapped configure() method, then sets timeouts</summary>
			/// <param name="conn">
			/// the
			/// <see cref="Sharpen.HttpURLConnection"/>
			/// instance to configure.
			/// </param>
			/// <returns>the connection</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual HttpURLConnection Configure(HttpURLConnection conn)
			{
				if (cc != null)
				{
					conn = cc.Configure(conn);
				}
				conn.SetConnectTimeout(timeout * 1000);
				// conversion to milliseconds
				conn.SetReadTimeout(timeout * 1000);
				return conn;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public KMSClientProvider(URI uri, Configuration conf)
			: base(conf)
		{
			kmsUrl = CreateServiceURL(ExtractKMSPath(uri));
			if (Sharpen.Runtime.EqualsIgnoreCase("https", new Uri(kmsUrl).Scheme))
			{
				sslFactory = new SSLFactory(SSLFactory.Mode.Client, conf);
				try
				{
					sslFactory.Init();
				}
				catch (GeneralSecurityException ex)
				{
					throw new IOException(ex);
				}
			}
			int timeout = conf.GetInt(TimeoutAttr, DefaultTimeout);
			authRetry = conf.GetInt(AuthRetry, DefaultAuthRetry);
			configurator = new KMSClientProvider.TimeoutConnConfigurator(timeout, sslFactory);
			encKeyVersionQueue = new ValueQueue<KeyProviderCryptoExtension.EncryptedKeyVersion
				>(conf.GetInt(CommonConfigurationKeysPublic.KmsClientEncKeyCacheSize, CommonConfigurationKeysPublic
				.KmsClientEncKeyCacheSizeDefault), conf.GetFloat(CommonConfigurationKeysPublic.KmsClientEncKeyCacheLowWatermark
				, CommonConfigurationKeysPublic.KmsClientEncKeyCacheLowWatermarkDefault), conf.GetInt
				(CommonConfigurationKeysPublic.KmsClientEncKeyCacheExpiryMs, CommonConfigurationKeysPublic
				.KmsClientEncKeyCacheExpiryDefault), conf.GetInt(CommonConfigurationKeysPublic.KmsClientEncKeyCacheNumRefillThreads
				, CommonConfigurationKeysPublic.KmsClientEncKeyCacheNumRefillThreadsDefault), new 
				KMSClientProvider.EncryptedQueueRefiller(this));
			authToken = new DelegationTokenAuthenticatedURL.Token();
			actualUgi = (UserGroupInformation.GetCurrentUser().GetAuthenticationMethod() == UserGroupInformation.AuthenticationMethod
				.Proxy) ? UserGroupInformation.GetCurrentUser().GetRealUser() : UserGroupInformation
				.GetCurrentUser();
		}

		/// <exception cref="System.UriFormatException"/>
		/// <exception cref="System.IO.IOException"/>
		private static Path ExtractKMSPath(URI uri)
		{
			return ProviderUtils.UnnestUri(uri);
		}

		/// <exception cref="System.IO.IOException"/>
		private static string CreateServiceURL(Path path)
		{
			string str = new Uri(path.ToString()).ToExternalForm();
			if (str.EndsWith("/"))
			{
				str = Sharpen.Runtime.Substring(str, 0, str.Length - 1);
			}
			return new Uri(str + KMSRESTConstants.ServiceVersion + "/").ToExternalForm();
		}

		/// <exception cref="System.IO.IOException"/>
		private Uri CreateURL(string collection, string resource, string subResource, IDictionary
			<string, object> parameters)
		{
			try
			{
				StringBuilder sb = new StringBuilder();
				sb.Append(kmsUrl);
				if (collection != null)
				{
					sb.Append(collection);
					if (resource != null)
					{
						sb.Append("/").Append(URLEncoder.Encode(resource, Utf8));
						if (subResource != null)
						{
							sb.Append("/").Append(subResource);
						}
					}
				}
				URIBuilder uriBuilder = new URIBuilder(sb.ToString());
				if (parameters != null)
				{
					foreach (KeyValuePair<string, object> param in parameters)
					{
						object value = param.Value;
						if (value is string)
						{
							uriBuilder.AddParameter(param.Key, (string)value);
						}
						else
						{
							foreach (string s in (string[])value)
							{
								uriBuilder.AddParameter(param.Key, s);
							}
						}
					}
				}
				return uriBuilder.Build().ToURL();
			}
			catch (URISyntaxException ex)
			{
				throw new IOException(ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private HttpURLConnection ConfigureConnection(HttpURLConnection conn)
		{
			if (sslFactory != null)
			{
				HttpsURLConnection httpsConn = (HttpsURLConnection)conn;
				try
				{
					httpsConn.SetSSLSocketFactory(sslFactory.CreateSSLSocketFactory());
				}
				catch (GeneralSecurityException ex)
				{
					throw new IOException(ex);
				}
				httpsConn.SetHostnameVerifier(sslFactory.GetHostnameVerifier());
			}
			return conn;
		}

		/// <exception cref="System.IO.IOException"/>
		private HttpURLConnection CreateConnection(Uri url, string method)
		{
			HttpURLConnection conn;
			try
			{
				// if current UGI is different from UGI at constructor time, behave as
				// proxyuser
				UserGroupInformation currentUgi = UserGroupInformation.GetCurrentUser();
				string doAsUser = (currentUgi.GetAuthenticationMethod() == UserGroupInformation.AuthenticationMethod
					.Proxy) ? currentUgi.GetShortUserName() : null;
				// creating the HTTP connection using the current UGI at constructor time
				conn = actualUgi.DoAs(new _PrivilegedExceptionAction_478(this, url, doAsUser));
			}
			catch (IOException ex)
			{
				throw;
			}
			catch (UndeclaredThrowableException ex)
			{
				throw new IOException(ex.GetUndeclaredThrowable());
			}
			catch (Exception ex)
			{
				throw new IOException(ex);
			}
			conn.SetUseCaches(false);
			conn.SetRequestMethod(method);
			if (method.Equals(HttpPost) || method.Equals(HttpPut))
			{
				conn.SetDoOutput(true);
			}
			conn = ConfigureConnection(conn);
			return conn;
		}

		private sealed class _PrivilegedExceptionAction_478 : PrivilegedExceptionAction<HttpURLConnection
			>
		{
			public _PrivilegedExceptionAction_478(KMSClientProvider _enclosing, Uri url, string
				 doAsUser)
			{
				this._enclosing = _enclosing;
				this.url = url;
				this.doAsUser = doAsUser;
			}

			/// <exception cref="System.Exception"/>
			public HttpURLConnection Run()
			{
				DelegationTokenAuthenticatedURL authUrl = new DelegationTokenAuthenticatedURL(this
					._enclosing.configurator);
				return authUrl.OpenConnection(url, this._enclosing.authToken, doAsUser);
			}

			private readonly KMSClientProvider _enclosing;

			private readonly Uri url;

			private readonly string doAsUser;
		}

		/// <exception cref="System.IO.IOException"/>
		private T Call<T>(HttpURLConnection conn, IDictionary jsonOutput, int expectedResponse
			)
		{
			System.Type klass = typeof(T);
			return Call(conn, jsonOutput, expectedResponse, klass, authRetry);
		}

		/// <exception cref="System.IO.IOException"/>
		private T Call<T>(HttpURLConnection conn, IDictionary jsonOutput, int expectedResponse
			, int authRetryCount)
		{
			System.Type klass = typeof(T);
			T ret = null;
			try
			{
				if (jsonOutput != null)
				{
					WriteJson(jsonOutput, conn.GetOutputStream());
				}
			}
			catch (IOException ex)
			{
				conn.GetInputStream().Close();
				throw;
			}
			if ((conn.GetResponseCode() == HttpURLConnection.HttpForbidden && (conn.GetResponseMessage
				().Equals(AnonymousRequestsDisallowed) || conn.GetResponseMessage().Contains(InvalidSignature
				))) || conn.GetResponseCode() == HttpURLConnection.HttpUnauthorized)
			{
				// Ideally, this should happen only when there is an Authentication
				// failure. Unfortunately, the AuthenticationFilter returns 403 when it
				// cannot authenticate (Since a 401 requires Server to send
				// WWW-Authenticate header as well)..
				this.authToken = new DelegationTokenAuthenticatedURL.Token();
				if (authRetryCount > 0)
				{
					string contentType = conn.GetRequestProperty(ContentType);
					string requestMethod = conn.GetRequestMethod();
					Uri url = conn.GetURL();
					conn = CreateConnection(url, requestMethod);
					conn.SetRequestProperty(ContentType, contentType);
					return Call(conn, jsonOutput, expectedResponse, klass, authRetryCount - 1);
				}
			}
			try
			{
				AuthenticatedURL.ExtractToken(conn, authToken);
			}
			catch (AuthenticationException)
			{
			}
			// Ignore the AuthExceptions.. since we are just using the method to
			// extract and set the authToken.. (Workaround till we actually fix
			// AuthenticatedURL properly to set authToken post initialization)
			HttpExceptionUtils.ValidateResponse(conn, expectedResponse);
			if (Sharpen.Runtime.EqualsIgnoreCase(ApplicationJsonMime, conn.GetContentType()) 
				&& klass != null)
			{
				ObjectMapper mapper = new ObjectMapper();
				InputStream @is = null;
				try
				{
					@is = conn.GetInputStream();
					ret = mapper.ReadValue(@is, klass);
				}
				catch (IOException ex)
				{
					if (@is != null)
					{
						@is.Close();
					}
					throw;
				}
				finally
				{
					if (@is != null)
					{
						@is.Close();
					}
				}
			}
			return ret;
		}

		public class KMSKeyVersion : KeyProvider.KeyVersion
		{
			public KMSKeyVersion(string keyName, string versionName, byte[] material)
				: base(keyName, versionName, material)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion GetKeyVersion(string versionName)
		{
			CheckNotEmpty(versionName, "versionName");
			Uri url = CreateURL(KMSRESTConstants.KeyVersionResource, versionName, null, null);
			HttpURLConnection conn = CreateConnection(url, HttpGet);
			IDictionary response = Call<IDictionary>(conn, null, HttpURLConnection.HttpOk);
			return ParseJSONKeyVersion(response);
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion GetCurrentKey(string name)
		{
			CheckNotEmpty(name, "name");
			Uri url = CreateURL(KMSRESTConstants.KeyResource, name, KMSRESTConstants.CurrentVersionSubResource
				, null);
			HttpURLConnection conn = CreateConnection(url, HttpGet);
			IDictionary response = Call<IDictionary>(conn, null, HttpURLConnection.HttpOk);
			return ParseJSONKeyVersion(response);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> GetKeys()
		{
			Uri url = CreateURL(KMSRESTConstants.KeysNamesResource, null, null, null);
			HttpURLConnection conn = CreateConnection(url, HttpGet);
			IList response = Call<IList>(conn, null, HttpURLConnection.HttpOk);
			return (IList<string>)response;
		}

		public class KMSMetadata : KeyProvider.Metadata
		{
			public KMSMetadata(string cipher, int bitLength, string description, IDictionary<
				string, string> attributes, DateTime created, int versions)
				: base(cipher, bitLength, description, attributes, created, versions)
			{
			}
		}

		// breaking keyNames into sets to keep resulting URL undler 2000 chars
		private IList<string[]> CreateKeySets(string[] keyNames)
		{
			IList<string[]> list = new AList<string[]>();
			IList<string> batch = new AList<string>();
			int batchLen = 0;
			foreach (string name in keyNames)
			{
				int additionalLen = KMSRESTConstants.Key.Length + 1 + name.Length;
				batchLen += additionalLen;
				// topping at 1500 to account for initial URL and encoded names
				if (batchLen > 1500)
				{
					list.AddItem(Sharpen.Collections.ToArray(batch, new string[batch.Count]));
					batch = new AList<string>();
					batchLen = additionalLen;
				}
				batch.AddItem(name);
			}
			if (!batch.IsEmpty())
			{
				list.AddItem(Sharpen.Collections.ToArray(batch, new string[batch.Count]));
			}
			return list;
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.Metadata[] GetKeysMetadata(params string[] keyNames)
		{
			IList<KeyProvider.Metadata> keysMetadata = new AList<KeyProvider.Metadata>();
			IList<string[]> keySets = CreateKeySets(keyNames);
			foreach (string[] keySet in keySets)
			{
				if (keyNames.Length > 0)
				{
					IDictionary<string, object> queryStr = new Dictionary<string, object>();
					queryStr[KMSRESTConstants.Key] = keySet;
					Uri url = CreateURL(KMSRESTConstants.KeysMetadataResource, null, null, queryStr);
					HttpURLConnection conn = CreateConnection(url, HttpGet);
					IList<IDictionary> list = Call<IList>(conn, null, HttpURLConnection.HttpOk);
					foreach (IDictionary map in list)
					{
						keysMetadata.AddItem(ParseJSONMetadata(map));
					}
				}
			}
			return Sharpen.Collections.ToArray(keysMetadata, new KeyProvider.Metadata[keysMetadata
				.Count]);
		}

		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		private KeyProvider.KeyVersion CreateKeyInternal(string name, byte[] material, KeyProvider.Options
			 options)
		{
			CheckNotEmpty(name, "name");
			CheckNotNull(options, "options");
			IDictionary<string, object> jsonKey = new Dictionary<string, object>();
			jsonKey[KMSRESTConstants.NameField] = name;
			jsonKey[KMSRESTConstants.CipherField] = options.GetCipher();
			jsonKey[KMSRESTConstants.LengthField] = options.GetBitLength();
			if (material != null)
			{
				jsonKey[KMSRESTConstants.MaterialField] = Base64.EncodeBase64String(material);
			}
			if (options.GetDescription() != null)
			{
				jsonKey[KMSRESTConstants.DescriptionField] = options.GetDescription();
			}
			if (options.GetAttributes() != null && !options.GetAttributes().IsEmpty())
			{
				jsonKey[KMSRESTConstants.AttributesField] = options.GetAttributes();
			}
			Uri url = CreateURL(KMSRESTConstants.KeysResource, null, null, null);
			HttpURLConnection conn = CreateConnection(url, HttpPost);
			conn.SetRequestProperty(ContentType, ApplicationJsonMime);
			IDictionary response = Call<IDictionary>(conn, jsonKey, HttpURLConnection.HttpCreated
				);
			return ParseJSONKeyVersion(response);
		}

		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion CreateKey(string name, KeyProvider.Options
			 options)
		{
			return CreateKeyInternal(name, null, options);
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion CreateKey(string name, byte[] material, KeyProvider.Options
			 options)
		{
			CheckNotNull(material, "material");
			try
			{
				return CreateKeyInternal(name, material, options);
			}
			catch (NoSuchAlgorithmException ex)
			{
				throw new RuntimeException("It should not happen", ex);
			}
		}

		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		private KeyProvider.KeyVersion RollNewVersionInternal(string name, byte[] material
			)
		{
			CheckNotEmpty(name, "name");
			IDictionary<string, string> jsonMaterial = new Dictionary<string, string>();
			if (material != null)
			{
				jsonMaterial[KMSRESTConstants.MaterialField] = Base64.EncodeBase64String(material
					);
			}
			Uri url = CreateURL(KMSRESTConstants.KeyResource, name, null, null);
			HttpURLConnection conn = CreateConnection(url, HttpPost);
			conn.SetRequestProperty(ContentType, ApplicationJsonMime);
			IDictionary response = Call<IDictionary>(conn, jsonMaterial, HttpURLConnection.HttpOk
				);
			KeyProvider.KeyVersion keyVersion = ParseJSONKeyVersion(response);
			encKeyVersionQueue.Drain(name);
			return keyVersion;
		}

		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion RollNewVersion(string name)
		{
			return RollNewVersionInternal(name, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion RollNewVersion(string name, byte[] material
			)
		{
			CheckNotNull(material, "material");
			try
			{
				return RollNewVersionInternal(name, material);
			}
			catch (NoSuchAlgorithmException ex)
			{
				throw new RuntimeException("It should not happen", ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.GeneralSecurityException"/>
		public virtual KeyProviderCryptoExtension.EncryptedKeyVersion GenerateEncryptedKey
			(string encryptionKeyName)
		{
			try
			{
				return encKeyVersionQueue.GetNext(encryptionKeyName);
			}
			catch (ExecutionException e)
			{
				if (e.InnerException is SocketTimeoutException)
				{
					throw (SocketTimeoutException)e.InnerException;
				}
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.GeneralSecurityException"/>
		public virtual KeyProvider.KeyVersion DecryptEncryptedKey(KeyProviderCryptoExtension.EncryptedKeyVersion
			 encryptedKeyVersion)
		{
			CheckNotNull(encryptedKeyVersion.GetEncryptionKeyVersionName(), "versionName");
			CheckNotNull(encryptedKeyVersion.GetEncryptedKeyIv(), "iv");
			Preconditions.CheckArgument(encryptedKeyVersion.GetEncryptedKeyVersion().GetVersionName
				().Equals(KeyProviderCryptoExtension.Eek), "encryptedKey version name must be '%s', is '%s'"
				, KeyProviderCryptoExtension.Eek, encryptedKeyVersion.GetEncryptedKeyVersion().GetVersionName
				());
			CheckNotNull(encryptedKeyVersion.GetEncryptedKeyVersion(), "encryptedKey");
			IDictionary<string, string> @params = new Dictionary<string, string>();
			@params[KMSRESTConstants.EekOp] = KMSRESTConstants.EekDecrypt;
			IDictionary<string, object> jsonPayload = new Dictionary<string, object>();
			jsonPayload[KMSRESTConstants.NameField] = encryptedKeyVersion.GetEncryptionKeyName
				();
			jsonPayload[KMSRESTConstants.IvField] = Base64.EncodeBase64String(encryptedKeyVersion
				.GetEncryptedKeyIv());
			jsonPayload[KMSRESTConstants.MaterialField] = Base64.EncodeBase64String(encryptedKeyVersion
				.GetEncryptedKeyVersion().GetMaterial());
			Uri url = CreateURL(KMSRESTConstants.KeyVersionResource, encryptedKeyVersion.GetEncryptionKeyVersionName
				(), KMSRESTConstants.EekSubResource, @params);
			HttpURLConnection conn = CreateConnection(url, HttpPost);
			conn.SetRequestProperty(ContentType, ApplicationJsonMime);
			IDictionary response = Call<IDictionary>(conn, jsonPayload, HttpURLConnection.HttpOk
				);
			return ParseJSONKeyVersion(response);
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<KeyProvider.KeyVersion> GetKeyVersions(string name)
		{
			CheckNotEmpty(name, "name");
			Uri url = CreateURL(KMSRESTConstants.KeyResource, name, KMSRESTConstants.VersionsSubResource
				, null);
			HttpURLConnection conn = CreateConnection(url, HttpGet);
			IList response = Call<IList>(conn, null, HttpURLConnection.HttpOk);
			IList<KeyProvider.KeyVersion> versions = null;
			if (!response.IsEmpty())
			{
				versions = new AList<KeyProvider.KeyVersion>();
				foreach (object obj in response)
				{
					versions.AddItem(ParseJSONKeyVersion((IDictionary)obj));
				}
			}
			return versions;
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.Metadata GetMetadata(string name)
		{
			CheckNotEmpty(name, "name");
			Uri url = CreateURL(KMSRESTConstants.KeyResource, name, KMSRESTConstants.MetadataSubResource
				, null);
			HttpURLConnection conn = CreateConnection(url, HttpGet);
			IDictionary response = Call<IDictionary>(conn, null, HttpURLConnection.HttpOk);
			return ParseJSONMetadata(response);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteKey(string name)
		{
			CheckNotEmpty(name, "name");
			Uri url = CreateURL(KMSRESTConstants.KeyResource, name, null, null);
			HttpURLConnection conn = CreateConnection(url, HttpDelete);
			Call(conn, null, HttpURLConnection.HttpOk, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Flush()
		{
		}

		// NOP
		// the client does not keep any local state, thus flushing is not required
		// because of the client.
		// the server should not keep in memory state on behalf of clients either.
		/// <exception cref="System.IO.IOException"/>
		public virtual void WarmUpEncryptedKeys(params string[] keyNames)
		{
			try
			{
				encKeyVersionQueue.InitializeQueuesForKeys(keyNames);
			}
			catch (ExecutionException e)
			{
				throw new IOException(e);
			}
		}

		public virtual void Drain(string keyName)
		{
			encKeyVersionQueue.Drain(keyName);
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public virtual int GetEncKeyQueueSize(string keyName)
		{
			try
			{
				return encKeyVersionQueue.GetSize(keyName);
			}
			catch (ExecutionException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual Org.Apache.Hadoop.Security.Token.Token<object>[] AddDelegationTokens
			(string renewer, Credentials credentials)
		{
			Org.Apache.Hadoop.Security.Token.Token<object>[] tokens = null;
			Org.Apache.Hadoop.IO.Text dtService = GetDelegationTokenService();
			Org.Apache.Hadoop.Security.Token.Token<object> token = credentials.GetToken(dtService
				);
			if (token == null)
			{
				Uri url = CreateURL(null, null, null, null);
				DelegationTokenAuthenticatedURL authUrl = new DelegationTokenAuthenticatedURL(configurator
					);
				try
				{
					// 'actualUGI' is the UGI of the user creating the client 
					// It is possible that the creator of the KMSClientProvier
					// calls this method on behalf of a proxyUser (the doAsUser).
					// In which case this call has to be made as the proxy user.
					UserGroupInformation currentUgi = UserGroupInformation.GetCurrentUser();
					string doAsUser = (currentUgi.GetAuthenticationMethod() == UserGroupInformation.AuthenticationMethod
						.Proxy) ? currentUgi.GetShortUserName() : null;
					token = actualUgi.DoAs(new _PrivilegedExceptionAction_870(authUrl, url, renewer, 
						doAsUser));
					// Not using the cached token here.. Creating a new token here
					// everytime.
					if (token != null)
					{
						credentials.AddToken(token.GetService(), token);
						tokens = new Org.Apache.Hadoop.Security.Token.Token<object>[] { token };
					}
					else
					{
						throw new IOException("Got NULL as delegation token");
					}
				}
				catch (Exception)
				{
					Sharpen.Thread.CurrentThread().Interrupt();
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
			}
			return tokens;
		}

		private sealed class _PrivilegedExceptionAction_870 : PrivilegedExceptionAction<Org.Apache.Hadoop.Security.Token.Token
			<object>>
		{
			public _PrivilegedExceptionAction_870(DelegationTokenAuthenticatedURL authUrl, Uri
				 url, string renewer, string doAsUser)
			{
				this.authUrl = authUrl;
				this.url = url;
				this.renewer = renewer;
				this.doAsUser = doAsUser;
			}

			/// <exception cref="System.Exception"/>
			public Org.Apache.Hadoop.Security.Token.Token<object> Run()
			{
				return authUrl.GetDelegationToken(url, new DelegationTokenAuthenticatedURL.Token(
					), renewer, doAsUser);
			}

			private readonly DelegationTokenAuthenticatedURL authUrl;

			private readonly Uri url;

			private readonly string renewer;

			private readonly string doAsUser;
		}

		/// <exception cref="System.IO.IOException"/>
		private Org.Apache.Hadoop.IO.Text GetDelegationTokenService()
		{
			Uri url = new Uri(kmsUrl);
			IPEndPoint addr = new IPEndPoint(url.GetHost(), url.Port);
			Org.Apache.Hadoop.IO.Text dtService = SecurityUtil.BuildTokenService(addr);
			return dtService;
		}

		/// <summary>Shutdown valueQueue executor threads</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			try
			{
				encKeyVersionQueue.Shutdown();
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
			finally
			{
				if (sslFactory != null)
				{
					sslFactory.Destroy();
				}
			}
		}

		[VisibleForTesting]
		internal virtual string GetKMSUrl()
		{
			return kmsUrl;
		}
	}
}
