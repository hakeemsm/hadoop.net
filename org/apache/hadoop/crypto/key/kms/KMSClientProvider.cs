using Sharpen;

namespace org.apache.hadoop.crypto.key.kms
{
	/// <summary>KMS client <code>KeyProvider</code> implementation.</summary>
	public class KMSClientProvider : org.apache.hadoop.crypto.key.KeyProvider, org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension
		, org.apache.hadoop.crypto.key.KeyProviderDelegationTokenExtension.DelegationTokenExtension
	{
		private const string INVALID_SIGNATURE = "Invalid signature";

		private const string ANONYMOUS_REQUESTS_DISALLOWED = "Anonymous requests are disallowed";

		public const string TOKEN_KIND = "kms-dt";

		public const string SCHEME_NAME = "kms";

		private const string UTF8 = "UTF-8";

		private const string CONTENT_TYPE = "Content-Type";

		private const string APPLICATION_JSON_MIME = "application/json";

		private const string HTTP_GET = "GET";

		private const string HTTP_POST = "POST";

		private const string HTTP_PUT = "PUT";

		private const string HTTP_DELETE = "DELETE";

		private const string CONFIG_PREFIX = "hadoop.security.kms.client.";

		public const string TIMEOUT_ATTR = CONFIG_PREFIX + "timeout";

		public const int DEFAULT_TIMEOUT = 60;

		public const string AUTH_RETRY = CONFIG_PREFIX + "authentication.retry-count";

		public const int DEFAULT_AUTH_RETRY = 1;

		private readonly org.apache.hadoop.crypto.key.kms.ValueQueue<org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
			> encKeyVersionQueue;

		private class EncryptedQueueRefiller : org.apache.hadoop.crypto.key.kms.ValueQueue.QueueRefiller
			<org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion>
		{
			/* It's possible to specify a timeout, in seconds, in the config file */
			/* Number of times to retry authentication in the event of auth failure
			* (normally happens due to stale authToken)
			*/
			/// <exception cref="System.IO.IOException"/>
			public virtual void fillQueueForKey(string keyName, java.util.Queue<org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
				> keyQueue, int numEKVs)
			{
				org.apache.hadoop.crypto.key.kms.KMSClientProvider.checkNotNull(keyName, "keyName"
					);
				System.Collections.Generic.IDictionary<string, string> @params = new System.Collections.Generic.Dictionary
					<string, string>();
				@params[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.EEK_OP] = org.apache.hadoop.crypto.key.kms.KMSRESTConstants
					.EEK_GENERATE;
				@params[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.EEK_NUM_KEYS] = string.Empty
					 + numEKVs;
				java.net.URL url = this._enclosing.createURL(org.apache.hadoop.crypto.key.kms.KMSRESTConstants
					.KEY_RESOURCE, keyName, org.apache.hadoop.crypto.key.kms.KMSRESTConstants.EEK_SUB_RESOURCE
					, @params);
				java.net.HttpURLConnection conn = this._enclosing.createConnection(url, org.apache.hadoop.crypto.key.kms.KMSClientProvider
					.HTTP_GET);
				conn.setRequestProperty(org.apache.hadoop.crypto.key.kms.KMSClientProvider.CONTENT_TYPE
					, org.apache.hadoop.crypto.key.kms.KMSClientProvider.APPLICATION_JSON_MIME);
				System.Collections.IList response = this._enclosing.call<System.Collections.IList
					>(conn, null, java.net.HttpURLConnection.HTTP_OK);
				System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
					> ekvs = org.apache.hadoop.crypto.key.kms.KMSClientProvider.parseJSONEncKeyVersion
					(keyName, response);
				Sharpen.Collections.AddAll(keyQueue, ekvs);
			}

			internal EncryptedQueueRefiller(KMSClientProvider _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly KMSClientProvider _enclosing;
		}

		public class KMSEncryptedKeyVersion : org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
		{
			public KMSEncryptedKeyVersion(string keyName, string keyVersionName, byte[] iv, string
				 encryptedVersionName, byte[] keyMaterial)
				: base(keyName, keyVersionName, iv, new org.apache.hadoop.crypto.key.kms.KMSClientProvider.KMSKeyVersion
					(null, encryptedVersionName, keyMaterial))
			{
			}
		}

		private static System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
			> parseJSONEncKeyVersion(string keyName, System.Collections.IList valueList)
		{
			System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
				> ekvs = new System.Collections.Generic.LinkedList<org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
				>();
			if (!valueList.isEmpty())
			{
				foreach (object values in valueList)
				{
					System.Collections.IDictionary valueMap = (System.Collections.IDictionary)values;
					string versionName = checkNotNull((string)valueMap[org.apache.hadoop.crypto.key.kms.KMSRESTConstants
						.VERSION_NAME_FIELD], org.apache.hadoop.crypto.key.kms.KMSRESTConstants.VERSION_NAME_FIELD
						);
					byte[] iv = org.apache.commons.codec.binary.Base64.decodeBase64(checkNotNull((string
						)valueMap[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.IV_FIELD], org.apache.hadoop.crypto.key.kms.KMSRESTConstants
						.IV_FIELD));
					System.Collections.IDictionary encValueMap = checkNotNull((System.Collections.IDictionary
						)valueMap[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.ENCRYPTED_KEY_VERSION_FIELD
						], org.apache.hadoop.crypto.key.kms.KMSRESTConstants.ENCRYPTED_KEY_VERSION_FIELD
						);
					string encVersionName = checkNotNull((string)encValueMap[org.apache.hadoop.crypto.key.kms.KMSRESTConstants
						.VERSION_NAME_FIELD], org.apache.hadoop.crypto.key.kms.KMSRESTConstants.VERSION_NAME_FIELD
						);
					byte[] encKeyMaterial = org.apache.commons.codec.binary.Base64.decodeBase64(checkNotNull
						((string)encValueMap[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.MATERIAL_FIELD
						], org.apache.hadoop.crypto.key.kms.KMSRESTConstants.MATERIAL_FIELD));
					ekvs.add(new org.apache.hadoop.crypto.key.kms.KMSClientProvider.KMSEncryptedKeyVersion
						(keyName, versionName, iv, encVersionName, encKeyMaterial));
				}
			}
			return ekvs;
		}

		private static org.apache.hadoop.crypto.key.KeyProvider.KeyVersion parseJSONKeyVersion
			(System.Collections.IDictionary valueMap)
		{
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion keyVersion = null;
			if (!valueMap.isEmpty())
			{
				byte[] material = (valueMap.Contains(org.apache.hadoop.crypto.key.kms.KMSRESTConstants
					.MATERIAL_FIELD)) ? org.apache.commons.codec.binary.Base64.decodeBase64((string)
					valueMap[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.MATERIAL_FIELD]) : null;
				string versionName = (string)valueMap[org.apache.hadoop.crypto.key.kms.KMSRESTConstants
					.VERSION_NAME_FIELD];
				string keyName = (string)valueMap[org.apache.hadoop.crypto.key.kms.KMSRESTConstants
					.NAME_FIELD];
				keyVersion = new org.apache.hadoop.crypto.key.kms.KMSClientProvider.KMSKeyVersion
					(keyName, versionName, material);
			}
			return keyVersion;
		}

		private static org.apache.hadoop.crypto.key.KeyProvider.Metadata parseJSONMetadata
			(System.Collections.IDictionary valueMap)
		{
			org.apache.hadoop.crypto.key.KeyProvider.Metadata metadata = null;
			if (!valueMap.isEmpty())
			{
				metadata = new org.apache.hadoop.crypto.key.kms.KMSClientProvider.KMSMetadata((string
					)valueMap[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.CIPHER_FIELD], (int)
					valueMap[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.LENGTH_FIELD], (string
					)valueMap[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.DESCRIPTION_FIELD], 
					(System.Collections.Generic.IDictionary<string, string>)valueMap[org.apache.hadoop.crypto.key.kms.KMSRESTConstants
					.ATTRIBUTES_FIELD], new System.DateTime((long)valueMap[org.apache.hadoop.crypto.key.kms.KMSRESTConstants
					.CREATED_FIELD]), (int)valueMap[org.apache.hadoop.crypto.key.kms.KMSRESTConstants
					.VERSIONS_FIELD]);
			}
			return metadata;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void writeJson(System.Collections.IDictionary map, java.io.OutputStream
			 os)
		{
			System.IO.TextWriter writer = new java.io.OutputStreamWriter(os, org.apache.commons.io.Charsets
				.UTF_8);
			org.codehaus.jackson.map.ObjectMapper jsonMapper = new org.codehaus.jackson.map.ObjectMapper
				();
			jsonMapper.writerWithDefaultPrettyPrinter().writeValue(writer, map);
		}

		/// <summary>
		/// The factory to create KMSClientProvider, which is used by the
		/// ServiceLoader.
		/// </summary>
		public class Factory : org.apache.hadoop.crypto.key.KeyProviderFactory
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
			public override org.apache.hadoop.crypto.key.KeyProvider createProvider(java.net.URI
				 providerUri, org.apache.hadoop.conf.Configuration conf)
			{
				if (SCHEME_NAME.Equals(providerUri.getScheme()))
				{
					java.net.URL origUrl = new java.net.URL(extractKMSPath(providerUri).ToString());
					string authority = origUrl.getAuthority();
					// check for ';' which delimits the backup hosts
					if (com.google.common.@base.Strings.isNullOrEmpty(authority))
					{
						throw new System.IO.IOException("No valid authority in kms uri [" + origUrl + "]"
							);
					}
					// Check if port is present in authority
					// In the current scheme, all hosts have to run on the same port
					int port = -1;
					string hostsPart = authority;
					if (authority.contains(":"))
					{
						string[] t = authority.split(":");
						try
						{
							port = System.Convert.ToInt32(t[1]);
						}
						catch (System.Exception)
						{
							throw new System.IO.IOException("Could not parse port in kms uri [" + origUrl + "]"
								);
						}
						hostsPart = t[0];
					}
					return createProvider(providerUri, conf, origUrl, port, hostsPart);
				}
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			private org.apache.hadoop.crypto.key.KeyProvider createProvider(java.net.URI providerUri
				, org.apache.hadoop.conf.Configuration conf, java.net.URL origUrl, int port, string
				 hostsPart)
			{
				string[] hosts = hostsPart.split(";");
				if (hosts.Length == 1)
				{
					return new org.apache.hadoop.crypto.key.kms.KMSClientProvider(providerUri, conf);
				}
				else
				{
					org.apache.hadoop.crypto.key.kms.KMSClientProvider[] providers = new org.apache.hadoop.crypto.key.kms.KMSClientProvider
						[hosts.Length];
					for (int i = 0; i < hosts.Length; i++)
					{
						try
						{
							providers[i] = new org.apache.hadoop.crypto.key.kms.KMSClientProvider(new java.net.URI
								("kms", origUrl.getProtocol(), hosts[i], port, origUrl.getPath(), null, null), conf
								);
						}
						catch (java.net.URISyntaxException e)
						{
							throw new System.IO.IOException("Could not instantiate KMSProvider..", e);
						}
					}
					return new org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider(providers
						, conf);
				}
			}
		}

		/// <exception cref="System.ArgumentException"/>
		public static T checkNotNull<T>(T o, string name)
		{
			if (o == null)
			{
				throw new System.ArgumentException("Parameter '" + name + "' cannot be null");
			}
			return o;
		}

		/// <exception cref="System.ArgumentException"/>
		public static string checkNotEmpty(string s, string name)
		{
			checkNotNull(s, name);
			if (s.isEmpty())
			{
				throw new System.ArgumentException("Parameter '" + name + "' cannot be empty");
			}
			return s;
		}

		private string kmsUrl;

		private org.apache.hadoop.security.ssl.SSLFactory sslFactory;

		private org.apache.hadoop.security.authentication.client.ConnectionConfigurator configurator;

		private org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
			 authToken;

		private readonly int authRetry;

		private readonly org.apache.hadoop.security.UserGroupInformation actualUgi;

		public override string ToString()
		{
			java.lang.StringBuilder sb = new java.lang.StringBuilder("KMSClientProvider[");
			sb.Append(kmsUrl).Append("]");
			return sb.ToString();
		}

		/// <summary>This small class exists to set the timeout values for a connection</summary>
		private class TimeoutConnConfigurator : org.apache.hadoop.security.authentication.client.ConnectionConfigurator
		{
			private org.apache.hadoop.security.authentication.client.ConnectionConfigurator cc;

			private int timeout;

			/// <summary>Sets the timeout and wraps another connection configurator</summary>
			/// <param name="timeout">- will set both connect and read timeouts - in seconds</param>
			/// <param name="cc">- another configurator to wrap - may be null</param>
			public TimeoutConnConfigurator(int timeout, org.apache.hadoop.security.authentication.client.ConnectionConfigurator
				 cc)
			{
				this.timeout = timeout;
				this.cc = cc;
			}

			/// <summary>Calls the wrapped configure() method, then sets timeouts</summary>
			/// <param name="conn">
			/// the
			/// <see cref="java.net.HttpURLConnection"/>
			/// instance to configure.
			/// </param>
			/// <returns>the connection</returns>
			/// <exception cref="System.IO.IOException"/>
			public virtual java.net.HttpURLConnection configure(java.net.HttpURLConnection conn
				)
			{
				if (cc != null)
				{
					conn = cc.configure(conn);
				}
				conn.setConnectTimeout(timeout * 1000);
				// conversion to milliseconds
				conn.setReadTimeout(timeout * 1000);
				return conn;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public KMSClientProvider(java.net.URI uri, org.apache.hadoop.conf.Configuration conf
			)
			: base(conf)
		{
			kmsUrl = createServiceURL(extractKMSPath(uri));
			if (Sharpen.Runtime.equalsIgnoreCase("https", new java.net.URL(kmsUrl).getProtocol
				()))
			{
				sslFactory = new org.apache.hadoop.security.ssl.SSLFactory(org.apache.hadoop.security.ssl.SSLFactory.Mode
					.CLIENT, conf);
				try
				{
					sslFactory.init();
				}
				catch (java.security.GeneralSecurityException ex)
				{
					throw new System.IO.IOException(ex);
				}
			}
			int timeout = conf.getInt(TIMEOUT_ATTR, DEFAULT_TIMEOUT);
			authRetry = conf.getInt(AUTH_RETRY, DEFAULT_AUTH_RETRY);
			configurator = new org.apache.hadoop.crypto.key.kms.KMSClientProvider.TimeoutConnConfigurator
				(timeout, sslFactory);
			encKeyVersionQueue = new org.apache.hadoop.crypto.key.kms.ValueQueue<org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
				>(conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_SIZE
				, org.apache.hadoop.fs.CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_SIZE_DEFAULT
				), conf.getFloat(org.apache.hadoop.fs.CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK
				, org.apache.hadoop.fs.CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_LOW_WATERMARK_DEFAULT
				), conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_MS
				, org.apache.hadoop.fs.CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_EXPIRY_DEFAULT
				), conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_NUM_REFILL_THREADS
				, org.apache.hadoop.fs.CommonConfigurationKeysPublic.KMS_CLIENT_ENC_KEY_CACHE_NUM_REFILL_THREADS_DEFAULT
				), new org.apache.hadoop.crypto.key.kms.KMSClientProvider.EncryptedQueueRefiller
				(this));
			authToken = new org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
				();
			actualUgi = (org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getAuthenticationMethod
				() == org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.PROXY
				) ? org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getRealUser
				() : org.apache.hadoop.security.UserGroupInformation.getCurrentUser();
		}

		/// <exception cref="java.net.MalformedURLException"/>
		/// <exception cref="System.IO.IOException"/>
		private static org.apache.hadoop.fs.Path extractKMSPath(java.net.URI uri)
		{
			return org.apache.hadoop.security.ProviderUtils.unnestUri(uri);
		}

		/// <exception cref="System.IO.IOException"/>
		private static string createServiceURL(org.apache.hadoop.fs.Path path)
		{
			string str = new java.net.URL(path.ToString()).toExternalForm();
			if (str.EndsWith("/"))
			{
				str = Sharpen.Runtime.substring(str, 0, str.Length - 1);
			}
			return new java.net.URL(str + org.apache.hadoop.crypto.key.kms.KMSRESTConstants.SERVICE_VERSION
				 + "/").toExternalForm();
		}

		/// <exception cref="System.IO.IOException"/>
		private java.net.URL createURL(string collection, string resource, string subResource
			, System.Collections.Generic.IDictionary<string, object> parameters)
		{
			try
			{
				java.lang.StringBuilder sb = new java.lang.StringBuilder();
				sb.Append(kmsUrl);
				if (collection != null)
				{
					sb.Append(collection);
					if (resource != null)
					{
						sb.Append("/").Append(java.net.URLEncoder.encode(resource, UTF8));
						if (subResource != null)
						{
							sb.Append("/").Append(subResource);
						}
					}
				}
				org.apache.http.client.utils.URIBuilder uriBuilder = new org.apache.http.client.utils.URIBuilder
					(sb.ToString());
				if (parameters != null)
				{
					foreach (System.Collections.Generic.KeyValuePair<string, object> param in parameters)
					{
						object value = param.Value;
						if (value is string)
						{
							uriBuilder.addParameter(param.Key, (string)value);
						}
						else
						{
							foreach (string s in (string[])value)
							{
								uriBuilder.addParameter(param.Key, s);
							}
						}
					}
				}
				return uriBuilder.build().toURL();
			}
			catch (java.net.URISyntaxException ex)
			{
				throw new System.IO.IOException(ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private java.net.HttpURLConnection configureConnection(java.net.HttpURLConnection
			 conn)
		{
			if (sslFactory != null)
			{
				javax.net.ssl.HttpsURLConnection httpsConn = (javax.net.ssl.HttpsURLConnection)conn;
				try
				{
					httpsConn.setSSLSocketFactory(sslFactory.createSSLSocketFactory());
				}
				catch (java.security.GeneralSecurityException ex)
				{
					throw new System.IO.IOException(ex);
				}
				httpsConn.setHostnameVerifier(sslFactory.getHostnameVerifier());
			}
			return conn;
		}

		/// <exception cref="System.IO.IOException"/>
		private java.net.HttpURLConnection createConnection(java.net.URL url, string method
			)
		{
			java.net.HttpURLConnection conn;
			try
			{
				// if current UGI is different from UGI at constructor time, behave as
				// proxyuser
				org.apache.hadoop.security.UserGroupInformation currentUgi = org.apache.hadoop.security.UserGroupInformation
					.getCurrentUser();
				string doAsUser = (currentUgi.getAuthenticationMethod() == org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
					.PROXY) ? currentUgi.getShortUserName() : null;
				// creating the HTTP connection using the current UGI at constructor time
				conn = actualUgi.doAs(new _PrivilegedExceptionAction_478(this, url, doAsUser));
			}
			catch (System.IO.IOException ex)
			{
				throw;
			}
			catch (java.lang.reflect.UndeclaredThrowableException ex)
			{
				throw new System.IO.IOException(ex.getUndeclaredThrowable());
			}
			catch (System.Exception ex)
			{
				throw new System.IO.IOException(ex);
			}
			conn.setUseCaches(false);
			conn.setRequestMethod(method);
			if (method.Equals(HTTP_POST) || method.Equals(HTTP_PUT))
			{
				conn.setDoOutput(true);
			}
			conn = configureConnection(conn);
			return conn;
		}

		private sealed class _PrivilegedExceptionAction_478 : java.security.PrivilegedExceptionAction
			<java.net.HttpURLConnection>
		{
			public _PrivilegedExceptionAction_478(KMSClientProvider _enclosing, java.net.URL 
				url, string doAsUser)
			{
				this._enclosing = _enclosing;
				this.url = url;
				this.doAsUser = doAsUser;
			}

			/// <exception cref="System.Exception"/>
			public java.net.HttpURLConnection run()
			{
				org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL authUrl
					 = new org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL
					(this._enclosing.configurator);
				return authUrl.openConnection(url, this._enclosing.authToken, doAsUser);
			}

			private readonly KMSClientProvider _enclosing;

			private readonly java.net.URL url;

			private readonly string doAsUser;
		}

		/// <exception cref="System.IO.IOException"/>
		private T call<T>(java.net.HttpURLConnection conn, System.Collections.IDictionary
			 jsonOutput, int expectedResponse)
		{
			System.Type klass = typeof(T);
			return call(conn, jsonOutput, expectedResponse, klass, authRetry);
		}

		/// <exception cref="System.IO.IOException"/>
		private T call<T>(java.net.HttpURLConnection conn, System.Collections.IDictionary
			 jsonOutput, int expectedResponse, int authRetryCount)
		{
			System.Type klass = typeof(T);
			T ret = null;
			try
			{
				if (jsonOutput != null)
				{
					writeJson(jsonOutput, conn.getOutputStream());
				}
			}
			catch (System.IO.IOException ex)
			{
				conn.getInputStream().close();
				throw;
			}
			if ((conn.getResponseCode() == java.net.HttpURLConnection.HTTP_FORBIDDEN && (conn
				.getResponseMessage().Equals(ANONYMOUS_REQUESTS_DISALLOWED) || conn.getResponseMessage
				().contains(INVALID_SIGNATURE))) || conn.getResponseCode() == java.net.HttpURLConnection
				.HTTP_UNAUTHORIZED)
			{
				// Ideally, this should happen only when there is an Authentication
				// failure. Unfortunately, the AuthenticationFilter returns 403 when it
				// cannot authenticate (Since a 401 requires Server to send
				// WWW-Authenticate header as well)..
				this.authToken = new org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
					();
				if (authRetryCount > 0)
				{
					string contentType = conn.getRequestProperty(CONTENT_TYPE);
					string requestMethod = conn.getRequestMethod();
					java.net.URL url = conn.getURL();
					conn = createConnection(url, requestMethod);
					conn.setRequestProperty(CONTENT_TYPE, contentType);
					return call(conn, jsonOutput, expectedResponse, klass, authRetryCount - 1);
				}
			}
			try
			{
				org.apache.hadoop.security.authentication.client.AuthenticatedURL.extractToken(conn
					, authToken);
			}
			catch (org.apache.hadoop.security.authentication.client.AuthenticationException)
			{
			}
			// Ignore the AuthExceptions.. since we are just using the method to
			// extract and set the authToken.. (Workaround till we actually fix
			// AuthenticatedURL properly to set authToken post initialization)
			org.apache.hadoop.util.HttpExceptionUtils.validateResponse(conn, expectedResponse
				);
			if (Sharpen.Runtime.equalsIgnoreCase(APPLICATION_JSON_MIME, conn.getContentType()
				) && klass != null)
			{
				org.codehaus.jackson.map.ObjectMapper mapper = new org.codehaus.jackson.map.ObjectMapper
					();
				java.io.InputStream @is = null;
				try
				{
					@is = conn.getInputStream();
					ret = mapper.readValue(@is, klass);
				}
				catch (System.IO.IOException ex)
				{
					if (@is != null)
					{
						@is.close();
					}
					throw;
				}
				finally
				{
					if (@is != null)
					{
						@is.close();
					}
				}
			}
			return ret;
		}

		public class KMSKeyVersion : org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
		{
			public KMSKeyVersion(string keyName, string versionName, byte[] material)
				: base(keyName, versionName, material)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion getKeyVersion
			(string versionName)
		{
			checkNotEmpty(versionName, "versionName");
			java.net.URL url = createURL(org.apache.hadoop.crypto.key.kms.KMSRESTConstants.KEY_VERSION_RESOURCE
				, versionName, null, null);
			java.net.HttpURLConnection conn = createConnection(url, HTTP_GET);
			System.Collections.IDictionary response = call<System.Collections.IDictionary>(conn
				, null, java.net.HttpURLConnection.HTTP_OK);
			return parseJSONKeyVersion(response);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion getCurrentKey
			(string name)
		{
			checkNotEmpty(name, "name");
			java.net.URL url = createURL(org.apache.hadoop.crypto.key.kms.KMSRESTConstants.KEY_RESOURCE
				, name, org.apache.hadoop.crypto.key.kms.KMSRESTConstants.CURRENT_VERSION_SUB_RESOURCE
				, null);
			java.net.HttpURLConnection conn = createConnection(url, HTTP_GET);
			System.Collections.IDictionary response = call<System.Collections.IDictionary>(conn
				, null, java.net.HttpURLConnection.HTTP_OK);
			return parseJSONKeyVersion(response);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> getKeys()
		{
			java.net.URL url = createURL(org.apache.hadoop.crypto.key.kms.KMSRESTConstants.KEYS_NAMES_RESOURCE
				, null, null, null);
			java.net.HttpURLConnection conn = createConnection(url, HTTP_GET);
			System.Collections.IList response = call<System.Collections.IList>(conn, null, java.net.HttpURLConnection
				.HTTP_OK);
			return (System.Collections.Generic.IList<string>)response;
		}

		public class KMSMetadata : org.apache.hadoop.crypto.key.KeyProvider.Metadata
		{
			public KMSMetadata(string cipher, int bitLength, string description, System.Collections.Generic.IDictionary
				<string, string> attributes, System.DateTime created, int versions)
				: base(cipher, bitLength, description, attributes, created, versions)
			{
			}
		}

		// breaking keyNames into sets to keep resulting URL undler 2000 chars
		private System.Collections.Generic.IList<string[]> createKeySets(string[] keyNames
			)
		{
			System.Collections.Generic.IList<string[]> list = new System.Collections.Generic.List
				<string[]>();
			System.Collections.Generic.IList<string> batch = new System.Collections.Generic.List
				<string>();
			int batchLen = 0;
			foreach (string name in keyNames)
			{
				int additionalLen = org.apache.hadoop.crypto.key.kms.KMSRESTConstants.KEY.Length 
					+ 1 + name.Length;
				batchLen += additionalLen;
				// topping at 1500 to account for initial URL and encoded names
				if (batchLen > 1500)
				{
					list.add(Sharpen.Collections.ToArray(batch, new string[batch.Count]));
					batch = new System.Collections.Generic.List<string>();
					batchLen = additionalLen;
				}
				batch.add(name);
			}
			if (!batch.isEmpty())
			{
				list.add(Sharpen.Collections.ToArray(batch, new string[batch.Count]));
			}
			return list;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.Metadata[] getKeysMetadata
			(params string[] keyNames)
		{
			System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider.Metadata
				> keysMetadata = new System.Collections.Generic.List<org.apache.hadoop.crypto.key.KeyProvider.Metadata
				>();
			System.Collections.Generic.IList<string[]> keySets = createKeySets(keyNames);
			foreach (string[] keySet in keySets)
			{
				if (keyNames.Length > 0)
				{
					System.Collections.Generic.IDictionary<string, object> queryStr = new System.Collections.Generic.Dictionary
						<string, object>();
					queryStr[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.KEY] = keySet;
					java.net.URL url = createURL(org.apache.hadoop.crypto.key.kms.KMSRESTConstants.KEYS_METADATA_RESOURCE
						, null, null, queryStr);
					java.net.HttpURLConnection conn = createConnection(url, HTTP_GET);
					System.Collections.Generic.IList<System.Collections.IDictionary> list = call<System.Collections.IList
						>(conn, null, java.net.HttpURLConnection.HTTP_OK);
					foreach (System.Collections.IDictionary map in list)
					{
						keysMetadata.add(parseJSONMetadata(map));
					}
				}
			}
			return Sharpen.Collections.ToArray(keysMetadata, new org.apache.hadoop.crypto.key.KeyProvider.Metadata
				[keysMetadata.Count]);
		}

		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.crypto.key.KeyProvider.KeyVersion createKeyInternal(string
			 name, byte[] material, org.apache.hadoop.crypto.key.KeyProvider.Options options
			)
		{
			checkNotEmpty(name, "name");
			checkNotNull(options, "options");
			System.Collections.Generic.IDictionary<string, object> jsonKey = new System.Collections.Generic.Dictionary
				<string, object>();
			jsonKey[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.NAME_FIELD] = name;
			jsonKey[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.CIPHER_FIELD] = options
				.getCipher();
			jsonKey[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.LENGTH_FIELD] = options
				.getBitLength();
			if (material != null)
			{
				jsonKey[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.MATERIAL_FIELD] = org.apache.commons.codec.binary.Base64
					.encodeBase64String(material);
			}
			if (options.getDescription() != null)
			{
				jsonKey[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.DESCRIPTION_FIELD] = options
					.getDescription();
			}
			if (options.getAttributes() != null && !options.getAttributes().isEmpty())
			{
				jsonKey[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.ATTRIBUTES_FIELD] = options
					.getAttributes();
			}
			java.net.URL url = createURL(org.apache.hadoop.crypto.key.kms.KMSRESTConstants.KEYS_RESOURCE
				, null, null, null);
			java.net.HttpURLConnection conn = createConnection(url, HTTP_POST);
			conn.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON_MIME);
			System.Collections.IDictionary response = call<System.Collections.IDictionary>(conn
				, jsonKey, java.net.HttpURLConnection.HTTP_CREATED);
			return parseJSONKeyVersion(response);
		}

		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion createKey(string
			 name, org.apache.hadoop.crypto.key.KeyProvider.Options options)
		{
			return createKeyInternal(name, null, options);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion createKey(string
			 name, byte[] material, org.apache.hadoop.crypto.key.KeyProvider.Options options
			)
		{
			checkNotNull(material, "material");
			try
			{
				return createKeyInternal(name, material, options);
			}
			catch (java.security.NoSuchAlgorithmException ex)
			{
				throw new System.Exception("It should not happen", ex);
			}
		}

		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.crypto.key.KeyProvider.KeyVersion rollNewVersionInternal
			(string name, byte[] material)
		{
			checkNotEmpty(name, "name");
			System.Collections.Generic.IDictionary<string, string> jsonMaterial = new System.Collections.Generic.Dictionary
				<string, string>();
			if (material != null)
			{
				jsonMaterial[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.MATERIAL_FIELD] = 
					org.apache.commons.codec.binary.Base64.encodeBase64String(material);
			}
			java.net.URL url = createURL(org.apache.hadoop.crypto.key.kms.KMSRESTConstants.KEY_RESOURCE
				, name, null, null);
			java.net.HttpURLConnection conn = createConnection(url, HTTP_POST);
			conn.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON_MIME);
			System.Collections.IDictionary response = call<System.Collections.IDictionary>(conn
				, jsonMaterial, java.net.HttpURLConnection.HTTP_OK);
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion keyVersion = parseJSONKeyVersion
				(response);
			encKeyVersionQueue.drain(name);
			return keyVersion;
		}

		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion rollNewVersion
			(string name)
		{
			return rollNewVersionInternal(name, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion rollNewVersion
			(string name, byte[] material)
		{
			checkNotNull(material, "material");
			try
			{
				return rollNewVersionInternal(name, material);
			}
			catch (java.security.NoSuchAlgorithmException ex)
			{
				throw new System.Exception("It should not happen", ex);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.security.GeneralSecurityException"/>
		public virtual org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
			 generateEncryptedKey(string encryptionKeyName)
		{
			try
			{
				return encKeyVersionQueue.getNext(encryptionKeyName);
			}
			catch (java.util.concurrent.ExecutionException e)
			{
				if (e.InnerException is java.net.SocketTimeoutException)
				{
					throw (java.net.SocketTimeoutException)e.InnerException;
				}
				throw new System.IO.IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.security.GeneralSecurityException"/>
		public virtual org.apache.hadoop.crypto.key.KeyProvider.KeyVersion decryptEncryptedKey
			(org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion encryptedKeyVersion
			)
		{
			checkNotNull(encryptedKeyVersion.getEncryptionKeyVersionName(), "versionName");
			checkNotNull(encryptedKeyVersion.getEncryptedKeyIv(), "iv");
			com.google.common.@base.Preconditions.checkArgument(encryptedKeyVersion.getEncryptedKeyVersion
				().getVersionName().Equals(org.apache.hadoop.crypto.key.KeyProviderCryptoExtension
				.EEK), "encryptedKey version name must be '%s', is '%s'", org.apache.hadoop.crypto.key.KeyProviderCryptoExtension
				.EEK, encryptedKeyVersion.getEncryptedKeyVersion().getVersionName());
			checkNotNull(encryptedKeyVersion.getEncryptedKeyVersion(), "encryptedKey");
			System.Collections.Generic.IDictionary<string, string> @params = new System.Collections.Generic.Dictionary
				<string, string>();
			@params[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.EEK_OP] = org.apache.hadoop.crypto.key.kms.KMSRESTConstants
				.EEK_DECRYPT;
			System.Collections.Generic.IDictionary<string, object> jsonPayload = new System.Collections.Generic.Dictionary
				<string, object>();
			jsonPayload[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.NAME_FIELD] = encryptedKeyVersion
				.getEncryptionKeyName();
			jsonPayload[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.IV_FIELD] = org.apache.commons.codec.binary.Base64
				.encodeBase64String(encryptedKeyVersion.getEncryptedKeyIv());
			jsonPayload[org.apache.hadoop.crypto.key.kms.KMSRESTConstants.MATERIAL_FIELD] = org.apache.commons.codec.binary.Base64
				.encodeBase64String(encryptedKeyVersion.getEncryptedKeyVersion().getMaterial());
			java.net.URL url = createURL(org.apache.hadoop.crypto.key.kms.KMSRESTConstants.KEY_VERSION_RESOURCE
				, encryptedKeyVersion.getEncryptionKeyVersionName(), org.apache.hadoop.crypto.key.kms.KMSRESTConstants
				.EEK_SUB_RESOURCE, @params);
			java.net.HttpURLConnection conn = createConnection(url, HTTP_POST);
			conn.setRequestProperty(CONTENT_TYPE, APPLICATION_JSON_MIME);
			System.Collections.IDictionary response = call<System.Collections.IDictionary>(conn
				, jsonPayload, java.net.HttpURLConnection.HTTP_OK);
			return parseJSONKeyVersion(response);
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
			> getKeyVersions(string name)
		{
			checkNotEmpty(name, "name");
			java.net.URL url = createURL(org.apache.hadoop.crypto.key.kms.KMSRESTConstants.KEY_RESOURCE
				, name, org.apache.hadoop.crypto.key.kms.KMSRESTConstants.VERSIONS_SUB_RESOURCE, 
				null);
			java.net.HttpURLConnection conn = createConnection(url, HTTP_GET);
			System.Collections.IList response = call<System.Collections.IList>(conn, null, java.net.HttpURLConnection
				.HTTP_OK);
			System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
				> versions = null;
			if (!response.isEmpty())
			{
				versions = new System.Collections.Generic.List<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
					>();
				foreach (object obj in response)
				{
					versions.add(parseJSONKeyVersion((System.Collections.IDictionary)obj));
				}
			}
			return versions;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.Metadata getMetadata(string
			 name)
		{
			checkNotEmpty(name, "name");
			java.net.URL url = createURL(org.apache.hadoop.crypto.key.kms.KMSRESTConstants.KEY_RESOURCE
				, name, org.apache.hadoop.crypto.key.kms.KMSRESTConstants.METADATA_SUB_RESOURCE, 
				null);
			java.net.HttpURLConnection conn = createConnection(url, HTTP_GET);
			System.Collections.IDictionary response = call<System.Collections.IDictionary>(conn
				, null, java.net.HttpURLConnection.HTTP_OK);
			return parseJSONMetadata(response);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void deleteKey(string name)
		{
			checkNotEmpty(name, "name");
			java.net.URL url = createURL(org.apache.hadoop.crypto.key.kms.KMSRESTConstants.KEY_RESOURCE
				, name, null, null);
			java.net.HttpURLConnection conn = createConnection(url, HTTP_DELETE);
			call(conn, null, java.net.HttpURLConnection.HTTP_OK, null);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void flush()
		{
		}

		// NOP
		// the client does not keep any local state, thus flushing is not required
		// because of the client.
		// the server should not keep in memory state on behalf of clients either.
		/// <exception cref="System.IO.IOException"/>
		public virtual void warmUpEncryptedKeys(params string[] keyNames)
		{
			try
			{
				encKeyVersionQueue.initializeQueuesForKeys(keyNames);
			}
			catch (java.util.concurrent.ExecutionException e)
			{
				throw new System.IO.IOException(e);
			}
		}

		public virtual void drain(string keyName)
		{
			encKeyVersionQueue.drain(keyName);
		}

		/// <exception cref="System.IO.IOException"/>
		[com.google.common.annotations.VisibleForTesting]
		public virtual int getEncKeyQueueSize(string keyName)
		{
			try
			{
				return encKeyVersionQueue.getSize(keyName);
			}
			catch (java.util.concurrent.ExecutionException e)
			{
				throw new System.IO.IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.security.token.Token<object>[] addDelegationTokens
			(string renewer, org.apache.hadoop.security.Credentials credentials)
		{
			org.apache.hadoop.security.token.Token<object>[] tokens = null;
			org.apache.hadoop.io.Text dtService = getDelegationTokenService();
			org.apache.hadoop.security.token.Token<object> token = credentials.getToken(dtService
				);
			if (token == null)
			{
				java.net.URL url = createURL(null, null, null, null);
				org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL authUrl
					 = new org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL
					(configurator);
				try
				{
					// 'actualUGI' is the UGI of the user creating the client 
					// It is possible that the creator of the KMSClientProvier
					// calls this method on behalf of a proxyUser (the doAsUser).
					// In which case this call has to be made as the proxy user.
					org.apache.hadoop.security.UserGroupInformation currentUgi = org.apache.hadoop.security.UserGroupInformation
						.getCurrentUser();
					string doAsUser = (currentUgi.getAuthenticationMethod() == org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
						.PROXY) ? currentUgi.getShortUserName() : null;
					token = actualUgi.doAs(new _PrivilegedExceptionAction_870(authUrl, url, renewer, 
						doAsUser));
					// Not using the cached token here.. Creating a new token here
					// everytime.
					if (token != null)
					{
						credentials.addToken(token.getService(), token);
						tokens = new org.apache.hadoop.security.token.Token<object>[] { token };
					}
					else
					{
						throw new System.IO.IOException("Got NULL as delegation token");
					}
				}
				catch (System.Exception)
				{
					java.lang.Thread.currentThread().interrupt();
				}
				catch (System.Exception e)
				{
					throw new System.IO.IOException(e);
				}
			}
			return tokens;
		}

		private sealed class _PrivilegedExceptionAction_870 : java.security.PrivilegedExceptionAction
			<org.apache.hadoop.security.token.Token<object>>
		{
			public _PrivilegedExceptionAction_870(org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL
				 authUrl, java.net.URL url, string renewer, string doAsUser)
			{
				this.authUrl = authUrl;
				this.url = url;
				this.renewer = renewer;
				this.doAsUser = doAsUser;
			}

			/// <exception cref="System.Exception"/>
			public org.apache.hadoop.security.token.Token<object> run()
			{
				return authUrl.getDelegationToken(url, new org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL.Token
					(), renewer, doAsUser);
			}

			private readonly org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticatedURL
				 authUrl;

			private readonly java.net.URL url;

			private readonly string renewer;

			private readonly string doAsUser;
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.io.Text getDelegationTokenService()
		{
			java.net.URL url = new java.net.URL(kmsUrl);
			java.net.InetSocketAddress addr = new java.net.InetSocketAddress(url.getHost(), url
				.getPort());
			org.apache.hadoop.io.Text dtService = org.apache.hadoop.security.SecurityUtil.buildTokenService
				(addr);
			return dtService;
		}

		/// <summary>Shutdown valueQueue executor threads</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			try
			{
				encKeyVersionQueue.shutdown();
			}
			catch (System.Exception e)
			{
				throw new System.IO.IOException(e);
			}
			finally
			{
				if (sslFactory != null)
				{
					sslFactory.destroy();
				}
			}
		}

		[com.google.common.annotations.VisibleForTesting]
		internal virtual string getKMSUrl()
		{
			return kmsUrl;
		}
	}
}
