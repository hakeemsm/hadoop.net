using System;
using System.Collections.Generic;
using System.Net;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>
	/// This class contains a set of utilities which help converting data structures
	/// from/to 'serializableFormat' to/from hadoop/nativejava data structures.
	/// </summary>
	public class ConverterUtils
	{
		public const string ApplicationPrefix = "application";

		public const string ContainerPrefix = "container";

		public const string ApplicationAttemptPrefix = "appattempt";

		/// <summary>return a hadoop path from a given url</summary>
		/// <param name="url">url to convert</param>
		/// <returns>
		/// path from
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.URL"/>
		/// </returns>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public static Path GetPathFromYarnURL(URL url)
		{
			string scheme = url.GetScheme() == null ? string.Empty : url.GetScheme();
			string authority = string.Empty;
			if (url.GetHost() != null)
			{
				authority = url.GetHost();
				if (url.GetUserInfo() != null)
				{
					authority = url.GetUserInfo() + "@" + authority;
				}
				if (url.GetPort() > 0)
				{
					authority += ":" + url.GetPort();
				}
			}
			return new Path((new URI(scheme, authority, url.GetFile(), null, null)).Normalize
				());
		}

		/// <summary>change from CharSequence to string for map key and value</summary>
		/// <param name="env">map for converting</param>
		/// <returns>string,string map</returns>
		public static IDictionary<string, string> ConvertToString(IDictionary<CharSequence
			, CharSequence> env)
		{
			IDictionary<string, string> stringMap = new Dictionary<string, string>();
			foreach (KeyValuePair<CharSequence, CharSequence> entry in env)
			{
				stringMap[entry.Key.ToString()] = entry.Value.ToString();
			}
			return stringMap;
		}

		public static URL GetYarnUrlFromPath(Path path)
		{
			return GetYarnUrlFromURI(path.ToUri());
		}

		public static URL GetYarnUrlFromURI(URI uri)
		{
			URL url = RecordFactoryProvider.GetRecordFactory(null).NewRecordInstance<URL>();
			if (uri.GetHost() != null)
			{
				url.SetHost(uri.GetHost());
			}
			if (uri.GetUserInfo() != null)
			{
				url.SetUserInfo(uri.GetUserInfo());
			}
			url.SetPort(uri.GetPort());
			url.SetScheme(uri.GetScheme());
			url.SetFile(uri.GetPath());
			return url;
		}

		public static string ToString(ApplicationId appId)
		{
			return appId.ToString();
		}

		public static ApplicationId ToApplicationId(RecordFactory recordFactory, string appIdStr
			)
		{
			IEnumerator<string> it = StringHelper._split(appIdStr).GetEnumerator();
			it.Next();
			// prefix. TODO: Validate application prefix
			return ToApplicationId(recordFactory, it);
		}

		private static ApplicationId ToApplicationId(RecordFactory recordFactory, IEnumerator
			<string> it)
		{
			ApplicationId appId = ApplicationId.NewInstance(long.Parse(it.Next()), System.Convert.ToInt32
				(it.Next()));
			return appId;
		}

		/// <exception cref="System.FormatException"/>
		private static ApplicationAttemptId ToApplicationAttemptId(IEnumerator<string> it
			)
		{
			ApplicationId appId = ApplicationId.NewInstance(long.Parse(it.Next()), System.Convert.ToInt32
				(it.Next()));
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, System.Convert.ToInt32
				(it.Next()));
			return appAttemptId;
		}

		/// <exception cref="System.FormatException"/>
		private static ApplicationId ToApplicationId(IEnumerator<string> it)
		{
			ApplicationId appId = ApplicationId.NewInstance(long.Parse(it.Next()), System.Convert.ToInt32
				(it.Next()));
			return appId;
		}

		public static string ToString(ContainerId cId)
		{
			return cId == null ? null : cId.ToString();
		}

		public static NodeId ToNodeIdWithDefaultPort(string nodeIdStr)
		{
			if (nodeIdStr.IndexOf(":") < 0)
			{
				return ToNodeId(nodeIdStr + ":0");
			}
			return ToNodeId(nodeIdStr);
		}

		public static NodeId ToNodeId(string nodeIdStr)
		{
			string[] parts = nodeIdStr.Split(":");
			if (parts.Length != 2)
			{
				throw new ArgumentException("Invalid NodeId [" + nodeIdStr + "]. Expected host:port"
					);
			}
			try
			{
				NodeId nodeId = NodeId.NewInstance(parts[0].Trim(), System.Convert.ToInt32(parts[
					1]));
				return nodeId;
			}
			catch (FormatException e)
			{
				throw new ArgumentException("Invalid port: " + parts[1], e);
			}
		}

		public static ContainerId ToContainerId(string containerIdStr)
		{
			return ContainerId.FromString(containerIdStr);
		}

		public static ApplicationAttemptId ToApplicationAttemptId(string applicationAttmeptIdStr
			)
		{
			IEnumerator<string> it = StringHelper._split(applicationAttmeptIdStr).GetEnumerator
				();
			if (!it.Next().Equals(ApplicationAttemptPrefix))
			{
				throw new ArgumentException("Invalid AppAttemptId prefix: " + applicationAttmeptIdStr
					);
			}
			try
			{
				return ToApplicationAttemptId(it);
			}
			catch (FormatException n)
			{
				throw new ArgumentException("Invalid AppAttemptId: " + applicationAttmeptIdStr, n
					);
			}
			catch (NoSuchElementException e)
			{
				throw new ArgumentException("Invalid AppAttemptId: " + applicationAttmeptIdStr, e
					);
			}
		}

		public static ApplicationId ToApplicationId(string appIdStr)
		{
			IEnumerator<string> it = StringHelper._split(appIdStr).GetEnumerator();
			if (!it.Next().Equals(ApplicationPrefix))
			{
				throw new ArgumentException("Invalid ApplicationId prefix: " + appIdStr + ". The valid ApplicationId should start with prefix "
					 + ApplicationPrefix);
			}
			try
			{
				return ToApplicationId(it);
			}
			catch (FormatException n)
			{
				throw new ArgumentException("Invalid ApplicationId: " + appIdStr, n);
			}
			catch (NoSuchElementException e)
			{
				throw new ArgumentException("Invalid ApplicationId: " + appIdStr, e);
			}
		}

		/// <summary>Convert a protobuf token into a rpc token and set its service.</summary>
		/// <remarks>
		/// Convert a protobuf token into a rpc token and set its service. Supposed
		/// to be used for tokens other than RMDelegationToken. For
		/// RMDelegationToken, use
		/// <see cref="ConvertFromYarn{T}(Org.Apache.Hadoop.Yarn.Api.Records.Token, Org.Apache.Hadoop.IO.Text)
		/// 	"/>
		/// instead.
		/// </remarks>
		/// <param name="protoToken">the yarn token</param>
		/// <param name="serviceAddr">the connect address for the service</param>
		/// <returns>rpc token</returns>
		public static Org.Apache.Hadoop.Security.Token.Token<T> ConvertFromYarn<T>(Org.Apache.Hadoop.Yarn.Api.Records.Token
			 protoToken, IPEndPoint serviceAddr)
			where T : TokenIdentifier
		{
			Org.Apache.Hadoop.Security.Token.Token<T> token = new Org.Apache.Hadoop.Security.Token.Token
				<T>(((byte[])protoToken.GetIdentifier().Array()), ((byte[])protoToken.GetPassword
				().Array()), new Text(protoToken.GetKind()), new Text(protoToken.GetService()));
			if (serviceAddr != null)
			{
				SecurityUtil.SetTokenService(token, serviceAddr);
			}
			return token;
		}

		/// <summary>Convert a protobuf token into a rpc token and set its service.</summary>
		/// <param name="protoToken">the yarn token</param>
		/// <param name="service">the service for the token</param>
		public static Org.Apache.Hadoop.Security.Token.Token<T> ConvertFromYarn<T>(Org.Apache.Hadoop.Yarn.Api.Records.Token
			 protoToken, Text service)
			where T : TokenIdentifier
		{
			Org.Apache.Hadoop.Security.Token.Token<T> token = new Org.Apache.Hadoop.Security.Token.Token
				<T>(((byte[])protoToken.GetIdentifier().Array()), ((byte[])protoToken.GetPassword
				().Array()), new Text(protoToken.GetKind()), new Text(protoToken.GetService()));
			if (service != null)
			{
				token.SetService(service);
			}
			return token;
		}
	}
}
