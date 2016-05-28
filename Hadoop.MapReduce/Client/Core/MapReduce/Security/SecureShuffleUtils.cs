using System;
using System.IO;
using Com.Google.Common.Base;
using Javax.Servlet.Http;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Org.Apache.Hadoop.Record;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security
{
	/// <summary>utilities for generating kyes, hashes and verifying them for shuffle</summary>
	public class SecureShuffleUtils
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(SecureShuffleUtils));

		public const string HttpHeaderUrlHash = "UrlHash";

		public const string HttpHeaderReplyUrlHash = "ReplyHash";

		/// <summary>Base64 encoded hash of msg</summary>
		/// <param name="msg"/>
		public static string GenerateHash(byte[] msg, SecretKey key)
		{
			return new string(Base64.EncodeBase64(GenerateByteHash(msg, key)), Charsets.Utf8);
		}

		/// <summary>calculate hash of msg</summary>
		/// <param name="msg"/>
		/// <returns/>
		private static byte[] GenerateByteHash(byte[] msg, SecretKey key)
		{
			return JobTokenSecretManager.ComputeHash(msg, key);
		}

		/// <summary>verify that hash equals to HMacHash(msg)</summary>
		/// <param name="newHash"/>
		/// <returns>true if is the same</returns>
		private static bool VerifyHash(byte[] hash, byte[] msg, SecretKey key)
		{
			byte[] msg_hash = GenerateByteHash(msg, key);
			return Utils.CompareBytes(msg_hash, 0, msg_hash.Length, hash, 0, hash.Length) == 
				0;
		}

		/// <summary>Aux util to calculate hash of a String</summary>
		/// <param name="enc_str"/>
		/// <param name="key"/>
		/// <returns>Base64 encodedHash</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string HashFromString(string enc_str, SecretKey key)
		{
			return GenerateHash(Sharpen.Runtime.GetBytesForString(enc_str, Charsets.Utf8), key
				);
		}

		/// <summary>verify that base64Hash is same as HMacHash(msg)</summary>
		/// <param name="base64Hash">(Base64 encoded hash)</param>
		/// <param name="msg"/>
		/// <exception cref="System.IO.IOException">if not the same</exception>
		public static void VerifyReply(string base64Hash, string msg, SecretKey key)
		{
			byte[] hash = Base64.DecodeBase64(Sharpen.Runtime.GetBytesForString(base64Hash, Charsets
				.Utf8));
			bool res = VerifyHash(hash, Sharpen.Runtime.GetBytesForString(msg, Charsets.Utf8)
				, key);
			if (res != true)
			{
				throw new IOException("Verification of the hashReply failed");
			}
		}

		/// <summary>Shuffle specific utils - build string for encoding from URL</summary>
		/// <param name="url"/>
		/// <returns>string for encoding</returns>
		public static string BuildMsgFrom(Uri url)
		{
			return BuildMsgFrom(url.AbsolutePath, url.GetQuery(), url.Port);
		}

		/// <summary>Shuffle specific utils - build string for encoding from URL</summary>
		/// <param name="request"/>
		/// <returns>string for encoding</returns>
		public static string BuildMsgFrom(HttpServletRequest request)
		{
			return BuildMsgFrom(request.GetRequestURI(), request.GetQueryString(), request.GetLocalPort
				());
		}

		/// <summary>Shuffle specific utils - build string for encoding from URL</summary>
		/// <param name="uri_path"/>
		/// <param name="uri_query"/>
		/// <returns>string for encoding</returns>
		private static string BuildMsgFrom(string uri_path, string uri_query, int port)
		{
			return port.ToString() + uri_path + "?" + uri_query;
		}

		/// <summary>byte array to Hex String</summary>
		/// <param name="ba"/>
		/// <returns>string with HEX value of the key</returns>
		public static string ToHex(byte[] ba)
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			string strHex = string.Empty;
			try
			{
				TextWriter ps = new TextWriter(baos, false, "UTF-8");
				foreach (byte b in ba)
				{
					ps.Printf("%x", b);
				}
				strHex = baos.ToString("UTF-8");
			}
			catch (UnsupportedEncodingException)
			{
			}
			return strHex;
		}
	}
}
