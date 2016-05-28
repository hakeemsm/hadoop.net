using System;
using System.Collections.Generic;
using System.Text;
using IO.Netty.Handler.Codec.Http;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Web.Resources;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Web.Webhdfs
{
	internal class ParameterParser
	{
		private readonly Configuration conf;

		private readonly string path;

		private readonly IDictionary<string, IList<string>> @params;

		internal ParameterParser(QueryStringDecoder decoder, Configuration conf)
		{
			this.path = DecodeComponent(Sharpen.Runtime.Substring(decoder.Path(), WebHdfsHandler
				.WebhdfsPrefixLength), Charsets.Utf8);
			this.@params = decoder.Parameters();
			this.conf = conf;
		}

		internal virtual string Path()
		{
			return path;
		}

		internal virtual string Op()
		{
			return Param(HttpOpParam.Name);
		}

		internal virtual long Offset()
		{
			return new OffsetParam(Param(OffsetParam.Name)).GetOffset();
		}

		internal virtual long Length()
		{
			return new LengthParam(Param(LengthParam.Name)).GetLength();
		}

		internal virtual string NamenodeId()
		{
			return new NamenodeAddressParam(Param(NamenodeAddressParam.Name)).GetValue();
		}

		internal virtual string DoAsUser()
		{
			return new DoAsParam(Param(DoAsParam.Name)).GetValue();
		}

		internal virtual string UserName()
		{
			return new UserParam(Param(UserParam.Name)).GetValue();
		}

		internal virtual int BufferSize()
		{
			return new BufferSizeParam(Param(BufferSizeParam.Name)).GetValue(conf);
		}

		internal virtual long BlockSize()
		{
			return new BlockSizeParam(Param(BlockSizeParam.Name)).GetValue(conf);
		}

		internal virtual short Replication()
		{
			return new ReplicationParam(Param(ReplicationParam.Name)).GetValue(conf);
		}

		internal virtual FsPermission Permission()
		{
			return new PermissionParam(Param(PermissionParam.Name)).GetFsPermission();
		}

		internal virtual bool Overwrite()
		{
			return new OverwriteParam(Param(OverwriteParam.Name)).GetValue();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier
			> DelegationToken()
		{
			string delegation = Param(DelegationParam.Name);
			Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier>();
			token.DecodeFromUrlString(delegation);
			URI nnUri = URI.Create(HdfsConstants.HdfsUriScheme + "://" + NamenodeId());
			bool isLogical = HAUtil.IsLogicalUri(conf, nnUri);
			if (isLogical)
			{
				token.SetService(HAUtil.BuildTokenServiceForLogicalUri(nnUri, HdfsConstants.HdfsUriScheme
					));
			}
			else
			{
				token.SetService(SecurityUtil.BuildTokenService(nnUri));
			}
			return token;
		}

		internal virtual Configuration Conf()
		{
			return conf;
		}

		private string Param(string key)
		{
			IList<string> p = @params[key];
			return p == null ? null : p[0];
		}

		/// <summary>
		/// The following function behaves exactly the same as netty's
		/// <code>QueryStringDecoder#decodeComponent</code> except that it
		/// does not decode the '+' character as space.
		/// </summary>
		/// <remarks>
		/// The following function behaves exactly the same as netty's
		/// <code>QueryStringDecoder#decodeComponent</code> except that it
		/// does not decode the '+' character as space. WebHDFS takes this scheme
		/// to maintain the backward-compatibility for pre-2.7 releases.
		/// </remarks>
		private static string DecodeComponent(string s, Encoding charset)
		{
			if (s == null)
			{
				return string.Empty;
			}
			int size = s.Length;
			bool modified = false;
			for (int i = 0; i < size; i++)
			{
				char c = s[i];
				if (c == '%' || c == '+')
				{
					modified = true;
					break;
				}
			}
			if (!modified)
			{
				return s;
			}
			byte[] buf = new byte[size];
			int pos = 0;
			// position in `buf'.
			for (int i_1 = 0; i_1 < size; i_1++)
			{
				char c = s[i_1];
				if (c == '%')
				{
					if (i_1 == size - 1)
					{
						throw new ArgumentException("unterminated escape sequence at" + " end of string: "
							 + s);
					}
					c = s[++i_1];
					if (c == '%')
					{
						buf[pos++] = (byte)('%');
						// "%%" -> "%"
						break;
					}
					if (i_1 == size - 1)
					{
						throw new ArgumentException("partial escape sequence at end " + "of string: " + s
							);
					}
					c = DecodeHexNibble(c);
					char c2 = DecodeHexNibble(s[++i_1]);
					if (c == char.MaxValue || c2 == char.MaxValue)
					{
						throw new ArgumentException("invalid escape sequence `%" + s[i_1 - 1] + s[i_1] + 
							"' at index " + (i_1 - 2) + " of: " + s);
					}
					c = (char)(c * 16 + c2);
				}
				// Fall through.
				buf[pos++] = unchecked((byte)c);
			}
			return new string(buf, 0, pos, charset);
		}

		/// <summary>Helper to decode half of a hexadecimal number from a string.</summary>
		/// <param name="c">
		/// The ASCII character of the hexadecimal number to decode.
		/// Must be in the range
		/// <c>[0-9a-fA-F]</c>
		/// .
		/// </param>
		/// <returns>
		/// The hexadecimal value represented in the ASCII character
		/// given, or
		/// <see cref="char.MaxValue"/>
		/// if the character is invalid.
		/// </returns>
		private static char DecodeHexNibble(char c)
		{
			if ('0' <= c && c <= '9')
			{
				return (char)(c - '0');
			}
			else
			{
				if ('a' <= c && c <= 'f')
				{
					return (char)(c - 'a' + 10);
				}
				else
				{
					if ('A' <= c && c <= 'F')
					{
						return (char)(c - 'A' + 10);
					}
					else
					{
						return char.MaxValue;
					}
				}
			}
		}
	}
}
