using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.Codec;
using Org.Apache.Commons.Codec.Binary;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// The value of <code>XAttr</code> is byte[], this class is to
	/// covert byte[] to some kind of string representation or convert back.
	/// </summary>
	/// <remarks>
	/// The value of <code>XAttr</code> is byte[], this class is to
	/// covert byte[] to some kind of string representation or convert back.
	/// String representation is convenient for display and input. For example
	/// display in screen as shell response and json response, input as http
	/// or shell parameter.
	/// </remarks>
	[System.Serializable]
	public sealed class XAttrCodec
	{
		/// <summary>
		/// Value encoded as text
		/// string is enclosed in double quotes (\").
		/// </summary>
		public static readonly XAttrCodec Text = new XAttrCodec();

		/// <summary>
		/// Value encoded as hexadecimal string
		/// is prefixed with 0x.
		/// </summary>
		public static readonly XAttrCodec Hex = new XAttrCodec();

		/// <summary>
		/// Value encoded as base64 string
		/// is prefixed with 0s.
		/// </summary>
		public static readonly XAttrCodec Base64 = new XAttrCodec();

		private const string HexPrefix = "0x";

		private const string Base64Prefix = "0s";

		private static readonly Base64 base64 = new Base64(0);

		/// <summary>
		/// Decode string representation of a value and check whether it's
		/// encoded.
		/// </summary>
		/// <remarks>
		/// Decode string representation of a value and check whether it's
		/// encoded. If the given string begins with 0x or 0X, it expresses
		/// a hexadecimal number. If the given string begins with 0s or 0S,
		/// base64 encoding is expected. If the given string is enclosed in
		/// double quotes, the inner string is treated as text. Otherwise
		/// the given string is treated as text.
		/// </remarks>
		/// <param name="value">string representation of the value.</param>
		/// <returns>byte[] the value</returns>
		/// <exception cref="System.IO.IOException"/>
		public static byte[] DecodeValue(string value)
		{
			byte[] result = null;
			if (value != null)
			{
				if (value.Length >= 2)
				{
					string en = Sharpen.Runtime.Substring(value, 0, 2);
					if (value.StartsWith("\"") && value.EndsWith("\""))
					{
						value = Sharpen.Runtime.Substring(value, 1, value.Length - 1);
						result = Sharpen.Runtime.GetBytesForString(value, "utf-8");
					}
					else
					{
						if (Sharpen.Runtime.EqualsIgnoreCase(en, XAttrCodec.HexPrefix))
						{
							value = Sharpen.Runtime.Substring(value, 2, value.Length);
							try
							{
								result = Hex.DecodeHex(value.ToCharArray());
							}
							catch (DecoderException e)
							{
								throw new IOException(e);
							}
						}
						else
						{
							if (Sharpen.Runtime.EqualsIgnoreCase(en, XAttrCodec.Base64Prefix))
							{
								value = Sharpen.Runtime.Substring(value, 2, value.Length);
								result = XAttrCodec.base64.Decode(value);
							}
						}
					}
				}
				if (result == null)
				{
					result = Sharpen.Runtime.GetBytesForString(value, "utf-8");
				}
			}
			return result;
		}

		/// <summary>Encode byte[] value to string representation with encoding.</summary>
		/// <remarks>
		/// Encode byte[] value to string representation with encoding.
		/// Values encoded as text strings are enclosed in double quotes (\"),
		/// while strings encoded as hexadecimal and base64 are prefixed with
		/// 0x and 0s, respectively.
		/// </remarks>
		/// <param name="value">byte[] value</param>
		/// <param name="encoding"/>
		/// <returns>String string representation of value</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string EncodeValue(byte[] value, XAttrCodec encoding)
		{
			Preconditions.CheckNotNull(value, "Value can not be null.");
			if (encoding == XAttrCodec.Hex)
			{
				return XAttrCodec.HexPrefix + Hex.EncodeHexString(value);
			}
			else
			{
				if (encoding == XAttrCodec.Base64)
				{
					return XAttrCodec.Base64Prefix + XAttrCodec.base64.EncodeToString(value);
				}
				else
				{
					return "\"" + Sharpen.Runtime.GetStringForBytes(value, "utf-8") + "\"";
				}
			}
		}
	}
}
