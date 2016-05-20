using Sharpen;

namespace org.apache.hadoop.fs
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
		public static readonly org.apache.hadoop.fs.XAttrCodec TEXT = new org.apache.hadoop.fs.XAttrCodec
			();

		/// <summary>
		/// Value encoded as hexadecimal string
		/// is prefixed with 0x.
		/// </summary>
		public static readonly org.apache.hadoop.fs.XAttrCodec HEX = new org.apache.hadoop.fs.XAttrCodec
			();

		/// <summary>
		/// Value encoded as base64 string
		/// is prefixed with 0s.
		/// </summary>
		public static readonly org.apache.hadoop.fs.XAttrCodec BASE64 = new org.apache.hadoop.fs.XAttrCodec
			();

		private const string HEX_PREFIX = "0x";

		private const string BASE64_PREFIX = "0s";

		private static readonly org.apache.commons.codec.binary.Base64 base64 = new org.apache.commons.codec.binary.Base64
			(0);

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
		public static byte[] decodeValue(string value)
		{
			byte[] result = null;
			if (value != null)
			{
				if (value.Length >= 2)
				{
					string en = Sharpen.Runtime.substring(value, 0, 2);
					if (value.StartsWith("\"") && value.EndsWith("\""))
					{
						value = Sharpen.Runtime.substring(value, 1, value.Length - 1);
						result = Sharpen.Runtime.getBytesForString(value, "utf-8");
					}
					else
					{
						if (Sharpen.Runtime.equalsIgnoreCase(en, org.apache.hadoop.fs.XAttrCodec.HEX_PREFIX
							))
						{
							value = Sharpen.Runtime.substring(value, 2, value.Length);
							try
							{
								result = org.apache.commons.codec.binary.Hex.decodeHex(value.ToCharArray());
							}
							catch (org.apache.commons.codec.DecoderException e)
							{
								throw new System.IO.IOException(e);
							}
						}
						else
						{
							if (Sharpen.Runtime.equalsIgnoreCase(en, org.apache.hadoop.fs.XAttrCodec.BASE64_PREFIX
								))
							{
								value = Sharpen.Runtime.substring(value, 2, value.Length);
								result = org.apache.hadoop.fs.XAttrCodec.base64.decode(value);
							}
						}
					}
				}
				if (result == null)
				{
					result = Sharpen.Runtime.getBytesForString(value, "utf-8");
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
		public static string encodeValue(byte[] value, org.apache.hadoop.fs.XAttrCodec encoding
			)
		{
			com.google.common.@base.Preconditions.checkNotNull(value, "Value can not be null."
				);
			if (encoding == org.apache.hadoop.fs.XAttrCodec.HEX)
			{
				return org.apache.hadoop.fs.XAttrCodec.HEX_PREFIX + org.apache.commons.codec.binary.Hex
					.encodeHexString(value);
			}
			else
			{
				if (encoding == org.apache.hadoop.fs.XAttrCodec.BASE64)
				{
					return org.apache.hadoop.fs.XAttrCodec.BASE64_PREFIX + org.apache.hadoop.fs.XAttrCodec
						.base64.encodeToString(value);
				}
				else
				{
					return "\"" + Sharpen.Runtime.getStringForBytes(value, "utf-8") + "\"";
				}
			}
		}
	}
}
