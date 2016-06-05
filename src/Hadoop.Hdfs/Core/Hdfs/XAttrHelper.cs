using System.Collections.Generic;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class XAttrHelper
	{
		/// <summary>Build <code>XAttr</code> from xattr name with prefix.</summary>
		public static XAttr BuildXAttr(string name)
		{
			return BuildXAttr(name, null);
		}

		/// <summary>Build <code>XAttr</code> from name with prefix and value.</summary>
		/// <remarks>
		/// Build <code>XAttr</code> from name with prefix and value.
		/// Name can not be null. Value can be null. The name and prefix
		/// are validated.
		/// Both name and namespace are case sensitive.
		/// </remarks>
		public static XAttr BuildXAttr(string name, byte[] value)
		{
			Preconditions.CheckNotNull(name, "XAttr name cannot be null.");
			int prefixIndex = name.IndexOf(".");
			if (prefixIndex < 3)
			{
				// Prefix length is at least 3.
				throw new HadoopIllegalArgumentException("An XAttr name must be " + "prefixed with user/trusted/security/system/raw, followed by a '.'"
					);
			}
			else
			{
				if (prefixIndex == name.Length - 1)
				{
					throw new HadoopIllegalArgumentException("XAttr name cannot be empty.");
				}
			}
			XAttr.NameSpace ns;
			string prefix = Sharpen.Runtime.Substring(name, 0, prefixIndex);
			if (StringUtils.EqualsIgnoreCase(prefix, XAttr.NameSpace.User.ToString()))
			{
				ns = XAttr.NameSpace.User;
			}
			else
			{
				if (StringUtils.EqualsIgnoreCase(prefix, XAttr.NameSpace.Trusted.ToString()))
				{
					ns = XAttr.NameSpace.Trusted;
				}
				else
				{
					if (StringUtils.EqualsIgnoreCase(prefix, XAttr.NameSpace.System.ToString()))
					{
						ns = XAttr.NameSpace.System;
					}
					else
					{
						if (StringUtils.EqualsIgnoreCase(prefix, XAttr.NameSpace.Security.ToString()))
						{
							ns = XAttr.NameSpace.Security;
						}
						else
						{
							if (StringUtils.EqualsIgnoreCase(prefix, XAttr.NameSpace.Raw.ToString()))
							{
								ns = XAttr.NameSpace.Raw;
							}
							else
							{
								throw new HadoopIllegalArgumentException("An XAttr name must be " + "prefixed with user/trusted/security/system/raw, followed by a '.'"
									);
							}
						}
					}
				}
			}
			XAttr xAttr = (new XAttr.Builder()).SetNameSpace(ns).SetName(Sharpen.Runtime.Substring
				(name, prefixIndex + 1)).SetValue(value).Build();
			return xAttr;
		}

		/// <summary>Build xattr name with prefix as <code>XAttr</code> list.</summary>
		public static IList<XAttr> BuildXAttrAsList(string name)
		{
			XAttr xAttr = BuildXAttr(name);
			IList<XAttr> xAttrs = Lists.NewArrayListWithCapacity(1);
			xAttrs.AddItem(xAttr);
			return xAttrs;
		}

		/// <summary>Get value of first xattr from <code>XAttr</code> list</summary>
		public static byte[] GetFirstXAttrValue(IList<XAttr> xAttrs)
		{
			byte[] value = null;
			XAttr xAttr = GetFirstXAttr(xAttrs);
			if (xAttr != null)
			{
				value = xAttr.GetValue();
				if (value == null)
				{
					value = new byte[0];
				}
			}
			// xattr exists, but no value.
			return value;
		}

		/// <summary>Get first xattr from <code>XAttr</code> list</summary>
		public static XAttr GetFirstXAttr(IList<XAttr> xAttrs)
		{
			if (xAttrs != null && !xAttrs.IsEmpty())
			{
				return xAttrs[0];
			}
			return null;
		}

		/// <summary>
		/// Build xattr map from <code>XAttr</code> list, the key is
		/// xattr name with prefix, and value is xattr value.
		/// </summary>
		public static IDictionary<string, byte[]> BuildXAttrMap(IList<XAttr> xAttrs)
		{
			if (xAttrs == null)
			{
				return null;
			}
			IDictionary<string, byte[]> xAttrMap = Maps.NewHashMap();
			foreach (XAttr xAttr in xAttrs)
			{
				string name = GetPrefixName(xAttr);
				byte[] value = xAttr.GetValue();
				if (value == null)
				{
					value = new byte[0];
				}
				xAttrMap[name] = value;
			}
			return xAttrMap;
		}

		/// <summary>Get name with prefix from <code>XAttr</code></summary>
		public static string GetPrefixName(XAttr xAttr)
		{
			if (xAttr == null)
			{
				return null;
			}
			string @namespace = xAttr.GetNameSpace().ToString();
			return StringUtils.ToLowerCase(@namespace) + "." + xAttr.GetName();
		}

		/// <summary>Build <code>XAttr</code> list from xattr name list.</summary>
		public static IList<XAttr> BuildXAttrs(IList<string> names)
		{
			if (names == null || names.IsEmpty())
			{
				throw new HadoopIllegalArgumentException("XAttr names can not be " + "null or empty."
					);
			}
			IList<XAttr> xAttrs = Lists.NewArrayListWithCapacity(names.Count);
			foreach (string name in names)
			{
				xAttrs.AddItem(BuildXAttr(name, null));
			}
			return xAttrs;
		}
	}
}
