using Org.Apache.Commons.Lang.Builder;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// XAttr is the POSIX Extended Attribute model similar to that found in
	/// traditional Operating Systems.
	/// </summary>
	/// <remarks>
	/// XAttr is the POSIX Extended Attribute model similar to that found in
	/// traditional Operating Systems.  Extended Attributes consist of one
	/// or more name/value pairs associated with a file or directory. Five
	/// namespaces are defined: user, trusted, security, system and raw.
	/// 1) USER namespace attributes may be used by any user to store
	/// arbitrary information. Access permissions in this namespace are
	/// defined by a file directory's permission bits. For sticky directories,
	/// only the owner and privileged user can write attributes.
	/// <br />
	/// 2) TRUSTED namespace attributes are only visible and accessible to
	/// privileged users. This namespace is available from both user space
	/// (filesystem API) and fs kernel.
	/// <br />
	/// 3) SYSTEM namespace attributes are used by the fs kernel to store
	/// system objects.  This namespace is only available in the fs
	/// kernel. It is not visible to users.
	/// <br />
	/// 4) SECURITY namespace attributes are used by the fs kernel for
	/// security features. It is not visible to users.
	/// <br />
	/// 5) RAW namespace attributes are used for internal system attributes that
	/// sometimes need to be exposed. Like SYSTEM namespace attributes they are
	/// not visible to the user except when getXAttr/getXAttrs is called on a file
	/// or directory in the /.reserved/raw HDFS directory hierarchy.  These
	/// attributes can only be accessed by the superuser.
	/// <p/>
	/// </remarks>
	/// <seealso><a href="http://en.wikipedia.org/wiki/Extended_file_attributes">
	/// * http://en.wikipedia.org/wiki/Extended_file_attributes</a></seealso>
	public class XAttr
	{
		public enum NameSpace
		{
			User,
			Trusted,
			Security,
			System,
			Raw
		}

		private readonly XAttr.NameSpace ns;

		private readonly string name;

		private readonly byte[] value;

		public class Builder
		{
			private XAttr.NameSpace ns = XAttr.NameSpace.User;

			private string name;

			private byte[] value;

			public virtual XAttr.Builder SetNameSpace(XAttr.NameSpace ns)
			{
				this.ns = ns;
				return this;
			}

			public virtual XAttr.Builder SetName(string name)
			{
				this.name = name;
				return this;
			}

			public virtual XAttr.Builder SetValue(byte[] value)
			{
				this.value = value;
				return this;
			}

			public virtual XAttr Build()
			{
				return new XAttr(ns, name, value);
			}
		}

		private XAttr(XAttr.NameSpace ns, string name, byte[] value)
		{
			this.ns = ns;
			this.name = name;
			this.value = value;
		}

		public virtual XAttr.NameSpace GetNameSpace()
		{
			return ns;
		}

		public virtual string GetName()
		{
			return name;
		}

		public virtual byte[] GetValue()
		{
			return value;
		}

		public override int GetHashCode()
		{
			return new HashCodeBuilder(811, 67).Append(name).Append(ns).Append(value).ToHashCode
				();
		}

		public override bool Equals(object obj)
		{
			if (obj == null)
			{
				return false;
			}
			if (obj == this)
			{
				return true;
			}
			if (obj.GetType() != GetType())
			{
				return false;
			}
			XAttr rhs = (XAttr)obj;
			return new EqualsBuilder().Append(ns, rhs.ns).Append(name, rhs.name).Append(value
				, rhs.value).IsEquals();
		}

		/// <summary>
		/// Similar to
		/// <see cref="Equals(object)"/>
		/// , except ignores the XAttr value.
		/// </summary>
		/// <param name="obj">to compare equality</param>
		/// <returns>if the XAttrs are equal, ignoring the XAttr value</returns>
		public virtual bool EqualsIgnoreValue(object obj)
		{
			if (obj == null)
			{
				return false;
			}
			if (obj == this)
			{
				return true;
			}
			if (obj.GetType() != GetType())
			{
				return false;
			}
			XAttr rhs = (XAttr)obj;
			return new EqualsBuilder().Append(ns, rhs.ns).Append(name, rhs.name).IsEquals();
		}

		public override string ToString()
		{
			return "XAttr [ns=" + ns + ", name=" + name + ", value=" + Arrays.ToString(value)
				 + "]";
		}
	}
}
