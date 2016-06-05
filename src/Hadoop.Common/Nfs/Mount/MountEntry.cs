

namespace Org.Apache.Hadoop.Mount
{
	/// <summary>Represents a mount entry.</summary>
	public class MountEntry
	{
		/// <summary>Host corresponding to the mount entry</summary>
		private readonly string host;

		/// <summary>Path corresponding to the mount entry</summary>
		private readonly string path;

		public MountEntry(string host, string path)
		{
			this.host = host;
			this.path = path;
		}

		public virtual string GetHost()
		{
			return this.host;
		}

		public virtual string GetPath()
		{
			return this.path;
		}

		public override bool Equals(object o)
		{
			if (this == o)
			{
				return true;
			}
			if (!(o is Org.Apache.Hadoop.Mount.MountEntry))
			{
				return false;
			}
			Org.Apache.Hadoop.Mount.MountEntry m = (Org.Apache.Hadoop.Mount.MountEntry)o;
			return GetHost().Equals(m.GetHost()) && GetPath().Equals(m.GetPath());
		}

		public override int GetHashCode()
		{
			return host.GetHashCode() * 31 + path.GetHashCode();
		}
	}
}
