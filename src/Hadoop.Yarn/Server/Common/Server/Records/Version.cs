using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Records
{
	/// <summary>
	/// The version information for state get stored in YARN components,
	/// i.e.
	/// </summary>
	/// <remarks>
	/// The version information for state get stored in YARN components,
	/// i.e. RMState, NMState, etc., which include: majorVersion and
	/// minorVersion.
	/// The major version update means incompatible changes happen while
	/// minor version update indicates compatible changes.
	/// </remarks>
	public abstract class Version
	{
		public static Version NewInstance(int majorVersion, int minorVersion)
		{
			Version version = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<Version>();
			version.SetMajorVersion(majorVersion);
			version.SetMinorVersion(minorVersion);
			return version;
		}

		public abstract int GetMajorVersion();

		public abstract void SetMajorVersion(int majorVersion);

		public abstract int GetMinorVersion();

		public abstract void SetMinorVersion(int minorVersion);

		public override string ToString()
		{
			return GetMajorVersion() + "." + GetMinorVersion();
		}

		public virtual bool IsCompatibleTo(Version version)
		{
			return GetMajorVersion() == version.GetMajorVersion();
		}

		public override int GetHashCode()
		{
			int prime = 31;
			int result = 1;
			result = prime * result + GetMajorVersion();
			result = prime * result + GetMinorVersion();
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (GetType() != obj.GetType())
			{
				return false;
			}
			Version other = (Version)obj;
			if (this.GetMajorVersion() == other.GetMajorVersion() && this.GetMinorVersion() ==
				 other.GetMinorVersion())
			{
				return true;
			}
			else
			{
				return false;
			}
		}
	}
}
