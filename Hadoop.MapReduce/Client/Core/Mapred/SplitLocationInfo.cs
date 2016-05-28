using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class SplitLocationInfo
	{
		private bool inMemory;

		private string location;

		public SplitLocationInfo(string location, bool inMemory)
		{
			this.location = location;
			this.inMemory = inMemory;
		}

		public virtual bool IsOnDisk()
		{
			return true;
		}

		public virtual bool IsInMemory()
		{
			return inMemory;
		}

		public virtual string GetLocation()
		{
			return location;
		}
	}
}
