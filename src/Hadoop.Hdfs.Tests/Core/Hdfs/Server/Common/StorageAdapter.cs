using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Common
{
	/// <summary>
	/// Test methods that need to access package-private parts of
	/// Storage
	/// </summary>
	public abstract class StorageAdapter
	{
		/// <summary>Inject and return a spy on a storage directory</summary>
		public static Storage.StorageDirectory SpyOnStorageDirectory(Storage s, int idx)
		{
			Storage.StorageDirectory dir = Org.Mockito.Mockito.Spy(s.GetStorageDir(idx));
			s.storageDirs.Set(idx, dir);
			return dir;
		}
	}
}
