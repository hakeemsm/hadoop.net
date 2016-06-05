using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>
	/// A factory for creating
	/// <see cref="FsDatasetImpl"/>
	/// objects.
	/// </summary>
	public class FsDatasetFactory : FsDatasetSpi.Factory<FsDatasetImpl>
	{
		/// <exception cref="System.IO.IOException"/>
		public override FsDatasetImpl NewInstance(DataNode datanode, DataStorage storage, 
			Configuration conf)
		{
			return new FsDatasetImpl(datanode, storage, conf);
		}
	}
}
