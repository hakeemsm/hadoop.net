using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Extdataset
{
	/// <summary>Tests the ability to create external FsDatasetSpi implementations.</summary>
	/// <remarks>
	/// Tests the ability to create external FsDatasetSpi implementations.
	/// The purpose of this suite of tests is to ensure that it is possible to
	/// construct subclasses of FsDatasetSpi outside the Hadoop tree
	/// (specifically, outside of the org.apache.hadoop.hdfs.server.datanode
	/// package).  This consists of creating subclasses of the two key classes
	/// (FsDatasetSpi and FsVolumeSpi) *and* instances or subclasses of any
	/// classes/interfaces their methods need to produce.  If methods are added
	/// to or changed in any superclasses, or if constructors of other classes
	/// are changed, this package will fail to compile.  In fixing this
	/// compilation error, any new class dependencies should receive the same
	/// treatment.
	/// It is worth noting what these tests do *not* accomplish.  Just as
	/// important as being able to produce instances of the appropriate classes
	/// is being able to access all necessary methods on those classes as well
	/// as on any additional classes accepted as inputs to FsDatasetSpi's
	/// methods.  It wouldn't be correct to mandate all methods be public, as
	/// that would defeat encapsulation.  Moreover, there is no natural
	/// mechanism that would prevent a manually-constructed list of methods
	/// from becoming stale.  Rather than creating tests with no clear means of
	/// maintaining them, this problem is left unsolved for now.
	/// Lastly, though merely compiling this package should signal success,
	/// explicit testInstantiate* unit tests are included below so as to have a
	/// tangible means of referring to each case.
	/// </remarks>
	public class TestExternalDataset
	{
		/// <summary>Tests instantiating an FsDatasetSpi subclass.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInstantiateDatasetImpl()
		{
			FsDatasetSpi<object> inst = new ExternalDatasetImpl();
		}

		/// <summary>Tests instantiating a Replica subclass.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIntantiateExternalReplica()
		{
			Replica inst = new ExternalReplica();
		}

		/// <summary>Tests instantiating a ReplicaInPipelineInterface subclass.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInstantiateReplicaInPipeline()
		{
			ReplicaInPipelineInterface inst = new ExternalReplicaInPipeline();
		}

		/// <summary>Tests instantiating an FsVolumeSpi subclass.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInstantiateVolumeImpl()
		{
			FsVolumeSpi inst = new ExternalVolumeImpl();
		}
	}
}
