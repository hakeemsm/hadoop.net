using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset
{
	public class TestRoundRobinVolumeChoosingPolicy
	{
		// Test the Round-Robin block-volume choosing algorithm.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRR()
		{
			RoundRobinVolumeChoosingPolicy<FsVolumeSpi> policy = ReflectionUtils.NewInstance<
				RoundRobinVolumeChoosingPolicy>(null);
			TestRR(policy);
		}

		/// <exception cref="System.Exception"/>
		public static void TestRR(VolumeChoosingPolicy<FsVolumeSpi> policy)
		{
			IList<FsVolumeSpi> volumes = new AList<FsVolumeSpi>();
			// First volume, with 100 bytes of space.
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[0].GetAvailable()).ThenReturn(100L);
			// Second volume, with 200 bytes of space.
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[1].GetAvailable()).ThenReturn(200L);
			// Test two rounds of round-robin choosing
			NUnit.Framework.Assert.AreEqual(volumes[0], policy.ChooseVolume(volumes, 0));
			NUnit.Framework.Assert.AreEqual(volumes[1], policy.ChooseVolume(volumes, 0));
			NUnit.Framework.Assert.AreEqual(volumes[0], policy.ChooseVolume(volumes, 0));
			NUnit.Framework.Assert.AreEqual(volumes[1], policy.ChooseVolume(volumes, 0));
			// The first volume has only 100L space, so the policy should
			// wisely choose the second one in case we ask for more.
			NUnit.Framework.Assert.AreEqual(volumes[1], policy.ChooseVolume(volumes, 150));
			// Fail if no volume can be chosen?
			try
			{
				policy.ChooseVolume(volumes, long.MaxValue);
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException)
			{
			}
		}

		// Passed.
		// ChooseVolume should throw DiskOutOfSpaceException
		// with volume and block sizes in exception message.
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRRPolicyExceptionMessage()
		{
			RoundRobinVolumeChoosingPolicy<FsVolumeSpi> policy = new RoundRobinVolumeChoosingPolicy
				<FsVolumeSpi>();
			TestRRPolicyExceptionMessage(policy);
		}

		/// <exception cref="System.Exception"/>
		public static void TestRRPolicyExceptionMessage(VolumeChoosingPolicy<FsVolumeSpi>
			 policy)
		{
			IList<FsVolumeSpi> volumes = new AList<FsVolumeSpi>();
			// First volume, with 500 bytes of space.
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[0].GetAvailable()).ThenReturn(500L);
			// Second volume, with 600 bytes of space.
			volumes.AddItem(Org.Mockito.Mockito.Mock<FsVolumeSpi>());
			Org.Mockito.Mockito.When(volumes[1].GetAvailable()).ThenReturn(600L);
			int blockSize = 700;
			try
			{
				policy.ChooseVolume(volumes, blockSize);
				NUnit.Framework.Assert.Fail("expected to throw DiskOutOfSpaceException");
			}
			catch (DiskChecker.DiskOutOfSpaceException e)
			{
				NUnit.Framework.Assert.AreEqual("Not returnig the expected message", "Out of space: The volume with the most available space (="
					 + 600 + " B) is less than the block size (=" + blockSize + " B).", e.Message);
			}
		}
	}
}
